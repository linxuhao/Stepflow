"""Pipeline graph definition and resolution.

Provides the data model (Transition, StepNode, PipelineGraph,
EndConditions) and the GraphResolver that validates graph structure
and resolves next nodes during traversal.
"""

from __future__ import annotations

import copy
import json
import re
from collections import defaultdict, deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from skillflow.exceptions import CycleLimitExceeded, GraphValidationError, NoMatchingTransition

# ── Data classes ────────────────────────────────────────────────────


@dataclass
class Transition:
    """A directed edge between two steps.

    Attributes:
        to: Target step id.
        match: If set, result.flags must contain all these key-value
               pairs for this transition to be taken. ``None`` means
               always match (default / fallback).
               Special key ``from`` with value ``"checkpoint"`` matches
               against a reserved ``_checkpoint_approved`` flag.
        max_loop: Maximum times this edge can fire per run.
                  ``None`` means unlimited.
        label: Human-readable description of this branch.
        feedback: If True, the current step's outputs are injected as
                  ``_feedback`` into the target step's inputs on retry.
    """

    to: str | None
    match: dict[str, Any] | None = None
    max_loop: int | None = None
    label: str = ""
    feedback: bool = False


@dataclass
class LoopConfig:
    """Configuration for ``step_type="loop"`` steps.

    Reads a JSON list from a workspace file and iterates over items,
    injecting each into the loop body's context.
    """

    source: dict = field(default_factory=dict)
    # {step: "3", file: "tasks_manifest.json", field: "execution_order"}
    item_as: str = ""       # context key for the current item
    max_iterations: int = 200


@dataclass
class StepNode:
    """A node in the pipeline graph.

    Attributes:
        id: Unique step identifier within the pipeline.
        name: Human-readable display name.
        step_type: ``"agent"`` (requires StepRunner), ``"gate"``
                   (auto-resolved by skillflow), ``"loop"``
                   (iterates over a list from a workspace file),
                   or ``"tool"`` (auto-executed via ToolLoader).
        transitions: Outgoing edges, evaluated in order.
        checkpoint: If True, pause the run after this step completes
                    and wait for user approval.
        checkpoint_label: Label shown in the checkpoint UI.
        config: Opaque configuration dict passed through to StepRunner
                for agent nodes. skillflow never inspects this.
        max_retries: Maximum execution retries before the step is
                     considered permanently failed.
        output_schema: Optional dotted path to a Pydantic model for
                       output validation (e.g. ``"mypkg.schemas.Result"``).
        output_schema_retries: Max validation retries before treating
                               as permanent failure. 0 = skip validation.

        (v2 fields — all default to empty/falsy for backward compat)
        tool_name: For ``step_type="tool"``: tool directory name.
        tool_params: Parameters passed to the tool function.
        agent_config: For agent nodes: key into agent config YAML.
        context: List of context source specs for prompt assembly.
        output_mode: ``"content"`` (constrained write) or ``"write"`` (free).
        output_fixed: Fixed output filename mapping (content mode).
        validation: List of validation specs (files + tool + params).
    """

    id: str
    name: str = ""
    step_type: str = "agent"  # "agent" | "gate" | "tool" | "loop"
    loop: LoopConfig | None = None  # for step_type="loop"
    transitions: list[Transition] = field(default_factory=list)
    checkpoint: bool = False
    checkpoint_label: str = ""
    checkpoint_reject_to: str = ""
    config: dict = field(default_factory=dict)
    max_retries: int = 3
    max_tool_turns: int = 0  # 0 = use agent config default
    output_schema: str | None = None
    output_schema_retries: int = 0
    tool_name: str = ""
    tool_params: dict = field(default_factory=dict)
    agent_config: str = ""
    context: list[dict] = field(default_factory=list)
    output_mode: str = ""
    output_fixed: dict = field(default_factory=dict)
    validation: list[dict] = field(default_factory=list)
    notify: list[str] | None = None  # event types to push (None = outbox only)
    lifecycle: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.step_type not in ("agent", "gate", "tool", "loop"):
            raise ValueError(
                f"StepNode '{self.id}': step_type must be 'agent', 'gate', 'tool', or 'loop', "
                f"got '{self.step_type}'"
            )
        if self.step_type == "loop" and not self.loop:
            raise ValueError(
                f"StepNode '{self.id}': step_type='loop' requires a 'loop' config"
            )


@dataclass
class EndCondition:
    """A single termination predicate.

    Attributes:
        type: ``"node_reached"``, ``"max_total_steps"``,
              ``"max_run_duration"``, or ``"flag_match"``.
        node: For ``"node_reached"``: which node triggers termination.
        result: For ``"node_reached"``: ``"completed"`` or ``"failed"``.
        require_completed: For ``"node_reached"``: if True, the node's
              step must be in completed status before the condition fires.
              Use this when the terminal node is a real agent step that
              must execute before the pipeline ends.
        limit: For ``"max_total_steps"`` / ``"max_run_duration"``:
               the threshold value.
        flag: For ``"flag_match"``: flag key-values that trigger
              termination when present in a StepResult.
    """

    type: str
    node: str = ""
    result: str = "completed"
    require_completed: bool = False
    limit: int = 0
    flag: dict[str, Any] = field(default_factory=dict)


@dataclass
class EndConditions:
    """Composable run termination predicates.

    Evaluated in ``advance_run()`` after each step confirmation.

    Attributes:
        combinator: ``"or"`` (any condition triggers) or ``"and"``
                    (all conditions must be met).
        conditions: The list of predicates.
    """

    combinator: str = "or"
    conditions: list[EndCondition] = field(default_factory=list)


@dataclass
class EndResult:
    """The outcome of evaluating EndConditions."""

    status: str  # "completed" | "failed"
    reason: str  # e.g. "Node '5' reached" | "Max steps (200) exceeded"


@dataclass
class PipelineGraph:
    """Complete pipeline definition.

    Attributes:
        name: Unique graph name (used as key in skillflow_graphs).
        description: Human-readable description.
        begin: Entry point step id.
        steps: All StepNodes in the graph.
        end_conditions: Optional composable termination predicates.
    """

    name: str
    description: str = ""
    begin: str = ""
    steps: list[StepNode] = field(default_factory=list)
    end_conditions: EndConditions | None = None

    # ── YAML serialization ──────────────────────────────────────

    @classmethod
    def from_yaml(cls, path: str | Path) -> "PipelineGraph":
        """Load a pipeline graph from a YAML file.

        Uses PyYAML if available, otherwise raises ImportError.
        """
        try:
            import yaml
        except ImportError:
            raise ImportError(
                "PyYAML is required for PipelineGraph.from_yaml(). "
                "Install it with: pip install pyyaml"
            )

        path = Path(path)
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        return cls._from_dict(data)

    @classmethod
    def _from_dict(cls, data: dict) -> "PipelineGraph":
        """Build a PipelineGraph from a parsed YAML dict."""
        steps = []
        for s in data.get("steps", []):
            transitions = [
                Transition(
                    to=t["to"],
                    match=t.get("match"),
                    max_loop=t.get("max_loop"),
                    label=t.get("label", ""),
                    feedback=t.get("feedback", False),
                )
                for t in s.get("transitions", [])
            ]
            steps.append(
                StepNode(
                    id=s["id"],
                    name=s.get("name", ""),
                    step_type=s.get("step_type", "agent"),
                    transitions=transitions,
                    checkpoint=s.get("checkpoint", False),
                    checkpoint_label=s.get("checkpoint_label", ""),
                    checkpoint_reject_to=s.get("checkpoint_reject_to", ""),
                    config=s.get("config", {}),
                    max_retries=s.get("max_retries", 3),
                    max_tool_turns=s.get("max_tool_turns", 0),
                    output_schema=s.get("output_schema"),
                    output_schema_retries=s.get("output_schema_retries", 0),
                    tool_name=s.get("tool_name", ""),
                    tool_params=s.get("tool_params", {}),
                    agent_config=s.get("agent_config", ""),
                    context=s.get("context", []),
                    output_mode=(s.get("output") or {}).get("mode", "") or s.get("output_mode", ""),
                    output_fixed=(s.get("output") or {}).get("fixed", {}),
                    validation=s.get("validation", []),
                    notify=s.get("notify"),
                    lifecycle=s.get("lifecycle", {}),
                    loop=LoopConfig(**s["loop"]) if s.get("loop") else None,
                )
            )

        end_conditions = None
        ec_data = data.get("end_conditions")
        if ec_data:
            end_conditions = EndConditions(
                combinator=ec_data.get("combinator", "or"),
                conditions=[
                    EndCondition(
                        type=c["type"],
                        node=c.get("node", ""),
                        result=c.get("result", "completed"),
                        require_completed=c.get("require_completed", False),
                        limit=c.get("limit", 0),
                        flag=c.get("flag", {}),
                    )
                    for c in ec_data.get("conditions", [])
                ],
            )

        return cls(
            name=data["name"],
            description=data.get("description", ""),
            begin=data.get("begin", ""),
            steps=steps,
            end_conditions=end_conditions,
        )

    def to_dict(self) -> dict:
        """Serialize back to a dict suitable for YAML dumping."""
        steps_data = []
        for s in self.steps:
            sd: dict = {
                "id": s.id,
                "step_type": s.step_type,
            }
            if s.name:
                sd["name"] = s.name
            if s.checkpoint:
                sd["checkpoint"] = True
                if s.checkpoint_label:
                    sd["checkpoint_label"] = s.checkpoint_label
                if s.checkpoint_reject_to:
                    sd["checkpoint_reject_to"] = s.checkpoint_reject_to
            if s.max_retries != 3:
                sd["max_retries"] = s.max_retries
            if s.max_tool_turns:
                sd["max_tool_turns"] = s.max_tool_turns
            if s.output_schema:
                sd["output_schema"] = s.output_schema
            if s.output_schema_retries:
                sd["output_schema_retries"] = s.output_schema_retries
            if s.config:
                sd["config"] = s.config
            if s.tool_name:
                sd["tool_name"] = s.tool_name
            if s.tool_params:
                sd["tool_params"] = s.tool_params
            if s.notify is not None:
                sd["notify"] = s.notify
            if s.agent_config:
                sd["agent_config"] = s.agent_config
            if s.context:
                sd["context"] = s.context
            if s.output_mode:
                sd["output_mode"] = s.output_mode
                if s.output_fixed:
                    sd["output"] = {"fixed": s.output_fixed}
            if s.validation:
                sd["validation"] = s.validation
            if s.lifecycle:
                sd["lifecycle"] = s.lifecycle
            if s.loop:
                sd["loop"] = {
                    "source": s.loop.source,
                    "item_as": s.loop.item_as,
                    "max_iterations": s.loop.max_iterations,
                }
            if s.transitions:
                sd["transitions"] = [
                    ({"to": t.to} if t.to is not None else {"to": None})
                    | ({"match": t.match} if t.match else {})
                    | ({"max_loop": t.max_loop} if t.max_loop is not None else {})
                    | ({"label": t.label} if t.label else {})
                    | ({"feedback": True} if t.feedback else {})
                    for t in s.transitions
                ]
            steps_data.append(sd)

        result: dict = {
            "name": self.name,
            "begin": self.begin,
            "steps": steps_data,
        }
        if self.description:
            result["description"] = self.description
        if self.end_conditions:
            result["end_conditions"] = {
                "combinator": self.end_conditions.combinator,
                "conditions": [
                    {"type": c.type}
                    | ({"node": c.node} if c.node else {})
                    | ({"result": c.result} if c.result != "completed" else {})
                    | ({"require_completed": True} if c.require_completed else {})
                    | ({"limit": c.limit} if c.limit else {})
                    | ({"flag": c.flag} if c.flag else {})
                    for c in self.end_conditions.conditions
                ],
            }
        return result

    # ── Validation ───────────────────────────────────────────────

    def validate(self) -> list[str]:
        """Validate graph structure. Returns a list of issues (empty = valid)."""
        resolver = GraphResolver(self)
        return resolver.validate()


# ── GraphResolver ────────────────────────────────────────────────────


class GraphResolver:
    """Validates a PipelineGraph and resolves next nodes during traversal.

    Built once per graph and reused across runs.
    """

    def __init__(self, graph: PipelineGraph):
        self._graph = graph
        self._step_map: dict[str, StepNode] = {s.id: s for s in graph.steps}
        self._adj: dict[str, list[Transition]] = {}
        for s in graph.steps:
            self._adj[s.id] = s.transitions

    # ── Public API ────────────────────────────────────────────────

    @property
    def graph(self) -> PipelineGraph:
        return self._graph

    def begin_node(self) -> str:
        return self._graph.begin

    def is_gate(self, step_id: str) -> bool:
        node = self._step_map.get(step_id)
        return node is not None and node.step_type == "gate"

    def is_tool(self, step_id: str) -> bool:
        node = self._step_map.get(step_id)
        return node is not None and node.step_type == "tool"

    def is_agent(self, step_id: str) -> bool:
        node = self._step_map.get(step_id)
        return node is not None and node.step_type == "agent"

    def is_loop(self, step_id: str) -> bool:
        node = self._step_map.get(step_id)
        return node is not None and node.step_type == "loop"

    def get_node(self, step_id: str) -> StepNode | None:
        return self._step_map.get(step_id)

    def find_error_transition(self, step_id: str) -> str | None:
        """Return the target of the first transition with ``match: {_error: true}``, or None."""
        node = self._step_map.get(step_id)
        if not node:
            return None
        for t in node.transitions:
            if t.match and t.match.get("_error") is True:
                return t.to
        return None

    def next_node(
        self,
        current_id: str,
        result_flags: dict,
        edge_counts: dict[tuple[str, str], int],
        file_reader: callable = None,
    ) -> str | None:
        """Resolve the next node after ``current_id`` completes.

        Transitions are evaluated in order; the first whose ``match``
        is a subset of ``result_flags`` *and* whose edge count is below
        ``max_loop`` wins.

        Returns:
            The target step id, or None if no transition matches.
            The caller must handle None (usually fail_run).

        Raises:
            CycleLimitExceeded: All transitions are blocked by max_loop.
        """
        t, target = self.resolve_transition(current_id, result_flags, edge_counts,
                                             file_reader=file_reader)
        return target

    def resolve_transition(
        self,
        current_id: str,
        result_flags: dict,
        edge_counts: dict[tuple[str, str], int],
        checkpoint_approved: bool | None = None,
        file_reader: callable = None,
    ) -> tuple[Transition | None, str | None]:
        """Resolve the next node and return the matched Transition.

        Args:
            checkpoint_approved: If set, synthesises ``_checkpoint_approved``
                flag used by ``match: {from: "checkpoint", value: "approved"}``.
            file_reader: Optional callable(path) → str for resolving
                ``from_file`` match conditions. Receives a relative path
                and should return the file content.
        """
        node = self._step_map.get(current_id)
        if not node:
            return None, None

        # Synthesise checkpoint flag
        flags = dict(result_flags)
        if checkpoint_approved is not None:
            flags["_checkpoint_approved"] = checkpoint_approved

        exhausted_reasons: list[str] = []

        for t in node.transitions:
            # Check match condition
            if t.match is not None:
                if not _flags_match(t.match, flags, file_reader=file_reader):
                    continue

            # Check cycle limit
            if t.max_loop is not None:
                key = (current_id, t.to)
                current_count = edge_counts.get(key, 0)
                if current_count >= t.max_loop:
                    exhausted_reasons.append(
                        f"'{current_id}' -> '{t.to}' (max_loop={t.max_loop} reached)"
                    )
                    continue

            return t, t.to

        if exhausted_reasons:
            raise CycleLimitExceeded(
                f"All transitions from '{current_id}' are exhausted: "
                + "; ".join(exhausted_reasons)
            )
        return None, None

    def resolve_gate_transitions(
        self,
        gate_id: str,
        result_flags: dict,
        edge_counts: dict[tuple[str, str], int],
        file_reader: callable = None,
    ) -> str | None:
        """Resolve a gate node's transitions against the given flags."""
        return self.next_node(gate_id, result_flags, edge_counts,
                             file_reader=file_reader)

    # ── Validation ────────────────────────────────────────────────

    def validate(self) -> list[str]:
        """Run all static validations. Returns list of human-readable issues."""
        issues: list[str] = []

        issues.extend(self._validate_basic())
        if issues:
            # Don't run deeper checks if basic structure is broken.
            # Still run cycle check though — it's independent.
            pass
        issues.extend(self._validate_transition_targets())
        issues.extend(self._validate_reachability())
        issues.extend(self._validate_cycle_safety())

        return issues

    def _validate_basic(self) -> list[str]:
        issues: list[str] = []

        if not self._graph.name:
            issues.append("Graph name is required")
        if not self._graph.begin:
            issues.append("Graph begin node is required")
        elif self._graph.begin not in self._step_map:
            issues.append(f"Begin node '{self._graph.begin}' not found in steps")
        if not self._graph.steps:
            issues.append("Graph must have at least one step")

        # Check for duplicate step ids
        seen: set[str] = set()
        for s in self._graph.steps:
            if s.id in seen:
                issues.append(f"Duplicate step id: '{s.id}'")
            seen.add(s.id)

        return issues

    def _validate_transition_targets(self) -> list[str]:
        issues: list[str] = []
        for s in self._graph.steps:
            for t in s.transitions:
                if t.to is None:
                    continue  # terminal transition (end of graph)
                if t.to not in self._step_map:
                    issues.append(
                        f"Step '{s.id}': transition to '{t.to}' "
                        f"which is not a defined step"
                    )
        return issues

    def _validate_reachability(self) -> list[str]:
        """Find nodes that are unreachable from begin."""
        issues: list[str] = []
        if self._graph.begin not in self._step_map:
            return issues  # basic validation already caught this

        reachable: set[str] = set()

        def _dfs(node_id: str, visited: set[str]):
            if node_id in visited:
                return
            visited.add(node_id)
            reachable.add(node_id)
            node = self._step_map.get(node_id)
            if node:
                for t in node.transitions:
                    _dfs(t.to, visited | {node_id})

        _dfs(self._graph.begin, set())

        for s in self._graph.steps:
            if s.id not in reachable:
                issues.append(f"Step '{s.id}' is unreachable from begin '{self._graph.begin}'")

        return issues

    def _validate_cycle_safety(self) -> list[str]:
        """Ensure every cycle has at least one edge with max_loop set."""
        issues: list[str] = []

        cycles = self._find_all_cycles()
        for i, cycle_path in enumerate(cycles):
            edges = list(zip(cycle_path, cycle_path[1:] + [cycle_path[0]]))
            if not any(self._edge_has_limit(src, dst) for src, dst in edges):
                issues.append(
                    f"Cycle {i+1}: {' → '.join(cycle_path)} → {cycle_path[0]} "
                    f"has no max_loop constraint on any edge"
                )

        return issues

    def _edge_has_limit(self, from_id: str, to_id: str) -> bool:
        node = self._step_map.get(from_id)
        if not node:
            return False
        for t in node.transitions:
            if t.to == to_id and t.max_loop is not None:
                return True
        return False

    def _find_all_cycles(self) -> list[list[str]]:
        """Find all elementary cycles in the graph using Johnson's algorithm.

        Returns a list of cycles, each as a list of step ids.
        """
        nodes = list(self._step_map.keys())
        if not nodes:
            return []

        # Map node id → index for Johnson's algorithm
        node_to_idx = {n: i for i, n in enumerate(nodes)}
        idx_to_node = {i: n for n, i in node_to_idx.items()}
        n = len(nodes)

        # Build adjacency list as indices
        adj: list[list[int]] = [[] for _ in range(n)]
        for s in self._graph.steps:
            u = node_to_idx[s.id]
            for t in s.transitions:
                v = node_to_idx.get(t.to)
                if v is not None:
                    adj[u].append(v)

        # Johnson's algorithm
        blocked = [False] * n
        B: list[set[int]] = [set() for _ in range(n)]
        stack: list[int] = []
        cycles: list[list[int]] = []

        def _unblock(u: int):
            blocked[u] = False
            for w in list(B[u]):
                B[u].discard(w)
                if blocked[w]:
                    _unblock(w)

        def _circuit(v: int, s: int):
            f = False
            stack.append(v)
            blocked[v] = True

            for w in adj[v]:
                if w == s:
                    # Found a cycle
                    cycles.append(list(stack) + [s])
                    f = True
                elif not blocked[w]:
                    if _circuit(w, s):
                        f = True

            if f:
                _unblock(v)
            else:
                for w in adj[v]:
                    if v not in B[w]:
                        B[w].add(v)

            stack.pop()
            return f

        for s in range(n):
            # Subgraph of nodes >= s
            blocked = [False] * n
            B = [set() for _ in range(n)]
            _circuit(s, s)
            # Remove node s from graph for next iteration
            # (Johnson's algorithm handles this via the s index)
            for i in range(n):
                adj[i] = [w for w in adj[i] if w >= s]

        # Convert back to node ids, deduplicate
        seen: set[tuple[str, ...]] = set()
        result: list[list[str]] = []
        for cycle_idx in cycles:
            path = tuple(idx_to_node[i] for i in cycle_idx)
            # Normalize: rotate so smallest index element is first
            if path not in seen:
                seen.add(path)
                result.append([idx_to_node[i] for i in cycle_idx])

        return result


# ── Helpers ──────────────────────────────────────────────────────────


def _flags_match(match: dict, flags: dict, *,
                 file_reader: callable = None) -> bool:
    """Return True if all conditions in ``match`` are satisfied.

    Match patterns (evaluated in order):
    1. ``{from_file: "path", field: "name", value: val}`` — read the
       output file, parse JSON, check ``data[field] == val``.
    2. ``{field: "name", value: val}`` — indirect: ``flags["name"] == val``.
    3. ``{from: "checkpoint", value: "approved"}`` — special checkpoint.
    4. ``{key: val}`` — direct: ``flags[key] == val``.
    """
    if match is None:
        return True

    # from_file: read step output file and check a field
    if "from_file" in match and "field" in match and "value" in match:
        if file_reader is None:
            return False
        try:
            content = file_reader(match["from_file"])
            data = json.loads(content)
            return data.get(match["field"]) == match["value"]
        except Exception:
            return False

    # field/value indirect pattern
    if "field" in match and "value" in match:
        flag_key = match["field"]
        expected = match["value"]
        return flags.get(flag_key) == expected

    for key, expected in match.items():
        if key == "from":
            source = match.get("from")
            if source == "checkpoint":
                val = match.get("value", "approved")
                if val == "approved":
                    if flags.get("_checkpoint_approved") is not True:
                        return False
                elif val == "rejected":
                    if flags.get("_checkpoint_approved") is not False:
                        return False
                else:
                    if flags.get("_checkpoint_approved") != val:
                        return False
                continue
            return False
        if key == "value":
            continue  # handled above
        if key == "field":
            continue  # handled at top
        if key not in flags:
            return False
        if flags[key] != expected:
            return False
    return True
