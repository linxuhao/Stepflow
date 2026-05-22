"""SkillFlow main class.

Provides the full run lifecycle: create, claim, execute (application),
confirm/fail, advance, checkpoint, and recovery. Uses a persistent
SQLite connection (single-worker model) with WAL mode for safety.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    from skillflow.tool_loader import ToolLoader

from skillflow.schema import ALL_DDL
from skillflow.graph import (
    EndConditions,
    EndResult,
    GraphResolver,
    PipelineGraph,
    StepNode,
    Transition,
)
from skillflow.exceptions import (
    CycleLimitExceeded,
    GraphValidationError,
    NoMatchingTransition,
    OutputValidationError,
    StepVersionConflict,
    SkillFlowError,
)


# ── Internal abort signal for intentional rollback within _tx ────────

class _TxRollback(Exception):
    """Raised inside a _tx block to intentionally roll back."""
    pass


@dataclass(frozen=True)
class ClaimToken:
    step_id: str
    run_id: str
    step_instance_id: int
    version: int
    claimed_at: float


@dataclass(frozen=True)
class ClaimedStep:
    token: ClaimToken
    step_id: str
    step_config: dict
    run_context: dict
    inputs: dict[str, dict]
    validation_error: str | None = None
    error_context: dict | None = None
    emit: Callable[[str, dict], Awaitable[None]] = field(
        default=lambda event_type, payload: _noop_emit(event_type, payload)
    )

    def flat_inputs(self) -> dict:
        result: dict = {}
        for step_outputs in self.inputs.values():
            result.update(step_outputs)
        return result


async def _noop_emit(event_type: str, payload: dict) -> None:
    pass


@dataclass(frozen=True)
class StepResult:
    outputs: dict = field(default_factory=dict)
    flags: dict = field(default_factory=dict)


@dataclass(frozen=True)
class OutboxEvent:
    id: int
    event_type: str
    payload_json: str
    stream_target: str


class StepRunner(Protocol):
    async def execute(self, step: ClaimedStep) -> StepResult: ...


class SkillFlow:
    """Transactional graph orchestrator with embedded SQLite."""

    def __init__(self, db_path: str = ":memory:", *,
                 tool_loader: "ToolLoader | None" = None,
                 stale_threshold_seconds: float = 300,
                 notification_bus: "NotificationBus | None" = None,
                 workspace_base: str = "",
                 projects_base: str = "",
                 delegate_tools_to_agent: bool = False):
        self._db_path = db_path
        self._graphs: dict[str, PipelineGraph] = {}
        self._resolvers: dict[str, GraphResolver] = {}
        self._lock = threading.RLock()
        self._tool_loader = tool_loader
        self._load_native_tools()
        self._stale_threshold = stale_threshold_seconds
        self._workspace = None
        self.delegate_tools_to_agent = delegate_tools_to_agent
        if workspace_base:
            from skillflow.workspace import WorkspaceManager
            self._workspace = WorkspaceManager(workspace_base, projects_base=projects_base)

        from skillflow.agent_registry import AgentRegistry
        self.agent_registry = AgentRegistry()

        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA foreign_keys = ON;")
        self._conn.execute("PRAGMA busy_timeout = 5000;")
        # Main DDL (CREATE TABLE IF NOT EXISTS — always safe)
        for stmt in ALL_DDL:
            self._conn.execute(stmt)
        # Indexes
        from skillflow.schema import SKILLFLOW_INDEXES
        for stmt in SKILLFLOW_INDEXES:
            self._conn.execute(stmt)
        # Migrations — idempotent DDL (skip if already applied)
        from skillflow.schema import SKILLFLOW_MIGRATIONS
        for stmt in SKILLFLOW_MIGRATIONS:
            try:
                self._conn.execute(stmt)
            except sqlite3.OperationalError:
                # Column/index already exists or DB locked — fine
                pass
        self._conn.commit()

        # Notification bus — shared with host app for real-time push
        if notification_bus is not None:
            self.notifications = notification_bus
        else:
            from skillflow.notifications import NotificationBus
            self.notifications = NotificationBus(db_path=db_path)
        self.notifications.set_connection(self._conn)

    def _load_native_tools(self):
        """Ensure the built-in tools directory is loaded as the native source."""
        native_dir = Path(__file__).parent / "tools"
        if self._tool_loader is None:
            from skillflow.tool_loader import ToolLoader
            self._tool_loader = ToolLoader(native_dir)
        elif hasattr(self._tool_loader, '_tools_dirs'):
            # Only manipulate real ToolLoader instances, not duck-typed mocks
            if native_dir not in self._tool_loader._tools_dirs:
                self._tool_loader._tools_dirs.insert(0, native_dir)
                self._tool_loader._cache.clear()
                self._tool_loader._tool_dir_cache.clear()
        # Register plugin tools (e.g. skillflow_lint)
        linter_dir = Path(__file__).parent / "plugins" / "linter" / "tools"
        if linter_dir.exists() and hasattr(self._tool_loader, '_tools_dirs'):
            if linter_dir not in self._tool_loader._tools_dirs:
                self._tool_loader._tools_dirs.append(linter_dir)
                self._tool_loader._cache.clear()
                self._tool_loader._tool_dir_cache.clear()

    def _should_delegate_tool(self, tool_name: str) -> bool:
        """Return True if this tool should be delegated to the agent.

        In framework mode (delegate_tools_to_agent=False), never delegate.
        In runner mode (delegate_tools_to_agent=True), only native tools
        are auto-executed; everything else goes to the agent.
        """
        if not self.delegate_tools_to_agent:
            return False
        if self._tool_loader is None:
            return True
        return not self._tool_loader.is_native(tool_name)

    @contextmanager
    def _tx(self):
        """Serialised transaction context.

        Yields the persistent connection with BEGIN IMMEDIATE already
        started.  Commits on clean exit, rolls back on any exception
        (including _TxRollback, which is used for intentional abort).
        """
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE;")
            try:
                yield self._conn
            except _TxRollback:
                self._conn.rollback()
            except Exception:
                self._conn.rollback()
                raise
            else:
                self._conn.commit()

    @staticmethod
    def _serialize(obj: dict) -> str:
        return json.dumps(obj, ensure_ascii=False)

    @staticmethod
    def _deserialize(text: str) -> dict:
        if not text:
            return {}
        if isinstance(text, dict):
            return text  # SQLite json_set may return pre-parsed dict
        return json.loads(text)

    # ── Graph management ──────────────────────────────────────────

    def register_graph(self, graph: PipelineGraph) -> None:
        issues = graph.validate()
        if issues:
            raise GraphValidationError(issues)
        # Validate agent_config references exist in registry
        missing = self._check_agent_configs(graph)
        if missing:
            raise GraphValidationError([
                f"Agent config '{name}' referenced in graph but not registered"
                for name in missing
            ])
        resolver = GraphResolver(graph)
        self._graphs[graph.name] = graph
        self._resolvers[graph.name] = resolver
        with self._tx() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO skillflow_graphs (name, yaml_text, version, updated_at)
                VALUES (?, ?, COALESCE((SELECT version + 1 FROM skillflow_graphs WHERE name=?), 1),
                        datetime('now'))
                """,
                (graph.name, json.dumps(graph.to_dict()), graph.name),
            )

    def register_agent_config(self, name: str, **kwargs) -> None:
        """Register an agent config so graph validation can check references."""
        self.agent_registry.register(name, **kwargs)
        if self._tool_loader:
            self.agent_registry.resolve_tool_schemas(self._tool_loader)

    def register_agent_config_from_dict(self, name: str, d: dict) -> None:
        """Register from a flat dict (convenience for YAML-loaded configs)."""
        self.agent_registry.register_dict(name, d)
        if self._tool_loader:
            self.agent_registry.resolve_tool_schemas(self._tool_loader)

    def _check_agent_configs(self, graph: PipelineGraph) -> list[str]:
        """Return names of agent_configs referenced in graph but not registered."""
        missing: list[str] = []
        for node in graph.steps:
            if node.agent_config and node.agent_config not in self.agent_registry:
                missing.append(node.agent_config)
        return missing

    def _get_resolver(self, graph_name: str) -> GraphResolver:
        resolver = self._resolvers.get(graph_name)
        if resolver is not None:
            return resolver
        with self._lock:
            row = self._conn.execute(
                "SELECT yaml_text FROM skillflow_graphs WHERE name = ?", (graph_name,)
            ).fetchone()
        if not row:
            raise SkillFlowError(f"Graph '{graph_name}' not registered")
        data = json.loads(row["yaml_text"])
        graph = PipelineGraph._from_dict(data)
        resolver = GraphResolver(graph)
        self._graphs[graph_name] = graph
        self._resolvers[graph_name] = resolver
        return resolver

    def _get_resolver_for_run(self, run_id: str) -> GraphResolver:
        with self._lock:
            row = self._conn.execute(
                "SELECT graph_name FROM skillflow_runs WHERE id = ?", (run_id,)
            ).fetchone()
        if not row:
            raise SkillFlowError(f"Run '{run_id}' not found")
        return self._get_resolver(row["graph_name"])

    # ── Run lifecycle ──────────────────────────────────────────────

    def create_run(self, graph_name: str, context: dict | None = None,
                   project_id: str = None, *,
                   graph_path: str | None = None) -> str:
        resolver = self._get_resolver(graph_name)
        graph = resolver.graph
        run_id = str(uuid.uuid4())
        ctx = context or {}

        # Extract project_id from context if not explicitly given
        if project_id is None:
            project_id = ctx.get("project_id")

        with self._tx() as conn:
            conn.execute(
                """
                INSERT INTO skillflow_runs (id, graph_name, graph_path, project_id, context_json, current_node, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                """,
                (run_id, graph_name, graph_path, project_id, self._serialize(ctx), graph.begin),
            )
            for node in graph.steps:
                conn.execute(
                    """
                    INSERT INTO skillflow_steps
                        (run_id, step_id, step_config_json, max_retries, status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, 'pending', datetime('now'), datetime('now'))
                    """,
                    (run_id, node.id, self._serialize(node.config), node.max_retries),
                )
            for node in graph.steps:
                for trans in node.transitions:
                    if trans.max_loop is not None:
                        conn.execute(
                            """
                            INSERT INTO skillflow_edge_counts
                                (run_id, from_step, to_step, count, max_loop)
                            VALUES (?, ?, ?, 0, ?)
                            """,
                            (run_id, node.id, trans.to, trans.max_loop),
                        )
            conn.execute(
                """
                INSERT INTO skillflow_outbox (event_type, payload_json, created_at)
                VALUES ('run_created', ?, datetime('now'))
                """,
                (self._serialize({"run_id": run_id, "graph_name": graph_name}),),
            )
        return run_id

    def start_run(self, run_id: str) -> None:
        with self._tx() as conn:
            cur = conn.execute(
                """
                UPDATE skillflow_runs SET status = 'running', started_at = datetime('now'),
                    updated_at = datetime('now')
                WHERE id = ? AND status = 'pending'
                """,
                (run_id,),
            )
            if cur.rowcount == 0:
                raise SkillFlowError(f"Run '{run_id}' not found or not in 'pending' status")
            conn.execute(
                """
                INSERT INTO skillflow_outbox (event_type, payload_json, created_at)
                VALUES ('run_started', ?, datetime('now'))
                """,
                (self._serialize({"run_id": run_id}),),
            )

    def pause_run(self, run_id: str) -> None:
        self._update_run_state(run_id, "paused")

    def resume_run(self, run_id: str) -> None:
        self._update_run_state(run_id, "running")

    def reactivate_run(self, run_id: str) -> None:
        """Reactivate a failed run back to running state.

        Used when a host app detects new work and needs to restart
        a previously failed pipeline run. Clears error_reason and
        current_node so advance_run re-resolves from the last
        completed step.

        Raises ValueError if the run is already completed. Use
        re_run() to explicitly restart a completed run.
        """
        with self._tx() as conn:
            run = conn.execute(
                "SELECT status FROM skillflow_runs WHERE id = ?", (run_id,)
            ).fetchone()
            if not run:
                raise ValueError(f"Run not found: {run_id}")
            if run["status"] == "completed":
                raise ValueError(
                    f"Run {run_id} is already completed. "
                    f"Use re_run() to explicitly re-run a completed pipeline."
                )
            conn.execute(
                """UPDATE skillflow_runs SET status = 'running',
                   error_reason = NULL, current_node = NULL,
                   updated_at = datetime('now') WHERE id = ?""",
                (run_id,),
            )

    def re_run(self, run_id: str) -> str:
        """Explicitly restart a completed/failed run as a fresh run.

        Creates a NEW run_id with the same graph and project,
        resetting all step state. Returns the new run_id.
        """
        with self._tx() as conn:
            old = conn.execute(
                "SELECT graph_name, graph_path, project_id, context_json "
                "FROM skillflow_runs WHERE id = ?", (run_id,)
            ).fetchone()
            if not old:
                raise ValueError(f"Run not found: {run_id}")

        import json
        ctx = json.loads(old["context_json"]) if old["context_json"] else {}
        new_id = self.create_run(
            old["graph_name"],
            context=ctx,
            project_id=old["project_id"],
            graph_path=old["graph_path"],
        )
        self.start_run(new_id)
        return new_id

    def fail_run(self, run_id: str, reason: str) -> None:
        with self._tx() as conn:
            self._fail_run_in_tx(conn, run_id, reason)

    def complete_run(self, run_id: str) -> None:
        with self._tx() as conn:
            self._complete_run_in_tx(conn, run_id, "Run completed")

    def _update_run_state(self, run_id: str, status: str) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE skillflow_runs SET status = ?, updated_at = datetime('now') WHERE id = ?",
                (status, run_id),
            )

    def get_run(self, run_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM skillflow_runs WHERE id = ?", (run_id,)
            ).fetchone()
            return dict(row) if row else None

    # ── Project CRUD (Wolverine-style: framework owns project state) ─

    def create_project(self, project_id: str, name: str = "",
                       meta: dict | None = None) -> None:
        with self._tx() as conn:
            conn.execute(
                """INSERT OR IGNORE INTO skillflow_projects (id, name, meta_json, created_at, updated_at)
                   VALUES (?, ?, ?, datetime('now'), datetime('now'))""",
                (project_id, name, self._serialize(meta or {})),
            )

    def get_project(self, project_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM skillflow_projects WHERE id = ?", (project_id,)
            ).fetchone()
            return dict(row) if row else None

    def list_projects(self, status: str = None) -> list[dict]:
        with self._lock:
            if status:
                rows = self._conn.execute(
                    "SELECT * FROM skillflow_projects WHERE status = ? ORDER BY created_at DESC",
                    (status,),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM skillflow_projects ORDER BY created_at DESC"
                ).fetchall()
            return [dict(r) for r in rows]

    def update_project_status(self, project_id: str, status: str) -> None:
        with self._tx() as conn:
            conn.execute(
                "UPDATE skillflow_projects SET status = ?, updated_at = datetime('now') WHERE id = ?",
                (status, project_id),
            )

    def delete_project(self, project_id: str) -> None:
        """Delete all skillflow state for a project.

        Removes runs, steps, edge counts, loop state, outbox events,
        and the project row itself.  Safe to call even if the project
        has no runs.
        """
        with self._tx() as conn:
            # Collect all run IDs for this project
            run_ids = [
                r["id"] for r in conn.execute(
                    "SELECT id FROM skillflow_runs WHERE project_id = ?",
                    (project_id,),
                ).fetchall()
            ]
            for run_id in run_ids:
                conn.execute("DELETE FROM skillflow_steps WHERE run_id = ?", (run_id,))
                conn.execute("DELETE FROM skillflow_edge_counts WHERE run_id = ?", (run_id,))
                conn.execute("DELETE FROM skillflow_loop_state WHERE run_id = ?", (run_id,))
                conn.execute("DELETE FROM skillflow_outbox WHERE payload_json LIKE ?",
                             (f"%{run_id}%",))
            conn.execute("DELETE FROM skillflow_runs WHERE project_id = ?", (project_id,))
            conn.execute("DELETE FROM skillflow_projects WHERE id = ?", (project_id,))

    # ── Query APIs ──────────────────────────────────────────────────

    def list_runs(self, project_id: str = None, status: str = None) -> list[dict]:
        with self._lock:
            clauses = []
            params: list = []
            if project_id:
                clauses.append("project_id = ?")
                params.append(project_id)
            if status:
                clauses.append("status = ?")
                params.append(status)
            where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
            rows = self._conn.execute(
                f"SELECT * FROM skillflow_runs {where} ORDER BY created_at DESC",
                params,
            ).fetchall()
            return [dict(r) for r in rows]

    def get_steps(self, run_id: str) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT * FROM skillflow_steps
                   WHERE run_id = ? ORDER BY id ASC""",
                (run_id,),
            ).fetchall()
            return [dict(r) for r in rows]

    def get_run_by_project(self, project_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                """SELECT * FROM skillflow_runs
                   WHERE project_id = ? AND status NOT IN ('completed','failed')
                   ORDER BY created_at DESC LIMIT 1""",
                (project_id,),
            ).fetchone()
            return dict(row) if row else None

    def get_or_create_run(self, graph_name: str, project_id: str,
                          context: dict | None = None) -> str:
        existing = self.get_run_by_project(project_id)
        if existing:
            return existing["id"]
        return self.create_run(graph_name, context, project_id=project_id)

    def start_project(self, project_id: str, graph_name: str,
                      context: dict | None = None) -> str:
        self.create_project(project_id)
        run_id = self.create_run(graph_name, context, project_id=project_id)
        self.start_run(run_id)
        return run_id

    # ── Claim / Confirm / Fail ─────────────────────────────────────

    def claim_next_step(self, run_id: str) -> ClaimedStep | None:
        with self._tx() as conn:
            run = conn.execute(
                "SELECT * FROM skillflow_runs WHERE id = ?", (run_id,)
            ).fetchone()
            if not run or run["status"] not in ("running",) or not run["current_node"]:
                raise _TxRollback()

            graph_name = run["graph_name"]
            resolver = self._get_resolver(graph_name)

            if resolver.is_gate(run["current_node"]):
                raise _TxRollback()

            node = resolver.get_node(run["current_node"])
            if not node:
                raise _TxRollback()

            current_version = conn.execute(
                "SELECT version FROM skillflow_steps WHERE run_id = ? AND step_id = ? AND status = 'pending'",
                (run_id, run["current_node"]),
            ).fetchone()
            if not current_version:
                # For cyclic graphs: if the step has already been executed
                # (completed/failed), create a new instance for the next iteration
                # For cyclic graphs: create a new instance if the step was
                # previously completed or failed (not if it's currently claimed)
                existing = conn.execute(
                    "SELECT id, status FROM skillflow_steps WHERE run_id = ? AND step_id = ?",
                    (run_id, run["current_node"]),
                ).fetchone()
                if existing and existing["status"] in ("completed", "failed"):
                    node = resolver.get_node(run["current_node"])
                    if node:
                        conn.execute(
                            """
                            INSERT INTO skillflow_steps
                                (run_id, step_id, step_config_json, max_retries, status, created_at, updated_at)
                            VALUES (?, ?, ?, ?, 'pending', datetime('now'), datetime('now'))
                            """,
                            (run_id, run["current_node"], self._serialize(node.config), node.max_retries),
                        )
                        current_version = conn.execute(
                            "SELECT version FROM skillflow_steps WHERE run_id = ? AND step_id = ? AND status = 'pending'",
                            (run_id, run["current_node"]),
                        ).fetchone()
                if not current_version:
                    raise _TxRollback()

            ver = current_version["version"]
            claimed_at = time.time()
            claimed_at_str = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(claimed_at))

            cursor = conn.execute(
                """
                UPDATE skillflow_steps SET status = 'claimed', version = version + 1,
                    claimed_at = ?, claimed_by = ?, updated_at = datetime('now')
                WHERE run_id = ? AND step_id = ? AND version = ? AND status = 'pending'
                """,
                (claimed_at_str, "worker", run_id, run["current_node"], ver),
            )
            if cursor.rowcount == 0:
                raise _TxRollback()

            step_row = conn.execute(
                "SELECT id FROM skillflow_steps WHERE run_id = ? AND step_id = ? AND status = 'claimed'",
                (run_id, run["current_node"]),
            ).fetchone()

            completed_steps = conn.execute(
                """
                SELECT step_id, outputs_json FROM skillflow_steps
                WHERE run_id = ? AND status = 'completed'
                ORDER BY completed_at ASC
                """,
                (run_id,),
            ).fetchall()

            inputs: dict[str, dict] = {}
            for cs in completed_steps:
                inputs[cs["step_id"]] = self._deserialize(cs["outputs_json"])

            error_context = None
            validation_error = None
            feedback = None
            existing = conn.execute(
                "SELECT inputs_json FROM skillflow_steps WHERE run_id = ? AND step_id = ?",
                (run_id, run["current_node"]),
            ).fetchone()
            if existing:
                existing_inputs = self._deserialize(existing["inputs_json"])
                if "_error" in existing_inputs:
                    error_context = existing_inputs["_error"]
                if "_validation_error" in existing_inputs:
                    validation_error = existing_inputs["_validation_error"]
                if "_feedback" in existing_inputs:
                    feedback = existing_inputs["_feedback"]

            conn.execute(
                """
                INSERT INTO skillflow_outbox (event_type, payload_json, created_at)
                VALUES ('step_claimed', ?, datetime('now'))
                """,
                (self._serialize({
                    "run_id": run_id, "step_id": run["current_node"],
                    "step_instance_id": step_row["id"] if step_row else None,
                }),),
            )

            token = ClaimToken(
                step_id=run["current_node"], run_id=run_id,
                step_instance_id=step_row["id"] if step_row else 0,
                version=ver + 1, claimed_at=claimed_at,
            )

            # Inject resolved tool schemas if agent config is registered
            tool_schemas: dict = {}
            agent_cfg = None
            if node.agent_config and node.agent_config in self.agent_registry:
                agent_cfg = self.agent_registry.get(node.agent_config)
                if agent_cfg and agent_cfg.tool_schemas:
                    tool_schemas = agent_cfg.tool_schemas
            inputs_with_tools = dict(inputs)
            if tool_schemas:
                inputs_with_tools["_tool_schemas"] = tool_schemas
            if agent_cfg:
                inputs_with_tools["_agent_config"] = agent_cfg.to_dict()

            # Resolve context specs from the graph step node
            if self._workspace and node.context:
                try:
                    from skillflow.context import ContextResolver
                    config_path = self._workspace.get_project_path(
                        run["project_id"]
                    )
                    resolver = ContextResolver(config_path, self._tool_loader)
                    resolved = resolver.resolve(node.context, current_config=run["graph_name"])
                    if resolved:
                        inputs_with_tools["_resolved_context"] = resolved
                except Exception:
                    pass  # Context resolution is best-effort

            # Inject loop item context if this step is inside a loop body
            loop_row = conn.execute(
                "SELECT current_index, items_json, item_context_key "
                "FROM skillflow_loop_state WHERE run_id = ?",
                (run_id,),
            ).fetchone()
            if loop_row:
                items = self._deserialize(loop_row["items_json"])
                idx = loop_row["current_index"]
                key = loop_row["item_context_key"] or "loop_item"
                if 0 <= idx < len(items):
                    item = items[idx]
                    if "_resolved_context" not in inputs_with_tools:
                        inputs_with_tools["_resolved_context"] = {}
                    inputs_with_tools["_resolved_context"][f"[{key}]"] = (
                        self._serialize(item) if not isinstance(item, str) else item
                    )

            # Merge dynamic write tool schemas from graph's output.fixed
            if node.output_mode and node.output_fixed:
                from skillflow.write_tools import generate_write_tool_schemas
                for ws in generate_write_tool_schemas(node.output_mode, node.output_fixed):
                    tool_schemas[ws["name"]] = ws

            inputs_with_tools["_tool_schemas"] = tool_schemas

            # Step-level max_tool_turns overrides agent config default (0 = use agent default)
            if node.max_tool_turns:
                inputs_with_tools["_max_tool_turns"] = node.max_tool_turns

            # Provide output directory + expected files
            if self._workspace:
                tmp_dir = self._workspace.get_step_tmp_dir(
                    run["project_id"], run["graph_name"], node.id
                )
                tmp_dir.mkdir(parents=True, exist_ok=True)
                inputs_with_tools["_output_dir"] = str(tmp_dir)
                if node.output_fixed:
                    from skillflow.write_tools import _get_pattern
                    inputs_with_tools["_expected_files"] = [
                        _get_pattern(s, node.output_fixed) for s in node.output_fixed
                    ]

            # Preserve injected context from previous attempts
            if feedback is not None:
                inputs_with_tools["_feedback"] = feedback
            if validation_error is not None:
                inputs_with_tools["_validation_error"] = validation_error
            if error_context is not None:
                inputs_with_tools["_error"] = error_context

            # Persist enriched inputs so DB state reflects claim-time resolution
            conn.execute(
                "UPDATE skillflow_steps SET inputs_json = ?, updated_at = datetime('now') WHERE id = ?",
                (self._serialize(inputs_with_tools), step_row["id"]),
            )

            return ClaimedStep(
                token=token, step_id=run["current_node"],
                step_config=node.config,
                run_context=self._deserialize(run["context_json"]),
                inputs=inputs_with_tools,
                validation_error=validation_error,
                error_context=error_context,
            )

    def confirm_step(self, token: ClaimToken, result: StepResult) -> None:
        resolver = self._get_resolver_for_run(token.run_id)
        node = resolver.get_node(token.step_id)

        if node and node.output_schema and node.output_schema_retries > 0:
            from skillflow.validation import OutputValidator
            try:
                validator = OutputValidator(node.output_schema)
                validator.validate(result.outputs)
            except OutputValidationError as e:
                self._handle_validation_failure(token, str(e))
                return
            except ImportError as e:
                self._handle_validation_failure(
                    token, f"Schema import failed: {e}"
                )
                return

        # Validation specs from graph (syntax_lint, py_compile, json_schema, etc.)
        if node and node.validation:
            val_result = self._validate_outputs(token, node)
            if not val_result.get("passed", False):
                errors = val_result.get("errors", [])
                error_msg = "Validation failed:\n" + "\n".join(
                    e.get("error", str(e)) for e in errors
                )
                self._handle_validation_failure(token, error_msg)
                return

        # ── Lifecycle hooks ──────────────────────────────────────────
        if node and self._workspace:
            lifecycle = self._resolve_lifecycle(node)
            for hook_name, hook_spec in lifecycle.items():
                hook_result = self._execute_lifecycle_hook(
                    token, node, hook_name, hook_spec
                )
                if not hook_result.get("passed", False):
                    error = hook_result.get("error", "Lifecycle hook failed")
                    on_failure = hook_spec.get("on_failure", "fail") if isinstance(hook_spec, dict) else "fail"
                    if on_failure == "retry":
                        self._handle_lifecycle_retry(token, error)
                        return
                    elif on_failure == "skip":
                        self._emit_lifecycle_event(token, hook_name, "skipped", error)
                        continue
                    else:
                        self._handle_lifecycle_failure(token, error)
                        return

        with self._tx() as conn:
            cursor = conn.execute(
                """
                UPDATE skillflow_steps
                SET status = 'completed', version = version + 1,
                    outputs_json = ?, result_flags_json = ?,
                    completed_at = datetime('now'), updated_at = datetime('now')
                WHERE id = ? AND version = ?
                """,
                (
                    self._serialize(result.outputs),
                    self._serialize(result.flags),
                    token.step_instance_id, token.version,
                ),
            )
            if cursor.rowcount == 0:
                raise StepVersionConflict(
                    f"Step '{token.step_id}' (instance {token.step_instance_id}) "
                    f"version mismatch: expected {token.version}"
                )

            # Resolve next transition inline to close the atomicity gap
            # between confirm_step and advance_run. If process dies here,
            # the run already knows its next step.
            next_node = self._resolve_next_in_tx(
                conn, token.run_id, token.step_id, result.flags, resolver
            )
            if next_node:
                conn.execute(
                    "UPDATE skillflow_runs SET current_node = ?, updated_at = datetime('now') WHERE id = ?",
                    (next_node, token.run_id),
                )
            else:
                conn.execute(
                    "UPDATE skillflow_runs SET current_node = NULL, updated_at = datetime('now') WHERE id = ?",
                    (token.run_id,),
                )

            conn.execute(
                """
                INSERT INTO skillflow_outbox (event_type, payload_json, created_at)
                VALUES ('step_completed', ?, datetime('now'))
                """,
                (self._serialize({
                    "run_id": token.run_id, "step_id": token.step_id,
                    "step_instance_id": token.step_instance_id,
                }),),
            )

    def _handle_validation_failure(self, token: ClaimToken, error: str) -> None:
        resolver = self._get_resolver_for_run(token.run_id)
        node = resolver.get_node(token.step_id)
        if not node:
            return
        with self._tx() as conn:
            row = conn.execute(
                "SELECT retry_count, validation_retry_count, max_retries FROM skillflow_steps WHERE id = ?",
                (token.step_instance_id,),
            ).fetchone()
            # Share retry budget between LLM retries and validation retries
            total_retries = (row["retry_count"] if row else 0) + (row["validation_retry_count"] if row else 0)
            max_allowed = row["max_retries"] if row else node.max_retries
            if row and total_retries < max_allowed:
                conn.execute(
                    """
                    UPDATE skillflow_steps
                    SET status = 'pending', version = version + 1,
                        validation_retry_count = validation_retry_count + 1,
                        inputs_json = json_set(inputs_json, '$._validation_error', ?),
                        updated_at = datetime('now')
                    WHERE id = ? AND version = ?
                    """,
                    (error, token.step_instance_id, token.version),
                )
                conn.execute(
                    """
                    INSERT INTO skillflow_outbox (event_type, payload_json, created_at)
                    VALUES ('step_validation_failed', ?, datetime('now'))
                    """,
                    (self._serialize({
                        "run_id": token.run_id, "step_id": token.step_id, "error": error,
                        "retry_count": row["retry_count"],
                        "validation_retry_count": row["validation_retry_count"] + 1,
                        "max_retries": max_allowed,
                    }),),
                )
            else:
                # Retry budget exhausted — permanent failure
                self._fail_step_in_tx(conn, token, f"Output validation failed: {error}", retryable=False)

    def _validate_outputs(self, token: ClaimToken, node: StepNode) -> dict:
        """Run graph validation specs against draft outputs. Returns {passed, errors}."""
        if not self._workspace:
            return {"passed": True}
        pid = self._get_project_id(token.run_id)
        gname = self._get_graph_name(token.run_id)
        tmp_dir = self._workspace.get_step_tmp_dir(pid, gname, token.step_id)
        from skillflow.step_validation import StepValidator
        validator = StepValidator(self._tool_loader, tmp_dir)
        return validator.validate(node.validation)

    # ── Lifecycle hooks ─────────────────────────────────────────────

    def _resolve_lifecycle(self, node: StepNode) -> dict:
        """Resolve lifecycle hooks with correct execution order.

        Order: after_validate → on_deliver → after_deliver.
        If after_validate is not declared but the step produces output,
        default to built-in step_commit.
        """
        declared = dict(node.lifecycle) if node.lifecycle else {}
        has_output = bool(node.output_fixed or node.output_mode)

        lifecycle: dict = {}
        if has_output:
            lifecycle["after_validate"] = declared.pop(
                "after_validate", {"tool": "step_commit"})
        if "on_deliver" in declared:
            lifecycle["on_deliver"] = declared.pop("on_deliver")
        if "after_deliver" in declared:
            lifecycle["after_deliver"] = declared.pop("after_deliver")
        lifecycle.update(declared)  # any unknown hooks
        return lifecycle

    def _execute_lifecycle_hook(self, token: ClaimToken, node: StepNode,
                                 hook_name: str, hook_spec) -> dict:
        """Execute a single lifecycle hook.

        hook_spec can be:
        - A dict with 'tool' (single tool call): used for after_validate, on_deliver
        - A list of dicts (multi-check): used for after_deliver

        Returns {passed: bool, error?: str}.
        """
        self._emit_lifecycle_event(token, hook_name, "started")

        if isinstance(hook_spec, list):
            return self._execute_check_hook(token, node, hook_name, hook_spec)
        elif isinstance(hook_spec, dict) and "tool" in hook_spec:
            return self._execute_tool_hook(token, node, hook_name, hook_spec)
        else:
            return {"passed": False, "error": f"Invalid hook spec for '{hook_name}'"}

    def _execute_tool_hook(self, token: ClaimToken, node: StepNode,
                            hook_name: str, hook_spec: dict) -> dict:
        """Execute a tool-type lifecycle hook (single tool call)."""
        tool_name = hook_spec["tool"]
        params = dict(hook_spec.get("params", {}))

        # Resolve variables
        if self._workspace:
            row = self._conn.execute(
                "SELECT project_id, graph_name FROM skillflow_runs WHERE id = ?",
                (token.run_id,),
            ).fetchone()
            if row:
                params = self._workspace.resolve_variables(
                    row["project_id"], row["graph_name"], token.step_id, params
                )
                params.setdefault("workspace_root",
                                  str(self._workspace.get_project_path(row["project_id"])))
                params.setdefault("project_root",
                                  str(self._workspace.projects_base / row["project_id"]))

        # Built-in step_commit: move tmp→step_dir atomically
        if tool_name == "step_commit":
            return self._step_commit(token)

        # Backward compat: draft_promote
        if tool_name == "draft_promote":
            return self._draft_promote(token)

        # External tool via ToolLoader
        if self._tool_loader:
            try:
                fn = self._tool_loader.load_fn(tool_name)
                params.setdefault("run_id", token.run_id)
                params.setdefault("step_id", token.step_id)
                # Filter kwargs to only what the function accepts
                import inspect as _inspect
                try:
                    sig = _inspect.signature(fn)
                    filtered = {k: v for k, v in params.items()
                               if k in sig.parameters}
                except (ValueError, TypeError):
                    filtered = params
                result = fn(**filtered)
                if isinstance(result, dict):
                    passed = result.get("passed", result.get("committed",
                               not result.get("error")))
                    return {"passed": bool(passed), "error": result.get("error", ""),
                            **result}
                return {"passed": True}
            except Exception as e:
                return {"passed": False, "error": str(e)}

        return {"passed": False, "error": f"Tool '{tool_name}' not available"}

    def _execute_check_hook(self, token: ClaimToken, node: StepNode,
                             hook_name: str, check_specs: list[dict]) -> dict:
        """Execute a check-type lifecycle hook (list of validation specs)."""
        if not self._workspace:
            return {"passed": True}
        pid = self._get_project_id(token.run_id)
        gname = self._get_graph_name(token.run_id)

        # after_deliver checks against the project repo, not step output
        if hook_name == "after_deliver":
            check_dir = self._workspace.projects_base / pid
        else:
            check_dir = self._workspace.get_step_dir(pid, gname, token.step_id)

        from skillflow.step_validation import StepValidator
        validator = StepValidator(self._tool_loader, check_dir)
        return validator.validate(check_specs)

    def _step_commit(self, token: ClaimToken) -> dict:
        """Built-in: atomic rename tmp_dir → step_dir."""
        if not self._workspace:
            return {"passed": True}
        pid = self._get_project_id(token.run_id)
        gname = self._get_graph_name(token.run_id)
        tmp_dir = self._workspace.get_step_tmp_dir(pid, gname, token.step_id)
        step_dir = self._workspace.get_step_dir(pid, gname, token.step_id)

        if not tmp_dir.exists() or not any(tmp_dir.iterdir()):
            return {"passed": True, "files": []}

        import shutil
        # Collect files before moving
        moved_files = []
        for item in sorted(tmp_dir.rglob("*")):
            if item.is_file():
                rel = item.relative_to(tmp_dir)
                moved_files.append(str(rel))

        # Atomic: remove old step dir, rename tmp → step
        if step_dir.exists():
            shutil.rmtree(str(step_dir))
        os.rename(str(tmp_dir), str(step_dir))

        return {"passed": True, "files": moved_files}

    def _draft_promote(self, token: ClaimToken) -> dict:
        """Deprecated: use _step_commit instead. Kept for backward compat."""
        # Delegate to _step_commit which uses the new .tmp → step_dir paths
        return self._step_commit(token)

    def _handle_lifecycle_retry(self, token: ClaimToken, error: str) -> None:
        """Reset step to pending so it retries with feedback injected."""
        with self._tx() as conn:
            conn.execute(
                """
                UPDATE skillflow_steps
                SET status = 'pending', version = version + 1,
                    retry_count = retry_count + 1,
                    last_error = ?, claimed_at = NULL, claimed_by = NULL,
                    inputs_json = json_set(
                        COALESCE(inputs_json, '{}'),
                        '$._feedback',
                        json(?)
                    ),
                    updated_at = datetime('now')
                WHERE id = ? AND version = ?
                """,
                (error, json.dumps({"lifecycle_error": error}),
                 token.step_instance_id, token.version),
            )
            conn.execute(
                "UPDATE skillflow_runs SET current_node = NULL, updated_at = datetime('now') WHERE id = ?",
                (token.run_id,),
            )

    def _handle_lifecycle_failure(self, token: ClaimToken, error: str) -> None:
        """Permanently fail the step due to lifecycle hook failure."""
        with self._tx() as conn:
            self._fail_step_in_tx(conn, token,
                f"Lifecycle hook failed: {error}", retryable=False)

    def _emit_lifecycle_event(self, token: ClaimToken, hook_name: str,
                               status: str, detail: str = ""):
        """Emit a lifecycle hook event to the outbox."""
        payload = {
            "run_id": token.run_id,
            "step_id": token.step_id,
            "hook": hook_name,
            "status": status,
        }
        if detail:
            payload["detail"] = detail
        with self._lock:
            self._conn.execute(
                """
                INSERT INTO skillflow_outbox (event_type, payload_json, created_at)
                VALUES ('lifecycle_hook', ?, datetime('now'))
                """,
                (self._serialize(payload),),
            )
            self._conn.commit()

    def fail_step(self, token: ClaimToken, error: str, retryable: bool = True) -> None:
        with self._tx() as conn:
            self._fail_step_in_tx(conn, token, error, retryable)

    def _fail_step_in_tx(self, conn: sqlite3.Connection, token: ClaimToken,
                         error: str, retryable: bool) -> None:
        """Fail a step within an already-open transaction."""
        step_row = conn.execute(
            "SELECT retry_count, max_retries, version FROM skillflow_steps WHERE id = ?",
            (token.step_instance_id,),
        ).fetchone()
        if not step_row:
            raise _TxRollback()

        retry_count = step_row["retry_count"]
        max_retries = step_row["max_retries"]
        current_version = step_row["version"]

        if retryable and retry_count < max_retries:
            cursor = conn.execute(
                """
                UPDATE skillflow_steps
                SET status = 'pending', version = version + 1,
                    retry_count = retry_count + 1,
                    last_error = ?, claimed_at = NULL, claimed_by = NULL,
                    updated_at = datetime('now')
                WHERE id = ? AND version = ?
                """,
                (error, token.step_instance_id, current_version),
            )
            if cursor.rowcount == 0:
                raise StepVersionConflict(
                    f"Step instance {token.step_instance_id} version mismatch in fail_step"
                )
            conn.execute(
                "UPDATE skillflow_runs SET current_node = NULL, updated_at = datetime('now') WHERE id = ?",
                (token.run_id,),
            )
            conn.execute(
                """
                INSERT INTO skillflow_outbox (event_type, payload_json, created_at)
                VALUES ('step_failed', ?, datetime('now'))
                """,
                (self._serialize({
                    "run_id": token.run_id, "step_id": token.step_id,
                    "step_instance_id": token.step_instance_id,
                    "error": error, "retryable": True, "retry_count": retry_count + 1,
                }),),
            )
            return

        # Retries exhausted
        resolver = self._get_resolver_for_run(token.run_id)
        error_handler = resolver.find_error_transition(token.step_id)

        if error_handler:
            conn.execute(
                """
                UPDATE skillflow_steps
                SET status = 'failed', version = version + 1,
                    last_error = ?, claimed_at = NULL, claimed_by = NULL,
                    updated_at = datetime('now')
                WHERE id = ? AND version = ?
                """,
                (error, token.step_instance_id, current_version),
            )
            error_context = {
                "_error": {
                    "source_step": token.step_id,
                    "error_type": "MaxRetriesExceeded",
                    "error_message": error,
                    "retry_count": retry_count,
                }
            }
            conn.execute(
                """
                UPDATE skillflow_steps
                SET inputs_json = ?, updated_at = datetime('now')
                WHERE run_id = ? AND step_id = ? AND status = 'pending'
                """,
                (self._serialize(error_context), token.run_id, error_handler),
            )
            conn.execute(
                "UPDATE skillflow_runs SET current_node = ?, updated_at = datetime('now') WHERE id = ?",
                (error_handler, token.run_id),
            )
            conn.execute(
                """
                INSERT INTO skillflow_outbox (event_type, payload_json, created_at)
                VALUES ('step_failed', ?, datetime('now'))
                """,
                (self._serialize({
                    "run_id": token.run_id, "step_id": token.step_id,
                    "step_instance_id": token.step_instance_id,
                    "error": error, "retryable": False, "routed_to": error_handler,
                }),),
            )
            # If the failed step had a checkpoint, emit a checkpoint-skipped event
            node = resolver.get_node(token.step_id)
            if node and node.checkpoint:
                conn.execute(
                    "INSERT INTO skillflow_outbox (event_type, payload_json, created_at) "
                    "VALUES ('checkpoint_skipped', ?, datetime('now'))",
                    (self._serialize({
                        "run_id": token.run_id,
                        "step_id": token.step_id,
                        "step_label": node.checkpoint_label or node.name or token.step_id,
                        "error": error,
                        "routed_to": error_handler,
                    }),),
                )
        else:
            conn.execute(
                """
                UPDATE skillflow_steps
                SET status = 'failed', version = version + 1,
                    last_error = ?, claimed_at = NULL, claimed_by = NULL,
                    updated_at = datetime('now')
                WHERE id = ? AND version = ?
                """,
                (error, token.step_instance_id, current_version),
            )
            self._fail_run_in_tx(conn, token.run_id, error)

    # ── Tool node helpers ───────────────────────────────────────────

    def _execute_tool_inline(self, tool_node: StepNode, *,
                              run_id: str = "",
                              graph_name: str = "") -> dict:
        """Execute a tool node synchronously and return the result dict.

        Auto-injects context fields so tools like ``notify`` can enrich
        messages without the agent passing them explicitly.
        """
        if self._tool_loader is None:
            raise SkillFlowError(
                f"Cannot execute tool node '{tool_node.id}': "
                "no ToolLoader configured on SkillFlow"
            )
        fn = self._tool_loader.load_fn(tool_node.tool_name)
        kwargs = dict(tool_node.tool_params)
        kwargs.setdefault("workspace_root", "")
        kwargs.setdefault("project_root", "")
        # Auto-inject context
        kwargs.setdefault("run_id", run_id)
        kwargs.setdefault("step_id", tool_node.id)
        kwargs.setdefault("config_name", graph_name)
        kwargs.setdefault("step_name", tool_node.tool_name or tool_node.agent_config or tool_node.id)
        kwargs.setdefault("step_type", tool_node.step_type)
        # Resolve $STEP_DRAFT_DIR etc. via workspace
        if self._workspace and run_id:
            try:
                row = self._conn.execute(
                    "SELECT project_id FROM skillflow_runs WHERE id = ?", (run_id,)
                ).fetchone()
                if row and row["project_id"]:
                    pid = row["project_id"]
                    kwargs = self._workspace.resolve_variables(
                        pid, graph_name, tool_node.id, kwargs
                    )
                    # Auto-resolve project_root from workspace
                    kwargs.setdefault("project_root",
                                      str(self._workspace.projects_base / pid))
            except Exception:
                pass  # variable resolution is best-effort
        result = fn(**kwargs)
        if not isinstance(result, dict):
            result = {"output": result}
        return result

    def _confirm_tool_in_tx(self, conn, run_id: str, step_id: str,
                            result: dict) -> None:
        """Confirm a tool node execution in the database."""
        # Create step instance if not exists
        step_row = conn.execute(
            "SELECT id, version FROM skillflow_steps WHERE run_id = ? AND step_id = ? ORDER BY id DESC LIMIT 1",
            (run_id, step_id),
        ).fetchone()
        if not step_row:
            conn.execute(
                """
                INSERT INTO skillflow_steps (run_id, step_id, step_config_json, status, version,
                    inputs_json, outputs_json, result_flags_json, created_at, updated_at)
                VALUES (?, ?, '{}', 'completed', 1, '{}', ?, ?, datetime('now'), datetime('now'))
                """,
                (run_id, step_id, self._serialize(result), self._serialize(result)),
            )
        else:
            conn.execute(
                """
                UPDATE skillflow_steps
                SET status = 'completed', version = version + 1,
                    outputs_json = ?, result_flags_json = ?,
                    completed_at = datetime('now'), updated_at = datetime('now')
                WHERE id = ? AND version = ?
                """,
                (self._serialize(result), self._serialize(result),
                 step_row["id"], step_row["version"]),
            )
        conn.execute(
            """
            INSERT INTO skillflow_outbox (event_type, payload_json, created_at)
            VALUES ('step_completed', ?, datetime('now'))
            """,
            (self._serialize({
                "run_id": run_id, "step_id": step_id,
                "step_instance_id": step_row["id"] if step_row else None,
            }),),
        )

    def _inject_feedback_in_tx(self, conn, run_id: str, target_step_id: str,
                               feedback: dict) -> None:
        """Inject feedback into a pending step's inputs for the target."""
        conn.execute(
            """
            UPDATE skillflow_steps
            SET inputs_json = json_set(inputs_json, '$._feedback', ?),
                updated_at = datetime('now')
            WHERE run_id = ? AND step_id = ? AND status = 'pending'
            """,
            (self._serialize(feedback), run_id, target_step_id),
        )

    # ── Graph traversal ─────────────────────────────────────────────

    def _resolve_loop(self, conn, run: dict, resolver, loop_step_id: str) -> str | None:
        """Resolve a loop step to either its body or done transition.

        On first encounter: reads the source file, extracts the list,
        initializes loop state, routes to body (or done if empty).
        On subsequent encounters: increments index, routes to body or done.
        """
        node = resolver.get_node(loop_step_id)
        if not node or not node.loop:
            return None

        loop_cfg = node.loop
        pid = run["project_id"]
        gname = run["graph_name"]

        # Identify body vs done transitions.
        # Convention: first transition with a target is the body;
        # any other transition is an exit/done path.
        body_target: str | None = None
        for t in node.transitions:
            if t.to:
                body_target = t.to
                break

        # Read or update loop state
        row = conn.execute(
            "SELECT current_index, items_json FROM skillflow_loop_state "
            "WHERE run_id = ? AND loop_step_id = ?",
            (run["id"], loop_step_id),
        ).fetchone()

        if row is None:
            # First time: read source file and init state
            source = loop_cfg.source
            source_step = source.get("step", "")
            source_file = source.get("file", "")
            source_field = source.get("field", "")

            if not self._workspace:
                return None
            step_dir = self._workspace.get_step_dir(pid, gname, source_step)
            file_path = step_dir / source_file
            if not file_path.exists():
                # Try legacy Outbox_Final path
                step_dir = self._workspace.get_final_dir(pid, gname, source_step)
                file_path = step_dir / source_file
            if not file_path.exists():
                # Source file missing — emit warning, treat as empty → done
                conn.execute(
                    "INSERT INTO skillflow_outbox (event_type, payload_json, created_at) "
                    "VALUES ('loop_source_missing', ?, datetime('now'))",
                    (self._serialize({
                        "run_id": run["id"], "loop_step_id": loop_step_id,
                        "source_step": source_step, "source_file": source_file,
                    }),),
                )
                items = []
            else:
                try:
                    import json
                    data = json.loads(file_path.read_text(encoding="utf-8"))
                    items = data.get(source_field, [])
                    if not isinstance(items, list):
                        items = []
                except Exception:
                    items = []

            # Flatten if items are lists (execution_order is list of lists)
            if items and isinstance(items[0], list):
                flat: list = []
                for group in items:
                    if isinstance(group, list):
                        flat.extend(group)
                    else:
                        flat.append(group)
                items = flat

            if not items:
                # Empty list → mark loop step completed, route to done transition
                conn.execute(
                    "UPDATE skillflow_steps SET status = 'completed', "
                    "completed_at = datetime('now'), updated_at = datetime('now') "
                    "WHERE run_id = ? AND step_id = ? AND status = 'pending'",
                    (run["id"], loop_step_id),
                )
                for t in node.transitions:
                    if t.to and t.to != body_target:
                        return t.to
                return None

            conn.execute(
                "INSERT INTO skillflow_loop_state (run_id, loop_step_id, current_index, "
                "items_json, item_context_key) VALUES (?, ?, 0, ?, ?)",
                (run["id"], loop_step_id, self._serialize(items),
                 loop_cfg.item_as or "loop_item"),
            )
            current_idx = 0
        else:
            current_idx = row["current_index"] + 1
            items = self._deserialize(row["items_json"])
            conn.execute(
                "UPDATE skillflow_loop_state SET current_index = ?, "
                "updated_at = datetime('now') WHERE run_id = ? AND loop_step_id = ?",
                (current_idx, run["id"], loop_step_id),
            )

        if current_idx >= len(items):
            # All items done → mark loop step completed, route to done transition
            conn.execute(
                "UPDATE skillflow_steps SET status = 'completed', "
                "completed_at = datetime('now'), updated_at = datetime('now') "
                "WHERE run_id = ? AND step_id = ? AND status = 'pending'",
                (run["id"], loop_step_id),
            )
            for t in node.transitions:
                if t.to and t.to != body_target:
                    return t.to
            return None

        # Route to body
        return body_target

    def _resolve_next_in_tx(self, conn, run_id: str, step_id: str,
                            flags: dict, resolver) -> str | None:
        """Resolve the immediate next step from transitions, within a transaction.

        Returns the next node ID, or None to let advance_run handle the full
        resolution (checkpoints, gates, loops, max_loop tracking).

        Only resolves simple agent→agent transitions:
        - No checkpoint steps (need user approval)
        - No gate or loop targets (need edge count / iteration tracking)
        - No checkpoint-guarded transitions
        """
        node = resolver.get_node(step_id)
        if not node or not node.transitions:
            return None

        if node.checkpoint:
            return None

        run = conn.execute(
            "SELECT project_id, graph_name FROM skillflow_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        fr = self._make_file_reader(
            run["project_id"], run["graph_name"], step_id
        ) if run else None

        from skillflow.graph import _flags_match
        for t in node.transitions:
            if t.match is not None:
                if t.match.get("from") == "checkpoint":
                    continue
                if not _flags_match(t.match, flags, file_reader=fr):
                    continue
            # Don't resolve to gates, loops, or native tools — advance_run handles them
            skip_tool = False
            if resolver.is_tool(t.to):
                tool_node = resolver.get_node(t.to)
                if tool_node and not self._should_delegate_tool(tool_node.tool_name):
                    skip_tool = True
            if resolver.is_gate(t.to) or resolver.is_loop(t.to) or skip_tool:
                return None
            return t.to
        return None

    def _make_file_reader(self, project_id: str, graph_name: str,
                          step_id: str) -> callable | None:
        """Return a callable for resolving from_file match conditions.

        Reads from the step's promoted output directory ({step_id}/)
        where _step_commit has atomically moved validated outputs.
        """
        if not self._workspace:
            return None
        step_dir = self._workspace.get_step_dir(project_id, graph_name, step_id)
        def read(path: str) -> str:
            f = step_dir / path
            if not f.exists():
                raise FileNotFoundError(f"Output file not found: {path}")
            return f.read_text(encoding="utf-8")
        return read

    def advance_run(self, run_id: str) -> str | None:
        # Recover stale claims before any traversal
        self.recover_stale_claims(self._stale_threshold)

        resolver = self._get_resolver_for_run(run_id)
        with self._tx() as conn:
            run = conn.execute(
                "SELECT * FROM skillflow_runs WHERE id = ?", (run_id,)
            ).fetchone()
            if not run:
                return None
            if run["status"] in ("completed", "failed", "paused"):
                return None

            if run["current_node"]:
                claimed = conn.execute(
                    "SELECT 1 FROM skillflow_steps WHERE run_id = ? AND status = 'claimed' LIMIT 1",
                    (run_id,),
                ).fetchone()
                if claimed:
                    return None
                # If current_node is a loop step, resolve its iteration
                current = run["current_node"]
                if resolver.is_loop(current):
                    current = self._resolve_loop(conn, run, resolver, current)
                    if current is None:
                        return None
                    conn.execute(
                        "UPDATE skillflow_runs SET current_node = ?, updated_at = datetime('now') WHERE id = ?",
                        (current, run_id),
                    )
                    return current

                # If current_node is a gate, resolve through it
                if resolver.is_gate(current):
                    gate_depth = 0
                    edge_counts = self._read_edge_counts(conn, run_id)
                    # Merge flags from all completed steps for gate resolution
                    all_rows = conn.execute(
                        "SELECT result_flags_json FROM skillflow_steps "
                        "WHERE run_id = ? AND status = 'completed'",
                        (run_id,),
                    ).fetchall()
                    flags: dict = {}
                    for row in all_rows:
                        flags.update(self._deserialize(row["result_flags_json"]))
                    while resolver.is_gate(current) and gate_depth < 1000:
                        gate_depth += 1
                        matched = resolver.resolve_gate_transitions(current, flags, edge_counts)
                        if matched is None:
                            self._fail_run_in_tx(conn, run_id, f"Gate '{current}': no matching transition")
                            return None
                        current = matched
                    if gate_depth >= 1000:
                        self._fail_run_in_tx(conn, run_id, "Gate resolution exceeded 1000 iterations")
                        return None
                    conn.execute(
                        "UPDATE skillflow_runs SET current_node = ?, updated_at = datetime('now') WHERE id = ?",
                        (current, run_id),
                    )
                    return current

                # If current_node is a tool, auto-execute it inline
                # (unless runner mode + non-native — then delegate to agent)
                if resolver.is_tool(current):
                    tool_node = resolver.get_node(current)
                    if tool_node and self._should_delegate_tool(tool_node.tool_name):
                        return current  # agent claims and executes the tool
                    tool_result = self._execute_tool_inline(
                        tool_node, run_id=run_id,
                        graph_name=run["graph_name"])
                    self._confirm_tool_in_tx(conn, run_id, current, tool_result)
                    step_flags = tool_result
                    fr = self._make_file_reader(
                        run["project_id"], run["graph_name"], current)
                    edge_counts = self._read_edge_counts(conn, run_id)
                    try:
                        t, target = resolver.resolve_transition(
                            current, step_flags, edge_counts, file_reader=fr)
                    except CycleLimitExceeded:
                        self._fail_run_in_tx(conn, run_id, "Cycle limit exceeded")
                        return None
                    if t and t.feedback and t.to:
                        error_str = tool_result.get("error", "Tool failed")
                        self._inject_feedback_in_tx(
                            conn, run_id, t.to, error_str)
                    if target:
                        # Check end conditions before returning the target
                        ec = resolver.graph.end_conditions
                        if ec and ec.conditions:
                            end_result = self._evaluate_end_conditions(
                                conn, run_id, ec, target
                            )
                            if end_result:
                                if end_result.status == "completed":
                                    self._complete_run_in_tx(conn, run_id, end_result.reason)
                                else:
                                    self._fail_run_in_tx(conn, run_id, end_result.reason)
                                return None
                        conn.execute(
                            "UPDATE skillflow_runs SET current_node = ?, updated_at = datetime('now') WHERE id = ?",
                            (target, run_id),
                        )
                        return target
                    return None

                # Check end conditions when current_node was pre-resolved
                # (e.g., by confirm_step inline transition resolution)
                ec = resolver.graph.end_conditions
                if ec and ec.conditions:
                    end_result = self._evaluate_end_conditions(
                        conn, run_id, ec, run["current_node"]
                    )
                    if end_result:
                        if end_result.status == "completed":
                            self._complete_run_in_tx(conn, run_id, end_result.reason)
                        else:
                            self._fail_run_in_tx(conn, run_id, end_result.reason)
                        return None
                return run["current_node"]

            claimed = conn.execute(
                "SELECT 1 FROM skillflow_steps WHERE run_id = ? AND status = 'claimed' LIMIT 1",
                (run_id,),
            ).fetchone()
            if claimed:
                return None

            last = conn.execute(
                """
                SELECT step_id, result_flags_json FROM skillflow_steps
                WHERE run_id = ? AND status = 'completed'
                ORDER BY id DESC LIMIT 1
                """,
                (run_id,),
            ).fetchone()

            edges_taken: list[tuple[str, str]] = []
            fr = self._make_file_reader(
                run["project_id"], run["graph_name"],
                last["step_id"] if last else "")
            if last is None:
                next_node = resolver.begin_node()
            else:
                flags = self._deserialize(last["result_flags_json"])
                edge_counts = self._read_edge_counts(conn, run_id)
                try:
                    first_target = resolver.next_node(last["step_id"], flags,
                                                       edge_counts, file_reader=fr)
                except CycleLimitExceeded:
                    self._fail_run_in_tx(conn, run_id, "Cycle limit exceeded")
                    return None
                if first_target is None:
                    # Check if this is a checkpoint step whose transition requires
                    # checkpoint approval. If so, pause instead of failing.
                    last_node = resolver.get_node(last["step_id"])
                    if last_node and last_node.checkpoint:
                        # Find the first checkpoint-guarded transition as the pending target
                        for t in last_node.transitions:
                            if t.match and t.match.get("from") == "checkpoint":
                                first_target = t.to
                                break
                    if first_target is None:
                        self._fail_run_in_tx(
                            conn, run_id,
                            f"No matching transition from '{last['step_id']}' with flags {flags}"
                        )
                        return None
                    # Fall through — first_target set from checkpoint transition
                edges_taken.append((last["step_id"], first_target))
                next_node = first_target

            # Checkpoint — pause BEFORE auto-advancing through gates/tools
            if last:
                last_node = resolver.get_node(last["step_id"])
                if last_node and last_node.checkpoint:
                    conn.execute(
                        "UPDATE skillflow_runs SET current_node = ?, updated_at = datetime('now') WHERE id = ?",
                        (next_node, run_id),
                    )
                    conn.execute(
                        "UPDATE skillflow_runs SET status = 'paused', updated_at = datetime('now') WHERE id = ?",
                        (run_id,),
                    )
                    return None

            # Auto-advance through gates AND auto-execute tool nodes
            # Merge flags from ALL completed steps so gates see flags
            # produced by earlier steps (e.g. task_gate needs step 3's
            # has_tasks, even though the last step is a _review step).
            all_completed = conn.execute(
                "SELECT step_id, result_flags_json FROM skillflow_steps "
                "WHERE run_id = ? AND status = 'completed'",
                (run_id,),
            ).fetchall()
            last_flags_for_gate: dict = {}
            for cs in all_completed:
                last_flags_for_gate.update(
                    self._deserialize(cs["result_flags_json"]))
            gate_depth = 0
            while gate_depth < 1000:
                if resolver.is_gate(next_node):
                    gate_depth += 1
                    edge_counts = self._read_edge_counts(conn, run_id)
                    matched = resolver.resolve_gate_transitions(
                        next_node, last_flags_for_gate, edge_counts, file_reader=fr)
                    if matched is None:
                        self._fail_run_in_tx(conn, run_id, f"Gate '{next_node}': no matching transition")
                        return None
                    edges_taken.append((next_node, matched))
                    next_node = matched
                elif resolver.is_tool(next_node):
                    tool_node = resolver.get_node(next_node)
                    if tool_node and self._should_delegate_tool(tool_node.tool_name):
                        break  # return the tool node for the agent
                    tool_result = self._execute_tool_inline(
                        tool_node, run_id=run_id,
                        graph_name=run["graph_name"])
                    self._confirm_tool_in_tx(conn, run_id, next_node, tool_result)
                    step_flags = tool_result
                    try:
                        t, target = resolver.resolve_transition(
                            next_node, step_flags, edge_counts, file_reader=fr)
                    except CycleLimitExceeded:
                        self._fail_run_in_tx(conn, run_id, "Cycle limit exceeded")
                        return None
                    if t and t.feedback and t.to:
                        # Inject tool error output into target step for retry context
                        self._inject_feedback_in_tx(conn, run_id, t.to, tool_result)
                    if target is None:
                        self._fail_run_in_tx(conn, run_id, f"Tool '{next_node}': no matching transition")
                        return None
                    edges_taken.append((next_node, target))
                    next_node = target
                    last_flags_for_gate.update(step_flags)
                elif resolver.is_loop(next_node):
                    resolved = self._resolve_loop(conn, run, resolver, next_node)
                    if resolved is None:
                        self._fail_run_in_tx(conn, run_id, f"Loop '{next_node}': failed to resolve")
                        return None
                    edges_taken.append((next_node, resolved))
                    next_node = resolved
                else:
                    break  # Agent node — needs external runner

            if gate_depth >= 1000:
                self._fail_run_in_tx(conn, run_id, "Gate/tool resolution exceeded 1000 iterations")
                return None

            # Increment edge counts for all traversed transitions
            for from_step, to_step in edges_taken:
                conn.execute(
                    """
                    INSERT INTO skillflow_edge_counts (run_id, from_step, to_step, count, max_loop)
                    VALUES (?, ?, ?, 1, NULL)
                    ON CONFLICT(run_id, from_step, to_step)
                    DO UPDATE SET count = count + 1
                    """,
                    (run_id, from_step, to_step),
                )

            # End conditions
            ec = resolver.graph.end_conditions
            if ec and ec.conditions:
                end_result = self._evaluate_end_conditions(conn, run_id, ec, next_node)
                if end_result:
                    if end_result.status == "completed":
                        self._complete_run_in_tx(conn, run_id, end_result.reason)
                    else:
                        self._fail_run_in_tx(conn, run_id, end_result.reason)
                    return None

            conn.execute(
                "UPDATE skillflow_runs SET current_node = ?, updated_at = datetime('now') WHERE id = ?",
                (next_node, run_id),
            )
            return next_node

    def reject_checkpoint(self, run_id: str, step_id: str, feedback: str,
                          redirect_to: str = "") -> None:
        with self._tx() as conn:
            run = conn.execute(
                "SELECT * FROM skillflow_runs WHERE id = ?", (run_id,)
            ).fetchone()
            if not run or run["status"] != "paused":
                raise SkillFlowError(f"Run '{run_id}' is not paused")

            step_row = conn.execute(
                "SELECT id, version FROM skillflow_steps WHERE run_id = ? AND step_id = ? AND status = 'completed'",
                (run_id, step_id),
            ).fetchone()
            if not step_row:
                raise SkillFlowError(f"Step '{step_id}' not found in completed status")

            conn.execute(
                """
                UPDATE skillflow_steps
                SET status = 'pending', version = version + 1,
                    updated_at = datetime('now')
                WHERE id = ? AND version = ?
                """,
                (step_row["id"], step_row["version"]),
            )
            conn.execute(
                """
                UPDATE skillflow_steps
                SET inputs_json = json_set(inputs_json, '$._rejection', ?),
                    updated_at = datetime('now')
                WHERE id = ?
                """,
                (feedback, step_row["id"]),
            )
            conn.execute(
                "UPDATE skillflow_runs SET current_node = ?, status = 'running', updated_at = datetime('now') WHERE id = ?",
                (redirect_to or step_id, run_id),
            )
            # When redirecting, inject feedback into the redirect target
            if redirect_to:
                conn.execute(
                    """
                    UPDATE skillflow_steps
                    SET inputs_json = json_set(inputs_json, '$._feedback', ?),
                        updated_at = datetime('now')
                    WHERE run_id = ? AND step_id = ? AND status = 'pending'
                    """,
                    (feedback, run_id, redirect_to),
                )
            conn.execute(
                """
                INSERT INTO skillflow_outbox (event_type, payload_json, created_at)
                VALUES ('step_checkpoint_rejected', ?, datetime('now'))
                """,
                (self._serialize({"run_id": run_id, "step_id": step_id}),),
            )

    # ── Recovery ──────────────────────────────────────────────────

    def recover_stale_claims(self, stale_threshold_seconds: float = 300) -> list[str]:
        threshold = time.strftime(
            "%Y-%m-%dT%H:%M:%SZ",
            time.gmtime(time.time() - stale_threshold_seconds),
        )
        with self._tx() as conn:
            stale = conn.execute(
                """
                SELECT id, run_id, step_id FROM skillflow_steps
                WHERE status = 'claimed' AND claimed_at < ?
                """,
                (threshold,),
            ).fetchall()
            run_ids: set[str] = set()
            for row in stale:
                conn.execute(
                    """
                    UPDATE skillflow_steps
                    SET status = 'pending', version = version + 1,
                        claimed_at = NULL, claimed_by = NULL,
                        updated_at = datetime('now')
                    WHERE id = ?
                    """,
                    (row["id"],),
                )
                # Keep current_node — the step was claimed but the worker
                # died before confirm.  advance_run will re-claim the same step.

                run_ids.add(row["run_id"])
            if stale:
                conn.execute(
                    """
                    INSERT INTO skillflow_outbox (event_type, payload_json, created_at)
                    VALUES ('stale_claims_recovered', ?, datetime('now'))
                    """,
                    (self._serialize({"count": len(stale), "run_ids": list(run_ids)}),),
                )
            return list(run_ids)

    # ── Outbox ────────────────────────────────────────────────────

    def drain_outbox(self, batch_size: int = 100) -> list[OutboxEvent]:
        with self._tx() as conn:
            rows = conn.execute(
                """
                SELECT id, event_type, payload_json, stream_target FROM skillflow_outbox
                WHERE status = 'pending'
                ORDER BY id ASC LIMIT ?
                """,
                (batch_size,),
            ).fetchall()
            events = []
            for row in rows:
                conn.execute(
                    "UPDATE skillflow_outbox SET status = 'draining', drain_started_at = datetime('now') WHERE id = ?",
                    (row["id"],),
                )
                events.append(OutboxEvent(
                    id=row["id"], event_type=row["event_type"],
                    payload_json=row["payload_json"],
                    stream_target=row["stream_target"],
                ))
            return events

    def ack_outbox(self, event_ids: list[int]) -> None:
        if not event_ids:
            return
        with self._tx() as conn:
            placeholders = ",".join("?" * len(event_ids))
            conn.execute(
                f"UPDATE skillflow_outbox SET status = 'delivered' WHERE id IN ({placeholders})",
                event_ids,
            )

    def _get_project_id(self, run_id: str) -> str:
        row = self._conn.execute(
            "SELECT project_id FROM skillflow_runs WHERE id = ?", (run_id,)
        ).fetchone()
        return row["project_id"] if row else ""

    def _get_graph_name(self, run_id: str) -> str:
        row = self._conn.execute(
            "SELECT graph_name FROM skillflow_runs WHERE id = ?", (run_id,)
        ).fetchone()
        return row["graph_name"] if row else ""

    # ── Host tool execution API ─────────────────────────────────────

    def execute_tool(self, name: str, params: dict, *,
                     run_id: str = "", step_id: str = "",
                     project_root: str = "") -> dict:
        """Execute a tool on behalf of the host's agent loop.

        Resolves the allowed tool list from the graph node internally.
        Write tools write to the skillflow-managed draft directory.
        Read/exploration tools receive ``project_root`` as their workspace.
        """
        if self._tool_loader is None:
            return {"error": "No ToolLoader configured"}

        # Resolve graph node for allowlist + output.fixed
        node = None
        if run_id and step_id:
            try:
                node = self._get_resolver_for_run(run_id).get_node(step_id)
            except Exception:
                pass

        # Build allowed tool set from agent config + write tool schemas
        allowed: set[str] = set()
        if node:
            if node.agent_config and node.agent_config in self.agent_registry:
                ac = self.agent_registry.get(node.agent_config)
                if ac:
                    allowed.update(ac.tools)
            if node.output_mode and node.output_fixed:
                from skillflow.write_tools import generate_write_tool_schemas
                for ws in generate_write_tool_schemas(node.output_mode, node.output_fixed):
                    allowed.add(ws["name"])

        if allowed and name not in allowed:
            return {"error": f"Tool '{name}' not allowed. Allowed: {sorted(allowed)}"}

        fixed = node.output_fixed if node else {}

        # Write/create/append tools — write to step tmp directory (atomic staging)
        if name.startswith("write_") or name.startswith("create_") or name.startswith("append_"):
            if not self._workspace:
                return {"error": "No workspace configured for write tool"}
            pid = self._get_project_id(run_id)
            gname = self._get_graph_name(run_id)
            tmp_dir = self._workspace.get_step_tmp_dir(pid, gname, step_id)
            from skillflow.write_tools import execute_write, execute_create, execute_append
            slot = name[name.index("_") + 1:]  # everything after first _
            if name.startswith("create_"):
                return execute_create(slot, fixed, params, str(tmp_dir))
            elif name.startswith("append_"):
                return execute_append(slot, fixed, params, str(tmp_dir))
            else:
                return execute_write(slot, fixed, params, str(tmp_dir))

        if name == "write":
            if not self._workspace:
                return {"error": "No workspace configured for write tool"}
            pid = self._get_project_id(run_id)
            gname = self._get_graph_name(run_id)
            tmp_dir = self._workspace.get_step_tmp_dir(pid, gname, step_id)
            from skillflow.write_tools import execute_generic_write
            return execute_generic_write(params, str(tmp_dir))

        # Read/exploration/validation tools via ToolLoader
        fn = self._tool_loader.load_fn(name)
        kwargs = dict(params)
        kwargs.setdefault("workspace_root", project_root or "")
        kwargs.setdefault("project_root", project_root or "")
        # Filter kwargs to only what the function accepts
        import inspect as _inspect
        try:
            sig = _inspect.signature(fn)
            kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}
        except (ValueError, TypeError):
            pass
        result = fn(**kwargs)
        return result if isinstance(result, dict) else {"output": result}

    def _read_edge_counts(self, conn: sqlite3.Connection, run_id: str) -> dict[tuple[str, str], int]:
        result: dict[tuple[str, str], int] = {}
        for er in conn.execute(
            "SELECT from_step, to_step, count FROM skillflow_edge_counts WHERE run_id = ?",
            (run_id,),
        ).fetchall():
            result[(er["from_step"], er["to_step"])] = er["count"]
        return result

    # ── Internal helpers ───────────────────────────────────────────

    def _evaluate_end_conditions(self, conn: sqlite3.Connection, run_id: str,
                                  ec: EndConditions, next_node: str) -> EndResult | None:
        results: list[EndResult] = []
        for cond in ec.conditions:
            if cond.type == "node_reached":
                if next_node == cond.node:
                    if cond.require_completed:
                        step_row = conn.execute(
                            "SELECT status FROM skillflow_steps "
                            "WHERE run_id = ? AND step_id = ?",
                            (run_id, cond.node),
                        ).fetchone()
                        if not step_row or step_row["status"] != "completed":
                            continue  # step hasn't executed yet, skip
                    results.append(EndResult(status=cond.result, reason=f"Node '{cond.node}' reached"))
            elif cond.type == "max_total_steps":
                total = conn.execute(
                    "SELECT COUNT(*) as cnt FROM skillflow_steps WHERE run_id = ? AND status IN ('completed', 'failed')",
                    (run_id,),
                ).fetchone()
                if total and total["cnt"] >= cond.limit:
                    results.append(EndResult(status="failed", reason=f"Max total steps ({cond.limit}) exceeded"))
            elif cond.type == "max_run_duration_seconds":
                run = conn.execute(
                    "SELECT started_at FROM skillflow_runs WHERE id = ?", (run_id,)
                ).fetchone()
                if run and run["started_at"]:
                    try:
                        import datetime as dt
                        started_dt = dt.datetime.strptime(run["started_at"], "%Y-%m-%dT%H:%M:%S")
                        elapsed = (dt.datetime.utcnow() - started_dt).total_seconds()
                        if elapsed >= cond.limit:
                            results.append(EndResult(status="failed", reason=f"Max run duration ({cond.limit}s) exceeded"))
                    except (ValueError, OverflowError):
                        pass
            elif cond.type == "flag_match":
                last = conn.execute(
                    """
                    SELECT result_flags_json FROM skillflow_steps
                    WHERE run_id = ? AND status = 'completed'
                    ORDER BY completed_at DESC LIMIT 1
                    """,
                    (run_id,),
                ).fetchone()
                if last:
                    flags = self._deserialize(last["result_flags_json"])
                    if _flags_match(cond.flag, flags):
                        results.append(EndResult(status="failed", reason=f"Flag match: {cond.flag}"))
        if not results:
            return None
        if ec.combinator == "or":
            return results[0]
        else:
            return results[0] if len(results) == len(ec.conditions) else None

    def _fail_run_in_tx(self, conn: sqlite3.Connection, run_id: str, reason: str):
        conn.execute(
            """
            UPDATE skillflow_runs SET status = 'failed', error_reason = ?,
                completed_at = datetime('now'), updated_at = datetime('now')
            WHERE id = ?
            """,
            (reason, run_id),
        )
        conn.execute(
            """
            INSERT INTO skillflow_outbox (event_type, payload_json, created_at)
            VALUES ('run_failed', ?, datetime('now'))
            """,
            (self._serialize({"run_id": run_id, "reason": reason}),),
        )

    def _complete_run_in_tx(self, conn: sqlite3.Connection, run_id: str, reason: str):
        conn.execute(
            """
            UPDATE skillflow_runs SET status = 'completed',
                completed_at = datetime('now'), updated_at = datetime('now')
            WHERE id = ?
            """,
            (run_id,),
        )
        conn.execute(
            """
            INSERT INTO skillflow_outbox (event_type, payload_json, created_at)
            VALUES ('run_completed', ?, datetime('now'))
            """,
            (self._serialize({"run_id": run_id, "reason": reason}),),
        )


def _flags_match(match: dict, flags: dict) -> bool:
    for key, expected in match.items():
        if key not in flags:
            return False
        if flags[key] != expected:
            return False
    return True
