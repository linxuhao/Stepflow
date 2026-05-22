"""SkillTool — stateful callable wrapping a skillflow pipeline as an agent tool.

Usage::

    sf = SkillFlow(":memory:", tool_loader=loader)
    tool = SkillTool(sf, "skill_review")
    tool.register_agent("analyst", model="...", tools=["read_file", "grep"])

    # Agent loop:
    resp = tool(action="next")
    while resp.status == "in_progress":
        # agent does work...
        resp = tool(action="submit", result={"findings": [...]})
    # resp.status == "completed" or "failed"
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from skillflow.core import SkillFlow, StepResult, ClaimedStep


@dataclass
class SkillResponse:
    """Returned by SkillTool to instruct the agent what to do next."""

    status: str  # "in_progress" | "paused" | "completed" | "failed"
    run_id: str = ""
    step: str = ""
    instruction: str = ""
    tools: dict[str, dict] = field(default_factory=dict)
    # Tool node delegation (when SkillFlow.delegate_tools_to_agent is True)
    tool_name: str = ""
    tool_params: dict = field(default_factory=dict)
    checkpoint_label: str = ""
    checkpoint_reject_to: str = ""
    outputs: dict = field(default_factory=dict)
    error: str = ""
    steps_completed: int = 0
    # Output file contract — where to write fixed-output files before submit
    output_dir: str = ""
    expected_files: list[str] = field(default_factory=list)
    validation_error: str = ""


class PromptAssembler:
    """Turns a ClaimedStep into an agent-facing instruction string.

    Override ``assemble()`` to customize prompt formatting.
    """

    def assemble(self, step: ClaimedStep) -> str:
        """Build the instruction from step context and agent config."""
        parts: list[str] = []
        inputs = step.inputs

        # Human-readable step label (from snake_case id)
        label = step.step_id.replace("_", " ").title()

        # Validation error from a previous submit failure
        validation_error = inputs.get("_validation_error")
        if validation_error:
            parts.append(f"## Validation failed\n\n{validation_error}\n")

        # Feedback from a rejected checkpoint or failed lifecycle hook
        feedback = inputs.get("_feedback")
        if feedback:
            parts.append(f"## Feedback from previous attempt\n\n{feedback}\n")

        # Error context from a previous failure (error_handler routing)
        error_ctx = inputs.get("_error")
        if error_ctx:
            parts.append(f"## Previous error\n\n{error_ctx}\n")

        # Resolved context from graph context specs
        resolved = inputs.get("_resolved_context", {})
        if resolved:
            parts.append("## Context\n")
            for key, val in resolved.items():
                parts.append(f"### {key}\n\n{val}\n")

        # Agent config system prompt (if provided)
        agent_cfg = inputs.get("_agent_config", {})
        system_prompt = agent_cfg.get("config", {}).get("system_prompt", "")
        if system_prompt:
            parts.append(f"## Role\n\n{system_prompt}\n")

        # The step task itself
        parts.append(f"## Task: {label}\n\nExecute step `{step.step_id}`.")

        # Expected output files
        expected_files = inputs.get("_expected_files", [])
        output_dir = inputs.get("_output_dir", "")
        if expected_files:
            files_list = "\n".join(f"- `{f}`" for f in expected_files)
            if output_dir:
                parts.append(f"Write output files to the staging directory (`{output_dir}/`):\n{files_list}")
            else:
                parts.append(f"Write output files to the output directory:\n{files_list}")

        # Output format hints from tool schemas (write_* / create_* tools)
        tool_schemas = inputs.get("_tool_schemas", {})
        for name, schema in tool_schemas.items():
            desc = schema.get("description", "")
            if desc:
                parts.append(f"### {name}\n{desc}")

        parts.append("Produce the expected output in the format specified.")

        return "\n\n".join(parts)


class SkillTool:
    """Stateful callable that wraps a skillflow pipeline as an agent tool.

    The host agent calls this like a tool function with:
    - action="next"     — start or advance the pipeline, get next instruction
    - action="submit"   — confirm the current step with result
    - action="approve"  — approve a paused checkpoint
    - action="reject"   — reject a checkpoint with feedback

    Parameters:
        sf: Configured SkillFlow instance (with registered graphs + agent configs).
        graph_name: Name of the registered graph to run.
        prompt_assembler: Custom PromptAssembler for instruction formatting.
    """

    def __init__(self, sf: SkillFlow, graph_name: str, *,
                 prompt_assembler: PromptAssembler | None = None,
                 project_id: str | None = None):
        self.sf = sf
        self.graph_name = graph_name
        self.run_id: str | None = None
        self._current_claim: ClaimedStep | None = None
        self._assembler = prompt_assembler or PromptAssembler()
        self._project_id = project_id

    # ── public API ─────────────────────────────────────────────────

    def register_agent(self, name: str, **kwargs):
        """Register a mock/default agent config referenced by the graph."""
        self.sf.register_agent_config(name, **kwargs)

    def __call__(self, action: str = "next", step_id: str = "",
                 result: dict | None = None,
                 feedback: str = "",
                 redirect_to: str = "",
                 run_id: str = "") -> SkillResponse:
        """Execute an action and return the next instruction.

        Args:
            action: "next" | "submit" | "approve" | "reject" | "abort"
            step_id: Required for "submit" and "reject" (to identify the step).
            result: Output dict for "submit".
            feedback: Rejection reason for "reject".
            redirect_to: Step ID to redirect to on reject (optional, from SkillResponse.checkpoint_reject_to).
            run_id: Resume an existing run. If empty and action is "next", a new run is created.

        Returns:
            SkillResponse with the next instruction or termination status.
        """
        # ── Reconnect to existing run if run_id provided ──────────
        if run_id and self.run_id is None:
            run = self.sf.get_run(run_id)
            if run is None:
                return SkillResponse(status="failed", error=f"Run not found: {run_id}")
            self.run_id = run_id
            self.graph_name = run.get("graph_name", self.graph_name)
            # Reset any step claimed by a previous process and re-claim
            with self.sf._tx() as conn:
                conn.execute(
                    "UPDATE skillflow_steps SET status = 'pending', version = version + 1, "
                    "claimed_at = NULL, claimed_by = NULL, updated_at = datetime('now') "
                    "WHERE run_id = ? AND status = 'claimed'",
                    (run_id,)
                )
            self.sf.advance_run(run_id)
            self._current_claim = self.sf.claim_next_step(run_id)

        # ── Handle action ─────────────────────────────────────────
        if action == "abort":
            if self.run_id is not None:
                try:
                    self.sf.fail_run(self.run_id, "aborted by agent")
                except Exception:
                    pass
            self.run_id = None
            self._current_claim = None
            return SkillResponse(status="aborted", run_id=self.run_id or "")

        if action == "next" and self.run_id is None:
            pid = self._project_id
            # Auto-generate project_id when workspace is configured
            if pid is None and self.sf._workspace is not None:
                import uuid
                pid = f"skill-{uuid.uuid4().hex[:8]}"
            self.run_id = self.sf.create_run(self.graph_name,
                                              project_id=pid)
            self.sf.start_run(self.run_id)

        elif action == "submit" and self._current_claim is not None:
            outputs = result or {}
            flags = outputs  # result dict doubles as flags for gate resolution
            self.sf.confirm_step(
                self._current_claim.token,
                StepResult(outputs=outputs, flags=flags),
            )
            self._current_claim = None

        elif action == "approve" and self.run_id is not None:
            run = self.sf.get_run(self.run_id)
            if run and run.get("status") == "paused":
                self.sf.resume_run(self.run_id)
                self._current_claim = None

        elif action == "reject" and self.run_id is not None:
            run = self.sf.get_run(self.run_id)
            if run and run.get("status") == "paused":
                # Find the checkpoint step (last completed step)
                if not step_id:
                    steps = self.sf.get_steps(self.run_id)
                    completed = [s for s in steps if s.get("status") == "completed"]
                    if completed:
                        step_id = completed[-1]["step_id"]
                self.sf.reject_checkpoint(
                    self.run_id, step_id or "",
                    feedback or "Rejected",
                    redirect_to=redirect_to,
                )
                self._current_claim = None

        # ── Advance and respond ───────────────────────────────────
        return self._advance_and_respond()

    # ── Helpers ───────────────────────────────────────────────────

    def _make_response(self, claimed: ClaimedStep) -> SkillResponse:
        """Build a SkillResponse from a claimed step."""
        tool_name = ""
        tool_params = {}
        try:
            resolver = self.sf._get_resolver(self.graph_name)
            node = resolver.get_node(claimed.step_id)
            if node and node.step_type == "tool":
                tool_name = node.tool_name
                tool_params = dict(node.tool_params)
        except Exception:
            pass

        return SkillResponse(
            status="in_progress",
            run_id=self.run_id or "",
            step=claimed.step_id,
            instruction=self._assembler.assemble(claimed),
            tools=claimed.inputs.get("_tool_schemas", {}),
            tool_name=tool_name,
            tool_params=tool_params,
            output_dir=claimed.inputs.get("_output_dir", ""),
            expected_files=claimed.inputs.get("_expected_files", []),
            validation_error=claimed.validation_error or claimed.inputs.get("_validation_error", ""),
        )

    def write_output_files(self, step_id: str, result: dict):
        """Write output_fixed files to step tmp_dir before confirm.

        Call this before ``action="submit"`` for steps that have
        ``output.mode="content"`` with ``output.fixed`` entries.
        SkillFlow's lifecycle hooks (step_commit) will promote the
        files from tmp_dir to step_dir during confirm_step.
        """
        if not self.sf._workspace or not self.run_id:
            return
        pid = self.sf._get_project_id(self.run_id)
        gname = self.sf._get_graph_name(self.run_id)
        try:
            resolver = self.sf._get_resolver(self.graph_name)
            node = resolver.get_node(step_id)
            if not node or not node.output_fixed:
                return
        except Exception:
            return
        import json as _json
        tmp_dir = self.sf._workspace.get_step_tmp_dir(pid, gname, step_id)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        for slot, spec in node.output_fixed.items():
            fname = spec if isinstance(spec, str) else spec.get("file", f"{slot}.json")
            content = result.get(slot, "")
            if isinstance(content, (dict, list)):
                content = _json.dumps(content, indent=2)
            (tmp_dir / fname).write_text(str(content), encoding="utf-8")

    # ── internal ──────────────────────────────────────────────────

    def _advance_and_respond(self) -> SkillResponse:
        """Advance the graph and return a SkillResponse for the agent."""
        # If a step is already claimed (waiting for submit), re-return it
        if self._current_claim is not None:
            return self._make_response(self._current_claim)

        loop_guard = 0
        while loop_guard < 100:
            loop_guard += 1
            self.sf.advance_run(self.run_id)

            run = self.sf.get_run(self.run_id)
            if run is None:
                return SkillResponse(status="failed", error="Run not found", run_id=self.run_id or "")

            status = run.get("status")

            if status == "completed":
                return self._completed_response(run)
            if status == "failed":
                return SkillResponse(status="failed",
                                     error=run.get("error_reason", "Unknown error"),
                                     run_id=self.run_id or "")
            if status == "paused":
                return self._paused_response(run)

            # Running — claim the next agent step
            claimed = self.sf.claim_next_step(self.run_id)
            if claimed is None:
                # Gate/tool auto-resolved, or no pending step — retry
                continue

            self._current_claim = claimed
            return self._make_response(claimed)

        return SkillResponse(status="failed",
                             error="Advance loop exceeded 100 iterations",
                             run_id=self.run_id or "")

    def _completed_response(self, run: dict) -> SkillResponse:
        """Collect final outputs from all completed steps."""
        steps = self.sf.get_steps(self.run_id)
        completed = [s for s in steps if s.get("status") == "completed"]
        outputs: dict = {}
        for s in completed:
            import json
            try:
                out = s.get("outputs_json", "{}")
                if isinstance(out, str):
                    out = json.loads(out) if out else {}
                outputs[s["step_id"]] = out
            except (json.JSONDecodeError, TypeError):
                outputs[s["step_id"]] = {}

        return SkillResponse(
            status="completed",
            run_id=self.run_id or "",
            outputs=outputs,
            steps_completed=len(completed),
        )

    def _paused_response(self, run: dict) -> SkillResponse:
        """Build a checkpoint pause response."""
        # The checkpoint step is the last completed step, not current_node
        # (current_node points to the NEXT step, the checkpoint target)
        label = "Review"
        checkpoint_step_id = ""
        checkpoint_reject_to = ""
        try:
            steps = self.sf.get_steps(self.run_id)
            completed = [s for s in steps if s.get("status") == "completed"]
            if completed:
                last_step_id = completed[-1]["step_id"]
                resolver = self.sf._get_resolver(self.graph_name)
                node = resolver.get_node(last_step_id)
                if node and node.checkpoint:
                    checkpoint_step_id = last_step_id
                    if node.checkpoint_label:
                        label = node.checkpoint_label
                    if node.checkpoint_reject_to:
                        checkpoint_reject_to = node.checkpoint_reject_to
        except Exception:
            pass

        return SkillResponse(
            status="paused",
            run_id=self.run_id or "",
            step=checkpoint_step_id,
            checkpoint_label=label,
            checkpoint_reject_to=checkpoint_reject_to,
            instruction=(
                f"Pipeline paused at checkpoint: {label}\n"
                f"Approve to continue, or reject with feedback to request changes."
            ),
        )
