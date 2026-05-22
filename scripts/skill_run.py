#!/usr/bin/env python3
"""Stateless CLI runner for skillflow pipelines.

Each invocation is a FRESH PROCESS — the only shared state is the SQLite DB.
An LLM agent drives the pipeline by calling this command, parsing the JSON
output, acting, and calling again.

Usage:
    # Start a pipeline (pass --graph once)
    skillflow-run --graph pipeline.yaml --action start

    # All subsequent calls use --run-id (no --graph needed)
    skillflow-run --action submit --run-id <id> --result '{"key": "val"}'
    skillflow-run --action approve --run-id <id>
    skillflow-run --action reject --run-id <id> --feedback "reason"
"""

import argparse
import json
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger("skillflow-run")

# Ensure package is importable when run from repo without pip install
_repo_root = Path(__file__).parent.parent
_src = _repo_root / "src"
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

from skillflow.core import SkillFlow
from skillflow.graph import PipelineGraph
from skillflow.tool_loader import ToolLoader
from skillflow.plugins.skill_runner import SkillTool


# ── Helpers ───────────────────────────────────────────────────────────

def _register_graph_and_agents(sf: SkillFlow, graph: PipelineGraph,
                                graph_path: str) -> None:
    """Register a graph and its referenced agent configs on a SkillFlow instance."""
    for step in graph.steps:
        if step.agent_config:
            sf.register_agent_config(step.agent_config, model="host")
    sf.register_graph(graph)

    if "skill_converter" in graph_path:
        try:
            from skillflow.plugins.skill_converter.converter import (
                _register_converter_agents,
            )
            _register_converter_agents(sf)
        except ImportError:
            logger.debug("Converter agents module not available")
        except Exception:
            logger.warning("Converter agent registration failed", exc_info=True)


def _ensure_graph_loaded(sf: SkillFlow, run_id: str) -> str:
    """Reload graph from the path stored in the DB.  Returns graph_name.

    If the graph is already registered on this SkillFlow instance (e.g.
    because start registered it moments ago), this is a no-op.
    """
    run = sf.get_run(run_id)
    if not run:
        raise SystemExit(json.dumps({
            "status": "failed",
            "error": f"Run not found: {run_id}",
        }))

    graph_name = run.get("graph_name", "")
    graph_path = run.get("graph_path", "")

    # Already registered?  (e.g. start registered it in the same process)
    try:
        sf._get_resolver(graph_name)
        return graph_name
    except Exception:
        pass

    if not graph_path:
        raise SystemExit(json.dumps({
            "status": "failed",
            "error": "Run has no graph_path stored. "
                     "Recreate the run with --action start --graph <file>.",
        }))

    if not Path(graph_path).exists():
        raise SystemExit(json.dumps({
            "status": "failed",
            "error": f"Graph file not found: {graph_path}",
        }))

    graph = PipelineGraph.from_yaml(graph_path)
    _register_graph_and_agents(sf, graph, graph_path)
    return graph.name


# ── Main ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Stateless skillflow pipeline runner. "
                    "Each call is a fresh process — state lives in SQLite. "
                    "An LLM agent calls this via shell, parses the JSON response, "
                    "acts, and calls again. "
                    "Use --action start --graph once; all subsequent calls use "
                    "--run-id (the graph path is stored in the DB).",
        epilog=(
            "Examples:\n"
            "  # Start a new run (--graph only on start)\n"
            "  skillflow-run --graph pipeline.yaml --action start\n"
            "\n"
            "  # Submit the result (no --graph needed)\n"
            "  skillflow-run --action submit --run-id <id> --result '{\"key\":\"val\"}'\n"
            "\n"
            "  # Approve or reject a checkpoint (no --graph needed)\n"
            "  skillflow-run --action approve --run-id <id>\n"
            "  skillflow-run --action reject --run-id <id> --feedback \"reason\"\n"
            "\n"
            "  # Reconnect if you lose state (no --graph needed)\n"
            "  skillflow-run --action next --run-id <id>\n"
            "\n"
            "Agent loop:\n"
            "  1. start    → {status:\"in_progress\", run_id, step, instruction, tools}\n"
            "  2. submit   → {status:\"in_progress\", ...} or {status:\"paused\"}\n"
            "     - Tool steps: {status:\"in_progress\", tool_name, tool_params}\n"
            "       → execute the tool, submit result\n"
            "     - Checkpoints: {status:\"paused\", checkpoint_label}\n"
            "       → approve or reject\n"
            "  3. Repeat until status = \"completed\" or \"failed\"\n"
            "\n"
            "State persists in the SQLite DB (default: ~/.skillflow/runs.db).\n"
            "The graph path is stored in the DB at start time — no need to pass\n"
            "--graph on every call."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--graph", default="",
                        help="Pipeline YAML file. Only for --action start.")
    parser.add_argument("--db", default=os.path.expanduser("~/.skillflow/runs.db"),
                        help="SQLite DB path (default: ~/.skillflow/runs.db)")
    parser.add_argument("--action", default="next",
                        choices=["start", "next", "submit", "approve", "reject", "abort"],
                        help="Action: start (create run, --graph required), "
                             "next (reconnect and claim current step), "
                             "submit (confirm step with --result), "
                             "approve/reject (checkpoint), abort (cancel run)")
    parser.add_argument("--run-id", default="", help="Run ID from previous JSON response")
    parser.add_argument("--step-id", default="", help="Step ID from response (for approve/reject)")
    parser.add_argument("--result", default="{}",
                        help="Result JSON for submit (e.g. '{\"issues\":[]}')")
    parser.add_argument("--feedback", default="", help="Feedback message for reject")
    parser.add_argument("--redirect-to", default="",
                        help="Step ID to redirect to on reject (from checkpoint_reject_to in SkillResponse)")
    parser.add_argument("--workspace", default=os.path.expanduser("~/.skillflow/workspaces"),
                        help="Workspace base path (default: ~/.skillflow/workspaces)")
    parser.add_argument("--project-id", default="",
                        help="Project ID for workspace-scoped runs")
    args = parser.parse_args()

    # ── Validation ──────────────────────────────────────────────────
    if args.action == "start":
        if not args.graph:
            parser.error("--action start requires --graph")
        if args.run_id:
            parser.error("--action start does not accept --run-id")
    else:
        if not args.run_id:
            parser.error(f"--action {args.action} requires --run-id")
        if args.graph:
            parser.error(f"--graph is only valid with --action start")

    # Parse result JSON early
    result = {}
    try:
        result = json.loads(args.result)
    except json.JSONDecodeError:
        print(json.dumps({"status": "failed", "error": f"Invalid JSON: {args.result}"}))
        sys.exit(1)

    # Ensure DB directory exists
    db_path = Path(args.db)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    # Init SkillFlow with file DB + tool loader + workspace
    loader = ToolLoader()
    sf = SkillFlow(
        str(db_path),
        tool_loader=loader,
        delegate_tools_to_agent=True,
        workspace_base=args.workspace,
        projects_base=os.path.join(args.workspace, "projects"),
    )

    project_id = args.project_id or None

    # ── start — create run with graph_path ─────────────────────────
    if args.action == "start":
        # Auto-generate project_id when not provided (matches SkillTool behavior)
        if project_id is None:
            import uuid
            project_id = f"skill-{uuid.uuid4().hex[:8]}"
        graph = PipelineGraph.from_yaml(args.graph)
        _register_graph_and_agents(sf, graph, args.graph)
        run_id = sf.create_run(graph.name, project_id=project_id,
                               graph_path=args.graph)
        sf.start_run(run_id)
        # Use SkillTool to advance + claim the first step
        tool = SkillTool(sf, graph.name, project_id=project_id)
        resp = tool(action="next", run_id=run_id)
    else:
        # ── All other actions — reload graph from DB, then execute ──
        graph_name = _ensure_graph_loaded(sf, args.run_id)
        run = sf.get_run(args.run_id)
        pid = project_id or (run.get("project_id") if run else None)
        tool = SkillTool(sf, graph_name, project_id=pid)
        resp = tool(
            action=args.action,
            run_id=args.run_id,
            step_id=args.step_id,
            result=result if result else None,
            feedback=args.feedback,
            redirect_to=args.redirect_to,
        )

    # Output response as JSON
    output = {
        "status": resp.status,
        "run_id": resp.run_id,
        "step": resp.step,
        "instruction": resp.instruction,
        "tools": resp.tools,
        "tool_name": resp.tool_name,
        "tool_params": resp.tool_params,
        "checkpoint_label": resp.checkpoint_label,
        "checkpoint_reject_to": resp.checkpoint_reject_to,
        "outputs": resp.outputs,
        "error": resp.error,
        "steps_completed": resp.steps_completed,
        "output_dir": resp.output_dir,
        "expected_files": resp.expected_files,
        "validation_error": resp.validation_error,
    }
    print(json.dumps(output))

    if resp.status == "failed":
        sys.exit(1)


if __name__ == "__main__":
    main()
