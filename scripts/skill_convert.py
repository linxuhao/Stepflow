#!/usr/bin/env python3
"""Stateless converter CLI — calls skillflow-run with the built-in converter pipeline.

Each invocation is a FRESH PROCESS. The only shared state is the SQLite DB.
Parse the JSON response, act, call again.

Usage:
    skillflow-convert --desc "Code review skill..." --action start
    skillflow-convert --desc-file my_skill.md --action start
    skillflow-convert --action submit --run-id <id> --result '{"analysis": {...}}'
    skillflow-convert --action approve --run-id <id>
    skillflow-convert --action reject --run-id <id> --feedback "reason"
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).parent.parent
_CONVERTER_YAML = _REPO_ROOT / "src" / "skillflow" / "plugins" / "skill_converter" / "skill_converter.yaml"
_WORKSPACE = os.path.expanduser("~/.skillflow/workspaces")
_PROJECT_ID = "skill-converter"


def main():
    parser = argparse.ArgumentParser(
        description="Stateless skill-to-pipeline converter. "
                    "Each call is a fresh process — state lives in SQLite. "
                    "Wraps skillflow-run with the built-in skill_converter pipeline. "
                    "Parse the JSON response, act, call again.",
        epilog=(
            "Examples:\n"
            "  # Start a conversion\n"
            "  skillflow-convert --desc \"Code review skill...\" --action start\n"
            "  skillflow-convert --desc-file my_skill.md --action start\n"
            "\n"
            "  # Submit work (no --desc needed — run_id reconnects state)\n"
            "  skillflow-convert --action submit --run-id <id> --result '{\"analysis\":{...}}'\n"
            "\n"
            "  # Approve/reject checkpoints\n"
            "  skillflow-convert --action approve --run-id <id>\n"
            "  skillflow-convert --action reject --run-id <id> --feedback \"reason\"\n"
            "\n"
            "On completion, the generated pipeline YAML is at:\n"
            "  ~/.skillflow/workspaces/skill-converter/.../skill_pipeline.yaml\n"
            "\n"
            "This is a thin wrapper — it writes the skill description to the workspace\n"
            "then calls skillflow-run with --action start once, and --action\n"
            "submit/approve/reject for subsequent steps (no --graph needed)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--desc", default="", help="Skill description text (required for --action start)")
    parser.add_argument("--desc-file", default="", help="Path to skill description markdown file")
    parser.add_argument("--action", default="start",
                        choices=["start", "next", "submit", "approve", "reject", "abort"],
                        help="Action: start (create run), next (advance), "
                             "submit (confirm step), approve/reject (checkpoint)")
    parser.add_argument("--run-id", default="", help="Run ID from previous JSON response")
    parser.add_argument("--step-id", default="", help="Step ID from response")
    parser.add_argument("--result", default="{}", help="Result JSON for submit")
    parser.add_argument("--feedback", default="", help="Feedback message for reject")
    args = parser.parse_args()

    # Validate
    if args.action == "start":
        if args.run_id:
            parser.error("--action start does not accept --run-id")
        if not args.desc and not args.desc_file:
            parser.error("--action start requires --desc or --desc-file")
    else:
        if not args.run_id:
            parser.error(f"--action {args.action} requires --run-id")

    # On first call (start), write the skill description to workspace
    if args.action == "start":
        desc_text = args.desc
        if args.desc_file:
            desc_text = Path(args.desc_file).read_text(encoding="utf-8")
        if not desc_text.strip():
            print(json.dumps({"status": "failed", "error": "No skill description provided"}))
            sys.exit(1)

        desc_dir = Path(_WORKSPACE) / _PROJECT_ID
        desc_dir.mkdir(parents=True, exist_ok=True)
        (desc_dir / "skill_description.md").write_text(desc_text, encoding="utf-8")

    # Build skillflow-run command
    runner = str(_REPO_ROOT / "scripts" / "skill_run.py")
    cmd = [
        sys.executable, runner,
        "--project-id", _PROJECT_ID,
        "--action", args.action,
    ]
    if args.action == "start":
        cmd.extend(["--graph", str(_CONVERTER_YAML)])
    if args.run_id:
        cmd.extend(["--run-id", args.run_id])
    if args.step_id:
        cmd.extend(["--step-id", args.step_id])
    if args.result:
        cmd.extend(["--result", args.result])
    if args.feedback:
        cmd.extend(["--feedback", args.feedback])

    # Ensure PYTHONPATH includes src/
    env = os.environ.copy()
    src_path = str(_REPO_ROOT / "src")
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{src_path}:{existing}" if existing else src_path

    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    sys.stdout.write(proc.stdout)
    sys.stderr.write(proc.stderr)
    sys.exit(proc.returncode)


if __name__ == "__main__":
    main()
