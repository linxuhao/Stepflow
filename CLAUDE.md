# CLAUDE.md — Skillflow

Skillflow is a config-agnostic LLM pipeline graph executor. It is a pure Python library with minimal dependencies (PyYAML, ruff).
Under development, no backward compatibility is needed.

## Project Rules

- **Zero AItelier imports.** Skillflow must never import from `core/`, `api/`, `cli/`, `aitelier/`, `models/`, or `templates/`.
- **All tools are in `src/skillflow/tools/{name}/`** with `tool.yaml` + `impl.py`. Function name must match directory name.
- **Tests in `tests/`** — 306 tests. Run: `pytest tests/ -v`
- **Backward compat:** New fields on StepNode/Transition must have defaults. Old YAML without new fields must still parse.
- **`type` field in tool.yaml is forbidden** — tools are callable by both agent steps and tool steps. Access control is via `agent_config.tools: [...]` (for agents) and `tool_name: "..."` (for tool nodes).

## Build & Test

Project uses `.venv/` at repo root (auto-detected by VS Code). First-time setup:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e ~/skillflow[dev]
```

Then:

```bash
pytest tests/ -v          # 306 tests
pytest plugins/ -v        # 21 plugin tests
```

## Architecture

```
PipelineGraph (YAML) → GraphResolver (validation + traversal)
                         ↓
SkillFlow (orchestrator)  ← SQLite (WAL mode)
  ├── claim_next_step()  → ClaimedStep → StepRunner (host app)
  ├── confirm_step()     → completed/failed
  ├── advance_run()      → resolve gates, auto-execute tools
  │   ├── recover_stale_claims() (built-in)
  │   └── feedback loopback (inject tool error into step inputs)
  ├── reject_checkpoint() → reset to pending
  └── drain_outbox()     → event stream

ToolLoader (multi-source)
  ├── Native: src/skillflow/tools/
  └── Custom: host app adds via add_tools_dir()

ContextResolver → assemble prompt context from:
  ├── {config, output}  (cross-config)
  ├── {step, output, mode}  (same-config)
  └── {tool}  (dynamic call)

StepValidator → run validation specs: [{files, tool, inline_schema}]
WriteTools → generate constrained write_* tools from output.fixed
```

## Key Data Structures

- `Transition(to, match, max_loop, label, feedback)` — directed edge
- `StepNode(id, step_type, transitions, checkpoint, config, tool_name, tool_params, agent_config, context, output_mode, output_fixed, validation)` — graph node
- `ClaimToken(step_id, run_id, step_instance_id, version, claimed_at)` — frozen claim
- `ClaimedStep(token, step_config, run_context, inputs, validation_error)` — ready to execute
- `StepResult(outputs, flags)` — execution result (flags used for transition matching)
- `StepRunner` — Protocol: `async def execute(step: ClaimedStep) -> StepResult`

## Tools

Each tool: `tool.yaml` (schema) + `impl.py` (function). Function signature:

```python
def tool_name(*params, *, workspace_root: str = "", project_root: str = "") -> dict:
    ...
    return {"verdict": "passed"} | {"verdict": "failed", "feedback": "..."}
```

Native tools (13): read_file, write, list_tree, dir_tree, json_schema, syntax_lint, py_compile, pytest, repo_apply, repo_validate, draft_commit, file_exists, notify.
