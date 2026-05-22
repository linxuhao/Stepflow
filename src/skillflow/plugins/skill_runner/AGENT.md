# skillflow-run — Stateless Pipeline Runner

Execute a skillflow pipeline by calling the `skillflow-run` CLI. Each invocation is a **fresh process** — state lives in SQLite, not in memory. You call it, parse the JSON response, do the work, call again.

You never see the graph — the runner tells you what to do next.

## CLI reference

```bash
# Start a new run (--graph only on start)
skillflow-run --graph pipeline.yaml --action start

# All subsequent calls use --run-id to reconnect state
skillflow-run --action submit --run-id <id> --result '{"key":"val"}'
skillflow-run --action approve --run-id <id>
skillflow-run --action reject --run-id <id> --feedback "reason"
skillflow-run --action abort --run-id <id>
```

Every call prints one JSON line to stdout. Parse it, act, call again.

## Interaction Protocol

```
Agent                         skillflow-run
  │                                │
  │── --action start ──────→      │  creates run, claims first step
  │    --graph pipeline.yaml       │
  │←── JSON ──────────────────    │  {status: "in_progress", step, instruction, tools}
  │                                │
  │  [do the work]                 │
  │                                │
  │── --action submit ──────→     │  confirms step, advances graph
  │    --run-id abc123             │  (auto-resolves gates, loops)
  │    --result '{"key":"val"}'    │
  │←── JSON ──────────────────    │  {status, step, instruction, tools}
  │                                │
  │  ... repeat ...                │
  │                                │
  │←── JSON ──────────────────    │  {status: "completed", outputs: {...}}
```

## SkillResponse Format

### In progress (work to do)
```json
{
  "status": "in_progress",
  "step": "analyze_diff",
  "instruction": "## Task\nExecute step `analyze_diff`.\nWrite output files to the output directory:\n- `findings.json`",
  "tools": {
    "write_findings": {"name": "write_findings", "description": "Replace findings.json...", "parameters": {"content": {"type": "string", "required": true}}}
  },
  "output_dir": "/path/to/analyze_diff.tmp",
  "expected_files": ["findings.json"],
  "validation_error": ""
}
```
If `expected_files` is non-empty, **write those files to `output_dir`** before calling `submit`. The `output_dir` is a `.tmp` staging directory — skillflow promotes files from `.tmp/` to the final step directory on successful submit. Use the `tools` (write_*/create_*/append_* helpers) to understand the expected format for each file. Call `submit` with your result to advance.

If `validation_error` is non-empty, the previous `submit` was rejected. Fix the issue described and re-submit.

### Paused at checkpoint
```json
{
  "status": "paused",
  "step": "summarize",
  "checkpoint_label": "Review Summary",
  "instruction": "Pipeline paused. Call approve or reject."
}
```
Call `--action approve` to continue, or `--action reject` with feedback to redo the step.

### Completed
```json
{
  "status": "completed",
  "outputs": {
    "analyze_diff": {"findings": [...]},
    "summarize": {"review": "..."}
  },
  "steps_completed": 5
}
```
The pipeline is done. Present `outputs` to the user.

### Failed
```json
{
  "status": "failed",
  "error": "No matching transition from 'review' with flags {...}"
}
```
Report the error to the user.

## Rules

1. Start with `--action start --graph pipeline.yaml` (no `--run-id`) — save `run_id` from the response
2. Always pass `--run-id` back on every subsequent call to resume the session
3. On `status="in_progress"`: if `expected_files` is non-empty, write those files to `output_dir` before submitting. Then `--action submit` with `--run-id` and `--result`
4. On `status="paused"`: decide — `--action approve` or `--action reject` with `--run-id` and `--feedback`
5. On `status="completed"`: done — present outputs
6. On `status="failed"`: report error
7. Never `submit` twice in a row — wait for a new `in_progress`
8. If `validation_error` is set on the response, fix the issue and re-submit (the step repeats)
9. If you lose state, call `--action next --run-id <id>` with the last known `run_id` to reconnect

## Tool nodes

Tool nodes are always delegated to the agent. They're presented as regular
steps with `tool_name` and `tool_params`:

```json
{
  "status": "in_progress",
  "step": "validate_design",
  "tool_name": "skillflow_lint",
  "tool_params": {"path": "/workspace/design/skill_pipeline.yaml"},
  "instruction": "Execute tool: skillflow_lint"
}
```

**You** execute the tool (using your own tool infrastructure), then submit
the result. The runner stores it and advances the graph.

Native tools (under `src/skillflow/tools/`) are auto-executed — you never see them.

### Variable substitution

You may encounter `$CONFIG_DIR`, `$STEP_DIR`, `$STEP_TMP_DIR`, `$PROJECT_ROOT`,
or `$TASK_DIR` in `tool_params`. These are path variables resolved at runtime:

| Variable | Resolves to |
|----------|------------|
| `$CONFIG_DIR` | The graph's per-config workspace directory |
| `$STEP_DIR` | The promoted output directory of the current step |
| `$STEP_TMP_DIR` | The temporary staging directory for step output |
| `$PROJECT_ROOT` | The project root directory on disk |
| `$TASK_DIR` | The project's tasks subdirectory |

You do not need to expand these yourself. They are resolved before the tool
executes. Example: `"$CONFIG_DIR/design_graph/skill_pipeline.yaml"` points to
the `skill_pipeline.yaml` output of the `design_graph` step.

## Checkpoints are for your user, not you

When the runner returns `{status: "paused"}`, **present the checkpoint to
the human user behind you**. Do NOT auto-approve or reject.

```json
{
  "status": "paused",
  "step": "summarize",
  "checkpoint_label": "Review Summary — approve to commit, reject to revise",
  "instruction": "Pipeline paused at checkpoint. Call approve or reject."
}
```

Your job:
1. Show the checkpoint label and outputs to the user
2. Ask if they approve
3. If yes → `--action approve`
4. If no → `--action reject --feedback "reason"`

## What you don't need to worry about

- **Gates** — auto-resolved, never shown to you
- **Native tools** — auto-executed inline, never shown
- **Loop steps** — auto-iterated, each iteration appears as a regular agent step
- **Error handlers** — routed automatically on retry exhaustion
- **Stale claims** — auto-recovered by advance_run
