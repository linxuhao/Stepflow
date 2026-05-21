# Skillflow

Config-agnostic LLM pipeline graph executor. Define multi-agent pipelines as YAML DAGs — skillflow handles traversal, tool execution, checkpoints, recovery, and event streaming on SQLite.

## Install

```bash
pip install skillflow-py      # PyPI
pip install -e ~/skillflow    # from repo (editable)
```

Or clone and use the install script, which also registers CLI commands:

```bash
git clone https://github.com/linxuhao/SkillFlow.git
bash skillflow/scripts/install.sh
```

CLI commands registered in `~/.local/bin/`:

| Command | Description |
|---------|-------------|
| `skillflow-lint` | Validate pipeline YAML files (one-shot) |
| `skillflow-run` | Run a pipeline interactively |
| `skillflow-convert` | Convert a skill description → pipeline YAML |

```bash
skillflow-lint configs/*.yaml          # one-shot validation
skillflow-run pipeline.yaml            # interactive (human or agent drives)
skillflow-convert my_skill.md -o pipeline.yaml
```

### PyPI publish

```bash
pip install build twine
python3 -m build
twine upload dist/*
```

## Getting Started

Skillflow runs pipelines in two modes.

### Framework Mode

Skillflow is embedded in a host application. The host drives the loop — skillflow handles traversal, tool execution, and state. The host only executes agent steps via `StepRunner`.

```python
from skillflow import SkillFlow, PipelineGraph, StepResult

graph = PipelineGraph.from_yaml("tests/fixtures/minimal_1step.yaml")

sf = SkillFlow(":memory:")
sf.register_graph(graph)
sf.register_agent_config("echo_agent", model="host")

run_id = sf.create_run("minimal_1step")
sf.start_run(run_id)

while True:
    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    if claimed is None:
        break  # completed or paused
    # Host StepRunner executes the agent step here
    sf.confirm_step(claimed.token, StepResult(outputs={}, flags={}))
```

In framework mode, **all tools auto-execute** inline — skillflow runs native tools and custom tools without involving the host agent.

Config reference: `tests/fixtures/minimal_1step.yaml`.

### Runner Mode

Skillflow is driven interactively via `SkillTool`. The pipeline exposes steps as instructions — a human or LLM agent calls `action="next"` / `"submit"` / `"approve"` / `"reject"`.

```python
from skillflow import SkillFlow, PipelineGraph
from skillflow.plugins.skill_runner import SkillTool

graph = PipelineGraph.from_yaml("tests/fixtures/skill_review.yaml")
sf = SkillFlow(":memory:", delegate_tools_to_agent=True)
sf.register_graph(graph)
sf.register_agent_config("review_analyst", model="host")
# ... register other agent configs referenced by the graph

tool = SkillTool(sf, "skill_review")
resp = tool(action="next")

while resp.status == "in_progress":
    print(resp.instruction)
    # Agent does work, produces output...
    resp = tool(action="submit", result={"findings": {...}})
# resp.status == "completed"
```

In runner mode, **native tools auto-execute** but **custom and unknown tools are delegated** to the agent (via `resp.tool_name` / `resp.tool_params`). Use `skillflow-run` or `skillflow-convert` to drive a pipeline interactively.

## Node Types

| Type | Description |
|------|-------------|
| `agent` | LLM step — host app executes via `StepRunner` protocol |
| `tool` | Auto-executed by skillflow (native), or delegated to agent in runner mode (custom) |
| `gate` | Auto-resolved using match conditions against step output flags |
| `loop` | Iterates over a JSON list from a workspace file, instantiating sub-steps per item |

## Transition Matching

Five match strategies. See `tests/fixtures/dpe_full.yaml` for a complete pipeline using all of them:

```yaml
match: { field: "passed", value: true }                          # step output flags
match: { from_file: "review_verdict.json", field: "passed", value: true }  # output file
match: { from: "checkpoint", value: "approved" }                 # checkpoint routing
match: { _error: true }                                          # error handler
# (no match key)                                                 # always match
```

## Context Injection

```yaml
context:
  - source: { step: "1" }
  - source: { step: "2", mode: "interfaces" }
  - source: { config: "meta", output: "brief.md" }
  - source: { tool: "dir_tree" }
```

## Checkpoints

Agent steps can pause for human approval (`tests/fixtures/checkpoint_cycle.yaml`):

```python
sf.reject_checkpoint(run_id, "draft", "Add more detail to the analysis")
```

## Output Validation

Steps declare validation specs auto-executed by skillflow. See `tests/fixtures/skill_review.yaml` for inline JSON Schema validation, or `tests/fixtures/lifecycle_hooks.yaml` for syntax_lint + py_compile validators.

Available validators: `json_schema`, `syntax_lint`, `py_compile`, `pytest`, `file_exists`.

## Lifecycle Hooks

Steps with `output.mode: "write"` can trigger deliver and post-deliver hooks. See `tests/fixtures/lifecycle_hooks.yaml`:

```yaml
lifecycle:
  on_deliver:
    tool: "repo_apply"
    params:
      source_dir: "$STEP_DIR"
    on_failure: "retry"
    max_retries: 2
  after_deliver:
    - tool: "syntax_lint"
      files: ["*.py"]
```

## Error Handling

Steps declare `max_retries` and an `_error` transition. See `tests/fixtures/error_handler.yaml`.

## Feedback Loopback

Tool failures can inject output into the next step's inputs (`feedback: true`). See `plugins/skill_converter/skill_converter.yaml` — the `validate_design` step feeds lint errors into `fix_issues`.

## End Conditions

Four termination strategies, combined with `and`/`or`. See `tests/fixtures/end_conditions.yaml` and `tests/fixtures/dpe_full.yaml`:

```yaml
end_conditions:
  combinator: or
  conditions:
    - type: node_reached
      node: "5_review"
      result: "completed"
    - type: max_total_steps
      limit: 200
    - type: max_run_duration_seconds
      limit: 3600
    - type: flag_match
      flag: { fatal_error: true }
```

## Stale Claim Recovery

Built into `advance_run`. Claims older than `stale_threshold_seconds` (default 300) are auto-reset:

```python
sf = SkillFlow("pipeline.db", stale_threshold_seconds=300)
```

## Event Streaming

All state transitions are written to `skillflow_outbox`. Poll for real-time notifications:

```python
events = sf.drain_outbox(batch_size=50)
for event in events:
    print(event.event_type, event.payload)
sf.ack_outbox([e.id for e in events])
```

In-process subscribers via `NotificationBus`:

```python
from skillflow import NotificationBus

bus = NotificationBus()
bus.subscribe("step_completed", lambda n: print(n.payload))
sf = SkillFlow(":memory:", notification_bus=bus)
```

## Tools

### Native (13 built-in)

| Tool | Description |
|------|-------------|
| `read_file` | Read a file with line numbers |
| `write` | Write content to workspace |
| `list_tree` | List directory structure |
| `dir_tree` | Context tree for prompt injection |
| `json_schema` | Validate JSON against inline schema |
| `syntax_lint` | Syntax check via ruff |
| `py_compile` | Python bytecode compile |
| `pytest` | Run pytest on test files |
| `repo_apply` | Copy files to repo + git commit |
| `repo_validate` | Multi-tool repo validation |
| `draft_commit` | Move draft files to final dir + commit |
| `file_exists` | Check files matching glob patterns |
| `notify` | Send user-visible notifications |

### Custom tools

Host apps add tool directories. Each tool: `{name}/tool.yaml` + `{name}/impl.py`. Function name must match directory name.

```python
from skillflow.tool_loader import ToolLoader

loader = ToolLoader()
loader.add_tools_dir("my_app/tools")
sf = SkillFlow(":memory:", tool_loader=loader)
```

## Use Cases

### 1. Framework mode — embed skillflow in your app

Use skillflow as a library. Read the [Getting Started](#getting-started) section above and the fixture examples in `tests/fixtures/`.

```python
from skillflow import SkillFlow, PipelineGraph
graph = PipelineGraph.from_yaml("my_pipeline.yaml")
sf = SkillFlow(":memory:")
sf.register_graph(graph)
# ... drive the loop with claim_next_step / confirm_step
```

### 2. Agent mode — convert skills to pipelines

LLM agents use the **converter** + **runner** plugins to turn skill descriptions into skillflow pipelines. The agent calls a tool named `run_skill` repeatedly — the runner tells it what to do at each step.

```python
from skillflow.plugins.skill_converter import setup_converter
from skillflow.plugins.skill_runner import load_agent_guide

# Give the agent its user manual
system_prompt = load_agent_guide()  # ← includes protocol, response format, rules

tool = setup_converter(sf, description_file="my_skill.md")
resp = tool(action="next")
# ... agent loops: submit result → get next instruction → ...
# resp.status == "completed" → pipeline YAML ready
```

Agent manuals are shipped in the package:

| Plugin | Manual | How to load |
|--------|--------|-------------|
| `skill_runner` | Agent protocol — actions, SkillResponse format, rules | `load_agent_guide()` from `skillflow.plugins.skill_runner` |
| `skill_converter` | Step-by-step guide — analyze → design → lint → fix | `load_agent_guide()` from `skillflow.plugins.skill_converter` |

Inject the runner manual into the agent's system prompt. Inject the converter manual when the agent is asked to convert a skill.

CLI tools for manual use:

```bash
skillflow-lint pipeline.yaml                # one-shot config validation
skillflow-run pipeline.yaml                 # drive a pipeline interactively
skillflow-convert my_skill.md -o out.yaml   # convert a skill description
```

### Linter (`skillflow.plugins.linter`)

Framework utility. Validates pipeline YAML — used as a skillflow tool (`skillflow_lint`) inside the converter's feedback loop, or standalone:

```bash
skillflow-lint tests/fixtures/skill_review.yaml
skillflow-lint configs/*.yaml
```

## Package

```
src/skillflow/
├── core.py              # SkillFlow orchestrator (create/claim/confirm/advance)
├── graph.py             # PipelineGraph, StepNode, Transition, GraphResolver
├── tool_loader.py       # Dynamic tool schema + implementation loading
├── context.py           # ContextResolver: cross-config, step, tool sources
├── step_validation.py   # StepValidator: multi-tool output validation
├── write_tools.py       # Constrained write tool generation from output.fixed
├── workspace.py         # Per-step atomic staging directories
├── validation.py        # Optional external-schema output validation
├── recovery.py          # Stale claim recovery
├── schema.py            # SQLite DDL + migrations
├── exceptions.py        # SkillFlowError hierarchy
├── outbox.py            # OutboxConsumer for event polling
├── notifications.py     # NotificationBus for in-process subscribers
├── agent_registry.py    # Agent config registry + schema resolution
├── plugins/             # Built-in plugins
│   ├── linter/          # Config validator + skillflow_lint tool
│   ├── skill_runner/    # SkillTool — interactive pipeline facade
│   └── skill_converter/ # Skill description → pipeline YAML
└── tools/               # Native tools (13)
    ├── read_file/       ├── write/          ├── list_tree/
    ├── dir_tree/        ├── json_schema/    ├── syntax_lint/
    ├── py_compile/      ├── pytest/         ├── repo_apply/
    ├── repo_validate/   ├── draft_commit/   ├── file_exists/
    └── notify/
```

## Tests

```bash
pytest tests/ -v       # 306 tests
pytest plugins/ -v     # 21 plugin tests
```
