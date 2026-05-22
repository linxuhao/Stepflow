# skillflow-convert — Skill-to-Pipeline Converter

Convert a skill description (markdown) into a valid skillflow pipeline YAML config. The converter is itself a skillflow pipeline — you drive it with the `skillflow-convert` CLI, one action per shell command.

## How to use

This is a **stateless CLI tool**. Each invocation is a fresh process. The only shared state is the SQLite database. You call it, parse the JSON response, act, call again.

```bash
# 1. Start the conversion
skillflow-convert --desc "Code review skill..." --action start
# → {"status":"in_progress", "run_id":"abc123", "step":"analyze_skill", "instruction":"..."}

# 2. Submit your analysis (no --desc needed — state is in the DB)
skillflow-convert --action submit --run-id abc123 --result '{"analysis":{...}}'

# 3. Submit your YAML design
skillflow-convert --action submit --run-id abc123 --result '{"pipeline":"name: ..."}'

# 4. Explain the design (then checkpoint pauses for human review)
skillflow-convert --action submit --run-id abc123 --result '{"explanation":"..."}'
# → {status:"paused", checkpoint_label:"Design Review"}
skillflow-convert --action approve --run-id abc123

# 5. Tool step: lint validates the YAML — you execute skillflow_lint, submit result
# → {status:"in_progress", step:"validate_design", tool_name:"skillflow_lint", ...}
skillflow-convert --action submit --run-id abc123 --result '{"passed":true,...}'

# 6. If linter fails, the fix step is presented:
# → {"status":"in_progress", "step":"fix_issues", "instruction":"...linter errors..."}

# 7. Approve or reject checkpoints
skillflow-convert --action approve --run-id abc123
skillflow-convert --action reject --run-id abc123 --feedback "reason"
```

You can also pass a file instead of inline text:

```bash
skillflow-convert --desc-file my_skill.md --action start
```

## Pipeline flow

```
analyze_skill     ← you parse the skill → phases, decisions, tools, checkpoints
    ↓
design_graph      ← you produce skillflow YAML
    ↓
explain_design    ← you explain the design (checkpoint: human reviews)
    ↓
validate_design   ← skillflow_lint checks the YAML (presented as tool step)
    ↓
  ├─ passed → done → completed
  └─ failed → fix_issues ← you fix errors (linter feedback in instruction)
                  ↓
            validate_fix ← re-check (presented as tool step)
                  ↓
              ├─ passed → done → completed
              └─ failed → fix_issues (up to 3 attempts)
```

Tool steps (`validate_design`, `validate_fix`) are presented to you with `tool_name` and `tool_params` — you execute the tool and submit the result, just like any other step.

`explain_design` is a checkpoint step. The pipeline pauses after it for human review before proceeding to validation.

## Step details

### analyze_skill

**Instruction**: "You are a pipeline architect. Analyze the given skill description..."

**You produce**: Analysis JSON:
```json
{
  "analysis": {
    "phases": ["phase_name", ...],
    "decisions": [
      {"condition": "when this happens", "branches": ["branch_a", "branch_b"]}
    ],
    "terminal_condition": "how the skill knows it's done",
    "tools_per_phase": {"phase_name": ["tool", ...]},
    "checkpoints": ["phase_where_human_approval_needed"]
  }
}
```

### design_graph

**Instruction**: "Design a skillflow graph based on the analysis..." Context includes your analysis from the previous step.

**You produce**: Complete skillflow YAML — must follow these rules:
- Every `agent` step has `agent_config`
- Decision points → `gate` steps with `match` transitions
- Cycles need `max_loop`
- Terminal nodes need `end_conditions`
- Checkpoint steps have `checkpoint: true` + checkpoint transitions

### explain_design

**Instruction**: "Explain the design..." Context includes your analysis and YAML from previous steps.

**You produce**: A markdown explanation of the pipeline design.

This is a **checkpoint** step. After you submit, the pipeline pauses for human review. The human can approve (continue to validation) or reject (go back to `design_graph` to revise).

### fix_issues

**Instruction**: "Fix the linter errors..." Context includes your broken YAML + **linter feedback**:

```
## Feedback from previous attempt
{"passed": false, "errors": 2, "issues": [
  {"severity": "error",
   "message": "Cycle has no max_loop constraint",
   "location": "steps[2].transitions[1]",
   "suggestion": "Add max_loop: 3 to the transition"}
]}
```

**You produce**: Corrected YAML that passes lint. Up to 3 fix attempts.

## Getting the result

When the converter completes, the generated pipeline YAML is at:

```
~/.skillflow/workspaces/skill-converter/.../skill_pipeline.yaml
```

Copy it to wherever the skill pipeline should live.

## Reference: SkillFlow YAML Structure

```yaml
name: "my_skill"
begin: "first_step_id"
end_conditions:
  combinator: or
  conditions:
    - type: node_reached
      node: "final_step"
      result: "completed"

steps:
  - id: "step_id"
    step_type: agent          # agent | gate | tool | loop
    agent_config: "role_name"
    checkpoint: false
    max_retries: 3
    context:
      - source: { step: "previous_step" }
      - source: { tool: "dir_tree" }
    output:
      mode: "content"
      fixed:
        output_slot: "filename.md"
    transitions:
      - to: "next"                                   # default
      - to: "branch"
        match: { flag_key: value }                   # conditional
      - to: "prev"
        match: { ok: false }
        max_loop: 3                                  # cycle limit
```

### Transition patterns

| Pattern | Use case |
|---------|----------|
| `{to: "next"}` | Default — always taken |
| `{to: "branch", match: {key: val}}` | Gate routing |
| `{to: "handler", match: {_error: true}}` | Error handler |
| `{to: "next", match: {from: checkpoint, value: approved}}` | Checkpoint |
| `{to: "prev", match: {ok: false}, max_loop: 3}` | Review loop |
