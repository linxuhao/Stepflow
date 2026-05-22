"""Integration tests driven by YAML fixture files.

These tests load PipelineGraphs from static YAML configs (adapted from
AItelier configs), register mock agent configs, and drive pipelines
with canned StepResults.  No real LLM calls or tool implementations.

The fixture files serve a dual purpose:
1. Verify skillflow features end-to-end against realistic configs.
2. Act as copy-pasteable examples for external skillflow users.
"""

from pathlib import Path

import pytest

from skillflow.core import SkillFlow, StepResult
from skillflow.graph import PipelineGraph, GraphValidationError
from mocks import create_standard_mock_tools
from conftest import register_dpe_agent_configs


FIXTURES = Path(__file__).parent / "fixtures"


# ── Helpers ───────────────────────────────────────────────────────────

def _execute(sf: SkillFlow, run_id: str, step_id: str,
             outputs=None, flags=None) -> StepResult:
    """Claim, execute (canned result), and confirm a step."""
    claimed = sf.claim_next_step(run_id)
    assert claimed is not None, f"Failed to claim '{step_id}'"
    assert claimed.step_id == step_id, f"Expected '{step_id}', got '{claimed.step_id}'"
    result = StepResult(outputs=outputs or {}, flags=flags or {})
    sf.confirm_step(claimed.token, result)
    return result


def _load_and_register(sf: SkillFlow, fixture_name: str,
                        agent_configs: dict[str, list[str]] | None = None):
    """Load a YAML graph and register it with mock agent configs."""
    configs = agent_configs or {"echo_agent": []}
    for name, tools in configs.items():
        sf.register_agent_config(name, model="mock", tools=tools)
    graph = PipelineGraph.from_yaml(str(FIXTURES / fixture_name))
    sf.register_graph(graph)
    return graph


def _confirm_with_file(sf: SkillFlow, claimed, outputs=None, flags=None,
                       extra_files: dict[str, str] | None = None):
    """Confirm a step after writing extra output files to tmp_dir.

    Needed for from_file match conditions: the output files must exist
    in the step tmp dir before confirm_step runs lifecycle hooks that
    promote them to the step dir.
    """
    # Write extra files to the step's tmp directory
    if extra_files and sf._workspace:
        pid = sf._get_project_id(claimed.token.run_id)
        gname = sf._get_graph_name(claimed.token.run_id)
        tmp_dir = sf._workspace.get_step_tmp_dir(pid, gname, claimed.step_id)
        tmp_dir.mkdir(parents=True, exist_ok=True)
        import json as _json
        for filename, content in extra_files.items():
            (tmp_dir / filename).write_text(content, encoding="utf-8")

    sf.confirm_step(claimed.token, StepResult(
        outputs=outputs or {}, flags=flags or {}
    ))


# ── YAML Loading ──────────────────────────────────────────────────────

def test_load_all_valid_fixtures():
    """Every valid fixture YAML parses without error."""
    valid = sorted(FIXTURES.glob("*.yaml"))
    assert len(valid) >= 9, f"Expected at least 9 fixtures, found {len(valid)}"
    for path in valid:
        graph = PipelineGraph.from_yaml(str(path))
        assert graph.name
        assert graph.begin
        assert len(graph.steps) > 0


def test_load_dpe_full():
    """DPE full config loads and parses correctly."""
    graph = PipelineGraph.from_yaml(str(FIXTURES / "dpe_full.yaml"))
    assert graph.name == "dpe_default_v2"
    assert graph.begin == "1"
    assert len(graph.steps) == 15
    assert graph.end_conditions is not None
    assert len(graph.end_conditions.conditions) == 4


# ── Basic Flow ────────────────────────────────────────────────────────

def test_minimal_1step_run(sf_with_tools):
    """Two-step pipeline: do_work → done, end_condition on done."""
    sf = sf_with_tools
    _load_and_register(sf, "minimal_1step.yaml")
    run_id = sf.create_run("minimal_1step")
    sf.start_run(run_id)

    sf.advance_run(run_id)
    _execute(sf, run_id, "do_work")

    # do_work → done → end_condition node_reached triggers completion
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


def test_gate_resolution_alpha(sf_with_tools):
    """Gate routes to branch_alpha when flag route=alpha."""
    sf = sf_with_tools
    _load_and_register(sf, "agent_gate_agent.yaml")
    run_id = sf.create_run("agent_gate_agent")
    sf.start_run(run_id)

    sf.advance_run(run_id)
    _execute(sf, run_id, "step_a", flags={"route": "alpha"})

    # gate resolves → branch_alpha → end_condition triggers
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


def test_gate_resolution_beta(sf_with_tools):
    """Gate routes to branch_beta when flag route=beta."""
    sf = sf_with_tools
    _load_and_register(sf, "agent_gate_agent.yaml")
    run_id = sf.create_run("agent_gate_agent")
    sf.start_run(run_id)

    sf.advance_run(run_id)
    _execute(sf, run_id, "step_a", flags={"route": "beta"})

    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


def test_gate_no_match_fails_run(sf_with_tools):
    """Gate with no matching transition fails the run."""
    sf = sf_with_tools
    # Build a gate graph without the fallback branch
    import yaml
    data = yaml.safe_load((FIXTURES / "agent_gate_agent.yaml").read_text())
    data["steps"] = [s for s in data["steps"] if s["id"] != "none_matched"]
    for s in data["steps"]:
        if s["id"] == "router":
            s["transitions"] = [t for t in s["transitions"] if t.get("match", {}).get("route") != "unknown"]
    data["end_conditions"]["conditions"] = [
        c for c in data["end_conditions"]["conditions"] if c["node"] != "none_matched"
    ]

    graph = PipelineGraph._from_dict(data)
    sf.register_agent_config("echo_agent", model="mock")
    sf.register_graph(graph)
    run_id = sf.create_run(graph.name)
    sf.start_run(run_id)

    sf.advance_run(run_id)
    _execute(sf, run_id, "step_a", flags={"route": "unknown"})

    sf.advance_run(run_id)  # gate fails to match → run fails
    assert sf.get_run(run_id)["status"] == "failed"


# ── Checkpoints ───────────────────────────────────────────────────────

def test_checkpoint_pause_resume(sf_with_tools):
    """Checkpoint pauses run; resume → execute publish → done → complete."""
    sf = sf_with_tools
    _load_and_register(sf, "checkpoint_cycle.yaml")
    run_id = sf.create_run("checkpoint_cycle")
    sf.start_run(run_id)

    sf.advance_run(run_id)
    _execute(sf, run_id, "draft")

    # Paused at checkpoint
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "paused"

    # Resume → checkpoint resolves to publish
    sf.resume_run(run_id)
    next_node = sf.advance_run(run_id)
    assert next_node == "publish"

    # Execute publish → done → end_condition
    _execute(sf, run_id, "publish")
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


def test_checkpoint_reject_reexecute(sf_with_tools):
    """Reject checkpoint → re-execute → pause → resume → complete."""
    sf = sf_with_tools
    _load_and_register(sf, "checkpoint_cycle.yaml")
    run_id = sf.create_run("checkpoint_cycle")
    sf.start_run(run_id)

    # First execution
    sf.advance_run(run_id)
    _execute(sf, run_id, "draft", outputs={"v": 1})
    sf.advance_run(run_id)  # paused
    assert sf.get_run(run_id)["status"] == "paused"

    # Reject
    sf.reject_checkpoint(run_id, "draft", "Not good enough")
    assert sf.get_run(run_id)["status"] == "running"

    # Re-execute
    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "draft"
    sf.confirm_step(claimed.token, StepResult(outputs={"v": 2}, flags={}))

    # Paused again
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "paused"

    # Resume → publish → done → complete
    sf.resume_run(run_id)
    next_node = sf.advance_run(run_id)
    assert next_node == "publish"
    _execute(sf, run_id, "publish")
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


# ── Review Loops ───────────────────────────────────────────────────────

def test_review_pass_first_try(sf_with_tools):
    """Writer passes review on first attempt (approved: true flag)."""
    sf = sf_with_tools
    _load_and_register(sf, "review_loop.yaml")
    run_id = sf.create_run("review_loop")
    sf.start_run(run_id)

    sf.advance_run(run_id)
    _execute(sf, run_id, "writer")

    sf.advance_run(run_id)
    _execute(sf, run_id, "reviewer", flags={"approved": True})

    # reviewer → done → end_condition → complete
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


def test_review_fail_then_pass(sf_with_tools):
    """Writer fails review once, loops back, then passes."""
    sf = sf_with_tools
    _load_and_register(sf, "review_loop.yaml")
    run_id = sf.create_run("review_loop")
    sf.start_run(run_id)

    # Attempt 1: fails review
    sf.advance_run(run_id)
    _execute(sf, run_id, "writer")
    sf.advance_run(run_id)
    _execute(sf, run_id, "reviewer", flags={"approved": False})

    # Loops back to writer
    next_node = sf.advance_run(run_id)
    assert next_node == "writer"

    # Attempt 2: passes
    sf.advance_run(run_id)
    _execute(sf, run_id, "writer")
    sf.advance_run(run_id)
    _execute(sf, run_id, "reviewer", flags={"approved": True})

    # reviewer → done → end_condition → complete
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


# ── Error Handling ────────────────────────────────────────────────────

def test_error_transition_routing(sf_with_tools):
    """Step fails with retries exhausted → routes to error_handler."""
    sf = sf_with_tools
    _load_and_register(sf, "error_handler.yaml")
    run_id = sf.create_run("error_handler")
    sf.start_run(run_id)

    # Fail risky_step until retries exhausted (max_retries=2 → 3 attempts)
    for attempt in range(3):
        sf.advance_run(run_id)
        claimed = sf.claim_next_step(run_id)
        assert claimed.step_id == "risky_step", f"Attempt {attempt+1}"
        sf.fail_step(claimed.token, f"Error {attempt+1}", retryable=True)

    # Routed to error_handler
    run = sf.get_run(run_id)
    assert run["current_node"] == "error_handler"

    sf.advance_run(run_id)
    _execute(sf, run_id, "error_handler")

    # error_handler → done → end_condition → complete
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


def test_single_retry_recovery(sf_with_tools):
    """Step fails once, retries, succeeds."""
    sf = sf_with_tools
    _load_and_register(sf, "error_handler.yaml")
    run_id = sf.create_run("error_handler")
    sf.start_run(run_id)

    # Fail once
    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "risky_step"
    sf.fail_step(claimed.token, "Transient error", retryable=True)

    # Retry succeeds
    sf.advance_run(run_id)
    _execute(sf, run_id, "risky_step", flags={})

    # risky_step → done → end_condition → complete
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


# ── Loop Steps ────────────────────────────────────────────────────────

def test_loop_iteration(sf_with_workspace):
    """Loop step iterates over items from a JSON manifest file."""
    sf = sf_with_workspace
    _load_and_register(sf, "loop_step.yaml")
    run_id = sf.create_run("loop_step", project_id="test-loop")
    sf.start_run(run_id)

    # Step 'prepare': write tasks_manifest.json to step dir
    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "prepare"

    pid = "test-loop"
    gname = sf._get_graph_name(run_id)
    step_dir = sf._workspace.get_step_tmp_dir(pid, gname, "prepare")
    step_dir.mkdir(parents=True, exist_ok=True)
    import json
    (step_dir / "tasks_manifest.json").write_text(json.dumps({
        "execution_order": [["task_a", "task_b"]],
        "tasks": [{"id": "task_a"}, {"id": "task_b"}],
    }))

    sf.confirm_step(claimed.token, StepResult(
        outputs={"manifest": {"execution_order": [["task_a", "task_b"]]}},
        flags={}
    ))

    # Advance to loop → body (process_task)
    sf.advance_run(run_id)  # prepare → task_iterator
    next_node = sf.advance_run(run_id)  # task_iterator → process_task (item 0)
    assert next_node == "process_task"

    # Process first task
    sf.advance_run(run_id)
    _execute(sf, run_id, "process_task")

    # Loop back → process second task
    sf.advance_run(run_id)  # process_task → task_iterator
    sf.advance_run(run_id)  # task_iterator → process_task (item 1)
    _execute(sf, run_id, "process_task")

    # Loop exhausted → all_done → end_condition → complete
    sf.advance_run(run_id)  # process_task → task_iterator
    assert sf.advance_run(run_id) is None  # loop exhausted → all_done → end_condition
    assert sf.get_run(run_id)["status"] == "completed"


# ── Tool Nodes ────────────────────────────────────────────────────────

def test_tool_node_auto_execute(sf_with_tools):
    """Tool node executes inline during advance_run, then agent runs."""
    sf = sf_with_tools
    _load_and_register(sf, "tool_node.yaml")
    run_id = sf.create_run("tool_node")
    sf.start_run(run_id)

    # tool node auto-executes, then advances to do_work
    next_node = sf.advance_run(run_id)
    assert next_node == "do_work"

    _execute(sf, run_id, "do_work")

    # do_work → done → end_condition → complete
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


# ── End Conditions ────────────────────────────────────────────────────

def test_end_condition_node_reached(sf_with_tools):
    """Pipeline completes when step_4 is reached (before executing it)."""
    sf = sf_with_tools
    _load_and_register(sf, "end_conditions.yaml")
    run_id = sf.create_run("end_conditions")
    sf.start_run(run_id)

    sf.advance_run(run_id)
    _execute(sf, run_id, "step_1")
    sf.advance_run(run_id)
    _execute(sf, run_id, "step_2")
    sf.advance_run(run_id)
    _execute(sf, run_id, "step_3")

    # step_3 → step_4 → end_condition node_reached "step_4" triggers
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


def test_end_condition_flag_match(sf_with_tools):
    """Pipeline fails when abort_early flag is set."""
    sf = sf_with_tools
    _load_and_register(sf, "end_conditions.yaml")
    run_id = sf.create_run("end_conditions")
    sf.start_run(run_id)

    sf.advance_run(run_id)
    _execute(sf, run_id, "step_1", flags={"abort_early": True})

    # flag_match triggers failure
    sf.advance_run(run_id)
    assert sf.get_run(run_id)["status"] == "failed"


# ── Invalid Configs ───────────────────────────────────────────────────

def test_invalid_missing_begin():
    """Graph without 'begin' field raises GraphValidationError."""
    with pytest.raises(GraphValidationError) as exc:
        graph = PipelineGraph.from_yaml(str(FIXTURES / "invalid" / "missing_begin.yaml"))
        sf = SkillFlow(":memory:")
        sf.register_agent_config("echo_agent", model="mock")
        sf.register_graph(graph)
    assert "begin" in str(exc.value).lower()


def test_invalid_unreachable_step():
    """Unreachable step raises GraphValidationError."""
    with pytest.raises(GraphValidationError) as exc:
        graph = PipelineGraph.from_yaml(str(FIXTURES / "invalid" / "unreachable_step.yaml"))
        sf = SkillFlow(":memory:")
        sf.register_agent_config("echo_agent", model="mock")
        sf.register_graph(graph)
    assert "unreachable" in str(exc.value).lower()


def test_invalid_cycle_no_max_loop():
    """Cycle without max_loop raises GraphValidationError."""
    with pytest.raises(GraphValidationError) as exc:
        graph = PipelineGraph.from_yaml(str(FIXTURES / "invalid" / "cycle_no_max_loop.yaml"))
        sf = SkillFlow(":memory:")
        sf.register_agent_config("echo_agent", model="mock")
        sf.register_graph(graph)
    assert "max_loop" in str(exc.value).lower() or "no max_loop" in str(exc.value).lower()


def test_invalid_duplicate_step_id():
    """Duplicate step IDs raise GraphValidationError."""
    with pytest.raises(GraphValidationError) as exc:
        graph = PipelineGraph.from_yaml(str(FIXTURES / "invalid" / "duplicate_step_id.yaml"))
        sf = SkillFlow(":memory:")
        sf.register_agent_config("echo_agent", model="mock")
        sf.register_graph(graph)
    assert "duplicate" in str(exc.value).lower()


def test_invalid_agent_config_missing(sf_with_tools):
    """Graph referencing unregistered agent_config raises GraphValidationError."""
    sf = sf_with_tools
    graph = PipelineGraph.from_yaml(str(FIXTURES / "minimal_1step.yaml"))
    with pytest.raises(GraphValidationError) as exc:
        sf.register_graph(graph)
    assert "echo_agent" in str(exc.value)


# ── DPE Full Pipeline ─────────────────────────────────────────────────

def test_dpe_full_no_tasks_flow(sf_with_workspace):
    """DPE pipeline with no tasks flows to completion via end_conditions."""
    sf = sf_with_workspace
    register_dpe_agent_configs(sf)
    graph = PipelineGraph.from_yaml(str(FIXTURES / "dpe_full.yaml"))
    sf.register_graph(graph)
    run_id = sf.create_run("dpe_default_v2", project_id="test-dpe-no-tasks")
    sf.start_run(run_id)
    pid = "test-dpe-no-tasks"
    gname = "dpe_default_v2"
    import json

    def verdict_json(passed):
        return json.dumps({"passed": passed, "feedback": "", "suggestions": []})

    # Step 1 (checkpoint)
    sf.advance_run(run_id)
    _execute(sf, run_id, "1")
    sf.advance_run(run_id)  # pause
    sf.resume_run(run_id)

    # Step 1_review → passes (from_file match needs verdict file)
    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "1_review"
    _confirm_with_file(sf, claimed, outputs={"verdict": {"passed": True}},
                       extra_files={"review_verdict.json": verdict_json(True)})
    next_node = sf.advance_run(run_id)
    if next_node:
        assert next_node == "2"

    # Step 2 (checkpoint)
    sf.advance_run(run_id)
    _execute(sf, run_id, "2")
    sf.advance_run(run_id)  # pause
    sf.resume_run(run_id)

    # Step 2_review → passes
    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "2_review"
    _confirm_with_file(sf, claimed, outputs={"verdict": {"passed": True}},
                       extra_files={"review_verdict.json": verdict_json(True)})
    sf.advance_run(run_id)  # resolves to 3

    # Step 3 (checkpoint) → has_tasks=False
    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "3"
    _confirm_with_file(sf, claimed,
                       outputs={"tasks_manifest": {"tasks": [], "execution_order": []}},
                       flags={"has_tasks": False},
                       extra_files={"tasks_manifest.json": json.dumps({"tasks": [], "execution_order": []})})
    sf.advance_run(run_id)  # pause
    sf.resume_run(run_id)

    # Step 3_review → passes → task_loop
    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "3_review"
    _confirm_with_file(sf, claimed, outputs={"verdict": {"passed": True}},
                       extra_files={"review_verdict.json": verdict_json(True)})
    sf.advance_run(run_id)  # to: task_loop

    # task_loop: no source file → routes to "5"
    next_node = sf.advance_run(run_id)
    if next_node == "5":
        sf.advance_run(run_id)
        _execute(sf, run_id, "5")
        # After executing 5, confirm_step resolves the transition to "5_review"
        # which triggers the end_condition (node_reached: 5_review) → run completes
        assert sf.advance_run(run_id) is None

    assert sf.get_run(run_id)["status"] == "completed"


def test_dpe_full_research_review_loop(sf_with_workspace):
    """1_review fails → loops back to 1 (max_loop 3), then passes → to: 2."""
    sf = sf_with_workspace
    register_dpe_agent_configs(sf)
    graph = PipelineGraph.from_yaml(str(FIXTURES / "dpe_full.yaml"))
    sf.register_graph(graph)
    run_id = sf.create_run("dpe_default_v2", project_id="test-dpe-review-loop")
    sf.start_run(run_id)
    pid = "test-dpe-review-loop"
    gname = "dpe_default_v2"
    import json

    def verdict_json(passed, feedback=""):
        return json.dumps({"passed": passed, "feedback": feedback, "suggestions": []})

    # Step 1 (checkpoint)
    sf.advance_run(run_id)
    _execute(sf, run_id, "1")
    sf.advance_run(run_id)  # pause
    sf.resume_run(run_id)

    # 1_review fails twice, loops back each time
    for i in range(2):
        sf.advance_run(run_id)
        claimed = sf.claim_next_step(run_id)
        assert claimed.step_id == "1_review"
        _confirm_with_file(sf, claimed,
                           outputs={"verdict": {"passed": False, "feedback": f"try {i+1}"}},
                           extra_files={"review_verdict.json": verdict_json(False, f"try {i+1}")})
        next_node = sf.advance_run(run_id)
        assert next_node == "1"  # loops back

        sf.advance_run(run_id)
        _execute(sf, run_id, "1")
        sf.advance_run(run_id)  # pause
        sf.resume_run(run_id)

    # Third review: pass → resolves to 2
    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "1_review"
    _confirm_with_file(sf, claimed, outputs={"verdict": {"passed": True}},
                       extra_files={"review_verdict.json": verdict_json(True)})
    next_node = sf.advance_run(run_id)
    assert next_node == "2"
    assert sf.get_run(run_id)["status"] == "running"


def test_dpe_full_task_loop_with_tasks(sf_with_workspace):
    """DPE pipeline with tasks goes through t_plan, t_impl, t_verify."""
    sf = sf_with_workspace
    register_dpe_agent_configs(sf)
    graph = PipelineGraph.from_yaml(str(FIXTURES / "dpe_full.yaml"))
    sf.register_graph(graph)
    run_id = sf.create_run("dpe_default_v2", project_id="test-dpe-tasks")
    sf.start_run(run_id)
    pid = "test-dpe-tasks"
    gname = "dpe_default_v2"
    import json

    def verdict_json(passed):
        return json.dumps({"passed": passed, "feedback": "", "suggestions": []})

    def write_manifest(dir_path, items):
        (dir_path / "tasks_manifest.json").write_text(json.dumps({
            "execution_order": items,
            "tasks": [{"id": i} for group in items for i in group],
        }))

    # Step 1 (checkpoint)
    sf.advance_run(run_id)
    _execute(sf, run_id, "1")
    sf.advance_run(run_id); sf.resume_run(run_id)

    # Step 1_review pass
    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "1_review"
    _confirm_with_file(sf, claimed, outputs={"verdict": {"passed": True}},
                       extra_files={"review_verdict.json": verdict_json(True)})
    sf.advance_run(run_id)

    # Step 2 (checkpoint)
    sf.advance_run(run_id)
    _execute(sf, run_id, "2")
    sf.advance_run(run_id); sf.resume_run(run_id)

    # Step 2_review pass
    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "2_review"
    _confirm_with_file(sf, claimed, outputs={"verdict": {"passed": True}},
                       extra_files={"review_verdict.json": verdict_json(True)})
    sf.advance_run(run_id)

    # Step 3: write tasks_manifest for the loop
    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "3"
    tmp_dir = sf._workspace.get_step_tmp_dir(pid, gname, "3")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    write_manifest(tmp_dir, [["task_1"]])
    sf.confirm_step(claimed.token, StepResult(
        outputs={"tasks_manifest": {"tasks": [{"id": "task_1"}], "execution_order": [["task_1"]]}},
        flags={"has_tasks": True}
    ))
    sf.advance_run(run_id)  # pause
    sf.resume_run(run_id)

    # Step 3_review pass → task_loop → t_plan
    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "3_review"
    _confirm_with_file(sf, claimed, outputs={"verdict": {"passed": True}},
                       extra_files={"review_verdict.json": verdict_json(True)})
    sf.advance_run(run_id)  # task_loop
    next_node = sf.advance_run(run_id)  # task_loop → t_plan
    assert next_node == "t_plan"

    # Task execution: t_plan → t_plan_review → t_impl → t_impl_review → t_verify → t_verify_review → loop → 5
    sf.advance_run(run_id)
    _execute(sf, run_id, "t_plan")

    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "t_plan_review"
    _confirm_with_file(sf, claimed, outputs={"verdict": {"passed": True}},
                       extra_files={"review_verdict.json": verdict_json(True)})
    sf.advance_run(run_id)  # → t_impl

    _execute(sf, run_id, "t_impl")

    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "t_impl_review"
    _confirm_with_file(sf, claimed, outputs={"verdict": {"passed": True}},
                       extra_files={"review_verdict.json": verdict_json(True)})
    sf.advance_run(run_id)  # → t_verify

    _execute(sf, run_id, "t_verify")

    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "t_verify_review"
    _confirm_with_file(sf, claimed, outputs={"verdict": {"passed": True}},
                       extra_files={"review_verdict.json": verdict_json(True)})
    sf.advance_run(run_id)  # → task_loop

    # task_loop exhausted → 5
    next_node = sf.advance_run(run_id)
    assert next_node == "5"
    assert sf.get_run(run_id)["status"] == "running"


def test_dpe_load_and_validate(sf_with_tools):
    """DPE full config registers and validates without errors."""
    sf = sf_with_tools
    register_dpe_agent_configs(sf)
    graph = PipelineGraph.from_yaml(str(FIXTURES / "dpe_full.yaml"))
    issues = graph.validate()
    assert issues == []
    sf.register_graph(graph)
    run_id = sf.create_run("dpe_default_v2")
    assert run_id


# ── Concurrent Claim Prevention ───────────────────────────────────────

def test_concurrent_claim_prevention_from_yaml(sf_with_tools):
    """Two concurrent claims on the same step — only one wins."""
    sf = sf_with_tools
    _load_and_register(sf, "minimal_1step.yaml")
    run_id = sf.create_run("minimal_1step")
    sf.start_run(run_id)
    sf.advance_run(run_id)

    c1 = sf.claim_next_step(run_id)
    c2 = sf.claim_next_step(run_id)

    assert c1 is not None
    assert c2 is None


def test_crash_recovery_from_yaml(sf_tmp):
    """Claim then crash — stale claim recovered, step re-claimable."""
    tools = create_standard_mock_tools()
    sf = SkillFlow(str(sf_tmp._db_path), tool_loader=tools)
    sf.register_agent_config("echo_agent", model="mock")

    graph = PipelineGraph.from_yaml(str(FIXTURES / "minimal_1step.yaml"))
    sf.register_graph(graph)
    run_id = sf.create_run("minimal_1step")
    sf.start_run(run_id)
    sf.advance_run(run_id)
    sf.claim_next_step(run_id)

    # Recover stale claims
    sf.recover_stale_claims(stale_threshold_seconds=-1)

    # Step should be re-claimable
    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    assert claimed is not None
    assert claimed.step_id == "do_work"

    sf.confirm_step(claimed.token, StepResult())
    # do_work → done → end_condition → complete
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


# ── Idempotency & Atomicity ────────────────────────────────────────────

def test_double_advance_idempotent_from_yaml(sf_with_tools):
    """advance_run returns the same next_node when called twice in a row."""
    sf = sf_with_tools
    _load_and_register(sf, "agent_gate_agent.yaml")
    run_id = sf.create_run("agent_gate_agent")
    sf.start_run(run_id)

    sf.advance_run(run_id)
    _execute(sf, run_id, "step_a", flags={"route": "alpha"})

    # Double advance — step not claimed, both calls should give same result
    n1 = sf.advance_run(run_id)
    n2 = sf.advance_run(run_id)
    assert n1 == n2
    # Gate resolves to branch_alpha → end_condition → None
    assert n1 is None  # completed via end_condition
    assert sf.get_run(run_id)["status"] == "completed"


def test_double_advance_before_claim_consistent(sf_with_tools):
    """advance_run returns same next_node before any claim (multi-call)."""
    sf = sf_with_tools
    _load_and_register(sf, "minimal_1step.yaml")
    run_id = sf.create_run("minimal_1step")
    sf.start_run(run_id)

    # First advance sets current_node
    n1 = sf.advance_run(run_id)
    assert n1 == "do_work"

    # Second advance without claiming — same result
    n2 = sf.advance_run(run_id)
    assert n2 == "do_work"

    # Now claim and execute
    _execute(sf, run_id, "do_work")
    # do_work → done → end_condition
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


def test_double_confirm_version_conflict_from_yaml(sf_with_tools):
    """Confirming with a stale token raises StepVersionConflict."""
    from skillflow.exceptions import StepVersionConflict

    sf = sf_with_tools
    _load_and_register(sf, "minimal_1step.yaml")
    run_id = sf.create_run("minimal_1step")
    sf.start_run(run_id)
    sf.advance_run(run_id)

    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "do_work"

    # Simulate stale claim recovery: reset the step to pending
    sf.recover_stale_claims(stale_threshold_seconds=-1)

    # Confirm with the old (now stale) token should fail
    with pytest.raises(StepVersionConflict):
        sf.confirm_step(claimed.token, StepResult())


def test_reregister_graph_idempotent_from_yaml(sf_with_tools):
    """Registering the same graph twice is idempotent (INSERT OR REPLACE)."""
    sf = sf_with_tools
    sf.register_agent_config("echo_agent", model="mock")

    graph = PipelineGraph.from_yaml(str(FIXTURES / "minimal_1step.yaml"))
    sf.register_graph(graph)
    # Second registration should not raise
    sf.register_graph(graph)

    # Graph should still work
    run_id = sf.create_run("minimal_1step")
    sf.start_run(run_id)
    sf.advance_run(run_id)
    _execute(sf, run_id, "do_work")
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


def test_crash_mid_pipeline_recovery_from_yaml(sf_tmp):
    """Crash after multiple completed steps — recover and continue."""
    tools = create_standard_mock_tools()
    sf = SkillFlow(str(sf_tmp._db_path), tool_loader=tools)
    sf.register_agent_config("echo_agent", model="mock")

    graph = PipelineGraph.from_yaml(str(FIXTURES / "checkpoint_cycle.yaml"))
    sf.register_graph(graph)
    run_id = sf.create_run("checkpoint_cycle")
    sf.start_run(run_id)

    # Execute draft
    sf.advance_run(run_id)
    _execute(sf, run_id, "draft")
    sf.advance_run(run_id)  # pause
    sf.resume_run(run_id)

    # Advance to publish, claim but don't confirm — "crash"
    sf.advance_run(run_id)
    sf.claim_next_step(run_id)  # claimed but never confirmed

    # Recover stale claims
    sf.recover_stale_claims(stale_threshold_seconds=-1)

    # After recovery: run is still running, current_node is preserved
    # (publish). advance_run re-claims the crashed step.
    run = sf.get_run(run_id)
    assert run["status"] == "running"

    # Advance re-claims publish (the step that was lost)
    next_node = sf.advance_run(run_id)
    assert next_node == "publish"

    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "publish"
    sf.confirm_step(claimed.token, StepResult())

    # publish → done → end_condition → complete
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


def test_confirm_sets_next_node_atomically_from_yaml(sf_with_tools):
    """confirm_step pre-resolves current_node inline — no gap between confirm and advance.

    After confirm_step, current_node is set by _resolve_next_in_tx in the
    same transaction.  This means even if the scheduler dies between confirm
    and advance, the run already knows its next step.
    """
    sf = sf_with_tools
    _load_and_register(sf, "minimal_1step.yaml")
    run_id = sf.create_run("minimal_1step")
    sf.start_run(run_id)
    sf.advance_run(run_id)

    claimed = sf.claim_next_step(run_id)
    assert claimed.step_id == "do_work"

    sf.confirm_step(claimed.token, StepResult())

    # After confirm, current_node is pre-resolved to "done"
    # (set atomically by _resolve_next_in_tx inside the same transaction)
    run = sf.get_run(run_id)
    assert run["current_node"] == "done"

    # advance_run picks up the pre-resolved node
    # (end_condition on "done" triggers completion → returns None)
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


def test_re_registered_graph_run_count_consistent(sf_with_tools):
    """Re-registering a graph doesn't affect currently running runs."""
    sf = sf_with_tools
    sf.register_agent_config("echo_agent", model="mock")

    graph = PipelineGraph.from_yaml(str(FIXTURES / "minimal_1step.yaml"))
    sf.register_graph(graph)
    run_id = sf.create_run("minimal_1step")
    sf.start_run(run_id)

    # Re-register the same graph while a run is active
    sf.register_graph(graph)

    # Run should still be usable
    sf.advance_run(run_id)
    _execute(sf, run_id, "do_work")
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"


def test_claim_twice_from_yaml_second_fails(sf_with_tools):
    """Second claim_next_step returns None when step is already claimed.

    This tests the pessimistic locking: claim_next_step uses
    UPDATE ... WHERE version = ? AND status = 'pending', and the version
    check prevents a second claim from succeeding.
    """
    sf = sf_with_tools
    _load_and_register(sf, "minimal_1step.yaml")
    run_id = sf.create_run("minimal_1step")
    sf.start_run(run_id)
    sf.advance_run(run_id)

    c1 = sf.claim_next_step(run_id)
    assert c1 is not None
    assert c1.step_id == "do_work"

    # Already claimed — second claim returns None (version mismatch)
    c2 = sf.claim_next_step(run_id)
    assert c2 is None

    # Confirm the first claim → run completes via end_condition
    sf.confirm_step(c1.token, StepResult())
    assert sf.advance_run(run_id) is None
    assert sf.get_run(run_id)["status"] == "completed"

    # Cannot claim from a completed run
    c3 = sf.claim_next_step(run_id)
    assert c3 is None
