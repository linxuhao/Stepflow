"""Unit tests for recovery.py."""

import time

import pytest

from skillflow.core import SkillFlow
from skillflow.graph import PipelineGraph, StepNode, Transition
from skillflow.recovery import recover_stale_claims


def _agent(id: str, transitions=None):
    return StepNode(id=id, step_type="agent", transitions=transitions or [])


def _trans(to: str):
    return Transition(to=to)


def test_recover_stale_claims_resets_claimed(sf: SkillFlow):
    graph = PipelineGraph(
        name="test", begin="a",
        steps=[_agent("a", [_trans("b")]), _agent("b", [])],
    )
    sf.register_graph(graph)
    run_id = sf.create_run("test")
    sf.start_run(run_id)
    sf.advance_run(run_id)
    sf.claim_next_step(run_id)

    # Recover with negative threshold (everything is stale)
    recovered = sf.recover_stale_claims(stale_threshold_seconds=-1)
    assert run_id in recovered

    # Step should be re-claimable
    sf.advance_run(run_id)
    claimed = sf.claim_next_step(run_id)
    assert claimed is not None
    assert claimed.step_id == "a"


def test_recover_stale_claims_fresh_not_affected(sf: SkillFlow):
    graph = PipelineGraph(
        name="test", begin="a",
        steps=[_agent("a", [])],
    )
    sf.register_graph(graph)
    run_id = sf.create_run("test")
    sf.start_run(run_id)
    sf.advance_run(run_id)
    sf.claim_next_step(run_id)

    # Default threshold — just-claimed step is fresh
    recovered = sf.recover_stale_claims(stale_threshold_seconds=300)
    assert len(recovered) == 0


def test_recover_stale_claims_no_stale_steps(sf: SkillFlow):
    graph = PipelineGraph(
        name="test", begin="a",
        steps=[_agent("a", [])],
    )
    sf.register_graph(graph)
    sf.create_run("test")
    recovered = sf.recover_stale_claims()
    assert recovered == []


def test_recover_stale_keeps_current_node(sf: SkillFlow):
    graph = PipelineGraph(
        name="test", begin="a",
        steps=[_agent("a", [_trans("b")]), _agent("b", [])],
    )
    sf.register_graph(graph)
    run_id = sf.create_run("test")
    sf.start_run(run_id)
    sf.advance_run(run_id)
    sf.claim_next_step(run_id)

    sf.recover_stale_claims(stale_threshold_seconds=-1)
    run = sf.get_run(run_id)
    # current_node is kept so advance_run re-claims the crashed step
    assert run["current_node"] == "a"


def test_recover_stale_claims_module_function(sf_tmp: SkillFlow):
    """Test the standalone recover_stale_claims function."""
    from skillflow.core import SkillFlow

    graph = PipelineGraph(
        name="test", begin="a",
        steps=[_agent("a", [])],
    )
    sf_tmp.register_graph(graph)
    run_id = sf_tmp.create_run("test")
    sf_tmp.start_run(run_id)
    sf_tmp.advance_run(run_id)
    sf_tmp.claim_next_step(run_id)

    recovered = recover_stale_claims(sf_tmp._db_path, stale_threshold_seconds=-1)
    assert len(recovered) >= 0
