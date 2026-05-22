"""Skill Runner — stateless CLI facade for LLM agents to execute skillflow pipelines.

The agent calls ``skillflow-run --action start --graph pipeline.yaml`` to begin,
does the work, then calls ``skillflow-run --action submit --run-id <id> --result '...'``
to hand in output. Each call is a fresh process — state lives in SQLite.
The agent never knows about the graph structure — skillflow handles
gates, loops, checkpoints, and error routing behind the tool facade.
"""

from pathlib import Path

from skillflow.plugins.skill_runner.runner import SkillTool, SkillResponse, PromptAssembler


def load_agent_guide() -> str:
    """Return the AGENT.md content — usage guide for LLM agents using skillflow-run CLI."""
    return (Path(__file__).parent / "AGENT.md").read_text(encoding="utf-8")


__all__ = ["SkillTool", "SkillResponse", "PromptAssembler", "load_agent_guide"]
