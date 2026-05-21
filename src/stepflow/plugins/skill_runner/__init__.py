"""Skill Runner — tool-facade for LLM agents to execute stepflow pipelines.

The agent calls ``run_skill(action="next")`` to get work, does it,
then calls ``run_skill(action="submit", result=...)`` to hand in output.
The agent never knows about the graph structure — stepflow handles
gates, loops, checkpoints, and error routing behind the tool facade.
"""

from pathlib import Path

from stepflow.plugins.skill_runner.runner import SkillTool, SkillResponse, PromptAssembler


def load_agent_guide() -> str:
    """Return the AGENT.md content — a user manual for LLM agents using run_skill."""
    return (Path(__file__).parent / "AGENT.md").read_text(encoding="utf-8")


__all__ = ["SkillTool", "SkillResponse", "PromptAssembler", "load_agent_guide"]
