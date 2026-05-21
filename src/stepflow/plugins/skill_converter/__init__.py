"""Skill Converter — converts skill descriptions into stepflow pipeline configs.

Runs a fixed converter pipeline (skill_converter.yaml) where each agent
step is executed by the provided SkillTool (which delegates to the host LLM).
The linter's stepflow_lint tool provides the validation feedback loop.
"""

from pathlib import Path

from stepflow.plugins.skill_converter.converter import setup_converter, get_output_file, save_output


def load_agent_guide() -> str:
    """Return the AGENT.md content — usage guide for LLM agents using the converter."""
    return (Path(__file__).parent / "AGENT.md").read_text(encoding="utf-8")


__all__ = ["setup_converter", "get_output_file", "save_output", "load_agent_guide"]
