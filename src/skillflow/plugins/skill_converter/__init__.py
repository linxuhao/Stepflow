"""Skill Converter — converts skill descriptions into skillflow pipeline configs.

Runs a fixed converter pipeline (skill_converter.yaml). Agents drive it via
the ``skillflow-convert`` CLI — each call is a fresh process.
The linter's skillflow_lint tool provides the validation feedback loop.
"""

from pathlib import Path

from skillflow.plugins.skill_converter.converter import setup_converter, get_output_file, save_output


def load_agent_guide() -> str:
    """Return the AGENT.md content — usage guide for LLM agents using skillflow-convert CLI."""
    return (Path(__file__).parent / "AGENT.md").read_text(encoding="utf-8")


__all__ = ["setup_converter", "get_output_file", "save_output", "load_agent_guide"]
