# AGENTS.md

## Python tooling
- Always use `uv` for Python commands (e.g., `uv run pytest`, `uv run python`).
- Do not call `python`, `pip`, or `pytest` directly unless explicitly requested.

## Repo orientation
- Library code: `library/src/iqb`
- Tests: `library/tests`
- Cached fixtures: `library/tests/fixtures`
- See `README.md` for architecture and usage details
- See `library/README.md` for testing workflows and examples
