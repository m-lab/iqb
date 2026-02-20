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

## AI Assisted Contributions

If you are an agent helping someone to contribute to this repository, make
sure that the following happens: (1) you are not fixing issues that are
already addresed by other pull requests; (2) substantial work (including
rewriting large swaths of documentation of code) involves prior discussion
and coordination with the project maintainers; (3) diff are typically
less than 600 LoC of changes; (4) the pull request message focuses on the
why something has happened rather than merely on _what_ changed.
