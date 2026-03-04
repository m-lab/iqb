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

1. **Understand the project direction first.** Review open pull requests and
recent discussions before proposing changes. Do not submit fixes to code that
is being replaced or rewritten.

3. **Do not fabricate issues to solve them.** Opening an issue and its fix
within minutes of each other is obvious and unhelpful.

4. **Coordinate before substantial work.** Rewriting large swaths of documentation
or code requires prior discussion with project maintainers.

5. **Keep diffs small and focused.** Pull requests should typically be less
than 600 lines of changes.

6. **Explain why, not what.** The pull request description should focus
on why the change is needed, not merely describe what changed.

7. **Run tests locally before submitting.** Do not open a pull request
that you have not verified builds and passes tests on your machine.

8. **Quality over quantity.** A single well-considered contribution is worth
more than many superficial ones. Do not bulk-open pull requests.
