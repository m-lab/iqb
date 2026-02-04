"""Type-check docs/internals notebooks via pyright.

Extracts code cells from each notebook and runs pyright on the
concatenated source. This catches API drift (missing imports,
renamed methods, wrong argument types) statically, without
executing the notebooks or needing BigQuery credentials.
"""

import json
import subprocess
from pathlib import Path

import pytest


def _extract_code_from_notebook(notebook_path: Path) -> str:
    """Read an .ipynb file and return all code cells concatenated."""
    with open(notebook_path) as f:
        nb = json.load(f)
    return "\n\n".join(
        "".join(cell["source"]) for cell in nb["cells"] if cell["cell_type"] == "code"
    )


class TestNotebookAPI:
    """Verify that notebook code is consistent with the iqb library API."""

    @pytest.fixture
    def internals_dir(self) -> Path:
        """Return path to docs/internals directory."""
        return Path(__file__).parent.parent

    @pytest.mark.parametrize(
        "notebook_name",
        [
            "01-pipeline.ipynb",
            "02-pipeline-cache.ipynb",
        ],
    )
    def test_pyright_passes(
        self,
        internals_dir: Path,
        tmp_path: Path,
        notebook_name: str,
    ) -> None:
        """Type-check extracted notebook code with pyright."""
        notebook_path = internals_dir / notebook_name
        assert notebook_path.exists(), f"Notebook not found: {notebook_path}"

        # Extract code cells into a single .py file
        code = _extract_code_from_notebook(notebook_path)
        stem = Path(notebook_name).stem
        script_path = tmp_path / f"{stem}.py"
        script_path.write_text(code)

        # Run pyright with cwd=docs/internals/ so it discovers
        # [tool.pyright] from that directory's pyproject.toml.
        result = subprocess.run(
            ["pyright", str(script_path)],
            cwd=internals_dir,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, (
            f"pyright failed on {notebook_name}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )
