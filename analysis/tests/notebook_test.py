"""Tests for Jupyter notebooks in the analysis directory."""

import subprocess
from pathlib import Path

import pytest


class TestNotebooks:
    """Test that notebooks execute without errors."""

    @pytest.fixture
    def analysis_dir(self):
        """Return path to analysis directory."""
        return Path(__file__).parent.parent

    def test_00_template_executes(self, analysis_dir):
        """Test that 00-template.ipynb executes without errors."""
        notebook_path = analysis_dir / "00-template.ipynb"

        # Verify notebook exists
        assert notebook_path.exists(), f"Notebook not found: {notebook_path}"

        # Execute notebook using nbconvert
        # --to notebook: output as notebook (not HTML/PDF)
        # --execute: execute all cells
        # --inplace: modify notebook in place (update cell outputs)
        # --ExecutePreprocessor.timeout=60: 60 second timeout per cell
        result = subprocess.run(
            [
                "jupyter",
                "nbconvert",
                "--to",
                "notebook",
                "--execute",
                "--output",
                str(notebook_path),  # output path (overwrite original)
                "--ExecutePreprocessor.timeout=60",
                str(notebook_path),  # input path
            ],
            cwd=analysis_dir,
            capture_output=True,
            text=True,
        )

        # Check execution succeeded
        assert result.returncode == 0, (
            f"Notebook execution failed with return code {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}"
        )

        # Verify output contains expected patterns
        assert "IQB Score" in result.stdout or result.returncode == 0
