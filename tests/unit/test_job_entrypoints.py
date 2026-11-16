"""Import public job modules through package names; no sys.path manipulation."""
import importlib
import subprocess
import sys

import pytest


@pytest.mark.parametrize("name", ["run_all_silver"])
def test_module_import_has_no_execution_side_effect(name):
    assert callable(importlib.import_module("jobs.silver." + name).main)


@pytest.mark.parametrize("module", ["jobs.silver.run_all_silver", "jobs.gold.run_gold"])
def test_cli_help_without_spark_session_or_catalog(module):
    result = subprocess.run([sys.executable, "-m", module, "--help"], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0 and "usage:" in result.stdout
