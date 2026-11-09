"""Import public job modules through package names; no sys.path manipulation."""
import importlib
import subprocess
import sys

import pytest


@pytest.mark.parametrize("name", ["run_all_silver", "validate_silver_e2e", "migrate_empty_silver_schema",
    "job_faostat_production", "job_faostat_monthly_price", "job_faostat_supply_utilization", "job_faostat_trade",
    "job_nso", "job_thitruongnongsan", "job_usda_export_price", "job_usda_psd", "job_worldbank", "verify_silver_trino"])
def test_module_import_has_no_execution_side_effect(name):
    assert callable(importlib.import_module("jobs.silver." + name).main)


@pytest.mark.parametrize("module", ["jobs.silver.job_nso", "jobs.silver.validate_silver_e2e", "jobs.silver.verify_silver_trino"])
def test_cli_help_without_spark_session_or_catalog(module):
    result = subprocess.run([sys.executable, "-m", module, "--help"], capture_output=True, text=True, timeout=15)
    assert result.returncode == 0 and "usage:" in result.stdout
