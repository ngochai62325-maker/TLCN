from unittest.mock import patch
from pathlib import Path

import pytest
import yaml

from jobs.silver.verify_silver_trino import verify_nso


def fixtures():
    contract = yaml.safe_load(Path("contracts/silver/nso.yaml").read_text(encoding="utf-8"))
    types = {"string": "varchar", "integer": "integer", "long": "bigint"}
    schema = [{"Column": c["name"], "Type": types.get(c["data_type"], c["data_type"])} for c in contract["schema"]["columns"]]
    counts = dict(rows=6, unique_keys=6, invalid_required=0, unknown_geography=2,
                  missing_observations=1, genuine_zero=1, provisional=1, unconfirmed=5)
    counts.update({r["rule_id"].lower(): 0 for r in contract["data_quality"]})
    counts["nso_v2_geography_review"] = 2
    return schema, counts


def test_unknown_missing_zero_and_review_are_valid():
    schema, counts = fixtures()
    with patch("jobs.silver.verify_silver_trino.query", side_effect=[schema, [counts]]) as query:
        assert verify_nso("http://trino:8080")["status"] == "PASSED"
        sql = query.call_args.args[1]
    assert '"iceberg"."silver_local"."nso_rice_statistics"' in sql
    assert "geography_name_vi" not in sql and "UNMAPPED" not in sql
    assert "to_hex(sha256(to_utf8(geography_raw)))" in sql
    assert "coalesce" in sql and "regexp_like" in sql


@pytest.mark.parametrize("bad", ["invalid_required", "nso_v2_grain", "nso_v2_missing", "nso_v2_units",
                                      "nso_v2_geography", "nso_v2_period"])
def test_invalid_contract_rule_fails(bad):
    schema, counts = fixtures()
    counts[bad] = 1
    with patch("jobs.silver.verify_silver_trino.query", side_effect=[schema, [counts]]):
        with pytest.raises(ValueError, match="violation"):
            verify_nso("url")


def test_schema_and_duplicates_fail():
    schema, counts = fixtures()
    with patch("jobs.silver.verify_silver_trino.query", return_value=schema[1:]):
        with pytest.raises(ValueError, match="schema mismatch"):
            verify_nso("url")
    counts["unique_keys"] = 5
    with patch("jobs.silver.verify_silver_trino.query", side_effect=[schema, [counts]]):
        with pytest.raises(ValueError, match="violation"):
            verify_nso("url")


def test_cli_failure_replaces_stale_pass_artifact(tmp_path):
    import json
    from jobs.silver.verify_silver_trino import main
    report = tmp_path / "result.json"
    report.write_text('{"status":"PASSED"}', encoding="utf-8")
    with patch("sys.argv", ["verify", "--nso-only", "--output-json", str(report)]), \
         patch("jobs.silver.verify_silver_trino.verify_nso", side_effect=ValueError("invalid source identity")):
        with pytest.raises(ValueError):
            main()
    assert json.loads(report.read_text(encoding="utf-8")) == {"status": "FAILED", "error": "invalid source identity"}
