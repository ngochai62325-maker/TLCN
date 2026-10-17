import copy
import json
from decimal import Decimal

import pytest

from silver.transformers import nso


def observation(table, geo, year, measure, season, value):
    return dict(zip(nso.NsoTransformer.business_keys,
                    (table, nso.source_geography_key(geo), year, measure, season, "quantity")),
                value_source=Decimal(value))


def test_mapping_revision_cannot_change_source_identity():
    raw = "H\u00e0 T\u00e2y"
    first = nso.source_geography_key(raw)
    metadata = nso.NsoTransformer().metadata
    metadata["geography_candidates"][raw] = ["reviewed candidate name", "unknown"]
    assert nso.source_geography_key(raw) == first
    assert first != nso.source_geography_key("H\u00e0 N?i")
    assert first != nso.source_geography_key(raw + " ")


def test_missing_or_mismatched_unit_evidence_fails_closed(tmp_path, monkeypatch):
    data = json.loads(nso.INVENTORY_PATH.read_text(encoding="utf-8"))
    for field in ["unit_reference", "unit_evidence"]:
        changed = copy.deepcopy(data)
        changed["tables"]["V06.14.csv"].pop(field)
        path = tmp_path / "tampered.json"
        path.write_text(json.dumps(changed), encoding="utf-8")
        monkeypatch.setattr(nso, "INVENTORY_PATH", path)
        with pytest.raises(ValueError, match="metadata/evidence checksum"):
            nso.NsoTransformer()


def test_diagnostic_tolerance_does_not_hide_cross_table_point_one():
    policy = nso.NsoTransformer().metadata["diagnostic_policy"]
    rows = [observation("V06.12.csv", None, 2021, "area", "annual", "7238.8"),
            observation("V06.13.csv", "C? NU?C", 2021, "area", "annual", "7238.9")]
    evidence = nso.discrepancy_evidence(rows, policy)
    assert len(evidence) == 2
    assert all(v[0]["rule_id"] == "NSO_CROSS_TABLE" for v in evidence.values())
    rows[1]["value_source"] = rows[0]["value_source"]
    assert not nso.discrepancy_evidence(rows, policy)


@pytest.mark.parametrize("annual,warning", [("3.2", False), ("3.3", True)])
def test_seasonal_rounding_envelope_boundary(annual, warning):
    policy = nso.NsoTransformer().metadata["diagnostic_policy"]
    rows = [observation("V06.12.csv", None, 2024, "production", "annual", annual)]
    rows += [observation("V06.12.csv", None, 2024, "production", season, "1.0")
             for season in ["winter_spring", "summer_autumn", "main_season"]]
    assert bool(nso.discrepancy_evidence(rows, policy)) == warning


def test_hanoi_arithmetic_is_warning_without_repair():
    policy = nso.NsoTransformer().metadata["diagnostic_policy"]
    rows = [observation(f"V06.{n}.csv", "H\u00e0 N?i", 2007, measure, "annual", value)
            for n, measure, value in [(13, "area", "51.3"), (14, "yield", "42.5"), (15, "production", "184.2")]]
    before = copy.deepcopy(rows)
    warnings = nso.discrepancy_evidence(rows, policy)
    assert len(warnings) == 3 and rows == before
    assert all(v[0]["rule_id"] == "NSO_AREA_YIELD_PRODUCTION" for v in warnings.values())
