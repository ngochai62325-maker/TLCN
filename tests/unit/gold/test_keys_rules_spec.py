"""Pure protocol/rule checks. These tests do not certify runtime Gold data."""
from datetime import date
from decimal import Decimal
import hashlib
import json

import pytest

from gold.keys import canonical_json, key
from gold.rules import checked_decimal, metric_rule, PSD, SUA
from gold.spec import load_spec, validate_mapping
from gold.api import command
from gold import BUILD_ORDER


def test_mapping_matches_all_current_contracts():
    assert validate_mapping() == {"tables": 17, "columns": 262, "mapping_branches": 488, "foreign_keys": 40}
    specs = load_spec()
    assert sum(n.startswith("dim_") for n in specs) == 8
    assert sum(n.startswith("fact_") for n in specs) == 9


def test_k1_exact_protocol():
    payload = '["K1","dim_source_product",["nso","Lúa gạo",null,2024]]'
    assert key("dim_source_product", ["nso", "Lúa gạo", None, 2024]) == hashlib.sha256(payload.encode()).hexdigest()


@pytest.mark.parametrize("a,b", [(None, "null"), (1, "1"), ("", None), ("é", "e\u0301"), (["a|b", "c"], ["a", "b|c"]), (["faostat", "704"], ["usda", "704"])])
def test_identity_distinctions(a, b):
    assert key("test", [a]) != key("test", [b])


def test_nested_source_tuple_and_input_order():
    tuples = [["V06.12", "geo", 2024, "area", "annual", "quantity"], ["V06.13", "geo", 2024, "area", "annual", "quantity"]]
    original = {canonical_json(x): key("observation", ["nso", x]) for x in tuples}
    rebuilt = {canonical_json(x): key("observation", ["nso", x]) for x in reversed(tuples)}
    assert original == rebuilt
    assert len(set(original.values())) == 2


def test_canonical_context_lossless_decimal_null_and_date():
    result = canonical_json({"z": Decimal("1.23000000"), "a": None, "date": date(2024, 2, 29), "evidence": '{"raw":  1}'})
    assert result == '{"a":null,"date":"2024-02-29","evidence":"{\\"raw\\":  1}","z":"1.23000000"}'
    with pytest.raises(ValueError):
        date(2023, 2, 29)


@pytest.mark.parametrize("value", [1.5, Decimal("1.5"), object()])
def test_invalid_key_types_rejected(value):
    with pytest.raises(TypeError):
        key("test", [value])


@pytest.mark.parametrize("metric,unit", PSD.items())
def test_all_fifteen_psd_rules(metric, unit):
    gold, factor, aggregation = metric_rule("usda_psd", metric, unit)
    assert factor == (1000 if unit.startswith("1000 ") else 1)
    assert aggregation
    assert gold not in {"1000 HA", "1000 MT"}


@pytest.mark.parametrize("metric", SUA)
def test_all_twelve_sua_rules(metric):
    unit, factor, aggregation = metric_rule("faostat_supply_utilization", metric, "tonne")
    assert (unit, factor) == ("tonne", Decimal(1))
    if metric in {"5071", "5166"}:
        assert aggregation == "SIGNED_FLOW"


@pytest.mark.parametrize("source,metric,unit", [("faostat_supply_utilization", "5123", "tonne"), ("faostat_trade", "5622", "USD"), ("usda_psd", "Milling Rate (.9999)", "1000 MT"), ("faostat_production", "5510", "kg")])
def test_unreviewed_rules_fail(source, metric, unit):
    with pytest.raises(ValueError):
        metric_rule(source, metric, unit)


@pytest.mark.parametrize("value,factor,p,s,expected", [("9999999999999999.99", 1, 20, 4, "9999999999999999.9900"), ("9999999999999999.99", 1000, 22, 2, "9999999999999999990.00"), ("0.6250", 1, 22, 4, "0.6250"), ("-12.50", 1, 18, 2, "-12.50"), ("0", 1, 18, 2, "0.00")])
def test_decimal_range_sign_zero(value, factor, p, s, expected):
    assert checked_decimal(Decimal(value), factor, p, s) == Decimal(expected)
    assert checked_decimal(None, factor, p, s) is None


@pytest.mark.parametrize("value,p,s", [("9999999999999999.99", 18, 4), ("1.001", 18, 2)])
def test_overflow_and_rounding_block(value, p, s):
    with pytest.raises(ValueError):
        checked_decimal(Decimal(value), 1, p, s)


@pytest.mark.parametrize("extra", [{"job": "../../danger.py"}, {"action": "shell"}, {"source": "unknown", "action": "fact"}, {"command": "anything"}, {"owner_run_id": ""}, {"profile": "production"}, {"source": "nso"}])
def test_api_rejects_unapproved_commands(extra):
    request = {"job": "run_gold.py", "profile": "local_nine", "action": "preflight", "owner_run_id": "manual__gold"}
    request.update(extra)
    with pytest.raises(ValueError):
        command(request, "/workspace")


def test_all_source_actions_and_no_shell():
    for source in BUILD_ORDER:
        args = command({"job": "run_gold.py", "profile": "local_nine", "action": "fact", "source": source, "owner_run_id": "run$(literal)"}, "/workspace")
        assert isinstance(args, list)
        assert args[-1] == source
        assert "run$(literal)" in args
