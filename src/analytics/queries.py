"""Finite templates and typed filters. No caller-supplied SQL/identifiers."""
import hashlib
import json
import re
from pathlib import Path
from analytics import VERSION

ROOT = Path(__file__).resolve().parents[2]
PERIODS = {
    "fact_nso_observation": "year_key", "fact_fao_production_observation": "year_key",
    "fact_fao_sua_observation": "year_key", "fact_fao_trade_observation": "year_key",
    "fact_usda_psd_observation": "market_year", "fact_price_monthly": "month_key",
    "fact_fao_price_annual": "year_key", "fact_usda_price_reference": "reference_year",
    "fact_vn_market_quote_daily": "date_key",
}
DIMENSIONS = {"sources": "dim_source_system", "geographies": "dim_source_geography",
              "products": "dim_source_product", "metrics": "dim_observation_type", "series": "dim_price_series"}
NAMES = ("publication", "catalogue", "fact_overview", "period_coverage", "nso_catalogue", "nso_observations",
         "fao_production", "sua", "psd", "price_observations", "price_coverage", "trade", "context_profile", "subject_products",
         "nso_scope_catalogue")


def integer(value, lo, hi):
    if type(value) is not int or not lo <= value <= hi:
        raise ValueError(f"Integer required in [{lo},{hi}]")
    return value


def choice(value, options):
    if not isinstance(value, str) or value not in options:
        raise ValueError("Value outside exact allowlist")
    return value


def literal(value, pattern=r"[A-Za-z0-9_. -]{1,80}"):
    if not isinstance(value, str) or not re.fullmatch(pattern, value):
        raise ValueError("Invalid typed string parameter")
    return "'" + value + "'"


def key(value):
    return literal(value, r"[0-9a-f]{64}")


def validate(name, params):
    choice(name, NAMES)
    if type(params) is not dict:
        raise ValueError("Parameters must be an object")
    required = {
        "publication": set(), "catalogue": {"kind"}, "fact_overview": {"fact"}, "period_coverage": {"fact"},
        "nso_catalogue": set(), "nso_observations": {"source_table", "start_year", "end_year", "level"},
        "nso_scope_catalogue": {"source_table", "level"},
        "fao_production": {"geographies", "product", "start_year", "end_year"},
        "sua": {"geography", "product", "start_year", "end_year"}, "psd": {"geography", "product", "start_year", "end_year"},
        "price_observations": {"fact", "series", "start_period", "end_period"},
        "price_coverage": {"fact"},
        "trade": {"reporter", "product", "flow", "start_year", "end_year"},
        "context_profile": {"fact", "field"},
        "subject_products": {"fact", "geography"},
    }[name]
    optional = {"geography_key"} if name == "nso_observations" else set()
    if not required <= set(params) or set(params) - required - optional:
        raise ValueError(f"Expected exactly parameters {sorted(required)}")
    p = dict(params)
    if "fact" in p:
        choice(p["fact"], PERIODS)
    if name == "catalogue":
        choice(p["kind"], DIMENSIONS)
    if name == "subject_products":
        choice(p["fact"], ("fact_fao_production_observation", "fact_fao_sua_observation", "fact_fao_trade_observation", "fact_usda_psd_observation"))
    if "start_year" in p:
        a, b = integer(p["start_year"], 1900, 2100), integer(p["end_year"], 1900, 2100)
        if a > b or b-a > (9 if name == "trade" else 70):
            raise ValueError("Year interval too wide/reversed")
    for k in ("geography", "geography_key", "product", "reporter", "series"):
        if k in p:
            key(p[k])
    if "geographies" in p:
        if (type(p["geographies"]) is not list or not 1 <= len(p["geographies"]) <= 8
                or not all(isinstance(v,str) for v in p["geographies"]) or len(set(p["geographies"])) != len(p["geographies"])):
            raise ValueError("Choose 1–8 distinct source geography keys")
        for v in p["geographies"]:
            key(v)
    if name in ("nso_observations", "nso_scope_catalogue"):
        literal(p["source_table"], r"V[0-9]{2}\.[0-9]{2}\.csv")
        choice(p["level"], ("national", "region", "province", "unknown"))
    if name in ("price_observations", "price_coverage"):
        choice(p["fact"], ("fact_price_monthly", "fact_fao_price_annual", "fact_usda_price_reference", "fact_vn_market_quote_daily"))
    if name == "price_observations":
        a, b = integer(p["start_period"], 1900, 21001231), integer(p["end_period"], 1900, 21001231)
        if a > b:
            raise ValueError("Reversed price periods")
        from datetime import datetime
        kind = PERIODS[p["fact"]]
        fmt = "%Y%m%d" if kind=="date_key" else "%Y%m" if kind=="month_key" else "%Y"
        first,last=datetime.strptime(str(a),fmt),datetime.strptime(str(b),fmt)
        if (last-first).days > (8*366 if kind=="date_key" else 100*366):
            raise ValueError("Price interval too wide")
    if name == "trade":
        choice(p["flow"], ("import", "export"))
    if name == "context_profile":
        choice(p["field"], ("flag", "unit", "release_status"))
        if p["field"] == "release_status" and p["fact"] != "fact_nso_observation":
            raise ValueError("Release status only exists for NSO")
    return p


def compile_query(name, params, manifest):
    p = validate(name, params)
    if name == "publication":
        return None, hashlib.sha256(VERSION.encode()).hexdigest()
    sql = (ROOT / "queries/analytics" / (name + ".sql")).read_text(encoding="utf-8").strip()
    if not sql.startswith("SELECT ") or re.search(r";|--|/\*|\b(CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|MERGE|CALL|EXECUTE|INTO)\b", sql, re.I):
        raise ValueError("Unsafe analytical template")
    if re.search(r"\b(?:FROM|JOIN)\s+(?!\(|\{\{)[\w.]", sql, re.I):
        raise ValueError("Every template base relation must be a verified binding")
    definition_hash = hashlib.sha256((VERSION + "\n" + name + "\n" + sql).encode()).hexdigest()
    def relation(table):
        from gold.spec import load_spec
        if table not in load_spec() or table not in manifest["outputs"]:
            raise ValueError("Relation outside published physical Gold inventory")
        output = manifest["outputs"][table]
        snap = output.get("snapshot_id")
        if not output.get("uuid") or not str(snap).isdigit() or not 0 < int(snap) < 2**63:
            raise ValueError("Missing snapshot/UUID binding")
        return f"iceberg.gold_local.{table} FOR VERSION AS OF {int(snap)}"
    tokens = {}
    if "fact" in p:
        tokens.update(fact=relation(p["fact"]), period_column=PERIODS[p["fact"]])
    if name == "catalogue":
        tokens["dimension"] = relation(DIMENSIONS[p["kind"]])
    if name == "subject_products":
        tokens["geography_column"] = "reporter_geography_key" if p["fact"]=="fact_fao_trade_observation" else "geography_key"
        tokens["geography"] = key(p["geography"])
    if name == "nso_observations":
        tokens["filters"] = (f"f.source_table={literal(p['source_table'])} AND f.year_key BETWEEN {p['start_year']} AND {p['end_year']} "
                             f"AND g.source_level={literal(p['level'])}")
        if "geography_key" in p:
            tokens["filters"] += " AND f.geography_key=" + key(p["geography_key"])
    if name == "nso_scope_catalogue":
        tokens["filters"] = f"f.source_table={literal(p['source_table'])} AND g.source_level={literal(p['level'])}"
    if name in ("fao_production", "sua", "trade"):
        from gold.views import load_definitions
        view, fact = {"fao_production": ("vw_fao_production_yield", "fact_fao_production_observation"),
                      "sua": ("vw_sua_supported_balance", "fact_fao_sua_observation"),
                      "trade": ("vw_trade_reported_unit_values", "fact_fao_trade_observation")}[name]
        filtered = f"SELECT * FROM {relation(fact)} WHERE year_key BETWEEN {p['start_year']} AND {p['end_year']} AND product_key={key(p['product'])}"
        if name == "fao_production":
            filtered += " AND geography_key IN (" + ",".join(key(v) for v in p["geographies"]) + ")"
            tokens["filtered_fact"] = "(" + filtered + ")"
        elif name == "sua":
            filtered += " AND geography_key=" + key(p["geography"])
        else:
            flow = p["flow"]
            codes = ("5610", "5622") if flow == "import" else ("5910", "5922")
            filtered += f" AND reporter_geography_key={key(p['reporter'])} AND observation_type_key IN (SELECT observation_type_key FROM {relation('dim_observation_type')} WHERE source_metric_id IN {codes})"
            tokens.update(quantity=flow+"_quantity_t", monetary=flow+"_value_usd", ratio="reported_"+flow+"_unit_value_usd_t",
                          present_quantity="present_"+codes[0], present_value="present_"+codes[1],
                          observation_quantity="observation_"+codes[0], observation_value="observation_"+codes[1])
        semantic = load_definitions()[view]["sql"].replace("{{"+fact+"}}", "("+filtered+")")
        tokens["semantic"] = semantic
    if name == "psd":
        tokens["filters"] = f"f.geography_key={key(p['geography'])} AND f.product_key={key(p['product'])} AND f.market_year BETWEEN {p['start_year']} AND {p['end_year']}"
    if name == "price_observations":
        tokens["reference_period"] = "f.reference_period" if p["fact"] == "fact_usda_price_reference" else "CAST(NULL AS varchar)"
        tokens["filters"] = f"f.price_series_key={key(p['series'])} AND f.{PERIODS[p['fact']]} BETWEEN {p['start_period']} AND {p['end_period']}"
    if name == "context_profile":
        field = p["field"]
        tokens["profile_expression"] = ("f.release_status" if field == "release_status" else
            "coalesce(json_extract_scalar(f.source_context_json,'$."+field+"'),'<unavailable>')")
    for token, value in tokens.items():
        sql = sql.replace("{{"+token+"}}", value)
    for table in set(re.findall(r"\{\{(\w+)\}\}", sql)):
        sql = sql.replace("{{"+table+"}}", relation(table))
    # This is an output overflow detector, never a sampled input or partial KPI.
    return "SELECT * FROM (" + sql + ") eda_result LIMIT 5001", definition_hash
