"""Small source observations materialized through Spark SQL (no Python UDFs)."""

def observations(spark, *overrides):
    baseline = {"domain_code": "QCL", "domain": "Crops and livestock products",
                "area_code_m49_": "'704", "area": " Viet Nam ",
                "item_code_cpc_": "113.0", "item": "Rice", "year": "2020",
                "element_code": "5510", "element": "Production", "unit": "t", "value": "12.5",
                "flag": "E", "flag_description": "Estimated value", "note": "source note",
                "months_code": "7021", "months": "Annual value",
                "_ingestion_run_id": "bronze-run", "_ingestion_batch_id": "batch-1",
                "_ingestion_chunk_id": "1", "_source_id": "fixture",
                "_source_file": "fixture.csv", "_source_checksum": "sha256-fixture",
                "_source_snapshot_id": "42", "_ingestion_timestamp": "2026-10-01 00:00:00"}
    rows = [dict(baseline, **row) for row in overrides] or [baseline]
    return sql_rows(spark, rows)


def sql_rows(spark, rows):
    columns = list(rows[0])
    def literal(value):
        if value is None:
            return "CAST(NULL AS STRING)"
        return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"
    values = ",".join("(" + ",".join(literal(row.get(c)) for c in columns) + ")" for row in rows)
    return spark.sql("SELECT * FROM VALUES " + values + " AS fixture(" + ",".join(f"`{c}`" for c in columns) + ")")
