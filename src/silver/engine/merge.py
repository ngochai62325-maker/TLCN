"""Deterministic deduplication and freshness-aware Iceberg upserts."""
import uuid
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def quote(name):
    return "`" + name.replace("`", "``") + "`"


def table_identifier(name):
    return ".".join(quote(part) for part in name.split("."))


class IcebergMergeEngine:
    def __init__(self, spark):
        self.spark = spark

    def deduplicate(self, df, business_keys, order_cols=None):
        if not business_keys:
            raise ValueError("Deduplication requires business keys")
        order_cols = order_cols if order_cols is not None else ["_ingestion_timestamp", "_source_file"]
        order = [F.col(c).desc_nulls_last() for c in order_cols if c in df.columns]
        # JSON for nested/map columns avoids unsupported ordering of Spark maps.
        for field in df.schema:
            if field.name not in order_cols and field.name != "_silver_processed_at":
                value = F.col(field.name)
                if field.dataType.typeName() in {"array", "map", "struct"}:
                    value = F.to_json(value)
                order.append(value.asc_nulls_first())
        window = Window.partitionBy(*business_keys).orderBy(*order)
        return df.withColumn("_row_num", F.row_number().over(window)).filter(F.col("_row_num") == 1).drop("_row_num")

    def ensure_table(self, df, target_table, partition_columns=None):
        if self._table_exists(target_table):
            existing = self.spark.table(target_table).schema
            incoming = {f.name: f.dataType for f in df.schema}
            stored = {f.name: f.dataType for f in existing}
            for name, dtype in incoming.items():
                if name in stored and stored[name] != dtype:
                    raise ValueError(f"Silver schema mismatch {target_table}.{name}: {stored[name]} != {dtype}; explicit migration required")
                if name not in stored:
                    self.spark.sql(f"ALTER TABLE {table_identifier(target_table)} ADD COLUMN {quote(name)} {dtype.simpleString()}")
            return
        parts = target_table.split(".")
        if len(parts) > 2 or (len(parts) == 2 and not self.spark.conf.get(f"spark.sql.catalog.{parts[0]}", None)):
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table_identifier('.'.join(parts[:-1]))}")
        writer = (df.limit(0).writeTo(target_table).using("iceberg")
                  .tableProperty("format-version", "2")
                  .tableProperty("write.parquet.compression-codec", "zstd")
                  .tableProperty("write.distribution-mode", "hash")
                  .tableProperty("write.merge.distribution-mode", "hash")
                  .tableProperty("write.spark.fanout.enabled", "false"))
        if partition_columns:
            writer = writer.partitionedBy(*[F.col(c) for c in partition_columns])
        writer.create()

    def merge(self, df, target_table, business_keys, partition_columns=None):
        if not business_keys or not set(business_keys).issubset(df.columns):
            raise ValueError("MERGE requires existing business keys")
        self.ensure_table(df, target_table, partition_columns)
        if df.isEmpty():
            return
        view = "incoming_updates_" + uuid.uuid4().hex
        df.createOrReplaceTempView(view)
        keys = " AND ".join(f"t.{quote(k)} = s.{quote(k)}" for k in business_keys)
        freshness = "true"
        if "_ingestion_timestamp" in df.columns:
            stamp = quote("_ingestion_timestamp")
            freshness = f"(t.{stamp} IS NULL OR s.{stamp} >= t.{stamp})"
        # Processing time changes only when source/quality/audit data changes.
        changed = [c for c in df.columns if c != "_silver_processed_at"]
        differences = " OR ".join(f"NOT (t.{quote(c)} <=> s.{quote(c)})" for c in changed)
        updates = ", ".join(f"t.{quote(c)} = s.{quote(c)}" for c in df.columns)
        columns = ", ".join(quote(c) for c in df.columns)
        values = ", ".join(f"s.{quote(c)}" for c in df.columns)
        try:
            self.spark.sql(f"""
                MERGE INTO {table_identifier(target_table)} t USING {quote(view)} s ON {keys}
                WHEN MATCHED AND {freshness} AND ({differences}) THEN UPDATE SET {updates}
                WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})
            """)
        finally:
            self.spark.catalog.dropTempView(view)

    def _table_exists(self, table_name):
        # Catalog/network failures must propagate, rather than masquerade as absence.
        return self.spark.catalog.tableExists(table_name)
