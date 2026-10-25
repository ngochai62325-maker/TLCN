"""Replay-safe dead letters preserving normalized and original observations."""
import uuid
from pyspark.sql import functions as F
from silver.engine.merge import table_identifier, quote
from silver.engine.quality import ERROR_TYPE


class SilverQuarantineManager:
    def __init__(self, spark, target_table="iceberg.silver.dead_letters"):
        self.spark = spark
        self.target_table = target_table

    def ensure_table(self):
        parts = self.target_table.split(".")
        if len(parts) > 2 or (len(parts) == 2 and not self.spark.conf.get(f"spark.sql.catalog.{parts[0]}", None)):
            self.spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table_identifier('.'.join(parts[:-1]))}")
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_identifier(self.target_table)} (
                quarantine_record_id STRING, source_dataset STRING, pipeline_run_id STRING,
                detected_at TIMESTAMP, business_key_hash STRING,
                errors ARRAY<STRUCT<rule_id:STRING,error_message:STRING,failed_column:STRING>>,
                payload STRING
            ) USING iceberg PARTITIONED BY (source_dataset, days(detected_at))
            TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd',
                'write.distribution-mode'='hash', 'write.merge.distribution-mode'='hash',
                'write.spark.fanout.enabled'='false')
        """)

    def route_quarantine(self, quarantine_df, dataset, run_id, business_keys):
        if quarantine_df.isEmpty():
            return
        self.ensure_table()
        payload = F.to_json(F.struct(*[F.col(c) for c in sorted(quarantine_df.columns) if c != "dq_errors"]),
                            options={"ignoreNullFields": "false"})
        key = F.to_json(F.struct(*[F.col(c) for c in business_keys]), options={"ignoreNullFields": "false"}) if business_keys else F.lit(None).cast("string")
        letters = quarantine_df.select(
            F.sha2(F.to_json(F.struct(F.lit(dataset).alias("dataset"), payload.alias("payload"))), 256).alias("quarantine_record_id"),
            F.lit(dataset).alias("source_dataset"), F.lit(run_id).alias("pipeline_run_id"),
            F.current_timestamp().alias("detected_at"), F.sha2(key, 256).alias("business_key_hash"),
            F.col("dq_errors").cast(ERROR_TYPE).alias("errors"), payload.alias("payload"),
        ).dropDuplicates(["quarantine_record_id"])

        incoming = letters.alias("s")
        stored = self.spark.table(self.target_table).select(
            "quarantine_record_id", "payload", "errors").alias("t")
        matched = incoming.join(stored,
                                F.col("s.quarantine_record_id") == F.col("t.quarantine_record_id"), "left")
        exists = F.col("t.quarantine_record_id").isNotNull()
        if not matched.filter(exists & ~F.col("s.payload").eqNullSafe(F.col("t.payload"))).isEmpty():
            # Payload is part of the deterministic ID. Never silently retain or
            # overwrite a different payload under an existing identity.
            raise ValueError("Quarantine identity matches a different persisted payload")
        letters = matched.filter(
            ~exists | ~F.col("s.errors").eqNullSafe(F.col("t.errors"))).select("s.*")
        # Even an empty Iceberg MERGE can commit metadata. Identical records must
        # retain their original detection/run audit fields without executing it.
        if letters.isEmpty():
            return
        view = "incoming_quarantine_" + uuid.uuid4().hex
        letters.createOrReplaceTempView(view)
        try:
            self.spark.sql(f"""
                MERGE INTO {table_identifier(self.target_table)} t USING {quote(view)} s
                ON t.quarantine_record_id = s.quarantine_record_id
                WHEN MATCHED THEN UPDATE SET t.pipeline_run_id = s.pipeline_run_id,
                    t.detected_at = s.detected_at, t.errors = s.errors
                WHEN NOT MATCHED THEN INSERT *
            """)
        finally:
            self.spark.catalog.dropTempView(view)

    def _table_exists(self, table_name):
        return self.spark.catalog.tableExists(table_name)
