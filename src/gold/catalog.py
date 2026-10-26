"""Guarded Iceberg writes: full snapshots, exact comparison, commit readback."""
from gold import OWNER, MODEL_VERSION, OUTPUT_NAMESPACE


def identity(spark, name):
    if not spark.catalog.tableExists(name):
        return None
    table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)
    table.refresh()
    meta = table.operations().current()
    snap = meta.currentSnapshot()
    return {"uuid": str(meta.uuid()), "snapshot_id": str(snap.snapshotId()) if snap else None,
            "metadata_location": str(meta.metadataFileLocation()), "location": str(meta.location()),
            "owner": str(meta.properties().get("gold.owner")), "model": str(meta.properties().get("gold.model"))}


def pinned_read(spark, name, pin):
    current = identity(spark, name)
    if not current or current["uuid"] != pin["uuid"]:
        raise ValueError(f"Input table identity changed: {name}")
    table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)
    if table.snapshot(int(pin["snapshot_id"])) is None:
        raise ValueError(f"Expired/missing pinned input snapshot: {name}")
    return spark.read.format("iceberg").option("snapshot-id", pin["snapshot_id"]).load(name)


def check_owned(state, name):
    if (not state or state["owner"] != OWNER or state["model"] != MODEL_VERSION
            or not state["location"].startswith("s3://warehouse/gold_local/")):
        raise ValueError(f"Uninventoried/unowned Gold table: {name}")


def file_state(spark, name):
    # Sorted file paths are a compact real physical replay proof; data never collected.
    return [r[0] for r in spark.sql(f"SELECT file_path FROM {name}.files ORDER BY file_path").collect()]


def write_snapshot(spark, spec, df, dq, guard=lambda: None):
    name = f"{OUTPUT_NAMESPACE}.{spec.name}"
    before = identity(spark, name)
    if before:
        check_owned(before, name)
        current = spark.table(name)
        actual = [(f.name, f.dataType.simpleString()) for f in current.schema]
        expected = [(f.name, f.dataType.simpleString()) for f in df.schema]
        if actual != expected:
            raise ValueError(f"Existing Gold physical schema drift: {name}")
        before_files = file_state(spark, name)
        changed = df.exceptAll(current).limit(1).count() or current.exceptAll(df).limit(1).count()
        if not changed:
            after = identity(spark, name)
            after_files = file_state(spark, name)
            dq.record("DQ16", spec.name, "physical NOOP: identical UUID/snapshot/metadata/files", before, after, before == after and before_files == after_files)
            return {**after, "rows": current.count(), "write": "NOOP", "data_files": after_files}
    else:
        fields = ", ".join(f"`{c.name}` {c.spark_type}" + (" NOT NULL" if c.required else "") for c in spec.columns)
        guard()
        spark.sql(f"CREATE TABLE {name} ({fields}) USING iceberg TBLPROPERTIES ('format-version'='2','gold.owner'='{OWNER}','gold.model'='{MODEL_VERSION}','write.target-file-size-bytes'='67108864','write.distribution-mode'='none')")
    # Overwrite the *entire* admitted snapshot; withdrawn Silver rows disappear.
    # Do not overwritePartitions (empty/removed partitions would survive).
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    # UDF outputs are nullable to Spark's analyzer even after the DQ non-null gate.
    # Bind requiredness explicitly and verify every row while streaming to writer;
    # keep Iceberg's write-time nullability checks enabled.
    schema = T.StructType([T.StructField(c.name, df.schema[c.name].dataType, not c.required) for c in spec.columns])
    write_df = spark.createDataFrame(df.rdd, schema, verifySchema=True)
    try:
        guard()
        write_df.writeTo(name).overwrite(F.lit(True))
    except Exception as exc:
        # A lost response is not proof the commit failed. Read catalog and data first.
        after = identity(spark, name)
        if after:
            check_owned(after, name)
            persisted = spark.table(name)
            same = not (df.exceptAll(persisted).limit(1).count() or persisted.exceptAll(df).limit(1).count())
            if same:
                return {**after, "rows": persisted.count(), "write": "COMMIT_VERIFIED_AFTER_ERROR", "data_files": file_state(spark, name)}
        raise RuntimeError(f"Unknown/failed Gold commit at {name}; retained snapshots; retry requires readback") from exc
    after = identity(spark, name)
    guard()
    check_owned(after, name)
    dq.same("DQ17", spec.name, df, spark.table(name), "committed full table readback")
    return {**after, "rows": spark.table(name).count(), "write": "COMMITTED", "data_files": file_state(spark, name)}
