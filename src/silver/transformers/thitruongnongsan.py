"""Daily IPSARD prices; source labels are retained without master-data guesses."""
from pyspark.sql import functions as F
from silver.transformers.faostat_source import sql_rule


class ThitruongnongsanTransformer:
    dataset_id = "thitruongnongsan"
    business_keys = ["market", "commodity", "price_type", "date", "unit", "currency"]

    def transform(self, df):
        mapping = {"commodity": "t_n_m_t_h_ng", "market": "th_tr_ng", "price_type": "lo_i_gi_",
                   "unit": "_n_v_t_nh", "currency": "lo_i_ti_n", "source": "ngu_n"}
        required = set(mapping.values()) | {"ng_y", "gi_"}
        if required - set(df.columns):
            raise ValueError(f"Missing IPSARD Bronze columns: {sorted(required - set(df.columns))}")
        if "_source_payload" not in df.columns:
            df = df.withColumn("_source_payload", F.to_json(F.struct(*[F.col(c) for c in sorted(df.columns)]),
                                                         {"ignoreNullFields": "false"}))
        columns = [F.when(F.trim(F.col(raw).cast("string")) != "", F.trim(F.col(raw).cast("string"))).alias(name)
                   for name, raw in mapping.items()]
        # Source workbook contains 7/31/YYYY, proving US month/day ordering.
        date = F.to_date(F.try_to_timestamp(F.col("ng_y"), F.lit("M/d/yyyy h:mm:ss a")))
        price = F.expr("try_cast(gi_ as decimal(18,2))")
        technical = [c for c in df.columns if c.startswith("_") and c != "_n_v_t_nh"]
        result = df.select(*columns, date.alias("date"), price.alias("price_value"),
            F.col("ng_y").cast("string").alias("_raw_date"), F.col("gi_").cast("string").alias("_raw_value"),
            (F.col("gi_").isNotNull() & price.isNull()).alias("_numeric_parse_error"),
            *[F.col(c) for c in technical])
        return (result.withColumn("unit", F.when(F.col("unit").isin("VNĐ/Kg", "VNĐ/kg", "Vnđ/Kg", "Đồng/kg"), "VND/kg").otherwise(F.col("unit")))
                .withColumn("currency", F.when(F.col("currency") == "VNĐ", "VND").otherwise(F.col("currency")))
                .withColumn("_province_mapping_status", F.lit("source_label_only")))

    preprocess = transform

    def quality_rules(self):
        return [sql_rule("IPSARD_NUMBER", "NOT _numeric_parse_error", "price_value", "Malformed price must be traceable"),
                sql_rule("IPSARD_UNIT", "unit = 'VND/kg' AND currency = 'VND'", "unit", "Unknown unit/currency requires review")]
