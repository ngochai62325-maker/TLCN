"""IPSARD workbook dates use US month/day and an AM/PM time suffix."""
from pyspark.sql import functions as F
from ingestion.storage.bronze_writer import sanitize_column_name


class ThitruongnongsanTransformer:
    def preprocess(self, df):
        date = sanitize_column_name("Ngày")
        if date not in df.columns:
            raise ValueError("IPSARD missing Ngày column")
        df = df.withColumn("_raw_date", F.trim(F.col(date).cast("string")))
        # Exact observed format plus ISO dates; never guess dd/MM vs MM/dd.
        parsed = F.to_date(F.try_to_timestamp("_raw_date", F.lit("M/d/yyyy h:mm:ss a")))
        iso = F.expr("try_cast(_raw_date as date)")
        df = df.withColumn(date, F.coalesce(parsed, iso))
        df = df.withColumn("year", F.year(F.col(date)))
        df = df.withColumn("_date_parse_error", F.col("_raw_date").isNotNull() & F.col(date).isNull())
        unit = sanitize_column_name("Đơn_vị_tính")
        currency = sanitize_column_name("Loại_tiền")
        if unit in df.columns:
            df = df.withColumn(unit, F.when(F.lower(F.trim(F.col(unit))).isin("vnđ/kg", "đồng/kg", "vnd/kg"), "VND/kg").otherwise(F.col(unit)))
        if currency in df.columns:
            df = df.withColumn(currency, F.when(F.upper(F.trim(F.col(currency))).isin("VNĐ", "VND"), "VND").otherwise(F.col(currency)))
        price = sanitize_column_name("Giá")
        # Accept grouped commas only when every group has exactly three digits.
        if price in df.columns:
            df = df.withColumn("_raw_price", F.trim(F.col(price).cast("string")))
            df = df.withColumn(price, F.when(F.col("_raw_price").rlike(r"^[0-9]{1,3}(,[0-9]{3})+(\.[0-9]+)?$"),
                                            F.regexp_replace("_raw_price", ",", "")).otherwise(F.col("_raw_price")))
            df = df.withColumn("_negative_value", F.coalesce(F.expr(f"try_cast(`{price}` as double)") < 0, F.lit(False)))
        return df

    def quality_rules(self):
        return [{"rule_id": "VN_DATE_PARSE", "sql_expr": "NOT _date_parse_error", "failed_column": "date", "rule": "Date does not match profiled workbook format"},
                {"rule_id": "VN_PRICE_SIGN", "sql_expr": "NOT _negative_value", "failed_column": "price_value", "rule": "Negative price requires review"}]
