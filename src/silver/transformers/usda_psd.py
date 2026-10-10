from pyspark.sql import DataFrame
import pyspark.sql.functions as F

class UsdaPsdTransformer:
    def preprocess(self, df: DataFrame) -> DataFrame:
        """
        Unpivot wide column headers (col_YYYY_YYYY+1) into long format
        so the Silver Framework can map 'market_year', 'crop_year', and 'value'.
        """
        value_cols = [c for c in df.columns if c.startswith("col_") and len(c) == 13]
        if not value_cols:
            return df
            
        # Contract rule: forward fill or default to 'Rice, Milled'
        if "commodity" in df.columns:
            df = df.withColumn("commodity", F.coalesce(F.col("commodity"), F.lit("Rice, Milled")))
            
        # Build stack expression
        stack_expr = f"stack({len(value_cols)}, " + ", ".join([f"'{c}', {c}" for c in value_cols]) + ") as (wide_col_name, value)"
        
        # Keep non-value columns
        keep_cols = [c for c in df.columns if c not in value_cols]
        
        # Apply unpivot
        df_long = df.select(*keep_cols, F.expr(stack_expr))
        
        # Extract market_year and crop_year from 'col_1960_1961'
        df_long = df_long.withColumn("market_year", F.substring("wide_col_name", 5, 4).cast("integer"))
        df_long = df_long.withColumn("crop_year", F.concat_ws("/", F.substring("wide_col_name", 5, 4), F.substring("wide_col_name", 10, 4)))
        
        # Drop the temporary wide_col_name
        df_long = df_long.drop("wide_col_name")
        
        return df_long
