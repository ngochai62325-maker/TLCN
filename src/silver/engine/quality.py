from typing import Tuple, List, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, array, when, struct, size, array_except, array_remove
import pyspark.sql.functions as F

class SilverQualityEngine:
    """Evaluates Data Quality rules on Spark DataFrames."""
    
    def __init__(self, dq_rules: List[Dict[str, Any]]):
        self.rules = dq_rules

    def apply_rules(self, df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """
        Applies DQ rules and returns two DataFrames: (valid_df, quarantine_df).
        """
        if not self.rules:
            # If no rules, return original df and empty quarantine df (with same schema plus dq_errors)
            df_with_errors = df.withColumn("dq_errors", array())
            return df, df_with_errors.filter(F.lit(False))

        # We will build an array of error structs.
        # Initialize an empty array of structs for dq_errors
        # We can't easily initialize an empty array of a specific struct type if we don't have a prototype.
        # Instead, we will construct the array by collecting the results of each rule.
        
        error_conditions = []
        warning_conditions = []
        for r in self.rules:
            rule_id = r.get("rule_id", "UNKNOWN")
            sql_rule = r.get("rule", "1=1") # In a real implementation we'd compile the YAML natural language to SQL or assume YAML contains SQL.
            
            # The YAML currently has natural language like "Value must be non-negative".
            # We need to translate known rules or assume the YAML is updated to have sql_expr.
            # Let's map the specific ones from faostat_production for the PoC.
            if "sql_expr" in r:
                sql_expr = r["sql_expr"]
                failed_column = r.get("failed_column", "unknown")
            elif rule_id == "PROD_DQ_001":
                sql_expr = "value >= 0 OR value IS NULL"
                failed_column = "value"
            elif rule_id == "PROD_DQ_002":
                sql_expr = "year <= year(current_date()) + 1 AND year >= 1960"
                failed_column = "year"
            elif rule_id == "PROD_DQ_003":
                if "business_key" in df.columns:
                    sql_expr = "business_key IS NOT NULL"
                else:
                    sql_expr = "country_code IS NOT NULL AND commodity_code IS NOT NULL AND year IS NOT NULL AND element_code IS NOT NULL"
                failed_column = "business_key"
            elif rule_id == "PSD_DQ_001":
                sql_expr = "country IS NOT NULL AND commodity IS NOT NULL AND attribute IS NOT NULL AND market_year IS NOT NULL"
                failed_column = "business_key"
            elif rule_id == "PSD_DQ_002":
                sql_expr = "market_year >= 1950 AND market_year <= 2050"
                failed_column = "market_year"
            elif rule_id == "PSD_DQ_003":
                sql_expr = "value IS NULL OR value >= 0"
                failed_column = "value"
            else:
                if "sql_expr" in r:
                    sql_expr = r["sql_expr"]
                    failed_column = r.get("failed_column", "unknown")
                else:
                    raise ValueError(f"Unsupported DQ rule: {rule_id}. Missing explicit sql_expr translation.")
                
            error_message = r.get("rule", "Rule failed")
            
            # Check condition: if condition is FALSE, it's an error.
            # So error is NOT (condition)
            condition_expr = expr(sql_expr)
            
            error_struct = when(
                ~F.coalesce(condition_expr, F.lit(False)),
                struct(
                    F.lit(rule_id).alias("rule_id"),
                    F.lit(error_message).alias("error_message"),
                    F.lit(failed_column).alias("failed_column")
                )
            ).otherwise(F.lit(None))
            
            action = r.get("action", "QUARANTINE").upper()
            if action in ("LOG", "KEEP"):
                warning_conditions.append(error_struct)
            elif action == "QUARANTINE":
                error_conditions.append(error_struct)
            else:
                raise ValueError(f"Unsupported DQ action: {action}")
            
        # Combine into an array and remove nulls
        empty_errors = F.expr("cast(array() as array<struct<rule_id:string,error_message:string,failed_column:string>>)")
        df_with_errors = df.withColumn("raw_dq_errors", array(*error_conditions) if error_conditions else empty_errors)
        if warning_conditions:
            df_with_errors = df_with_errors.withColumn("dq_warnings", F.filter(array(*warning_conditions), lambda x: x.isNotNull()))
        
        # Filter out nulls from the array
        # array_remove(col("raw_dq_errors"), None) doesn't work directly if the type is complex in some Spark versions,
        # but in recent Spark `filter` on arrays is better.
        df_with_errors = df_with_errors.withColumn(
            "dq_errors", 
            F.expr("filter(raw_dq_errors, x -> x is not null)")
        ).drop("raw_dq_errors")

        valid_df = df_with_errors.filter(size(col("dq_errors")) == 0).drop("dq_errors")
        quarantine_df = df_with_errors.filter(size(col("dq_errors")) > 0)

        return valid_df, quarantine_df
