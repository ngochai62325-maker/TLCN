"""Vectorized SQL quality checks with separate blocking errors and warnings."""
from pyspark.sql import DataFrame, functions as F

ERROR_TYPE = "array<struct<rule_id:string,error_message:string,failed_column:string>>"


def empty_errors():
    return F.array().cast(ERROR_TYPE)


class SilverQualityEngine:
    def __init__(self, dq_rules):
        self.rules = dq_rules

    def evaluate(self, df: DataFrame) -> DataFrame:
        errors, warnings = [], []
        for rule in self.rules:
            if not rule.get("sql_expr"):
                raise ValueError(f"Unsupported DQ rule: {rule.get('rule_id')}. Missing explicit sql_expr translation.")
            action = rule.get("action", "QUARANTINE").upper()
            if action not in {"QUARANTINE", "LOG", "KEEP"}:
                raise ValueError(f"Unsupported DQ action: {action}")
            passed = F.coalesce(F.expr(rule["sql_expr"]), F.lit(False))
            error = F.when(~passed, F.struct(
                F.lit(rule.get("rule_id", "UNKNOWN")).alias("rule_id"),
                F.lit(rule.get("rule", "Rule failed")).alias("error_message"),
                F.lit(rule.get("failed_column", "unknown")).alias("failed_column")))
            (errors if action == "QUARANTINE" else warnings).append(error)

        def compact(items):
            return F.filter(F.array(*items).cast(ERROR_TYPE), lambda x: x.isNotNull()) if items else empty_errors()

        system = F.coalesce(F.col("_sys_cast_errors"), empty_errors()) if "_sys_cast_errors" in df.columns else empty_errors()
        return (df.withColumn("dq_errors", F.concat(compact(errors), system))
                .withColumn("_dq_warnings", compact(warnings)).drop("_sys_cast_errors"))

    def apply_rules(self, df: DataFrame):
        checked = self.evaluate(df)
        return (checked.filter(F.size("dq_errors") == 0).drop("dq_errors"),
                checked.filter(F.size("dq_errors") > 0))
