"""
Generic Metadata-Driven PySpark ETL Processor
Executes dynamically based on control metadata:
1. Dynamic data ingestion from parameterized source paths.
2. Dynamic Data Quality (DQ) rule evaluation.
3. Automated quarantine routing for dirty/corrupt records.
4. Idempotent Delta Lake MERGE into target analytical tables.
"""

import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, when
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("GenericETLProcessor")

def init_spark():
    return SparkSession.builder \
        .appName("Generic_Metadata_ETL_Processor") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaBaseAttributeParser") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

class GenericETLProcessor:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config
        self.pipeline_id = config.get("pipeline_id", "1")
        self.pipeline_name = config.get("pipeline_name", "SamplePipeline")
        self.source_path = config.get("source_path", "data/sample/orders.parquet")
        self.target_path = config.get("target_path", f"data/delta/{config.get('target_table', 'silver_orders')}")
        self.quarantine_path = f"data/quarantine/{self.pipeline_name}"
        self.primary_key = config.get("primary_key", "OrderID")
        self.dq_rules = json.loads(config.get("dq_rules", "[]")) if isinstance(config.get("dq_rules"), str) else config.get("dq_rules", [])

    def load_source(self):
        logger.info(f"Loading source path: {self.source_path}")
        return self.spark.read.load(self.source_path)

    def evaluate_quality_rules(self, df):
        """
        Dynamically applies validation rules specified in metadata.
        Tags bad rows with failure reasons.
        """
        if not self.dq_rules:
            return df.withColumn("_is_valid", lit(True)).withColumn("_failed_rule", lit(None).cast("string"))

        condition_expr = lit(True)
        failure_reasons = lit("")

        for rule in self.dq_rules:
            rule_type = rule.get("rule")
            column_name = rule.get("column")

            if column_name not in df.columns:
                continue

            if rule_type == "not_null":
                is_valid = col(column_name).isNotNull()
                failure_msg = f"{column_name}_IS_NULL;"
            elif rule_type == "range":
                min_val = rule.get("min", float("-inf"))
                max_val = rule.get("max", float("inf"))
                is_valid = (col(column_name) >= min_val) & (col(column_name) <= max_val)
                failure_msg = f"{column_name}_OUT_OF_RANGE;"
            else:
                continue

            failure_reasons = when(~is_valid, failure_reasons + failure_msg).otherwise(failure_reasons)
            condition_expr = condition_expr & is_valid

        df_evaluated = df \
            .withColumn("_is_valid", condition_expr) \
            .withColumn("_failed_rule", when(~condition_expr, failure_reasons).otherwise(None))

        return df_evaluated

    def execute(self):
        df_raw = self.load_source()
        rows_read = df_raw.count()
        logger.info(f"Loaded {rows_read} rows from source.")

        df_evaluated = self.evaluate_quality_rules(df_raw)

        # Split into Valid and Quarantine
        df_valid = df_evaluated.filter(col("_is_valid") == True).drop("_is_valid", "_failed_rule")
        df_quarantine = df_evaluated.filter(col("_is_valid") == False) \
            .withColumn("_quarantined_at", current_timestamp())

        rows_written = df_valid.count()
        rows_quarantined = df_quarantine.count()

        logger.info(f"Data Quality results: {rows_written} valid, {rows_quarantined} quarantined.")

        # 1. Route Quarantined records to isolation table
        if rows_quarantined > 0:
            df_quarantine.write.format("delta").mode("append").save(self.quarantine_path)
            logger.warning(f"Quarantined {rows_quarantined} corrupt rows at: {self.quarantine_path}")

        # 2. Upsert Valid records to Target Delta Table
        if rows_written > 0:
            if not DeltaTable.isDeltaTable(self.spark, self.target_path):
                df_valid.write.format("delta").mode("overwrite").save(self.target_path)
            else:
                delta_target = DeltaTable.forPath(self.spark, self.target_path)
                pk = self.primary_key
                delta_target.alias("target").merge(
                    source=df_valid.alias("source"),
                    condition=f"target.{pk} = source.{pk}"
                ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        stats = {
            "pipeline_name": self.pipeline_name,
            "status": "SUCCEEDED",
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_quarantined": rows_quarantined
        }
        logger.info(f"Execution complete: {json.dumps(stats)}")
        return stats

if __name__ == "__main__":
    spark = init_spark()
    sample_config = {
        "pipeline_id": "1",
        "pipeline_name": "ERP_Orders",
        "source_path": "data/sample_erp/orders.parquet",
        "target_table": "silver_erp_orders",
        "primary_key": "OrderID",
        "dq_rules": [
            {"rule": "not_null", "column": "OrderID"},
            {"rule": "range", "column": "TotalAmount", "min": 0}
        ]
    }
    # Note: run after generating sample data
