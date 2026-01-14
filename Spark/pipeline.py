#!/usr/bin/env python3
"""
pipeline.py

PySpark pipeline that reads 3 CSV sources:
- manufacturing_factory_dataset.csv (time series production + telemetry)
- maintenance_events.csv (maintenance windows with downtime + cost)
- operators_roster.csv (operator dimension)

Outputs:
- fact_table in Parquet
- fact_table in CSV

Run (Windows CMD/PowerShell):
  spark-submit pipeline.py --mfg manufacturing_factory_dataset.csv --events maintenance_events.csv --operators operators_roster.csv --out out --csv-coalesce 1
"""

import argparse
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType
)
from pyspark.sql.window import Window


# -----------------------------
# Schemas (avoid infer surprises which means we didn't depend on Spark to decide the data types).
# StructType & StructField methods are used to construct the schema for each csv file.
# StructType takes a list of StructField objects representing all the fields in the csv file with their types & if they are nullable.
# -----------------------------
def schemas() -> Tuple[StructType, StructType, StructType]:
    mfg_schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("factory_id", StringType(), True),
        StructField("line_id", StringType(), True),
        StructField("shift", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("planned_qty", IntegerType(), True),
        StructField("produced_qty", IntegerType(), True),
        StructField("scrap_qty", IntegerType(), True),
        StructField("defects_count", IntegerType(), True),
        StructField("defect_type", StringType(), True),
        StructField("cycle_time_s", DoubleType(), True),
        StructField("oee", DoubleType(), True),
        StructField("availability", DoubleType(), True),
        StructField("performance", DoubleType(), True),
        StructField("quality", DoubleType(), True),
        StructField("machine_state", StringType(), True),
        StructField("downtime_reason", StringType(), True),
        StructField("maintenance_type", StringType(), True),
        StructField("maintenance_due_date", StringType(), True),
        StructField("vibration_mm_s", DoubleType(), True),
        StructField("temperature_c", DoubleType(), True),
        StructField("pressure_bar", DoubleType(), True),
        StructField("energy_kwh", DoubleType(), True),
        StructField("operator_id", StringType(), True),
        StructField("workorder_status", StringType(), True),
    ])

    events_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("factory_id", StringType(), True),
        StructField("line_id", StringType(), True),
        StructField("maintenance_type", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("start_time", StringType(), True),
        StructField("end_time", StringType(), True),
        StructField("downtime_min", IntegerType(), True),
        StructField("technician_id", StringType(), True),
        StructField("parts_used", StringType(), True),
        StructField("cost_eur", DoubleType(), True),
        StructField("outcome", StringType(), True),
        StructField("next_due_date", StringType(), True),
    ])

    ops_schema = StructType([
        StructField("operator_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("factory_id", StringType(), True),
        StructField("primary_line", StringType(), True),
        StructField("primary_shift", StringType(), True),
        StructField("skill_level", StringType(), True),
        StructField("certifications", StringType(), True),
        StructField("team", StringType(), True),
        StructField("hire_date", StringType(), True),
        StructField("overtime_eligible", BooleanType(), True),
        StructField("hourly_rate_eur", DoubleType(), True),
        StructField("reliability_score", DoubleType(), True),
    ])

    return mfg_schema, events_schema, ops_schema


# -----------------------------
# Read csv files function
# A SparkSession object starts reading a csv file based on the file path & schema passed.
# Permissive mode is on which is the default error-handling mode therefore no need to add it explicitly -> (Just For Documentation)
# -----------------------------
def read_csv(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    return (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(path)
    )


# -----------------------------
# Data Cleaning / Transformations
# -----------------------------

"""
normalize_str() typically does some combination of:

- trim() whitespace

- convert to lowercase / uppercase

- convert empty strings to null

"""
def normalize_str(col: F.Column) -> F.Column:
    return F.lower(F.trim(col))

"""
clean_manufacturing(df) takes the raw manufacturing CSV and produces a DataFrame that is:

- typed correctly (timestamp/date)

- string-normalized for consistent grouping/joining

- safe for metrics (nulls → 0, negatives removed)

- deduplicated at the intended “natural grain”

* Therefore the data cleaning & transformation operations are:
  1- Parsing fields with timestamps & dates to Spark timestamps & dates (avoiding malformation & blank values)
  2- Normalizing fields with string type by removing whitespaces, make them all with the same case (lower)
  3- Replace null numeric metrics with 0 (or 0.0) using coalesce() function
  4- Validating that required key fields don't have nulls
  5- Every row is uniquely identified by the fields passed to the dropDuplicates() function

"""
def clean_manufacturing(df: DataFrame) -> DataFrame:
    out = (
        df
        .withColumn("timestamp_ts", F.to_timestamp(F.col("timestamp")))
        .withColumn("maintenance_due_date_dt", F.to_date(F.col("maintenance_due_date")))
        .withColumn("shift_norm", normalize_str(F.col("shift")))
        .withColumn("machine_state_norm", normalize_str(F.col("machine_state")))
        .withColumn("downtime_reason_norm", normalize_str(F.col("downtime_reason")))
        .withColumn("maintenance_type_norm", normalize_str(F.col("maintenance_type")))
        .withColumn("workorder_status_norm", normalize_str(F.col("workorder_status")))
        .withColumn("planned_qty", F.coalesce(F.col("planned_qty"), F.lit(0)))
        .withColumn("produced_qty", F.coalesce(F.col("produced_qty"), F.lit(0)))
        .withColumn("scrap_qty", F.coalesce(F.col("scrap_qty"), F.lit(0)))
        .withColumn("defects_count", F.coalesce(F.col("defects_count"), F.lit(0)))
        .withColumn("energy_kwh", F.coalesce(F.col("energy_kwh"), F.lit(0.0)))
        .filter(F.col("factory_id").isNotNull())
        .filter(F.col("line_id").isNotNull())
        .filter(F.col("timestamp_ts").isNotNull())
        .filter(F.col("produced_qty") >= 0)
        .filter(F.col("planned_qty") >= 0)
        .filter(F.col("scrap_qty") >= 0)
        .filter(F.col("defects_count") >= 0)
    )

    # Deduplicate at the natural grain
    out = out.dropDuplicates(["timestamp_ts", "factory_id", "line_id", "order_id", "product_id"])
    return out


def clean_events(df: DataFrame) -> DataFrame:
    out = (
        df
        .withColumn("start_ts", F.to_timestamp(F.col("start_time")))
        .withColumn("end_ts", F.to_timestamp(F.col("end_time")))
        .withColumn("next_due_date_dt", F.to_date(F.col("next_due_date")))
        .withColumn("maintenance_type_norm", normalize_str(F.col("maintenance_type")))
        .withColumn("reason_norm", normalize_str(F.col("reason")))
        .withColumn("outcome_norm", normalize_str(F.col("outcome")))
        .withColumn("downtime_min", F.coalesce(F.col("downtime_min"), F.lit(0)))
        .withColumn("cost_eur", F.coalesce(F.col("cost_eur"), F.lit(0.0)))
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("factory_id").isNotNull())
        .filter(F.col("line_id").isNotNull())
        .filter(F.col("start_ts").isNotNull())
        .filter(F.col("end_ts").isNotNull())
        .filter(F.col("end_ts") >= F.col("start_ts"))
        .filter(F.col("downtime_min") >= 0)
        .filter(F.col("cost_eur") >= 0)
    )

    out = out.dropDuplicates(["event_id"])
    return out


def clean_operators(df: DataFrame) -> DataFrame:
    out = (
        df
        .withColumn("hire_date_dt", F.to_date(F.col("hire_date")))
        .withColumn("name_norm", F.trim(F.col("name")))
        .withColumn("team_norm", normalize_str(F.col("team")))
        .withColumn("skill_level_norm", normalize_str(F.col("skill_level")))
        .withColumn("hourly_rate_eur", F.coalesce(F.col("hourly_rate_eur"), F.lit(0.0)))
        .withColumn("reliability_score", F.coalesce(F.col("reliability_score"), F.lit(0.0)))
        .filter(F.col("operator_id").isNotNull())
    )

    # Deduplicate operators by operator_id (keep most recent hire_date if duplicates exist)
    w = Window.partitionBy("operator_id").orderBy(F.col("hire_date_dt").desc_nulls_last())
    out = out.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")
    return out


# -----------------------------
# Build fact table
# -----------------------------
def build_fact_table(df_mfg: DataFrame, df_events: DataFrame, df_ops: DataFrame) -> DataFrame:
    # 1) Prepare operator dimension and RENAME overlapping columns (factory_id, etc.)
    # This prevents ambiguous references after the join.
    ops_dim = (
        df_ops.select(
            F.col("operator_id"),
            F.col("name_norm").alias("operator_name"),
            F.col("team_norm").alias("operator_team"),
            F.col("skill_level_norm").alias("operator_skill_level"),
            F.col("reliability_score").alias("operator_reliability_score"),
            F.col("hourly_rate_eur").alias("operator_hourly_rate_eur"),
            # keep these if useful, but renamed to avoid collisions:
            F.col("factory_id").alias("operator_factory_id"),
            F.col("primary_line").alias("operator_primary_line"),
            F.col("primary_shift").alias("operator_primary_shift"),
        )
    )

    # 2) Join manufacturing to operators (simple equi-join)
    mfg_ops = df_mfg.join(ops_dim, on="operator_id", how="left")

    # 3) Time-window join to maintenance events (non-equi join)
    # Condition: same factory & line AND mfg timestamp within event window
    cond = (
        (F.col("m.factory_id") == F.col("e.factory_id")) &
        (F.col("m.line_id") == F.col("e.line_id")) &
        (F.col("m.timestamp_ts") >= F.col("e.start_ts")) &
        (F.col("m.timestamp_ts") <= F.col("e.end_ts"))
    )

    joined = (
        mfg_ops.alias("m")
        .join(df_events.alias("e"), on=cond, how="left")
    )

    # If multiple events overlap, pick the most recent event by start_ts
    row_id = F.sha2(
        F.concat_ws(
            "||",
            F.col("m.factory_id"),
            F.col("m.line_id"),
            F.col("m.order_id"),
            F.col("m.product_id"),
            F.col("m.timestamp_ts").cast("string"),
        ),
        256,
    )

    joined = joined.withColumn("_mfg_row_id", row_id)
    w = Window.partitionBy("_mfg_row_id").orderBy(F.col("e.start_ts").desc_nulls_last())

    picked = (
        joined
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Derive analytics flags / measures
    fact = (
        picked
        .withColumn("in_maintenance_window", F.when(F.col("e.event_id").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .withColumn(
            "is_unplanned_breakdown",
            F.when(F.col("e.reason_norm").rlike("unplanned|breakdown"), F.lit(1)).otherwise(F.lit(0))
        )
        .select(
            # Grain / keys
            F.col("m.timestamp_ts").alias("timestamp"),
            F.col("m.factory_id").alias("factory_id"),
            F.col("m.line_id").alias("line_id"),
            F.col("m.shift_norm").alias("shift"),
            F.col("m.order_id").alias("order_id"),
            F.col("m.product_id").alias("product_id"),
            F.col("m.operator_id").alias("operator_id"),

            # Manufacturing measures
            F.col("m.planned_qty"),
            F.col("m.produced_qty"),
            F.col("m.scrap_qty"),
            F.col("m.defects_count"),
            F.col("m.cycle_time_s"),
            F.col("m.oee"),
            F.col("m.availability"),
            F.col("m.performance"),
            F.col("m.quality"),
            F.col("m.machine_state_norm").alias("machine_state"),
            F.col("m.downtime_reason_norm").alias("downtime_reason"),
            F.col("m.maintenance_type_norm").alias("maintenance_type_signal"),
            F.col("m.maintenance_due_date_dt").alias("maintenance_due_date"),
            F.col("m.vibration_mm_s"),
            F.col("m.temperature_c"),
            F.col("m.pressure_bar"),
            F.col("m.energy_kwh"),
            F.col("m.workorder_status_norm").alias("workorder_status"),

            # Operator attributes (already renamed)
            F.col("m.operator_name"),
            F.col("m.operator_team"),
            F.col("m.operator_skill_level"),
            F.col("m.operator_reliability_score"),
            F.col("m.operator_hourly_rate_eur"),
            F.col("m.operator_factory_id"),
            F.col("m.operator_primary_line"),
            F.col("m.operator_primary_shift"),

            # Maintenance event enrichment (nullable)
            F.col("e.event_id").alias("maintenance_event_id"),
            F.col("e.maintenance_type_norm").alias("maintenance_type_event"),
            F.col("e.reason_norm").alias("maintenance_reason"),
            F.col("e.outcome_norm").alias("maintenance_outcome"),
            F.col("e.downtime_min").alias("maintenance_downtime_min"),
            F.col("e.cost_eur").alias("maintenance_cost_eur"),
            F.col("e.start_ts").alias("maintenance_start_time"),
            F.col("e.end_ts").alias("maintenance_end_time"),

            # Flags
            F.col("in_maintenance_window"),
            F.col("is_unplanned_breakdown"),
        )
    )

    return fact


# -----------------------------
# Write outputs
# -----------------------------
def write_outputs(df_fact: DataFrame, out_dir: str, csv_coalesce: int) -> None:
    out_dir = out_dir.rstrip("/\\")
    parquet_path = f"{out_dir}/fact_table_parquet"
    csv_path = f"{out_dir}/fact_table_csv"

    df_fact.write.mode("overwrite").format("parquet").save(parquet_path)

    if csv_coalesce and csv_coalesce > 0:
        (df_fact.coalesce(csv_coalesce)
               .write.mode("overwrite")
               .option("header", "true")
               .csv(csv_path))
    else:
        (df_fact.write.mode("overwrite")
               .option("header", "true")
               .csv(csv_path))


# -----------------------------
# Main function/method which is the starting point of our code
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Task 3 PySpark pipeline for 3 CSV sources -> fact table.")
    parser.add_argument("--mfg", required=True, help="Path to manufacturing_factory_dataset.csv (or wildcard/folder)")
    parser.add_argument("--events", required=True, help="Path to maintenance_events.csv")
    parser.add_argument("--operators", required=True, help="Path to operators_roster.csv")
    parser.add_argument("--out", required=True, help="Output directory (e.g., out/)")
    parser.add_argument("--csv-coalesce", type=int, default=1,
                        help="Number of output CSV partitions (1 = single CSV part file). Use 0 to disable.")
    args = parser.parse_args()

    spark = SparkSession.builder \
            .appName("Task3-DataPipeline-FactTable") \
            .config("spark.hadoop.io.native.lib.available", "false").getOrCreate()

    mfg_schema, events_schema, ops_schema = schemas()

    df_mfg_raw = read_csv(spark, args.mfg, mfg_schema)
    df_events_raw = read_csv(spark, args.events, events_schema)
    df_ops_raw = read_csv(spark, args.operators, ops_schema)

    df_mfg = clean_manufacturing(df_mfg_raw)
    df_events = clean_events(df_events_raw)
    df_ops = clean_operators(df_ops_raw)

    fact = build_fact_table(df_mfg, df_events, df_ops)

    print("Rows - manufacturing (clean):", df_mfg.count())
    print("Rows - events (clean):", df_events.count())
    print("Rows - operators (clean):", df_ops.count())
    print("Rows - fact:", fact.count())

    write_outputs(fact, args.out, args.csv_coalesce)

    spark.stop()


if __name__ == "__main__":
    main()
