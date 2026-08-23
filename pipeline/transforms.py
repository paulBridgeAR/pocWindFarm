"""All pipeline logic, as plain DataFrame -> DataFrame functions.
 
Nothing here imports the pipeline API. That is deliberate: declarative pipeline
code only executes inside a pipeline run, so any logic written inside the
decorated functions cannot be unit tested. Keeping it here means the same code
runs on Databricks and under pytest with a local SparkSession.
"""
 
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
 
# --- thresholds. One place, no magic numbers anywhere else -------------------
 
POWER_MIN_MW, POWER_MAX_MW = 0.0, 5.0     # 5.0 is headroom; the data tops out at 4.5
WIND_SPEED_MIN, WIND_SPEED_MAX = 0.0, 40.0  # turbines shut down around 25 m/s
WIND_DIR_MIN, WIND_DIR_MAX = 0, 359
 
EXPECTED_READINGS_PER_DAY = 24
ANOMALY_SIGMA = 2.0
MIN_TURBINES_FOR_FLEET_STATS = 5
 
 
def clean_readings(df: DataFrame) -> DataFrame:
    """Cast, remove unusable rows and outliers, deduplicate.
 
    Requirement 1. Three separate problems, handled differently on purpose:
 
    - Unusable rows (no turbine id, no timestamp) are dropped. Without a key
      the row cannot be attributed to anything.
    - Outliers (physically impossible values) are dropped. A negative power
      reading or 47 m/s wind is a sensor fault, not a measurement.
    - Missing power readings are dropped, and the loss is reported as
      completeness_pct in gold rather than being silently absorbed. I do not
      impute power output: it is the measured business figure, and filling it
      in invents generation that never happened.
    """
    typed = df.select(
        F.col("turbine_id").cast("int").alias("turbine_id"),
        F.to_timestamp("timestamp").alias("event_ts"),
        F.col("wind_speed").cast("double").alias("wind_speed"),
        F.col("wind_direction").cast("int").alias("wind_direction"),
        F.col("power_output").cast("double").alias("power_output"),
        F.col("source_file"),
        F.col("ingest_ts"),
    )
 
    usable = typed.filter(
        F.col("turbine_id").isNotNull() & F.col("event_ts").isNotNull()
    )
 
    # Outliers out. Null wind is tolerated - it is context, not the measure.
    in_range = usable.filter(
        F.col("power_output").between(POWER_MIN_MW, POWER_MAX_MW)
        & (F.col("wind_speed").isNull()
           | F.col("wind_speed").between(WIND_SPEED_MIN, WIND_SPEED_MAX))
        & (F.col("wind_direction").isNull()
           | F.col("wind_direction").between(WIND_DIR_MIN, WIND_DIR_MAX))
    )
 
    # between() is null-safe and returns null, so a missing power reading is
    # removed here too. That is the intent: "removed or imputed", and I remove.
    return (
        in_range
        .withColumn("stats_date", F.to_date("event_ts"))
        .dropDuplicates(["turbine_id", "event_ts"])
    )
 
 
def daily_stats(df: DataFrame, expected: int = EXPECTED_READINGS_PER_DAY) -> DataFrame:
    """Requirement 2: min, max and average power per turbine per day.
 
    completeness_pct sits beside every average so a consumer can tell an average
    over 24 readings from one over 6.
    """
    return (
        df.groupBy("turbine_id", "stats_date")
          .agg(
              F.min("power_output").alias("min_power_mw"),
              F.max("power_output").alias("max_power_mw"),
              F.round(F.avg("power_output"), 4).alias("avg_power_mw"),
              F.round(F.stddev_samp("power_output"), 4).alias("stddev_power_mw"),
              F.count("power_output").alias("reading_count"),
          )
          .withColumn("expected_count", F.lit(expected))
          .withColumn(
              "completeness_pct",
              F.round(100.0 * F.col("reading_count") / F.lit(expected), 2),
          )
    )
 
 
def find_anomalies(
    stats: DataFrame,
    sigma: float = ANOMALY_SIGMA,
    min_turbines: int = MIN_TURBINES_FOR_FLEET_STATS,
) -> DataFrame:
    """Requirement 3: turbines more than `sigma` sd from the mean for the period.
 
    Scored on the daily average, not on individual readings. The brief asks for
    turbines that deviated over a time period, and the grain matters: a single
    reading has a spread of ~0.87 MW while a daily average has ~0.18 MW. A
    six-hour outage scores -1.87 per reading (missed) and -2.53 daily (caught).
    """
    day = Window.partitionBy("stats_date")
 
    return (
        stats
        .withColumn("fleet_mean", F.avg("avg_power_mw").over(day))
        .withColumn("fleet_std", F.stddev_samp("avg_power_mw").over(day))
        .withColumn("turbines_reporting", F.count("turbine_id").over(day))
        .filter(F.col("turbines_reporting") >= min_turbines)
        # A zero standard deviation would divide by zero; when() returns null instead.
        .withColumn(
            "z_score",
            (F.col("avg_power_mw") - F.col("fleet_mean"))
            / F.when(F.col("fleet_std") != 0, F.col("fleet_std")),
        )
        .filter(F.abs(F.col("z_score")) > sigma)
        .select(
            "turbine_id", "stats_date", "avg_power_mw",
            F.round("fleet_mean", 4).alias("fleet_mean_mw"),
            F.round("fleet_std", 4).alias("fleet_std_mw"),
            F.round("z_score", 3).alias("z_score"),
            F.round(F.col("avg_power_mw") - F.col("fleet_mean"), 3).alias("deviation_mw"),
            F.when(F.col("z_score") < 0, "BELOW").otherwise("ABOVE").alias("direction"),
            "reading_count", "completeness_pct",
        )
    )