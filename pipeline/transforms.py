"""All pipeline logic, as plain DataFrame -> DataFrame functions.
 
Nothing here imports the pipeline API. That is deliberate: declarative pipeline
code only executes inside a pipeline run, so any logic written inside the
decorated functions cannot be unit tested. Keeping it here means the same code
runs on Databricks and under pytest with a local SparkSession.
 
Cleaning is not here. It lives in 02_silver.sql as declarative expectations, where
Databricks enforces the rules and records pass/fail counts per rule in the event
log. This module holds only the computation, which is what is worth testing.
"""
 
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
 
# --- thresholds. One place, no magic numbers anywhere else -------------------
# The physical range bounds are not here: they are expectations in 02_silver.sql,
# and duplicating them would give two sources of truth that can drift apart.
 
EXPECTED_READINGS_PER_DAY = 24
ANOMALY_SIGMA = 2.0
MIN_TURBINES_FOR_FLEET_STATS = 5
 
 
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
    reading has a spread of ~0.87 MW while a daily average has ~0.18 MW.
 
    Verified against an injected fault - a turbine at 35% output for 12 hours.
    Scoring individual readings the worst one reached only z = -1.87 and the fault
    went undetected. Scoring the daily average it came out at z = -3.04 and was
    caught.
 
    The comparison is against the fleet on the same day. All 15 turbines draw from
    the same distribution, so the fleet is a valid reference. This is a
    distributional test rather than a physical one: the variance explained by
    timestamp measured at zero, so in this data the turbines do not share weather
    and "the others were in the same wind" is not an argument I can make, so I need to beat
    the distruibution here...
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