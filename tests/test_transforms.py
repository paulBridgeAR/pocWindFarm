"""Unit tests for the gold transformations.

Only the computation is tested. Cleaning lives in the silver layer as declarative
expectations, so testing it would mean testing Databricks rather than my own logic.

Runs against local PySpark - no Databricks workspace required.
"""

import datetime as dt

from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, TimestampType, DateType,
)

from transforms import daily_stats, find_anomalies

DAY = dt.date(2022, 3, 1)

# Silver's shape, reduced to the columns gold actually reads.
SILVER = StructType([
    StructField("turbine_id",   IntegerType(),   False),
    StructField("event_ts",     TimestampType(), False),
    StructField("stats_date",   DateType(),      False),
    StructField("power_output", DoubleType(),    True),
])


def readings(turbine_id, powers, day=DAY):
    """One reading per hour for a turbine. `powers` may contain None."""
    return [
        (turbine_id, dt.datetime.combine(day, dt.time(hour)), day, power)
        for hour, power in enumerate(powers)
    ]


def silver(spark, rows):
    return spark.createDataFrame(rows, SILVER)


def fleet(spark, low_turbine_power=None, n_turbines=15, normal=3.0):
    """A day of readings for a whole fleet, optionally with one turbine running low."""
    rows = []
    for tid in range(1, n_turbines + 1):
        power = low_turbine_power if (low_turbine_power and tid == n_turbines) else normal
        rows += readings(tid, [power] * 24)
    return silver(spark, rows)


# --- Summary statistics - requirement 2 ------------------------------------

def test_min_max_average(spark):
    s = daily_stats(silver(spark, readings(1, [1.0, 2.0, 3.0]))).first()

    assert s["min_power_mw"] == 1.0
    assert s["max_power_mw"] == 3.0
    assert s["avg_power_mw"] == 2.0


def test_one_row_per_turbine_per_day(spark):
    rows = readings(1, [2.0] * 24) + readings(2, [3.0] * 24)
    assert daily_stats(silver(spark, rows)).count() == 2


def test_completeness_reflects_missing_readings(spark):
    """18 of an expected 24 readings arrived."""
    s = daily_stats(silver(spark, readings(1, [2.5] * 18))).first()

    assert s["reading_count"] == 18
    assert s["expected_count"] == 24
    assert s["completeness_pct"] == 75.0


def test_nulls_are_not_counted_as_readings(spark):
    """A null power reading is absent data, not a zero. Treating it as zero would
    drag the average down and every downstream number with it."""
    s = daily_stats(silver(spark, readings(1, [2.0] * 20 + [None] * 4))).first()

    assert s["reading_count"] == 20
    assert s["avg_power_mw"] == 2.0


# --- Anomalies - requirement 3 ---------------------------------------------

def test_flags_a_turbine_below_the_fleet(spark):
    """14 turbines at 3.0 MW, one at 1.0. Fleet mean 2.867, sd 0.516, so the low
    turbine scores z = -3.6 and the rest sit at +0.26."""
    found = find_anomalies(daily_stats(fleet(spark, low_turbine_power=1.0))).collect()

    assert len(found) == 1
    assert found[0]["turbine_id"] == 15
    assert found[0]["direction"] == "BELOW"
    assert found[0]["z_score"] < -2


def test_no_anomalies_when_the_fleet_agrees(spark):
    assert find_anomalies(daily_stats(fleet(spark))).count() == 0


def test_zero_variance_does_not_divide_by_zero(spark):
    """Identical readings give a standard deviation of zero. The guard must return
    null rather than raising or producing infinity."""
    assert find_anomalies(daily_stats(fleet(spark))).count() == 0


def test_too_few_turbines_produces_no_anomalies(spark):
    """A fleet mean built from three turbines is not worth scoring against."""
    df = fleet(spark, low_turbine_power=1.0, n_turbines=3)
    assert find_anomalies(daily_stats(df), min_turbines=5).count() == 0


def test_sigma_threshold_is_respected(spark):
    """The same data flags at a lower threshold and not at a higher one - proves the
    parameter is wired through rather than a hardcoded 2.0."""
    stats = daily_stats(fleet(spark, low_turbine_power=1.0))

    assert find_anomalies(stats, sigma=2.0).count() == 1
    assert find_anomalies(stats, sigma=4.0).count() == 0
