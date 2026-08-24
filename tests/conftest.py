"""Shared test fixtures.

getOrCreate() rather than a fresh builder, so the same tests run locally under pytest
and inside a Databricks notebook where a session already exists.
"""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("turbine-tests")
        .config("spark.sql.shuffle.partitions", "2")   # 200 is pointless on 15 rows
        .getOrCreate()
    )
