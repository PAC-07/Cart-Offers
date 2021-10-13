import pytest
from pyspark.sql.session import SparkSession


@pytest.fixture(scope="session")
def spark_test_session():
    return (
        SparkSession
        .builder
        .master('local[*]')
        .appName('unit-testing')
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )