import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Provides a SparkSession for tests.
    scope=session => one Spark session for the whole test run (faster).
    """
    # Make Spark quieter during tests (optional)
    os.environ["PYSPARK_PYTHON"] = os.environ.get("PYSPARK_PYTHON", "python")

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-pyspark")
        # avoids some Windows temp / warehouse weirdness
        .config("spark.sql.warehouse.dir", os.path.join(os.getcwd(), "spark-warehouse-test"))
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()