# Tests/conftest.py
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    s = SparkSession.getActiveSession()
    if s is None:
        # This will be the case if you run tests from a separate subprocess/kernel
        raise RuntimeError(
            "No active SparkSession found. Run tests in-process in a Databricks notebook "
            "(using pytest.main), not via subprocess."
        )
    return s