import os
import sys
import logging
from pyspark.sql import SparkSession

logger = logging.getLogger("RiverWatch.SparkSession")

_spark_session = None

def get_spark_session(app_name: str = "RiverWatch-Analytics") -> SparkSession:
    """
    Returns or initializes a local SparkSession optimized for student laptops.
    Ensures Windows environment variables are configured correctly.
    """
    global _spark_session
    if _spark_session is not None:
        try:
            # Check if active
            _spark_session.sparkContext.appName
            return _spark_session
        except Exception:
            _spark_session = None

    # Configure Python executable for PySpark worker processes on Windows
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    # Suppress verbose JVM loggers
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.ansi.enabled", "false")
    )

    try:
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        _spark_session = spark
        return spark
    except Exception as e:
        logger.error(f"Failed to create SparkSession: {e}")
        raise RuntimeError(f"PySpark session creation failed: {e}")

def stop_spark_session():
    """Stops the active SparkSession."""
    global _spark_session
    if _spark_session is not None:
        try:
            _spark_session.stop()
        except Exception:
            pass
        _spark_session = None
