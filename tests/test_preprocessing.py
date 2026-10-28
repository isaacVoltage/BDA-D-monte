import os
import sys
import pytest
from pyspark.sql import SparkSession

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.spark_session import get_spark_session
from src.preprocessing import standardize_column_names, clean_and_audit_water_quality_data, filter_dataframe

@pytest.fixture(scope="session")
def spark():
    """Provides a shared local SparkSession for tests."""
    session = get_spark_session("RiverWatch-Tests")
    yield session

def test_standardize_column_names(spark):
    sample_data = [(1, "Ganga", 7.5)]
    columns = ["Station ID", "River-Name", "pH.Value"]
    df = spark.createDataFrame(sample_data, columns)
    
    standardized_df = standardize_column_names(df)
    assert "station_id" in standardized_df.columns
    assert "river_name" in standardized_df.columns
    assert "ph_value" in standardized_df.columns

def test_clean_and_audit_pipeline(spark):
    # Create test data with: 1 pristine row, 1 duplicate row, 1 out-of-range pH, 1 invalid date
    data = [
        ("STN_01", "Rishikesh", "Ganga", "Uttarakhand", "2023-01-15", 7.4, 8.5, 1.5, 100),
        ("STN_01", "Rishikesh", "Ganga", "Uttarakhand", "2023-01-15", 7.4, 8.5, 1.5, 100),  # Duplicate
        ("STN_02", "Kanpur", "Ganga", "Uttar Pradesh", "2023-01-15", 15.5, 2.0, 16.0, 5000),  # Invalid pH > 14
        ("STN_03", "Varanasi", "Ganga", "Uttar Pradesh", "invalid-date", 7.6, 5.0, 4.0, 2000),  # Bad date
    ]
    cols = [
        "station_id", "station_name", "river_name", "state", "sampling_date",
        "ph", "dissolved_oxygen_mg_l", "bod_mg_l", "fecal_coliform_mpn_100ml"
    ]
    raw_df = spark.createDataFrame(data, cols)

    clean_df, audit = clean_and_audit_water_quality_data(raw_df)

    # Audited duplicate check
    assert audit["total_raw_records"] == 4
    assert audit["duplicates_detected"] == 1
    assert audit["out_of_range_counts"]["ph"] >= 1

    # In clean dataframe, out-of-range pH should have been converted to None/Null
    kanpur_row = clean_df.filter(clean_df.station_name == "Kanpur").collect()[0]
    assert kanpur_row.ph is None

def test_filter_dataframe(spark):
    data = [
        ("STN_01", "Haridwar", "Ganga", "Uttarakhand", "2023-01-10"),
        ("STN_02", "Delhi ITO", "Yamuna", "Delhi", "2023-02-15"),
        ("STN_03", "Nashik", "Godavari", "Maharashtra", "2023-03-20")
    ]
    cols = ["station_id", "station_name", "river_name", "state", "sampling_date"]
    df = spark.createDataFrame(data, cols)

    # Filter by river
    ganga_df = filter_dataframe(df, rivers=["Ganga"])
    assert ganga_df.count() == 1
    assert ganga_df.collect()[0]["river_name"] == "Ganga"

    # Filter by date
    date_filtered = filter_dataframe(df, start_date="2023-02-01", end_date="2023-02-28")
    assert date_filtered.count() == 1
    assert date_filtered.collect()[0]["river_name"] == "Yamuna"
