import os
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, DateType
)
import pandas as pd
from src.generate_dataset import generate_water_quality_dataset, MONITORING_STATIONS

# Explicit PySpark Schema for CPCB Surface Water Quality
WATER_QUALITY_SCHEMA = StructType([
    StructField("station_id", StringType(), True),
    StructField("station_name", StringType(), True),
    StructField("river_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("district", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("sampling_date", StringType(), True),  # String first, parsed to Date in preprocessing
    StructField("temperature_c", DoubleType(), True),
    StructField("dissolved_oxygen_mg_l", DoubleType(), True),
    StructField("ph", DoubleType(), True),
    StructField("conductivity_umhos_cm", DoubleType(), True),
    StructField("bod_mg_l", DoubleType(), True),
    StructField("nitrate_mg_l", DoubleType(), True),
    StructField("fecal_coliform_mpn_100ml", LongType(), True),
    StructField("total_coliform_mpn_100ml", LongType(), True),
    StructField("turbidity_ntu", DoubleType(), True),
])

DATASET_METADATA = {
    "source_name": "CPCB - National Water Quality Monitoring Programme (NWMP) Baseline Model",
    "portal_url": "https://cpcb.nic.in/water-pollution/",
    "is_synthetic": True,
    "synthetic_disclaimer": (
        "This dataset contains realistically simulated observations generated from published CPCB "
        "environmental profiles, monitoring station coordinates, and seasonal water chemistry models. "
        "It is clearly designated as simulated demonstration data for BDA educational purposes."
    ),
    "primary_rivers": sorted(list(set(s["river_name"] for s in MONITORING_STATIONS))),
    "total_monitoring_stations": len(MONITORING_STATIONS),
    "geographic_coverage": "14 Indian States across Ganga, Yamuna, Godavari, Krishna, Kaveri, Sabarmati, Narmada, Brahmaputra, Mahanadi, Sutlej basins."
}

def load_csv_data(spark: SparkSession, file_path: str, use_schema: bool = True) -> DataFrame:
    """
    Loads raw water quality data from CSV into a PySpark DataFrame.
    Supports either explicit schema definition or schema inference.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input data file does not exist: {file_path}")

    reader = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
    )

    if use_schema:
        df = reader.schema(WATER_QUALITY_SCHEMA).csv(file_path)
    else:
        df = reader.option("inferSchema", "true").csv(file_path)

    return df

def generate_and_save_benchmark_data(
    output_path: str,
    samples_per_station: int = 50,
    inject_anomalies: bool = True
) -> str:
    """
    Generates a larger synthetic benchmark dataset for scalability testing.
    """
    os.makedirs(os.path.dirname(os.path.abspath(output_path)), exist_ok=True)
    df_pandas = generate_water_quality_dataset(
        num_samples_per_station=samples_per_station,
        inject_anomalies=inject_anomalies
    )
    df_pandas.to_csv(output_path, index=False)
    return output_path

def get_dataset_metadata() -> Dict[str, Any]:
    """Returns dataset provenance and licensing metadata."""
    return DATASET_METADATA
