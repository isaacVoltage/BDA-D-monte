from typing import Dict, Any, Tuple, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, DateType

# Realistic physical bounds for environmental water quality measurements
PHYSICAL_VALID_RANGES = {
    "ph": (0.0, 14.0),
    "dissolved_oxygen_mg_l": (0.0, 25.0),
    "bod_mg_l": (0.0, 200.0),
    "temperature_c": (0.0, 55.0),
    "conductivity_umhos_cm": (0.0, 20000.0),
    "nitrate_mg_l": (0.0, 300.0),
    "turbidity_ntu": (0.0, 1000.0),
    "fecal_coliform_mpn_100ml": (0, 100000000),
    "total_coliform_mpn_100ml": (0, 100000000),
}

def standardize_column_names(df: DataFrame) -> DataFrame:
    """Standardizes DataFrame column names to lower_snake_case and strips whitespace."""
    for col_name in df.columns:
        clean_name = col_name.strip().lower().replace(" ", "_").replace("-", "_").replace(".", "_")
        if clean_name != col_name:
            df = df.withColumnRenamed(col_name, clean_name)
    return df

def clean_and_audit_water_quality_data(raw_df: DataFrame) -> Tuple[DataFrame, Dict[str, Any]]:
    """
    Executes a comprehensive PySpark preprocessing and data quality audit pipeline.
    
    1. Standardizes columns and dates.
    2. Identifies and quantifies duplicate records.
    3. Quantifies missing / NULL values across all fields.
    4. Identifies physically impossible / corrupted sensor readings.
    5. Produces a sanitized DataFrame and a transparent Data Quality Audit Report.
    """
    # 1. Standardize columns
    df = standardize_column_names(raw_df)
    total_raw_records = df.count()

    if total_raw_records == 0:
        empty_report = {
            "total_raw_records": 0,
            "total_clean_records": 0,
            "duplicates_detected": 0,
            "missing_values": {},
            "missing_percentages": {},
            "out_of_range_counts": {},
            "invalid_date_count": 0,
            "data_health_score_pct": 100.0,
        }
        return df, empty_report

    # 2. Date parsing & validation using try_to_date for fault-tolerant parsing
    parsed_date_col = F.coalesce(
        F.expr("try_to_date(sampling_date, 'yyyy-MM-dd')"),
        F.expr("try_to_date(sampling_date, 'dd-MM-yyyy')"),
        F.expr("try_to_date(sampling_date, 'yyyy/MM/dd')")
    )
    df = df.withColumn("parsed_sampling_date", parsed_date_col)
    invalid_dates_count = df.filter(F.col("parsed_sampling_date").isNull() & F.col("sampling_date").isNotNull()).count()
    df = df.drop("sampling_date").withColumnRenamed("parsed_sampling_date", "sampling_date")

    # 3. Duplicate detection
    # A true duplicate in river monitoring has identical station, river, and sampling date
    key_cols = ["station_id", "sampling_date"] if "station_id" in df.columns else ["sampling_date"]
    unique_count = df.dropDuplicates(key_cols).count()
    duplicates_detected = total_raw_records - unique_count
    
    # Deduplicate keeping first occurrence
    df_dedup = df.dropDuplicates(key_cols)

    # 4. Missing value audit using Spark aggregations (type-safe for all columns)
    missing_exprs = []
    for c, dtype in df_dedup.dtypes:
        if dtype in ("double", "float"):
            cond = F.col(c).isNull() | F.isnan(F.col(c))
        else:
            cond = F.col(c).isNull()
        missing_exprs.append(F.count(F.when(cond, 1)).alias(c))
    missing_row = df_dedup.agg(*missing_exprs).collect()[0].asDict()
    
    missing_values = {col: int(missing_row[col]) for col in df_dedup.columns}
    missing_percentages = {
        col: round((missing_values[col] / unique_count) * 100, 2)
        for col in df_dedup.columns
    }

    # 5. Out-of-range sensor reading audit in a single Spark aggregation pass
    out_of_range_exprs = []
    applicable_params = []
    for param, (low, high) in PHYSICAL_VALID_RANGES.items():
        if param in df_dedup.columns:
            invalid_expr = F.col(param).isNotNull() & ((F.col(param) < low) | (F.col(param) > high))
            out_of_range_exprs.append(F.count(F.when(invalid_expr, 1)).alias(param))
            applicable_params.append((param, invalid_expr))

    out_of_range_counts = {p: 0 for p in PHYSICAL_VALID_RANGES.keys()}
    if out_of_range_exprs:
        out_row = df_dedup.agg(*out_of_range_exprs).collect()[0].asDict()
        for p, cnt in out_row.items():
            out_of_range_counts[p] = int(cnt or 0)

    # Sanitize invalid readings to null
    for param, invalid_expr in applicable_params:
        df_dedup = df_dedup.withColumn(
            param,
            F.when(invalid_expr, F.lit(None).cast(df_dedup.schema[param].dataType)).otherwise(F.col(param))
        )

    # Clean records that have at least valid station and date
    clean_df = df_dedup.filter(F.col("station_name").isNotNull() & F.col("sampling_date").isNotNull())
    total_clean_records = clean_df.count()

    # Calculate overall data health score percentage
    total_out_of_range = sum(out_of_range_counts.values())
    total_missing_in_key_params = sum(missing_values.get(p, 0) for p in ["dissolved_oxygen_mg_l", "ph", "bod_mg_l"])
    total_anomalies = duplicates_detected + invalid_dates_count + total_out_of_range + total_missing_in_key_params
    total_evaluated_cells = max(1, total_raw_records * len(df.columns))
    data_health_score = max(0.0, round(100.0 - (total_anomalies / total_evaluated_cells * 100), 2))

    audit_report = {
        "total_raw_records": total_raw_records,
        "total_clean_records": total_clean_records,
        "duplicates_detected": duplicates_detected,
        "missing_values": missing_values,
        "missing_percentages": missing_percentages,
        "out_of_range_counts": out_of_range_counts,
        "invalid_date_count": invalid_dates_count,
        "data_health_score_pct": data_health_score
    }

    return clean_df, audit_report

def filter_dataframe(
    df: DataFrame,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    rivers: Optional[List[str]] = None,
    states: Optional[List[str]] = None,
    stations: Optional[List[str]] = None
) -> DataFrame:
    """
    Applies multi-dimensional filters on a PySpark DataFrame based on user criteria.
    """
    filtered = df

    if start_date:
        filtered = filtered.filter(F.col("sampling_date") >= F.lit(start_date))
    if end_date:
        filtered = filtered.filter(F.col("sampling_date") <= F.lit(end_date))
    if rivers and len(rivers) > 0 and "All" not in rivers:
        filtered = filtered.filter(F.col("river_name").isin(rivers))
    if states and len(states) > 0 and "All" not in states:
        filtered = filtered.filter(F.col("state").isin(states))
    if stations and len(stations) > 0 and "All" not in stations:
        filtered = filtered.filter(F.col("station_name").isin(stations))

    return filtered
