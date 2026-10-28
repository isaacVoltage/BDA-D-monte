from typing import Dict, Any, List, Optional
import pandas as pd
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

def compute_overall_kpis(df: DataFrame) -> Dict[str, Any]:
    """
    Computes top-level Big Data Analytics metrics across the active DataFrame using PySpark aggregations.
    """
    total_records = df.count()
    if total_records == 0:
        return {
            "total_records": 0,
            "total_stations": 0,
            "total_rivers": 0,
            "total_states": 0,
            "flagged_observations": 0,
            "hotspot_percentage": 0.0,
            "critical_observations": 0,
            "avg_wqi": 0.0,
            "avg_do": 0.0,
            "avg_bod": 0.0,
            "avg_ph": 0.0,
            "min_date": "N/A",
            "max_date": "N/A"
        }

    agg_row = df.agg(
        F.countDistinct("station_id").alias("total_stations"),
        F.countDistinct("river_name").alias("total_rivers"),
        F.countDistinct("state").alias("total_states"),
        F.sum(F.when(F.col("is_hotspot"), 1).otherwise(0)).alias("flagged_observations"),
        F.sum(F.when(F.col("hotspot_severity") == "Critical", 1).otherwise(0)).alias("critical_observations"),
        F.round(F.avg("wqi_score"), 1).alias("avg_wqi"),
        F.round(F.avg("dissolved_oxygen_mg_l"), 2).alias("avg_do"),
        F.round(F.avg("bod_mg_l"), 2).alias("avg_bod"),
        F.round(F.avg("ph"), 2).alias("avg_ph"),
        F.min("sampling_date").alias("min_date"),
        F.max("sampling_date").alias("max_date")
    ).collect()[0].asDict()

    flagged_cnt = int(agg_row["flagged_observations"] or 0)
    hotspot_pct = round((flagged_cnt / total_records) * 100.0, 1)

    return {
        "total_records": total_records,
        "total_stations": int(agg_row["total_stations"] or 0),
        "total_rivers": int(agg_row["total_rivers"] or 0),
        "total_states": int(agg_row["total_states"] or 0),
        "flagged_observations": flagged_cnt,
        "hotspot_percentage": hotspot_pct,
        "critical_observations": int(agg_row["critical_observations"] or 0),
        "avg_wqi": float(agg_row["avg_wqi"] or 0.0),
        "avg_do": float(agg_row["avg_do"] or 0.0),
        "avg_bod": float(agg_row["avg_bod"] or 0.0),
        "avg_ph": float(agg_row["avg_ph"] or 0.0),
        "min_date": str(agg_row["min_date"] or "N/A"),
        "max_date": str(agg_row["max_date"] or "N/A")
    }

def compute_river_summary(df: DataFrame) -> DataFrame:
    """
    Aggregates water quality metrics by river basin.
    """
    return (
        df.groupBy("river_name")
        .agg(
            F.countDistinct("station_id").alias("active_stations"),
            F.count("*").alias("samples_collected"),
            F.round(F.avg("wqi_score"), 1).alias("avg_wqi"),
            F.round(F.avg("bod_mg_l"), 2).alias("avg_bod"),
            F.round(F.avg("dissolved_oxygen_mg_l"), 2).alias("avg_do"),
            F.round(F.avg("ph"), 2).alias("avg_ph"),
            F.round(F.avg("turbidity_ntu"), 1).alias("avg_turbidity"),
            F.round(F.avg("fecal_coliform_mpn_100ml"), 0).alias("avg_fecal_coliform"),
            F.sum(F.when(F.col("is_hotspot"), 1).otherwise(0)).alias("hotspot_count"),
        )
        .withColumn(
            "hotspot_rate_pct",
            F.round((F.col("hotspot_count") / F.col("samples_collected")) * 100.0, 1)
        )
        .orderBy(F.col("hotspot_rate_pct").desc(), F.col("avg_wqi").asc())
    )

def compute_station_descriptive_stats(df: DataFrame) -> DataFrame:
    """
    Computes statistical descriptive metrics (Mean, Min, Max, StdDev) per monitoring station.
    """
    return (
        df.groupBy("station_name", "river_name", "state")
        .agg(
            F.count("*").alias("sample_count"),
            F.round(F.avg("wqi_score"), 1).alias("mean_wqi"),
            F.round(F.avg("dissolved_oxygen_mg_l"), 2).alias("mean_do"),
            F.round(F.min("dissolved_oxygen_mg_l"), 2).alias("min_do"),
            F.round(F.max("dissolved_oxygen_mg_l"), 2).alias("max_do"),
            F.round(F.stddev("dissolved_oxygen_mg_l"), 2).alias("std_do"),
            F.round(F.avg("bod_mg_l"), 2).alias("mean_bod"),
            F.round(F.min("bod_mg_l"), 2).alias("min_bod"),
            F.round(F.max("bod_mg_l"), 2).alias("max_bod"),
            F.round(F.stddev("bod_mg_l"), 2).alias("std_bod"),
            F.round(F.avg("ph"), 2).alias("mean_ph"),
            F.round(F.avg("fecal_coliform_mpn_100ml"), 0).alias("mean_fecal_coliform"),
        )
        .orderBy("river_name", "station_name")
    )

def compute_time_series_aggregations(df: DataFrame, group_col: str = "river_name") -> DataFrame:
    """
    Aggregates time-series trends by month and specified grouping column (river or station).
    """
    # Create year_month column (YYYY-MM)
    df_ym = df.withColumn(
        "year_month",
        F.date_format(F.col("sampling_date"), "yyyy-MM")
    )

    return (
        df_ym.groupBy("year_month", group_col)
        .agg(
            F.round(F.avg("wqi_score"), 1).alias("avg_wqi"),
            F.round(F.avg("dissolved_oxygen_mg_l"), 2).alias("avg_do"),
            F.round(F.avg("bod_mg_l"), 2).alias("avg_bod"),
            F.round(F.avg("ph"), 2).alias("avg_ph"),
            F.round(F.avg("temperature_c"), 1).alias("avg_temp"),
            F.round(F.avg("fecal_coliform_mpn_100ml"), 0).alias("avg_fc"),
            F.round(F.avg("turbidity_ntu"), 1).alias("avg_turbidity"),
            F.count("*").alias("monthly_sample_count")
        )
        .orderBy("year_month", group_col)
    )

def detect_statistical_anomalies(
    df: DataFrame,
    param_col: str = "bod_mg_l",
    z_threshold: float = 2.5
) -> DataFrame:
    """
    Identifies statistical anomalies using PySpark Window functions (Z-score approach).
    Z = |x - mean| / stddev. If Z > z_threshold, flagged as unusual statistical outlier.
    """
    # Window across the specific river or station for localized baseline
    window_spec = Window.partitionBy("station_name")

    df_z = (
        df.filter(F.col(param_col).isNotNull())
        .withColumn("mean_val", F.avg(F.col(param_col)).over(window_spec))
        .withColumn("std_val", F.stddev(F.col(param_col)).over(window_spec))
        .withColumn(
            "z_score",
            F.when(
                (F.col("std_val").isNotNull()) & (F.col("std_val") > 0.0001),
                F.abs((F.col(param_col) - F.col("mean_val")) / F.col("std_val"))
            ).otherwise(0.0)
        )
        .withColumn(
            "is_statistical_anomaly",
            F.col("z_score") >= z_threshold
        )
    )

    return df_z.filter(F.col("is_statistical_anomaly")).orderBy(F.col("z_score").desc())

def to_pandas_safe(spark_df: DataFrame) -> pd.DataFrame:
    """
    Safely collects a Spark DataFrame to a Pandas DataFrame for frontend rendering.
    """
    try:
        return spark_df.toPandas()
    except Exception as e:
        # Fallback if pyarrow / pandas incompatibility arises
        records = [row.asDict() for row in spark_df.collect()]
        return pd.DataFrame(records)
