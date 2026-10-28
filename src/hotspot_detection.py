from typing import Dict, Any, List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, DoubleType, ArrayType
)

# CPCB Reference Water Quality Criteria (Designated Best Use Class B - Outdoor Bathing)
CPCB_THRESHOLDS = {
    "do_min_standard": 5.0,        # mg/L (Minimum for Class B)
    "do_critical_threshold": 4.0,  # mg/L (Critical hypoxia indicator)
    "bod_max_standard": 3.0,       # mg/L (Maximum for Class B)
    "bod_critical_threshold": 8.0, # mg/L (Severe organic loading)
    "ph_min_standard": 6.5,
    "ph_max_standard": 8.5,
    "fc_max_standard": 2500,       # MPN / 100 mL (Max permissible for bathing)
    "nitrate_max_standard": 10.0,  # mg/L (Indicator limit)
}

# Sub-index weights for National Sanitation Foundation / CPCB WQI formulation
WQI_WEIGHTS = {
    "do": 0.30,
    "bod": 0.25,
    "ph": 0.15,
    "fecal_coliform": 0.15,
    "nitrate": 0.10,
    "turbidity": 0.05
}

def calculate_wqi_row(row_dict: Dict[str, Any]) -> float:
    """
    Computes a standardized Water Quality Index (0-100) based on weighted sub-indices.
    Gracefully handles missing parameters without inventing data by re-normalizing weights.
    100 = Pristine / Excellent, 0 = Severely Degraded.
    """
    sub_scores = {}

    # Dissolved Oxygen Sub-index (0 to 100)
    do_val = row_dict.get("dissolved_oxygen_mg_l")
    if do_val is not None:
        if do_val >= 7.0:
            sub_scores["do"] = 100.0
        elif do_val >= 5.0:
            sub_scores["do"] = 80.0 + (do_val - 5.0) * 10.0
        elif do_val >= 3.0:
            sub_scores["do"] = 40.0 + (do_val - 3.0) * 20.0
        else:
            sub_scores["do"] = max(0.0, do_val * 13.3)

    # BOD Sub-index (lower BOD is better)
    bod_val = row_dict.get("bod_mg_l")
    if bod_val is not None:
        if bod_val <= 2.0:
            sub_scores["bod"] = 100.0
        elif bod_val <= 3.0:
            sub_scores["bod"] = 80.0
        elif bod_val <= 6.0:
            sub_scores["bod"] = max(30.0, 80.0 - (bod_val - 3.0) * 16.6)
        elif bod_val <= 12.0:
            sub_scores["bod"] = max(10.0, 30.0 - (bod_val - 6.0) * 3.3)
        else:
            sub_scores["bod"] = 5.0

    # pH Sub-index (optimal 7.0 - 7.5)
    ph_val = row_dict.get("ph")
    if ph_val is not None:
        if 6.8 <= ph_val <= 7.8:
            sub_scores["ph"] = 100.0
        elif 6.5 <= ph_val <= 8.5:
            sub_scores["ph"] = 80.0
        elif 6.0 <= ph_val <= 9.0:
            sub_scores["ph"] = 50.0
        else:
            sub_scores["ph"] = 20.0

    # Fecal Coliform Sub-index
    fc_val = row_dict.get("fecal_coliform_mpn_100ml")
    if fc_val is not None:
        if fc_val <= 500:
            sub_scores["fecal_coliform"] = 100.0
        elif fc_val <= 2500:
            sub_scores["fecal_coliform"] = 70.0
        elif fc_val <= 10000:
            sub_scores["fecal_coliform"] = 40.0
        else:
            sub_scores["fecal_coliform"] = 10.0

    # Nitrate Sub-index
    nitrate_val = row_dict.get("nitrate_mg_l")
    if nitrate_val is not None:
        if nitrate_val <= 5.0:
            sub_scores["nitrate"] = 100.0
        elif nitrate_val <= 10.0:
            sub_scores["nitrate"] = 80.0
        elif nitrate_val <= 20.0:
            sub_scores["nitrate"] = 50.0
        else:
            sub_scores["nitrate"] = 20.0

    # Turbidity Sub-index
    turb_val = row_dict.get("turbidity_ntu")
    if turb_val is not None:
        if turb_val <= 5.0:
            sub_scores["turbidity"] = 100.0
        elif turb_val <= 15.0:
            sub_scores["turbidity"] = 75.0
        elif turb_val <= 30.0:
            sub_scores["turbidity"] = 50.0
        else:
            sub_scores["turbidity"] = 25.0

    if not sub_scores:
        return 50.0

    # Renormalize weights among present parameters
    total_weight = sum(WQI_WEIGHTS[param] for param in sub_scores.keys())
    weighted_sum = sum(sub_scores[param] * WQI_WEIGHTS[param] for param in sub_scores.keys())
    return round(weighted_sum / total_weight, 1)

def evaluate_hotspot_rules(row_dict: Dict[str, Any]) -> Tuple[bool, str, List[str], float]:
    """
    Evaluates transparent CPCB rules on a single record.
    Returns:
        is_hotspot (bool): True if flagged
        severity (str): 'Critical', 'High Priority', 'Moderate', 'Safe / Compliant'
        violations (list[str]): Explanation of triggered flags
        wqi (float): Water Quality Index score (0-100)
    """
    violations = []
    
    do_val = row_dict.get("dissolved_oxygen_mg_l")
    bod_val = row_dict.get("bod_mg_l")
    ph_val = row_dict.get("ph")
    fc_val = row_dict.get("fecal_coliform_mpn_100ml")
    nitrate_val = row_dict.get("nitrate_mg_l")

    # Rule 1: DO Evaluation
    if do_val is not None:
        if do_val < CPCB_THRESHOLDS["do_critical_threshold"]:
            violations.append(f"Critical DO Depletion: {do_val:.2f} mg/L (CPCB Bathing Min: 5.0)")
        elif do_val < CPCB_THRESHOLDS["do_min_standard"]:
            violations.append(f"Low Dissolved Oxygen: {do_val:.2f} mg/L (CPCB Bathing Min: 5.0)")

    # Rule 2: BOD Evaluation
    if bod_val is not None:
        if bod_val > CPCB_THRESHOLDS["bod_critical_threshold"]:
            violations.append(f"Severe Organic Pollution: BOD {bod_val:.2f} mg/L (CPCB Bathing Max: 3.0)")
        elif bod_val > CPCB_THRESHOLDS["bod_max_standard"]:
            violations.append(f"Elevated BOD: {bod_val:.2f} mg/L (CPCB Bathing Max: 3.0)")

    # Rule 3: pH Evaluation
    if ph_val is not None:
        if ph_val < CPCB_THRESHOLDS["ph_min_standard"]:
            violations.append(f"Acidic pH Anomaly: {ph_val:.2f} (Permissible: 6.5 - 8.5)")
        elif ph_val > CPCB_THRESHOLDS["ph_max_standard"]:
            violations.append(f"Alkaline pH Anomaly: {ph_val:.2f} (Permissible: 6.5 - 8.5)")

    # Rule 4: Fecal Coliform Evaluation
    if fc_val is not None and fc_val > CPCB_THRESHOLDS["fc_max_standard"]:
        violations.append(f"High Fecal Coliform: {int(fc_val):,} MPN/100mL (CPCB Bathing Limit: 2,500)")

    # Rule 5: Nitrate Evaluation
    if nitrate_val is not None and nitrate_val > CPCB_THRESHOLDS["nitrate_max_standard"]:
        violations.append(f"High Nitrate: {nitrate_val:.2f} mg/L (Threshold: 10.0)")

    violation_count = len(violations)
    is_hotspot = violation_count > 0
    wqi = calculate_wqi_row(row_dict)

    # Determine Severity based on transparent multi-criteria
    has_crit_bod = bod_val is not None and bod_val > CPCB_THRESHOLDS["bod_critical_threshold"]
    has_crit_do = do_val is not None and do_val < CPCB_THRESHOLDS["do_critical_threshold"]

    if (has_crit_bod and has_crit_do) or (violation_count >= 3) or (wqi < 35.0):
        severity = "Critical"
    elif has_crit_bod or has_crit_do or (violation_count >= 2) or (wqi < 55.0):
        severity = "High Priority"
    elif is_hotspot:
        severity = "Moderate"
    else:
        severity = "Safe / Compliant"

    return is_hotspot, severity, violations, wqi

def apply_hotspot_detection_spark(df: DataFrame) -> DataFrame:
    """
    Uses PySpark UDFs to execute transparent hotspot rules and compute WQI across all records.
    """
    spark = df.sparkSession

    # Register Python helper as UDF
    def _evaluate(do_val, bod_val, ph_val, fc_val, nitrate_val, turb_val):
        row = {
            "dissolved_oxygen_mg_l": do_val,
            "bod_mg_l": bod_val,
            "ph": ph_val,
            "fecal_coliform_mpn_100ml": fc_val,
            "nitrate_mg_l": nitrate_val,
            "turbidity_ntu": turb_val,
        }
        is_hotspot, severity, violations, wqi = evaluate_hotspot_rules(row)
        violation_str = " | ".join(violations) if violations else "Compliant with CPCB Standards"
        return (is_hotspot, severity, violation_str, len(violations), wqi)

    from pyspark.sql.types import StructType, StructField, BooleanType, StringType, IntegerType, DoubleType

    result_schema = StructType([
        StructField("is_hotspot", BooleanType(), False),
        StructField("hotspot_severity", StringType(), False),
        StructField("violation_reasons", StringType(), False),
        StructField("violation_count", IntegerType(), False),
        StructField("wqi_score", DoubleType(), False),
    ])

    eval_udf = F.udf(_evaluate, result_schema)

    df_evaluated = df.withColumn(
        "_eval_res",
        eval_udf(
            F.col("dissolved_oxygen_mg_l"),
            F.col("bod_mg_l"),
            F.col("ph"),
            F.col("fecal_coliform_mpn_100ml"),
            F.col("nitrate_mg_l"),
            F.col("turbidity_ntu")
        )
    )

    df_final = (
        df_evaluated
        .withColumn("is_hotspot", F.col("_eval_res.is_hotspot"))
        .withColumn("hotspot_severity", F.col("_eval_res.hotspot_severity"))
        .withColumn("violation_reasons", F.col("_eval_res.violation_reasons"))
        .withColumn("violation_count", F.col("_eval_res.violation_count"))
        .withColumn("wqi_score", F.col("_eval_res.wqi_score"))
        .drop("_eval_res")
    )

    return df_final

def get_station_hotspot_ranking(df_flagged: DataFrame) -> DataFrame:
    """
    Ranks river monitoring stations using PySpark aggregations based on:
    - Total sample count
    - Flagged observations count
    - Hotspot frequency percentage
    - Average WQI score
    - Average BOD and DO
    """
    station_stats = (
        df_flagged
        .groupBy("station_id", "station_name", "river_name", "state", "latitude", "longitude")
        .agg(
            F.count("*").alias("total_samples"),
            F.sum(F.when(F.col("is_hotspot"), 1).otherwise(0)).alias("flagged_samples"),
            F.sum(F.when(F.col("hotspot_severity") == "Critical", 1).otherwise(0)).alias("critical_samples"),
            F.round(F.avg("wqi_score"), 1).alias("avg_wqi"),
            F.round(F.avg("bod_mg_l"), 2).alias("avg_bod"),
            F.round(F.avg("dissolved_oxygen_mg_l"), 2).alias("avg_do"),
            F.round(F.avg("fecal_coliform_mpn_100ml"), 0).alias("avg_fc"),
        )
        .withColumn(
            "hotspot_percentage",
            F.round((F.col("flagged_samples") / F.col("total_samples")) * 100.0, 1)
        )
        .withColumn(
            "overall_status",
            F.when(F.col("critical_samples") > 0, "Critical Hotspot")
            .when(F.col("hotspot_percentage") >= 50.0, "High Risk Hotspot")
            .when(F.col("hotspot_percentage") > 0.0, "Moderate Hotspot")
            .otherwise("Safe / Compliant")
        )
        .orderBy(
            F.col("critical_samples").desc(),
            F.col("hotspot_percentage").desc(),
            F.col("avg_wqi").asc()
        )
    )

    return station_stats
