import io
from typing import Dict, Any, List
import pandas as pd
from datetime import datetime

def generate_markdown_analysis_report(
    kpis: Dict[str, Any],
    data_quality_report: Dict[str, Any],
    top_hotspots_df: pd.DataFrame,
    river_summary_df: pd.DataFrame,
    selected_filters: Dict[str, Any]
) -> str:
    """
    Generates a structured, data-driven Markdown analytical report
    summarizing all findings, station rankings, and PySpark data quality metrics.
    """
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Format Top Hotspots table
    hotspot_lines = []
    if not top_hotspots_df.empty:
        hotspot_lines.append("| Rank | Station Name | River | State | Flagged % | Avg BOD (mg/L) | Avg DO (mg/L) | Avg WQI | Status |")
        hotspot_lines.append("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
        for i, row in top_hotspots_df.head(10).reset_index(drop=True).iterrows():
            hotspot_lines.append(
                f"| {i+1} | {row.get('station_name', '')} | {row.get('river_name', '')} | {row.get('state', '')} | "
                f"{row.get('hotspot_percentage', 0)}% | {row.get('avg_bod', 'N/A')} | {row.get('avg_do', 'N/A')} | "
                f"{row.get('avg_wqi', 'N/A')} | {row.get('overall_status', '')} |"
            )
    else:
        hotspot_lines.append("No hotspot stations detected in the selected data slice.")
    hotspots_table_md = "\n".join(hotspot_lines)

    # Format River summary table
    river_lines = []
    if not river_summary_df.empty:
        river_lines.append("| River Basin | Stations | Samples | Avg WQI | Avg BOD (mg/L) | Avg DO (mg/L) | Hotspot Rate % |")
        river_lines.append("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
        for _, row in river_summary_df.iterrows():
            river_lines.append(
                f"| {row.get('river_name', '')} | {row.get('active_stations', 0)} | {row.get('samples_collected', 0)} | "
                f"{row.get('avg_wqi', 'N/A')} | {row.get('avg_bod', 'N/A')} | {row.get('avg_do', 'N/A')} | "
                f"{row.get('hotspot_rate_pct', 0)}% |"
            )
    else:
        river_lines.append("No river basin records available.")
    rivers_table_md = "\n".join(river_lines)

    # Missing values text
    missing_items = [f"- **{col}**: {cnt} missing ({data_quality_report.get('missing_percentages', {}).get(col, 0)}%)"
                     for col, cnt in data_quality_report.get("missing_values", {}).items() if cnt > 0]
    missing_summary = "\n".join(missing_items) if missing_items else "- No missing values encountered."

    # Out of range text
    out_items = [f"- **{param}**: {cnt} out-of-range anomalies identified and sanitized"
                 for param, cnt in data_quality_report.get("out_of_range_counts", {}).items() if cnt > 0]
    out_summary = "\n".join(out_items) if out_items else "- No out-of-range sensor readings found."

    report = f"""# RiverWatch: Indian River Water Quality Analytics & Hotspot Detection
**Big Data Analytics (BDA) Project Report**  
*Generated on:* `{now_str}`  
*Engine:* Apache Spark (PySpark) & Streamlit  

---

## 1. Executive Summary & KPIs
The RiverWatch analytics system processed surface water monitoring data modeled after the **Central Pollution Control Board (CPCB) National Water Quality Monitoring Programme (NWMP)**.

- **Total Records Ingested & Evaluated:** {kpis.get('total_records', 0):,}
- **Total Monitoring Stations:** {kpis.get('total_stations', 0)} across {kpis.get('total_rivers', 0)} river basins
- **Geographic Coverage:** {kpis.get('total_states', 0)} Indian States & UTs
- **Temporal Range:** {kpis.get('min_date', 'N/A')} to {kpis.get('max_date', 'N/A')}
- **Observations Flagged for Water Quality Non-Compliance:** {kpis.get('flagged_observations', 0):,} ({kpis.get('hotspot_percentage', 0)}%)
- **Critical Severity Incidents:** {kpis.get('critical_observations', 0):,}
- **Overall Mean Water Quality Index (WQI):** {kpis.get('avg_wqi', 0.0)} / 100
- **Mean Biochemical Oxygen Demand (BOD):** {kpis.get('avg_bod', 0.0)} mg/L (CPCB Bathing Max: 3.0 mg/L)
- **Mean Dissolved Oxygen (DO):** {kpis.get('avg_do', 0.0)} mg/L (CPCB Bathing Min: 5.0 mg/L)

---

## 2. Active Filter Scope
- **Rivers Included:** {', '.join(selected_filters.get('rivers', ['All']))}
- **States Included:** {', '.join(selected_filters.get('states', ['All']))}
- **Date Span:** {selected_filters.get('start_date', 'Earliest')} to {selected_filters.get('end_date', 'Latest')}

---

## 3. Ranked Pollution Hotspots Leaderboard
Locations where water quality repeatedly violated CPCB Designated Best Use Class B (Outdoor Bathing) thresholds:

{hotspots_table_md}

*Note: Results represent statistical non-compliance indicators requiring field validation and should not be construed as definitive proof without formal laboratory verification.*

---

## 4. River Basin Comparative Health Summary
Aggregated water health metrics categorized by river basin:

{rivers_table_md}

---

## 5. Big Data Pipeline & Data Quality Audit
PySpark performed data cleansing, schema enforcement, and deduplication prior to analytic aggregations:

- **Raw Records Received:** {data_quality_report.get('total_raw_records', 0):,}
- **Duplicate Records Removed:** {data_quality_report.get('duplicates_detected', 0)}
- **Clean Records Retained:** {data_quality_report.get('total_clean_records', 0):,}
- **Invalid Date Formats Fixed:** {data_quality_report.get('invalid_date_count', 0)}
- **Overall Data Health Score:** {data_quality_report.get('data_health_score_pct', 100.0)}%

### Missing Value Audit:
{missing_summary}

### Physical Range Anomaly Detection:
{out_summary}

---

## 6. Analytical Methodology
1. **CPCB Standards**: Evaluated in-situ Dissolved Oxygen (DO $\ge 5.0$ mg/L), Biochemical Oxygen Demand (BOD $\le 3.0$ mg/L), pH ($6.5 - 8.5$), and Fecal Coliform ($\le 2,500$ MPN/100mL).
2. **Water Quality Index (WQI)**: Calculated using a weighted arithmetic sub-index methodology with dynamic parameter re-normalization for missing fields.
3. **Severity Tiering**:
   - **Critical**: Depleted oxygen ($DO < 4.0$ mg/L) combined with high organic waste ($BOD > 8.0$ mg/L), or $\ge 3$ parameter breaches ($WQI < 35$).
   - **High Priority**: Significant single parameter failure or 2 simultaneous breaches ($WQI < 55$).
   - **Moderate**: Single parameter threshold breach.
   - **Safe / Compliant**: All parameters within permissible CPCB standards.

---
*Report auto-generated by RiverWatch BDA Platform.*
"""
    return report

def export_df_to_csv(df: pd.DataFrame) -> str:
    """Exports pandas DataFrame to CSV formatted string for in-browser download."""
    return df.to_csv(index=False)
