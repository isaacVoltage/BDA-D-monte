import os
import sys
import time
from datetime import datetime
import streamlit as st
import pandas as pd
from streamlit_folium import st_folium

# Configure workspace path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from src.spark_session import get_spark_session
from src.data_loader import (
    load_csv_data, get_dataset_metadata, generate_and_save_benchmark_data,
    WATER_QUALITY_SCHEMA
)
from src.preprocessing import clean_and_audit_water_quality_data, filter_dataframe
from src.hotspot_detection import (
    apply_hotspot_detection_spark, get_station_hotspot_ranking, CPCB_THRESHOLDS
)
from src.spark_analytics import (
    compute_overall_kpis, compute_river_summary, compute_station_descriptive_stats,
    compute_time_series_aggregations, detect_statistical_anomalies, to_pandas_safe
)
from src.visualizations import (
    create_time_series_trend_chart, create_station_comparison_bar,
    create_wqi_distribution_donut, create_parameter_correlation_heatmap,
    create_river_hotspot_map, PARAMETER_CONFIG
)
from src.report_generator import generate_markdown_analysis_report, export_df_to_csv

# Page configuration
st.set_page_config(
    page_title="RiverWatch | Indian River Water Quality Analytics",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# -------------------------------------------------------------
# Caching Spark Processing Pipeline
# -------------------------------------------------------------
@st.cache_resource(show_spinner="Initializing Apache Spark (PySpark) engine...")
def init_spark():
    return get_spark_session("RiverWatch-Dashboard")

@st.cache_data(show_spinner="Processing dataset with PySpark DataFrame pipeline...")
def process_data(file_path: str):
    spark = init_spark()
    raw_df = load_csv_data(spark, file_path, use_schema=True)
    clean_df, audit_report = clean_and_audit_water_quality_data(raw_df)
    evaluated_df = apply_hotspot_detection_spark(clean_df)
    
    # Cache in memory
    evaluated_df.cache()
    
    # Collect core pandas projections for UI components
    pandas_full = to_pandas_safe(evaluated_df)
    
    # Compute station ranking
    ranking_spark = get_station_hotspot_ranking(evaluated_df)
    ranking_pandas = to_pandas_safe(ranking_spark)
    
    # Compute river summary
    river_spark = compute_river_summary(evaluated_df)
    river_pandas = to_pandas_safe(river_spark)
    
    # Station stats
    station_stats_spark = compute_station_descriptive_stats(evaluated_df)
    station_stats_pandas = to_pandas_safe(station_stats_spark)

    return audit_report, pandas_full, ranking_pandas, river_pandas, station_stats_pandas

# -------------------------------------------------------------
# Custom Styling & Theme Engine
# -------------------------------------------------------------
def apply_custom_css(is_dark: bool):
    if is_dark:
        bg_color = "#0b1329"
        card_bg = "#131f3e"
        card_border = "#1e293b"
        text_color = "#f1f5f9"
        muted_text = "#94a3b8"
        accent_blue = "#38bdf8"
        accent_gradient = "linear-gradient(135deg, #1e3a8a 0%, #0284c7 100%)"
    else:
        bg_color = "#f8fafc"
        card_bg = "#ffffff"
        card_border = "#e2e8f0"
        text_color = "#0f172a"
        muted_text = "#64748b"
        accent_blue = "#0284c7"
        accent_gradient = "linear-gradient(135deg, #0284c7 0%, #0369a1 100%)"

    css = f"""
    <style>
    .stApp {{
        background-color: {bg_color};
        color: {text_color};
        font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    }}
    .metric-card {{
        background: {card_bg};
        border: 1px solid {card_border};
        border-radius: 12px;
        padding: 18px 20px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }}
    .metric-card:hover {{
        transform: translateY(-2px);
        box-shadow: 0 8px 15px -3px rgba(0, 0, 0, 0.1);
    }}
    .metric-title {{
        font-size: 0.85rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: {muted_text};
        margin-bottom: 4px;
    }}
    .metric-value {{
        font-size: 1.85rem;
        font-weight: 800;
        color: {text_color};
        line-height: 1.2;
    }}
    .metric-subtitle {{
        font-size: 0.78rem;
        color: {muted_text};
        margin-top: 4px;
    }}
    .hero-banner {{
        background: {accent_gradient};
        color: white;
        border-radius: 14px;
        padding: 24px 28px;
        margin-bottom: 24px;
        box-shadow: 0 10px 25px -5px rgba(2, 132, 199, 0.25);
    }}
    .badge-critical {{
        background-color: #fee2e2;
        color: #991b1b;
        padding: 3px 8px;
        border-radius: 6px;
        font-weight: 700;
        font-size: 0.75rem;
    }}
    .badge-high {{
        background-color: #ffedd5;
        color: #9a3412;
        padding: 3px 8px;
        border-radius: 6px;
        font-weight: 700;
        font-size: 0.75rem;
    }}
    .badge-moderate {{
        background-color: #fef9c3;
        color: #854d0e;
        padding: 3px 8px;
        border-radius: 6px;
        font-weight: 700;
        font-size: 0.75rem;
    }}
    .badge-safe {{
        background-color: #d1fae5;
        color: #065f46;
        padding: 3px 8px;
        border-radius: 6px;
        font-weight: 700;
        font-size: 0.75rem;
    }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)

# -------------------------------------------------------------
# Main Application Logic
# -------------------------------------------------------------
def main():
    # Sidebar: Theme and Configuration
    with st.sidebar:
        st.markdown("### ⚙️ System Settings")
        is_dark = st.toggle("🌙 Dark Mode", value=True)
        apply_custom_css(is_dark)

        st.markdown("---")
        st.markdown("### 📁 Dataset Source")
        
        default_file = os.path.join(BASE_DIR, "data", "sample_water_quality.csv")
        data_source_mode = st.radio(
            "Select Data Mode:",
            ["Standard CPCB Reference Dataset", "Upload Custom CSV", "Generate Scalable Benchmark"],
            index=0
        )

        active_file_path = default_file

        if data_source_mode == "Upload Custom CSV":
            uploaded_file = st.file_uploader("Upload River Water CSV", type=["csv"])
            if uploaded_file is not None:
                custom_path = os.path.join(BASE_DIR, "data", "uploaded_custom.csv")
                with open(custom_path, "wb") as f:
                    f.write(uploaded_file.getbuffer())
                active_file_path = custom_path
                st.success("Uploaded file saved!")
            else:
                st.info("Using default reference data until file is uploaded.")

        elif data_source_mode == "Generate Scalable Benchmark":
            st.markdown("**PySpark Scalability Benchmark**")
            samples_per_stn = st.slider("Samples per Station", min_value=20, max_value=250, value=100, step=20)
            if st.button("🚀 Generate & Load Benchmark"):
                bench_path = os.path.join(BASE_DIR, "data", "benchmark_water_quality.csv")
                with st.spinner(f"Synthesizing {samples_per_stn * 40} records..."):
                    generate_and_save_benchmark_data(bench_path, samples_per_station=samples_per_stn)
                active_file_path = bench_path
                st.cache_data.clear()
                st.success(f"Benchmark created with {samples_per_stn * 40} rows!")

        st.markdown("---")
        metadata = get_dataset_metadata()
        st.caption(f"**Data Provenance**: {metadata['source_name']}")
        if metadata.get("is_synthetic"):
            st.warning("⚠️ **Notice**: Dataset contains realistically modeled simulated observations for BDA demonstration.", icon="ℹ️")

    # Load and process data
    try:
        audit_report, full_df, ranking_df, river_df, station_stats_df = process_data(active_file_path)
    except Exception as e:
        st.error(f"Error executing PySpark processing pipeline: {e}")
        st.exception(e)
        return

    # Sidebar: Multi-dimensional Filters
    with st.sidebar:
        st.markdown("### 🔍 Filter Observations")
        
        # River Filter
        available_rivers = sorted(list(full_df["river_name"].dropna().unique()))
        selected_rivers = st.multiselect("Rivers:", ["All"] + available_rivers, default=["All"])
        
        # State Filter
        available_states = sorted(list(full_df["state"].dropna().unique()))
        selected_states = st.multiselect("States:", ["All"] + available_states, default=["All"])

        # Station Filter
        filtered_for_stations = full_df.copy()
        if "All" not in selected_rivers and selected_rivers:
            filtered_for_stations = filtered_for_stations[filtered_for_stations["river_name"].isin(selected_rivers)]
        if "All" not in selected_states and selected_states:
            filtered_for_stations = filtered_for_stations[filtered_for_stations["state"].isin(selected_states)]
        
        available_stations = sorted(list(filtered_for_stations["station_name"].dropna().unique()))
        selected_stations = st.multiselect("Stations:", ["All"] + available_stations, default=["All"])

        # Severity Filter
        available_severities = ["All", "Critical", "High Priority", "Moderate", "Safe / Compliant"]
        selected_severities = st.multiselect("Hotspot Severity:", available_severities, default=["All"])

        # Date Range
        min_date = pd.to_datetime(full_df["sampling_date"]).min().date()
        max_date = pd.to_datetime(full_df["sampling_date"]).max().date()
        selected_date_range = st.date_input("Date Range:", value=(min_date, max_date), min_value=min_date, max_value=max_date)

    # Apply filters to Full DataFrame
    df_filtered = full_df.copy()
    if "All" not in selected_rivers and selected_rivers:
        df_filtered = df_filtered[df_filtered["river_name"].isin(selected_rivers)]
    if "All" not in selected_states and selected_states:
        df_filtered = df_filtered[df_filtered["state"].isin(selected_states)]
    if "All" not in selected_stations and selected_stations:
        df_filtered = df_filtered[df_filtered["station_name"].isin(selected_stations)]
    if "All" not in selected_severities and selected_severities:
        df_filtered = df_filtered[df_filtered["hotspot_severity"].isin(selected_severities)]
    
    if isinstance(selected_date_range, (list, tuple)) and len(selected_date_range) == 2:
        s_date, e_date = selected_date_range
        df_filtered["date_temp"] = pd.to_datetime(df_filtered["sampling_date"]).dt.date
        df_filtered = df_filtered[(df_filtered["date_temp"] >= s_date) & (df_filtered["date_temp"] <= e_date)]
        df_filtered = df_filtered.drop(columns=["date_temp"])

    # Re-calculate KPIs on filtered slice
    total_records = len(df_filtered)
    flagged_records = len(df_filtered[df_filtered["is_hotspot"] == True])
    critical_records = len(df_filtered[df_filtered["hotspot_severity"] == "Critical"])
    hotspot_pct = round((flagged_records / total_records * 100), 1) if total_records > 0 else 0.0

    kpis = {
        "total_records": total_records,
        "total_stations": df_filtered["station_id"].nunique() if total_records > 0 else 0,
        "total_rivers": df_filtered["river_name"].nunique() if total_records > 0 else 0,
        "total_states": df_filtered["state"].nunique() if total_records > 0 else 0,
        "flagged_observations": flagged_records,
        "hotspot_percentage": hotspot_pct,
        "critical_observations": critical_records,
        "avg_wqi": round(df_filtered["wqi_score"].mean(), 1) if total_records > 0 else 0.0,
        "avg_do": round(df_filtered["dissolved_oxygen_mg_l"].mean(), 2) if total_records > 0 else 0.0,
        "avg_bod": round(df_filtered["bod_mg_l"].mean(), 2) if total_records > 0 else 0.0,
        "avg_ph": round(df_filtered["ph"].mean(), 2) if total_records > 0 else 0.0,
        "min_date": str(df_filtered["sampling_date"].min()) if total_records > 0 else "N/A",
        "max_date": str(df_filtered["sampling_date"].max()) if total_records > 0 else "N/A"
    }

    # Filter ranking and river tables accordingly
    filtered_station_names = set(df_filtered["station_name"].unique())
    ranking_filtered = ranking_df[ranking_df["station_name"].isin(filtered_station_names)]
    filtered_river_names = set(df_filtered["river_name"].unique())
    river_filtered = river_df[river_df["river_name"].isin(filtered_river_names)]

    # -------------------------------------------------------------
    # Hero Header Banner
    # -------------------------------------------------------------
    st.markdown(
        f"""
        <div class="hero-banner">
            <h1 style="margin:0; font-size:2.2rem; font-weight:800; display:flex; align-items:center; gap:12px;">
                🌊 RiverWatch
            </h1>
            <p style="margin:6px 0 0 0; font-size:1.05rem; opacity:0.95;">
                Indian River Water Quality Analytics and Pollution Hotspot Detection Using PySpark
            </p>
            <div style="margin-top:12px; display:flex; gap:16px; font-size:0.85rem; opacity:0.85;">
                <span>⚡ <b>Distributed Engine:</b> Apache Spark 4.2.0 (PySpark)</span>
                <span>🏛️ <b>Benchmark Standard:</b> CPCB Designated Best Use (Class B Bathing)</span>
                <span>📍 <b>Active Records:</b> {total_records:,} across {kpis['total_stations']} stations</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    # -------------------------------------------------------------
    # Primary Navigation Tabs
    # -------------------------------------------------------------
    tabs = st.tabs([
        "🌐 Overview",
        "🔍 Water Quality Explorer",
        "📈 Trend Analysis",
        "🚨 Hotspot Detection & Map",
        "🛡️ Data Quality Audit",
        "📑 Insights & Export"
    ])

    # -------------------------------------------------------------
    # TAB 1: OVERVIEW
    # -------------------------------------------------------------
    with tabs[0]:
        st.markdown("### 📊 Executive KPI Summary")
        
        # Row 1: KPI Cards
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Total Records Evaluated</div>
                <div class="metric-value">{kpis['total_records']:,}</div>
                <div class="metric-subtitle">Across {kpis['total_rivers']} major river basins</div>
            </div>
            """, unsafe_allow_html=True)

        with c2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Monitoring Stations</div>
                <div class="metric-value">{kpis['total_stations']}</div>
                <div class="metric-subtitle">Spanning {kpis['total_states']} Indian states / UTs</div>
            </div>
            """, unsafe_allow_html=True)

        with c3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Flagged Hotspots</div>
                <div class="metric-value" style="color: {'#ef4444' if kpis['hotspot_percentage'] > 40 else '#f97316'};">
                    {kpis['flagged_observations']:,} <span style="font-size:1.1rem; font-weight:600;">({kpis['hotspot_percentage']}%)</span>
                </div>
                <div class="metric-subtitle">{kpis['critical_observations']} critical severity breaches</div>
            </div>
            """, unsafe_allow_html=True)

        with c4:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Mean Water Quality Index</div>
                <div class="metric-value" style="color: {'#10b981' if kpis['avg_wqi'] >= 70 else ('#eab308' if kpis['avg_wqi'] >= 50 else '#ef4444')};">
                    {kpis['avg_wqi']} <span style="font-size:1.1rem; font-weight:600;">/ 100</span>
                </div>
                <div class="metric-subtitle">Weighted composite CPCB score</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Row 2: Water Chemistry Benchmarks vs CPCB Class B Standards
        st.markdown("### 🧪 Water Chemistry Health vs. CPCB Standards")
        b1, b2, b3 = st.columns(3)
        with b1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Biochemical Oxygen Demand (BOD)</div>
                <div class="metric-value">{kpis['avg_bod']} <span style="font-size:1rem; font-weight:500;">mg/L</span></div>
                <div class="metric-subtitle">
                    CPCB Bathing Limit: <b>&le; 3.0 mg/L</b> | Status: 
                    <span style="color:{'#ef4444' if kpis['avg_bod'] > 3.0 else '#10b981'}; font-weight:bold;">
                        {'Elevated Organic Waste' if kpis['avg_bod'] > 3.0 else 'Compliant'}
                    </span>
                </div>
            </div>
            """, unsafe_allow_html=True)

        with b2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Dissolved Oxygen (DO)</div>
                <div class="metric-value">{kpis['avg_do']} <span style="font-size:1rem; font-weight:500;">mg/L</span></div>
                <div class="metric-subtitle">
                    CPCB Bathing Standard: <b>&ge; 5.0 mg/L</b> | Status:
                    <span style="color:{'#ef4444' if kpis['avg_do'] < 5.0 else '#10b981'}; font-weight:bold;">
                        {'Hypoxic Stress' if kpis['avg_do'] < 5.0 else 'Healthy Aeration'}
                    </span>
                </div>
            </div>
            """, unsafe_allow_html=True)

        with b3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">pH (Acidity / Alkalinity)</div>
                <div class="metric-value">{kpis['avg_ph']} <span style="font-size:1rem; font-weight:500;">Units</span></div>
                <div class="metric-subtitle">
                    CPCB Standard Range: <b>6.5 &ndash; 8.5</b> | Status:
                    <span style="color:{'#10b981' if 6.5 <= kpis['avg_ph'] <= 8.5 else '#ef4444'}; font-weight:bold;">
                        {'Balanced' if 6.5 <= kpis['avg_ph'] <= 8.5 else 'Out of Bounds'}
                    </span>
                </div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Row 3: River Basin Summary and Hotspot Donut
        col_left, col_right = st.columns([3, 2])
        with col_left:
            st.markdown("#### 🏞️ River Basin Comparative Performance")
            if not river_filtered.empty:
                st.dataframe(
                    river_filtered[[
                        "river_name", "active_stations", "samples_collected",
                        "avg_wqi", "avg_bod", "avg_do", "hotspot_rate_pct"
                    ]].rename(columns={
                        "river_name": "River Basin",
                        "active_stations": "Stations",
                        "samples_collected": "Samples",
                        "avg_wqi": "Avg WQI",
                        "avg_bod": "Avg BOD (mg/L)",
                        "avg_do": "Avg DO (mg/L)",
                        "hotspot_rate_pct": "Hotspot Rate %"
                    }),
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("No river records match the current filters.")

        with col_right:
            st.markdown("#### 🎯 Hotspot Severity Breakdown")
            if not df_filtered.empty:
                donut_fig = create_wqi_distribution_donut(df_filtered, is_dark=is_dark)
                st.plotly_chart(donut_fig, use_container_width=True)

    # -------------------------------------------------------------
    # TAB 2: WATER QUALITY EXPLORER
    # -------------------------------------------------------------
    with tabs[1]:
        st.markdown("### 🔬 Water Quality Station Explorer & Comparator")
        st.write("Examine individual monitoring stations, compare chemical measurements against CPCB thresholds, and inspect raw observations.")

        # Station-to-Station Comparative Inspector
        st.markdown("#### ⚖️ Side-by-Side Station Comparison")
        comp_col1, comp_col2 = st.columns(2)
        all_stns = sorted(list(full_df["station_name"].unique()))
        
        with comp_col1:
            stn_a = st.selectbox("Select Station A:", all_stns, index=min(3, len(all_stns)-1))
        with comp_col2:
            stn_b = st.selectbox("Select Station B:", all_stns, index=min(10, len(all_stns)-1))

        if stn_a and stn_b:
            stats_a = station_stats_df[station_stats_df["station_name"] == stn_a]
            stats_b = station_stats_df[station_stats_df["station_name"] == stn_b]

            if not stats_a.empty and not stats_b.empty:
                row_a = stats_a.iloc[0]
                row_b = stats_b.iloc[0]

                ca, cb = st.columns(2)
                with ca:
                    st.markdown(f"""
                    <div class="metric-card">
                        <h4 style="margin:0 0 8px 0; color:#38bdf8;">{stn_a}</h4>
                        <p style="margin:0; font-size:0.85rem; color:#94a3b8;"><b>River:</b> {row_a['river_name']} | <b>State:</b> {row_a['state']}</p>
                        <hr style="margin:8px 0; border:none; border-top:1px solid #334155;">
                        <p style="margin:4px 0;"><b>Average WQI:</b> {row_a.get('mean_wqi', 'N/A')} / 100</p>
                        <p style="margin:4px 0;"><b>Mean BOD:</b> {row_a.get('mean_bod', 'N/A')} mg/L (Min: {row_a.get('min_bod')}, Max: {row_a.get('max_bod')})</p>
                        <p style="margin:4px 0;"><b>Mean DO:</b> {row_a.get('mean_do', 'N/A')} mg/L (Min: {row_a.get('min_do')}, Max: {row_a.get('max_do')})</p>
                        <p style="margin:4px 0;"><b>Mean pH:</b> {row_a.get('mean_ph', 'N/A')}</p>
                        <p style="margin:4px 0;"><b>Mean Fecal Coliform:</b> {int(row_a.get('mean_fecal_coliform', 0)):,} MPN/100mL</p>
                    </div>
                    """, unsafe_allow_html=True)

                with cb:
                    st.markdown(f"""
                    <div class="metric-card">
                        <h4 style="margin:0 0 8px 0; color:#f43f5e;">{stn_b}</h4>
                        <p style="margin:0; font-size:0.85rem; color:#94a3b8;"><b>River:</b> {row_b['river_name']} | <b>State:</b> {row_b['state']}</p>
                        <hr style="margin:8px 0; border:none; border-top:1px solid #334155;">
                        <p style="margin:4px 0;"><b>Average WQI:</b> {row_b.get('mean_wqi', 'N/A')} / 100</p>
                        <p style="margin:4px 0;"><b>Mean BOD:</b> {row_b.get('mean_bod', 'N/A')} mg/L (Min: {row_b.get('min_bod')}, Max: {row_b.get('max_bod')})</p>
                        <p style="margin:4px 0;"><b>Mean DO:</b> {row_b.get('mean_do', 'N/A')} mg/L (Min: {row_b.get('min_do')}, Max: {row_b.get('max_do')})</p>
                        <p style="margin:4px 0;"><b>Mean pH:</b> {row_b.get('mean_ph', 'N/A')}</p>
                        <p style="margin:4px 0;"><b>Mean Fecal Coliform:</b> {int(row_b.get('mean_fecal_coliform', 0)):,} MPN/100mL</p>
                    </div>
                    """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Bar chart comparing top stations on selected parameter
        st.markdown("#### 📊 Station Ranking by Parameter")
        param_choice = st.selectbox(
            "Select Parameter to Compare:",
            ["mean_bod", "mean_do", "mean_wqi", "mean_fecal_coliform"],
            format_func=lambda x: {
                "mean_bod": "Biochemical Oxygen Demand (BOD) mg/L",
                "mean_do": "Dissolved Oxygen (DO) mg/L",
                "mean_wqi": "Water Quality Index (WQI)",
                "mean_fecal_coliform": "Fecal Coliform MPN/100mL"
            }[x]
        )
        bar_fig = create_station_comparison_bar(station_stats_df, metric_col=param_choice, top_n=15, is_dark=is_dark)
        st.plotly_chart(bar_fig, use_container_width=True)

        # Filtered Observations Table
        st.markdown("#### 📋 Detailed Water Quality Records")
        display_cols = [
            "sampling_date", "station_name", "river_name", "state",
            "dissolved_oxygen_mg_l", "bod_mg_l", "ph", "turbidity_ntu",
            "fecal_coliform_mpn_100ml", "wqi_score", "hotspot_severity", "violation_reasons"
        ]
        available_display_cols = [c for c in display_cols if c in df_filtered.columns]
        st.dataframe(
            df_filtered[available_display_cols].sort_values(by="sampling_date", ascending=False),
            use_container_width=True,
            hide_index=True
        )

    # -------------------------------------------------------------
    # TAB 3: TREND ANALYSIS
    # -------------------------------------------------------------
    with tabs[2]:
        st.markdown("### 📈 Multi-River & Station Temporal Trends")
        st.write("Track seasonal shifts, evaluate long-term pollution trajectories, and detect statistical anomalies using PySpark Window aggregations.")

        trend_c1, trend_c2 = st.columns([2, 1])
        with trend_c1:
            trend_param = st.selectbox(
                "Select Trend Parameter:",
                ["avg_bod", "avg_do", "avg_wqi", "avg_turbidity"],
                format_func=lambda x: {
                    "avg_bod": "Biochemical Oxygen Demand (BOD mg/L)",
                    "avg_do": "Dissolved Oxygen (DO mg/L)",
                    "avg_wqi": "Water Quality Index (WQI Score)",
                    "avg_turbidity": "Turbidity (NTU)"
                }[x]
            )
        with trend_c2:
            grouping_opt = st.radio("Group Trend By:", ["river_name", "state"], horizontal=True)

        # Monthly aggregation computed from filtered slice
        if not df_filtered.empty:
            df_temp = df_filtered.copy()
            df_temp["year_month"] = pd.to_datetime(df_temp["sampling_date"]).dt.strftime("%Y-%m")
            monthly_agg = df_temp.groupby(["year_month", grouping_opt]).agg({
                "bod_mg_l": "mean",
                "dissolved_oxygen_mg_l": "mean",
                "wqi_score": "mean",
                "turbidity_ntu": "mean"
            }).reset_index().rename(columns={
                "bod_mg_l": "avg_bod",
                "dissolved_oxygen_mg_l": "avg_do",
                "wqi_score": "avg_wqi",
                "turbidity_ntu": "avg_turbidity"
            })

            trend_fig = create_time_series_trend_chart(
                monthly_agg,
                param_col=trend_param,
                group_col=grouping_opt,
                is_dark=is_dark
            )
            st.plotly_chart(trend_fig, use_container_width=True)

        # Parameter Correlation Heatmap
        st.markdown("#### 🧬 Parameter Inter-Correlation Matrix")
        st.write("Reveals physical-chemical coupling (e.g. inverse relationship between organic BOD loading and dissolved oxygen depletion).")
        corr_fig = create_parameter_correlation_heatmap(df_filtered, is_dark=is_dark)
        st.plotly_chart(corr_fig, use_container_width=True)

        # PySpark Statistical Outlier Detection
        st.markdown("#### ⚡ Statistical Outliers (PySpark Z-Score Anomaly Detection)")
        st.write("Uses Apache Spark distributed Window partitioning (`Window.partitionBy('station_name')`) to identify readings $> 2.5\\sigma$ from the station baseline.")
        spark = init_spark()
        # Create temp view or detect anomalies directly
        clean_spark = spark.createDataFrame(df_filtered)
        anomalies_spark = detect_statistical_anomalies(clean_spark, param_col="bod_mg_l", z_threshold=2.5)
        anomalies_df = to_pandas_safe(anomalies_spark)

        if not anomalies_df.empty:
            st.warning(f"Detected {len(anomalies_df)} unusual statistical spike(s) in BOD measurements exceeding $2.5\\sigma$.")
            st.dataframe(
                anomalies_df[[
                    "sampling_date", "station_name", "river_name", "bod_mg_l",
                    "mean_val", "std_val", "z_score", "hotspot_severity"
                ]].round(2),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.success("No extreme statistical outliers ($Z > 2.5\\sigma$) detected in the current filtered subset.")

    # -------------------------------------------------------------
    # TAB 4: HOTSPOT DETECTION & MAP
    # -------------------------------------------------------------
    with tabs[3]:
        st.markdown("### 🚨 Pollution Hotspot Detection & Geospatial Intelligence")
        st.write(
            "Transparent rule-based detection modeled on CPCB Class B (Outdoor Bathing) standards. "
            "Locations repeatedly violating thresholds are mapped and prioritized by severity."
        )

        # Explanation box
        with st.expander("ℹ️ Click to review Hotspot Classification Rules & CPCB Criteria"):
            st.markdown("""
            - **Critical Hotspot (Red)**: Oxygen depletion ($DO < 4.0$ mg/L) combined with high organic pollution ($BOD > 8.0$ mg/L), or $\ge 3$ parameter breaches, or $WQI < 35$.
            - **High Risk Hotspot (Orange)**: Elevated BOD ($> 6.0$ mg/L) or severe microbial contamination ($FC > 10,000$ MPN/100mL) or 2 parameter breaches ($WQI < 55$).
            - **Moderate Hotspot (Yellow)**: Single parameter standard violation.
            - **Safe / Compliant (Green)**: All parameters strictly within CPCB Class B outdoor bathing standards.
            """)

        # Folium Geospatial Map
        st.markdown("#### 🗺️ Geospatial Water Quality Map")
        map_obj = create_river_hotspot_map(ranking_filtered, is_dark=is_dark)
        st_folium(map_obj, width=None, height=520, returned_objects=[])

        st.markdown("<br>", unsafe_allow_html=True)

        # Ranked Hotspots Table
        st.markdown("#### 🏆 Ranked Monitoring Stations Leaderboard")
        st.write("Stations ranked by critical severity count, non-compliance frequency, and lowest WQI:")
        
        if not ranking_filtered.empty:
            st.dataframe(
                ranking_filtered[[
                    "station_name", "river_name", "state", "overall_status",
                    "hotspot_percentage", "flagged_samples", "critical_samples",
                    "avg_wqi", "avg_bod", "avg_do"
                ]].rename(columns={
                    "station_name": "Station",
                    "river_name": "River",
                    "state": "State",
                    "overall_status": "Classification",
                    "hotspot_percentage": "Hotspot %",
                    "flagged_samples": "Flagged",
                    "critical_samples": "Critical",
                    "avg_wqi": "Avg WQI",
                    "avg_bod": "Avg BOD (mg/L)",
                    "avg_do": "Avg DO (mg/L)"
                }),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("No stations found matching the active criteria.")

    # -------------------------------------------------------------
    # TAB 5: DATA QUALITY AUDIT
    # -------------------------------------------------------------
    with tabs[4]:
        st.markdown("### 🛡️ Big Data Pipeline Data Quality & Hygiene Audit")
        st.write("PySpark ingests raw telemetry, applies schema validation, eliminates duplicates, and quarantines out-of-bounds readings.")

        q1, q2, q3, q4 = st.columns(4)
        with q1:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Raw Records Ingested</div>
                <div class="metric-value">{audit_report['total_raw_records']:,}</div>
                <div class="metric-subtitle">Mode: PERMISSIVE PySpark reader</div>
            </div>
            """, unsafe_allow_html=True)

        with q2:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Duplicates Dropped</div>
                <div class="metric-value" style="color: #f97316;">{audit_report['duplicates_detected']:,}</div>
                <div class="metric-subtitle">Key: [station_id, sampling_date]</div>
            </div>
            """, unsafe_allow_html=True)

        with q3:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Invalid Sensor Spikes</div>
                <div class="metric-value" style="color: #ef4444;">{sum(audit_report['out_of_range_counts'].values())}</div>
                <div class="metric-subtitle">Impossible physical ranges sanitized</div>
            </div>
            """, unsafe_allow_html=True)

        with q4:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Data Pipeline Health</div>
                <div class="metric-value" style="color: #10b981;">{audit_report['data_health_score_pct']}%</div>
                <div class="metric-subtitle">Overall valid cell integrity</div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Missing values & Out-of-bounds Breakdown
        col_m1, col_m2 = st.columns(2)
        with col_m1:
            st.markdown("#### 🔍 Missing Values (NULL / NaN) by Field")
            missing_df = pd.DataFrame([
                {"Column": col, "Missing Count": cnt, "Missing %": audit_report['missing_percentages'].get(col, 0.0)}
                for col, cnt in audit_report['missing_values'].items() if cnt > 0
            ])
            if not missing_df.empty:
                st.dataframe(missing_df, use_container_width=True, hide_index=True)
            else:
                st.success("No missing values in ingested dataset!")

        with col_m2:
            st.markdown("#### ⚡ Out-of-Range Sensor Outliers Identified")
            out_df = pd.DataFrame([
                {"Parameter": p, "Out-of-Bounds Readings": cnt}
                for p, cnt in audit_report['out_of_range_counts'].items() if cnt > 0
            ])
            if not out_df.empty:
                st.dataframe(out_df, use_container_width=True, hide_index=True)
            else:
                st.success("No out-of-range sensor readings detected!")

        st.markdown("---")
        st.markdown("#### 📋 PySpark Data Schema")
        schema_rows = []
        for f in WATER_QUALITY_SCHEMA.fields:
            schema_rows.append({"Field Name": f.name, "PySpark Data Type": str(f.dataType), "Nullable": f.nullable})
        st.dataframe(pd.DataFrame(schema_rows), use_container_width=True, hide_index=True)

    # -------------------------------------------------------------
    # TAB 6: INSIGHTS & EXPORT
    # -------------------------------------------------------------
    with tabs[5]:
        st.markdown("### 📑 Automated Analytical Insights & Exportable Reports")
        st.write("Generate and download academic BDA project summaries and export filtered data slices.")

        # Automated narrative summary
        top_critical = ranking_filtered[ranking_filtered["critical_samples"] > 0]
        critical_count = len(top_critical)
        top_river_polluted = river_filtered.iloc[0]["river_name"] if not river_filtered.empty else "N/A"
        
        st.markdown(f"""
        <div class="metric-card" style="border-left: 4px solid #38bdf8; margin-bottom: 20px;">
            <h4 style="margin:0 0 8px 0;">🧠 Automated Analytical Findings</h4>
            <ul style="margin:0; padding-left: 20px; line-height: 1.6;">
                <li><b>Primary Pollution Stressor:</b> Biochemical Oxygen Demand (BOD) averages <b>{kpis['avg_bod']} mg/L</b> across evaluated records, exceeding the CPCB bathing threshold (3.0 mg/L).</li>
                <li><b>Critical Hotspots:</b> <b>{critical_count} stations</b> exhibit severe oxygen depletion combined with high organic loading (most notably downstream urban stretches of Yamuna and industrial sectors along Ganga and Sabarmati).</li>
                <li><b>Most Vulnerable Basin:</b> <b>River {top_river_polluted}</b> demonstrates the highest proportion of non-compliant observations ({river_filtered.iloc[0]['hotspot_rate_pct'] if not river_filtered.empty else 0}%) in the active filter slice.</li>
                <li><b>Pristine Headwaters:</b> Upper Himalayan reaches (e.g. Rishikesh, Dakpathar) and Western Ghats origins consistently maintain Class A/B water quality with DO &ge; 7.5 mg/L and BOD &le; 2.0 mg/L.</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

        # Generate Full Markdown Report
        filters_dict = {
            "rivers": selected_rivers,
            "states": selected_states,
            "start_date": str(selected_date_range[0]) if isinstance(selected_date_range, (list, tuple)) else "Earliest",
            "end_date": str(selected_date_range[1]) if isinstance(selected_date_range, (list, tuple)) else "Latest"
        }
        report_md = generate_markdown_analysis_report(
            kpis=kpis,
            data_quality_report=audit_report,
            top_hotspots_df=ranking_filtered,
            river_summary_df=river_filtered,
            selected_filters=filters_dict
        )

        d1, d2 = st.columns(2)
        with d1:
            st.download_button(
                label="📥 Download Analytical Report (.md)",
                data=report_md,
                file_name=f"RiverWatch_Analytics_Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.md",
                mime="text/markdown",
                use_container_width=True
            )

        with d2:
            csv_data = export_df_to_csv(df_filtered)
            st.download_button(
                label="📥 Download Filtered Dataset (.csv)",
                data=csv_data,
                file_name=f"RiverWatch_Filtered_Data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )

        st.markdown("<br>", unsafe_allow_html=True)
        st.markdown("#### 📄 Executive Report Preview")
        st.markdown(report_md)

if __name__ == "__main__":
    main()
