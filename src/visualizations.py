import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import folium
from folium.plugins import MarkerCluster

PARAMETER_CONFIG = {
    "dissolved_oxygen_mg_l": {
        "label": "Dissolved Oxygen (DO)",
        "unit": "mg/L",
        "threshold": 5.0,
        "threshold_label": "CPCB Min Bathing (5.0 mg/L)",
        "higher_is_better": True,
        "color": "#06b6d4"
    },
    "bod_mg_l": {
        "label": "Biochemical Oxygen Demand (BOD)",
        "unit": "mg/L",
        "threshold": 3.0,
        "threshold_label": "CPCB Max Bathing (3.0 mg/L)",
        "higher_is_better": False,
        "color": "#f43f5e"
    },
    "ph": {
        "label": "pH (Acidity / Alkalinity)",
        "unit": "pH units",
        "threshold": 8.5,
        "threshold_label": "CPCB Range (6.5 - 8.5)",
        "higher_is_better": None,
        "color": "#8b5cf6"
    },
    "turbidity_ntu": {
        "label": "Turbidity",
        "unit": "NTU",
        "threshold": 10.0,
        "threshold_label": "Desirable Bathing (10 NTU)",
        "higher_is_better": False,
        "color": "#f59e0b"
    },
    "nitrate_mg_l": {
        "label": "Nitrate (NO3)",
        "unit": "mg/L",
        "threshold": 10.0,
        "threshold_label": "CPCB Indicator (10 mg/L)",
        "higher_is_better": False,
        "color": "#10b981"
    },
    "wqi_score": {
        "label": "Water Quality Index (WQI)",
        "unit": "Score (0-100)",
        "threshold": 50.0,
        "threshold_label": "Passing Grade (50 WQI)",
        "higher_is_better": True,
        "color": "#3b82f6"
    }
}

def get_theme_colors(is_dark: bool = False):
    if is_dark:
        return {
            "template": "plotly_dark",
            "paper_bg": "#0f172a",
            "plot_bg": "#1e293b",
            "text": "#f8fafc",
            "grid": "#334155",
            "map_tile": "CartoDB dark_matter"
        }
    else:
        return {
            "template": "plotly_white",
            "paper_bg": "#ffffff",
            "plot_bg": "#f8fafc",
            "text": "#0f172a",
            "grid": "#e2e8f0",
            "map_tile": "CartoDB positron"
        }

def create_time_series_trend_chart(
    df_monthly: pd.DataFrame,
    param_col: str = "avg_bod",
    group_col: str = "river_name",
    is_dark: bool = False
) -> go.Figure:
    """Creates an interactive time-series line chart with CPCB standard threshold line."""
    theme = get_theme_colors(is_dark)
    
    # Map raw col name to config
    base_param = param_col.replace("avg_", "").replace("mean_", "")
    if base_param in ["bod", "bod_mg_l"]:
        cfg_key = "bod_mg_l"
    elif base_param in ["do", "dissolved_oxygen_mg_l"]:
        cfg_key = "dissolved_oxygen_mg_l"
    elif base_param in ["wqi", "wqi_score"]:
        cfg_key = "wqi_score"
    elif base_param in ["turbidity", "turbidity_ntu"]:
        cfg_key = "turbidity_ntu"
    else:
        cfg_key = "bod_mg_l"

    cfg = PARAMETER_CONFIG.get(cfg_key, PARAMETER_CONFIG["bod_mg_l"])

    fig = px.line(
        df_monthly,
        x="year_month",
        y=param_col,
        color=group_col,
        markers=True,
        title=f"<b>Monthly Trend: {cfg['label']}</b>",
        labels={"year_month": "Year-Month", param_col: f"{cfg['label']} ({cfg['unit']})"},
        template=theme["template"]
    )

    # Add reference benchmark horizontal line
    if cfg.get("threshold") is not None:
        fig.add_hline(
            y=cfg["threshold"],
            line_dash="dash",
            line_color="#ef4444" if not cfg["higher_is_better"] else "#10b981",
            annotation_text=f"Standard: {cfg['threshold_label']}",
            annotation_position="bottom right"
        )

    fig.update_layout(
        paper_bgcolor=theme["paper_bg"],
        plot_bgcolor=theme["plot_bg"],
        font=dict(color=theme["text"], family="Inter, sans-serif"),
        hovermode="x unified",
        margin=dict(l=40, r=40, t=60, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5)
    )
    fig.update_xaxes(showgrid=True, gridcolor=theme["grid"])
    fig.update_yaxes(showgrid=True, gridcolor=theme["grid"])
    return fig

def create_station_comparison_bar(
    df_stats: pd.DataFrame,
    metric_col: str = "mean_bod",
    top_n: int = 15,
    is_dark: bool = False
) -> go.Figure:
    """Creates a horizontal comparison bar chart of top stations."""
    theme = get_theme_colors(is_dark)
    df_sorted = df_stats.sort_values(by=metric_col, ascending=True).tail(top_n)

    fig = px.bar(
        df_sorted,
        x=metric_col,
        y="station_name",
        color="river_name",
        orientation="h",
        title=f"<b>Station Comparison: {metric_col.replace('_', ' ').title()}</b> (Top {top_n})",
        labels={metric_col: metric_col.replace('_', ' ').title(), "station_name": "Station"},
        template=theme["template"]
    )

    fig.update_layout(
        paper_bgcolor=theme["paper_bg"],
        plot_bgcolor=theme["plot_bg"],
        font=dict(color=theme["text"], family="Inter, sans-serif"),
        margin=dict(l=40, r=40, t=60, b=40),
        height=450
    )
    return fig

def create_wqi_distribution_donut(df_pandas: pd.DataFrame, is_dark: bool = False) -> go.Figure:
    """Creates a donut chart of observations by Hotspot Severity category."""
    theme = get_theme_colors(is_dark)
    counts = df_pandas["hotspot_severity"].value_counts().reset_index()
    counts.columns = ["Severity", "Count"]

    color_map = {
        "Critical": "#ef4444",
        "High Priority": "#f97316",
        "Moderate": "#eab308",
        "Safe / Compliant": "#10b981"
    }

    fig = px.pie(
        counts,
        names="Severity",
        values="Count",
        hole=0.55,
        title="<b>Hotspot Severity Distribution</b>",
        color="Severity",
        color_discrete_map=color_map,
        template=theme["template"]
    )

    fig.update_layout(
        paper_bgcolor=theme["paper_bg"],
        font=dict(color=theme["text"], family="Inter, sans-serif"),
        margin=dict(l=20, r=20, t=50, b=20)
    )
    return fig

def create_parameter_correlation_heatmap(df_pandas: pd.DataFrame, is_dark: bool = False) -> go.Figure:
    """Computes and renders a correlation matrix heatmap across physical & chemical parameters."""
    theme = get_theme_colors(is_dark)
    numeric_cols = [
        "dissolved_oxygen_mg_l", "bod_mg_l", "ph",
        "temperature_c", "conductivity_umhos_cm", "nitrate_mg_l", "turbidity_ntu", "wqi_score"
    ]
    existing = [c for c in numeric_cols if c in df_pandas.columns]
    corr_df = df_pandas[existing].corr().round(2)

    labels = [c.replace("_mg_l", "").replace("_umhos_cm", "").replace("_ntu", "").replace("_", " ").title() for c in existing]

    fig = px.imshow(
        corr_df,
        text_auto=True,
        aspect="auto",
        labels=dict(color="Correlation"),
        x=labels,
        y=labels,
        color_continuous_scale="RdBu_r",
        title="<b>Parameter Inter-Correlation Heatmap</b>",
        template=theme["template"]
    )

    fig.update_layout(
        paper_bgcolor=theme["paper_bg"],
        font=dict(color=theme["text"], family="Inter, sans-serif"),
        margin=dict(l=40, r=40, t=60, b=40)
    )
    return fig

def create_river_hotspot_map(df_stations: pd.DataFrame, is_dark: bool = False) -> folium.Map:
    """
    Renders an interactive Folium map centered on India with color-coded monitoring stations.
    """
    theme = get_theme_colors(is_dark)
    
    # Filter valid coordinates
    valid_stations = df_stations.dropna(subset=["latitude", "longitude"])
    if len(valid_stations) == 0:
        # Fallback empty India map
        return folium.Map(location=[22.5, 80.0], zoom_start=5, tiles=theme["map_tile"])

    # Center map on average coords
    center_lat = valid_stations["latitude"].mean()
    center_lon = valid_stations["longitude"].mean()

    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=5,
        tiles=theme["map_tile"],
        control_scale=True
    )

    severity_colors = {
        "Critical Hotspot": "#ef4444",
        "High Risk Hotspot": "#f97316",
        "Moderate Hotspot": "#eab308",
        "Safe / Compliant": "#10b981"
    }

    for _, row in valid_stations.iterrows():
        status = row.get("overall_status", "Safe / Compliant")
        color = severity_colors.get(status, "#3b82f6")
        
        radius = 8 + (float(row.get("hotspot_percentage", 0)) / 10.0)

        popup_html = f"""
        <div style="font-family: Arial, sans-serif; font-size: 13px; line-height: 1.5; min-width: 200px;">
            <b style="color: {color}; font-size: 14px;">{row.get('station_name')}</b><br>
            <b>River:</b> {row.get('river_name')}<br>
            <b>State:</b> {row.get('state')}<br>
            <hr style="margin: 6px 0; border: none; border-top: 1px solid #ddd;">
            <b>Status:</b> <span style="background: {color}; color: white; padding: 2px 6px; border-radius: 4px; font-weight: bold;">{status}</span><br>
            <b>Hotspot Frequency:</b> {row.get('hotspot_percentage', 0)}% ({int(row.get('flagged_samples', 0))}/{int(row.get('total_samples', 0))} samples)<br>
            <b>Avg WQI:</b> {row.get('avg_wqi', 'N/A')}<br>
            <b>Avg BOD:</b> {row.get('avg_bod', 'N/A')} mg/L<br>
            <b>Avg DO:</b> {row.get('avg_do', 'N/A')} mg/L
        </div>
        """

        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=min(radius, 18),
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.75,
            weight=2,
            popup=folium.Popup(popup_html, max_width=320),
            tooltip=f"{row.get('station_name')} ({row.get('river_name')}) - {status}"
        ).add_to(m)

    return m
