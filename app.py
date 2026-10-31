
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
PROJECT_DIR = Path(__file__).resolve().parent
AQI_PATH = PROJECT_DIR / "data" / "processed" / "aqi_final"
STREAM_PATH = PROJECT_DIR / "data" / "streaming" / "output"

st.set_page_config(
    page_title="AirStream | Air Quality Analytics",
    page_icon="🌫️",
    layout="wide",
    initial_sidebar_state="expanded",
)


st.markdown(
    """
    <style>
    /* ---------- Overall canvas and typography ---------- */
    .stApp {
        background:
            radial-gradient(ellipse at 10% 0%, rgba(13, 148, 136, 0.09), transparent 35%),
            #0b1120;
        color: #e5edf7;
    }

    [data-testid="stHeader"] {
        background: rgba(11, 17, 32, 0.88);
    }

    .block-container {
        max-width: 1500px;
        padding: 2rem 2.5rem 3rem 2.5rem;
    }

    h1, h2, h3 {
        color: #f1f5f9 !important;
        letter-spacing: -0.035em;
    }

    h1 {
        font-size: 2.35rem !important;
        font-weight: 750 !important;
    }

    h2 {
        font-size: 1.55rem !important;
        font-weight: 700 !important;
    }

    h3 {
        font-size: 1.12rem !important;
        font-weight: 650 !important;
    }

    p, label, .stMarkdown, [data-testid="stCaptionContainer"] {
        color: #b7c5d8;
    }

    /* ---------- Sidebar ---------- */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #111c30 0%, #0d1627 100%);
        border-right: 1px solid rgba(148, 163, 184, 0.15);
    }

    [data-testid="stSidebar"] > div:first-child {
        padding-top: 1.8rem;
    }

    [data-testid="stSidebar"] h1,
    [data-testid="stSidebar"] h2,
    [data-testid="stSidebar"] h3 {
        color: #f8fafc !important;
    }

    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
        color: #9fb1c9;
    }

    [data-testid="stSidebar"] [data-testid="stRadio"] label {
        color: #dbe7f5;
        padding-top: 0.18rem;
        padding-bottom: 0.18rem;
    }

    /* ---------- KPI metric cards ---------- */
    [data-testid="stMetric"] {
        background: linear-gradient(
            145deg,
            rgba(25, 39, 61, 0.96),
            rgba(17, 29, 47, 0.96)
        );
        border: 1px solid rgba(100, 116, 139, 0.26);
        border-radius: 15px;
        padding: 19px 20px;
        min-height: 118px;
        box-shadow: 0 8px 24px rgba(0, 0, 0, 0.12);
        transition: border-color 0.2s ease, transform 0.2s ease;
    }

    [data-testid="stMetric"]:hover {
        border-color: rgba(45, 212, 191, 0.55);
        transform: translateY(-2px);
    }

    [data-testid="stMetricLabel"] {
        color: #9fb1c9 !important;
        font-size: 0.86rem !important;
        font-weight: 600 !important;
    }

    [data-testid="stMetricValue"] {
        color: #f8fafc !important;
        font-size: 1.75rem !important;
        font-weight: 750 !important;
    }

    [data-testid="stMetricDelta"] {
        font-size: 0.82rem !important;
    }

    /* ---------- Charts and content containers ---------- */
    [data-testid="stPlotlyChart"] {
        background: #111c30;
        border: 1px solid rgba(100, 116, 139, 0.2);
        border-radius: 14px;
        padding: 8px 8px 0 8px;
        margin-bottom: 1rem;
    }

    [data-testid="stDataFrame"] {
        border: 1px solid rgba(100, 116, 139, 0.25);
        border-radius: 12px;
        overflow: hidden;
    }

    [data-testid="stExpander"] {
        background: rgba(17, 28, 48, 0.7);
        border: 1px solid rgba(100, 116, 139, 0.23);
        border-radius: 12px;
    }

    hr {
        border-color: rgba(148, 163, 184, 0.18);
        margin-top: 1.5rem;
        margin-bottom: 1.5rem;
    }

    /* ---------- Inputs ---------- */
    [data-baseweb="select"] > div,
    [data-baseweb="input"] > div,
    [data-baseweb="textarea"] > div {
        background-color: #111c30;
        border-color: #34445e;
        border-radius: 9px;
    }

    [data-baseweb="popover"] {
        color: #e5edf7;
    }

    /* ---------- Buttons ---------- */
    .stButton > button,
    .stDownloadButton > button {
        background: linear-gradient(135deg, #0f9f92, #0d9488);
        color: white;
        border: 1px solid rgba(45, 212, 191, 0.35);
        border-radius: 9px;
        padding: 0.55rem 1rem;
        font-weight: 650;
        transition: all 0.2s ease;
    }

    .stButton > button:hover,
    .stDownloadButton > button:hover {
        background: linear-gradient(135deg, #14b8a6, #0f9f92);
        color: white;
        border-color: #5eead4;
        transform: translateY(-1px);
    }

    /* ---------- Alerts and status messages ---------- */
    [data-testid="stAlert"] {
        border-radius: 12px;
        border: 1px solid rgba(100, 116, 139, 0.25);
    }

    /* ---------- Responsive spacing ---------- */
    @media (max-width: 768px) {
        .block-container {
            padding: 1.25rem 1rem 2rem 1rem;
        }

        h1 {
            font-size: 1.8rem !important;
        }

        [data-testid="stMetricValue"] {
            font-size: 1.35rem !important;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)



# ---------------------------------------------------------
# Data loading
# ---------------------------------------------------------
@st.cache_data(ttl=30, show_spinner="Loading AirStream datasets...")
def load_parquet_folder(folder_path: str) -> pd.DataFrame:
    folder = Path(folder_path)

    if not folder.exists():
        return pd.DataFrame()

    parquet_files = list(folder.rglob("*.parquet"))
    if not parquet_files:
        return pd.DataFrame()

    dataset = ds.dataset(str(folder), format="parquet", partitioning="hive")
    return dataset.to_table().to_pandas()


def prepare_aqi() -> pd.DataFrame:
    df = load_parquet_folder(str(AQI_PATH))

    if df.empty:
        return df

    required = {
        "location_id", "location", "aqi", "category", "date",
        "dominant_pollutant", "pm25_aqi", "pm10_aqi",
        "no2_aqi", "so2_aqi", "co_aqi", "o3_aqi",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"AQI dataset is missing columns: {sorted(missing)}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    for col in [
        "aqi", "pm25_aqi", "pm10_aqi", "no2_aqi",
        "so2_aqi", "co_aqi", "o3_aqi",
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["location_id"] = df["location_id"].astype(str)
    df["location"] = df["location"].fillna("Unknown station").astype(str)
    df["category"] = df["category"].fillna("Unknown").astype(str)
    df["dominant_pollutant"] = (
        df["dominant_pollutant"].fillna("Unknown").astype(str)
    )

    return df.dropna(subset=["date", "aqi"])


def prepare_stream() -> pd.DataFrame:
    df = load_parquet_folder(str(STREAM_PATH))

    if df.empty:
        return df

    required = {
        "window_start", "window_end", "location_id", "location",
        "parameter", "avg_value", "max_value", "measurement_count",
        "anomaly_threshold", "anomaly_flag", "anomaly_reason",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Streaming dataset is missing columns: {sorted(missing)}"
        )

    for col in ["window_start", "window_end"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in [
        "avg_value", "max_value", "measurement_count", "anomaly_threshold"
    ]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["location_id"] = df["location_id"].astype(str)
    df["location"] = df["location"].fillna("Unknown station").astype(str)
    df["parameter"] = df["parameter"].fillna("unknown").astype(str).str.lower()
    df["anomaly_reason"] = df["anomaly_reason"].fillna("").astype(str)

    # Normalize boolean values in case Parquet contains mixed representations.
    df["anomaly_flag"] = (
        df["anomaly_flag"]
        .astype(str)
        .str.lower()
        .isin(["true", "1", "yes"])
    )

    return df.dropna(subset=["window_start", "avg_value"])


try:
    aqi_df = prepare_aqi()
    stream_df = prepare_stream()
except Exception as exc:
    st.error(f"Could not load the project datasets: {exc}")
    st.info(
        "Check that the dashboard is saved in the project root and that "
        "the Parquet folders are present."
    )
    st.stop()


# ---------------------------------------------------------
# Sidebar and helpers
# ---------------------------------------------------------
st.sidebar.title("🌫️ AirStream")
st.sidebar.caption("Big Data Air Quality Analytics")

page = st.sidebar.radio(
    "Navigate",
    [
        "Overview",
        "Historical Analysis",
        "Streaming Monitor",
        "Data & Methodology",
    ],
)

if st.sidebar.button("↻ Refresh data", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

st.sidebar.divider()
st.sidebar.caption("Batch analytics + Kafka/Spark streaming simulation")


def metric_row(items):
    cols = st.columns(len(items))
    for col, (label, value, help_text) in zip(cols, items):
        col.metric(label, value, help=help_text)


def format_number(value):
    return f"{value:,.0f}"


def category_order(df):
    preferred = [
        "Good", "Satisfactory", "Moderate",
        "Poor", "Very Poor", "Severe",
    ]
    found = df["category"].dropna().unique().tolist()
    return [c for c in preferred if c in found] + [
        c for c in found if c not in preferred
    ]


POLLUTANT_COLUMNS = {
    "PM2.5": "pm25_aqi",
    "PM10": "pm10_aqi",
    "NO₂": "no2_aqi",
    "SO₂": "so2_aqi",
    "CO": "co_aqi",
    "O₃": "o3_aqi",
}


# ---------------------------------------------------------
# Overview
# ---------------------------------------------------------
if page == "Overview":

    st.markdown(
        """
        <div style="
            background: linear-gradient(120deg, #122b3c 0%, #124e4a 58%, #0f766e 100%);
            border: 1px solid rgba(94, 234, 212, 0.25);
            border-radius: 20px;
            padding: 30px 32px;
            margin-bottom: 26px;
            box-shadow: 0 15px 40px rgba(0,0,0,0.18);
        ">
            <div style="
                color: #99f6e4;
                font-size: 0.78rem;
                font-weight: 750;
                letter-spacing: 0.16em;
                text-transform: uppercase;
                margin-bottom: 12px;
            ">
                BIG DATA · ENVIRONMENTAL INTELLIGENCE
            </div>
            <div style="
                color: #f8fafc;
                font-size: clamp(2rem, 4vw, 3rem);
                line-height: 1.12;
                font-weight: 800;
                letter-spacing: -0.045em;
                margin-bottom: 14px;
            ">
                AirStream
            </div>
            <div style="
                color: #d1fae5;
                font-size: 1.08rem;
                font-weight: 550;
                margin-bottom: 10px;
            ">
                Air Quality Analytics &amp; Streaming Monitor
            </div>
            <div style="
                color: #cbd5e1;
                font-size: 0.94rem;
                max-width: 760px;
                line-height: 1.7;
            ">
                Explore historical air quality patterns, compare monitoring
                stations, and inspect threshold-based anomaly alerts from
                the Kafka and Spark streaming pipeline.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


    if aqi_df.empty:
        st.warning(f"No AQI records found at: {AQI_PATH}")
    else:
        min_date = aqi_df["date"].min().date()
        max_date = aqi_df["date"].max().date()
        valid_aqi = aqi_df["aqi"].dropna()

        metric_row([
            ("Station-day records", format_number(len(aqi_df)),
             "Rows in the current AQI dataset"),
            ("Stations", format_number(aqi_df["location_id"].nunique()),
             "Unique station identifiers"),
            ("Date coverage", f"{min_date} — {max_date}",
             "Earliest and latest dates in the data"),
            ("Mean AQI", f"{valid_aqi.mean():.1f}",
             "Average of available station-day AQI values"),
        ])

        st.divider()
        left, right = st.columns(2)

        with left:
            st.markdown("#### AQI category distribution")
            counts = (
                aqi_df["category"]
                .value_counts()
                .rename_axis("Category")
                .reset_index(name="Records")
            )
            fig = px.bar(
                counts,
                x="Category",
                y="Records",
                color="Category",
                category_orders={"Category": category_order(aqi_df)},
                title="Records by AQI category",
            )
            fig.update_layout(showlegend=False, xaxis_title="", yaxis_title="Records")
            st.plotly_chart(fig, use_container_width=True)

        with right:
            st.markdown("#### Historical AQI trend")
            daily = (
                aqi_df.groupby("date", as_index=False)["aqi"]
                .mean()
                .rename(columns={"aqi": "Mean AQI"})
            )
            fig = px.line(
                daily,
                x="date",
                y="Mean AQI",
                title="Average station-day AQI by date",
                labels={"date": "Date", "Mean AQI": "Mean AQI"},
            )
            fig.update_traces(line_width=2)
            st.plotly_chart(fig, use_container_width=True)

        st.markdown("#### Dominant pollutant")
        dominant = (
            aqi_df["dominant_pollutant"]
            .value_counts()
            .rename_axis("Pollutant")
            .reset_index(name="Records")
        )
        fig = px.bar(
            dominant,
            x="Pollutant",
            y="Records",
            color="Pollutant",
            title="Most frequently reported dominant pollutant",
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    st.divider()
    st.markdown("#### Streaming pipeline snapshot")

    if stream_df.empty:
        st.warning(f"No streaming output found at: {STREAM_PATH}")
    else:
        flagged = stream_df["anomaly_flag"]
        metric_row([
            ("Window records", format_number(len(stream_df)),
             "Rows written by the streaming aggregation"),
            ("Stations", format_number(stream_df["location_id"].nunique()),
             "Stations represented in the output"),
            ("Flagged records", format_number(flagged.sum()),
             "Records exceeding configured project thresholds"),
            ("Flag rate", f"{100 * flagged.mean():.1f}%",
             "Share of output rows marked as anomalous"),
        ])

    st.caption(
        "The streaming data shown here comes from a historical-data replay. "
        "It is not a direct feed from live air-quality sensors."
    )


# ---------------------------------------------------------
# Historical Analysis
# ---------------------------------------------------------
elif page == "Historical Analysis":
    st.title("Historical AQI Analysis")

    if aqi_df.empty:
        st.warning("The historical AQI dataset is empty or unavailable.")
        st.stop()

    with st.sidebar:
        st.subheader("Historical filters")

        station_options = sorted(aqi_df["location"].dropna().unique().tolist())
        selected_stations = st.multiselect(
            "Stations",
            station_options,
            default=[],
            help="Leave empty to include all stations.",
        )

        categories = category_order(aqi_df)
        selected_categories = st.multiselect(
            "AQI categories",
            categories,
            default=categories,
        )

        start_date, end_date = st.date_input(
            "Date range",
            value=(aqi_df["date"].min().date(), aqi_df["date"].max().date()),
            min_value=aqi_df["date"].min().date(),
            max_value=aqi_df["date"].max().date(),
            format="YYYY-MM-DD",
        )

    filtered = aqi_df[
        aqi_df["date"].dt.date.between(start_date, end_date)
    ].copy()

    if selected_stations:
        filtered = filtered[filtered["location"].isin(selected_stations)]

    if selected_categories:
        filtered = filtered[filtered["category"].isin(selected_categories)]
    else:
        filtered = filtered.iloc[0:0]

    if filtered.empty:
        st.warning("No records match the selected filters.")
        st.stop()

    metric_row([
        ("Matching records", format_number(len(filtered)), "After filters"),
        ("Stations", format_number(filtered["location_id"].nunique()),
         "Stations in the selection"),
        ("Mean AQI", f"{filtered['aqi'].mean():.1f}", "Mean selected AQI"),
        ("Maximum AQI", f"{filtered['aqi'].max():.0f}", "Highest selected AQI"),
    ])

    st.markdown("#### AQI trend by station")
    trend = (
        filtered.groupby(["date", "location"], as_index=False)["aqi"]
        .mean()
    )
    fig = px.line(
        trend,
        x="date",
        y="aqi",
        color="location",
        labels={"date": "Date", "aqi": "Mean AQI", "location": "Station"},
        title="Daily AQI trend",
    )
    st.plotly_chart(fig, use_container_width=True)

    left, right = st.columns(2)

    with left:
        st.markdown("#### Pollutant sub-index trends")
        pollutant_choice = st.multiselect(
            "Pollutant sub-indices",
            list(POLLUTANT_COLUMNS.keys()),
            default=["PM2.5", "PM10"],
        )
        if pollutant_choice:
            melted = filtered[["date", "location"]].copy()
            for pollutant in pollutant_choice:
                melted[pollutant] = filtered[POLLUTANT_COLUMNS[pollutant]].values
            melted = melted.groupby("date", as_index=False)[pollutant_choice].mean()
            long = melted.melt(
                id_vars="date",
                var_name="Pollutant",
                value_name="Sub-index",
            ).dropna()
            fig = px.line(
                long,
                x="date",
                y="Sub-index",
                color="Pollutant",
                title="Mean pollutant sub-index over time",
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Select one or more pollutant sub-indices.")

    with right:
        st.markdown("#### Dominant pollutant distribution")
        dominant = (
            filtered["dominant_pollutant"]
            .value_counts()
            .rename_axis("Pollutant")
            .reset_index(name="Records")
        )
        fig = px.pie(
            dominant,
            names="Pollutant",
            values="Records",
            title="Dominant pollutant share",
            hole=0.35,
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Station comparison")
    station_summary = (
        filtered.groupby(["location_id", "location"], as_index=False)
        .agg(
            Mean_AQI=("aqi", "mean"),
            Maximum_AQI=("aqi", "max"),
            Records=("aqi", "count"),
        )
        .sort_values("Mean_AQI", ascending=False)
    )
    fig = px.bar(
        station_summary.head(25),
        x="location",
        y="Mean_AQI",
        hover_data=["location_id", "Maximum_AQI", "Records"],
        title="Up to 25 stations with the highest mean AQI in the selection",
        labels={"location": "Station", "Mean_AQI": "Mean AQI"},
    )
    fig.update_layout(xaxis_tickangle=-35)
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("View filtered AQI records"):
        st.dataframe(
            filtered.sort_values("date", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

    csv = filtered.to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download filtered AQI data (CSV)",
        data=csv,
        file_name="airstream_filtered_aqi.csv",
        mime="text/csv",
    )


# ---------------------------------------------------------
# Streaming Monitor
# ---------------------------------------------------------
elif page == "Streaming Monitor":
    st.title("Streaming Monitor")
    st.warning(
        "Simulation mode: this page visualizes persisted output from the "
        "Kafka + Spark Structured Streaming pipeline. It does not subscribe "
        "to Kafka directly from the dashboard."
    )

    if stream_df.empty:
        st.warning(f"No streaming Parquet output found at: {STREAM_PATH}")
        st.stop()

    with st.sidebar:
        st.subheader("Streaming filters")

        stream_stations = sorted(stream_df["location"].dropna().unique().tolist())
        chosen_stream_stations = st.multiselect(
            "Stations",
            stream_stations,
            default=[],
            help="Leave empty to include all stations.",
        )

        pollutants = sorted(stream_df["parameter"].dropna().unique().tolist())
        chosen_pollutants = st.multiselect(
            "Pollutants",
            pollutants,
            default=pollutants,
        )

        anomaly_filter = st.selectbox(
            "Anomaly records",
            ["All records", "Flagged only", "Not flagged"],
        )

    view = stream_df.copy()

    if chosen_stream_stations:
        view = view[view["location"].isin(chosen_stream_stations)]

    if chosen_pollutants:
        view = view[view["parameter"].isin(chosen_pollutants)]
    else:
        view = view.iloc[0:0]

    if anomaly_filter == "Flagged only":
        view = view[view["anomaly_flag"]]
    elif anomaly_filter == "Not flagged":
        view = view[~view["anomaly_flag"]]

    if view.empty:
        st.warning("No streaming records match the selected filters.")
        st.stop()

    flagged = view["anomaly_flag"]
    anomaly_rate = 100 * flagged.mean() if len(view) else 0

    metric_row([
        ("Window records", format_number(len(view)),
         "Persisted aggregation output rows"),
        ("Stations", format_number(view["location_id"].nunique()),
         "Stations in the selection"),
        ("Flagged records", format_number(flagged.sum()),
         "Records above project-defined thresholds"),
        ("Flag rate", f"{anomaly_rate:.1f}%",
         "Percentage of selected output rows flagged"),
    ])

    left, right = st.columns(2)

    with left:
        st.markdown("#### Records over time")
        time_data = view.sort_values("window_start").copy()
        time_data["Status"] = time_data["anomaly_flag"].map(
            {True: "Flagged", False: "Not flagged"}
        )
        fig = px.scatter(
            time_data,
            x="window_start",
            y="avg_value",
            color="Status",
            symbol="Status",
            hover_data=[
                "location", "parameter", "max_value",
                "anomaly_threshold", "measurement_count",
            ],
            title="Window average by event-time window",
            labels={
                "window_start": "Window start",
                "avg_value": "Average measurement",
            },
        )
        st.plotly_chart(fig, use_container_width=True)

    with right:
        st.markdown("#### Average versus threshold")
        pollutant_summary = (
            view.groupby("parameter", as_index=False)
            .agg(
                Average=("avg_value", "mean"),
                Threshold=("anomaly_threshold", "mean"),
            )
        )
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=pollutant_summary["parameter"],
            y=pollutant_summary["Average"],
            name="Mean window average",
        ))
        fig.add_trace(go.Bar(
            x=pollutant_summary["parameter"],
            y=pollutant_summary["Threshold"],
            name="Mean configured threshold",
        ))
        fig.update_layout(
            barmode="group",
            title="Measurement values and configured thresholds",
            xaxis_title="Pollutant",
            yaxis_title="Value (units vary by pollutant)",
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("#### Anomaly records")
    anomalies = view[view["anomaly_flag"]].sort_values(
        "window_start", ascending=False
    )
    if anomalies.empty:
        st.success("No flagged records in the current selection.")
    else:
        st.dataframe(
            anomalies[
                [
                    "window_start", "window_end", "location",
                    "location_id", "parameter", "avg_value",
                    "max_value", "anomaly_threshold",
                    "measurement_count", "anomaly_reason",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

        st.download_button(
            "Download flagged records (CSV)",
            data=anomalies.to_csv(index=False).encode("utf-8"),
            file_name="airstream_anomaly_records.csv",
            mime="text/csv",
        )

    with st.expander("View all filtered streaming records"):
        st.dataframe(
            view.sort_values("window_start", ascending=False),
            use_container_width=True,
            hide_index=True,
        )

    st.caption(
        "The anomaly thresholds are illustrative project thresholds, not "
        "official CPCB AQI breakpoints. Measurement counts are the counts "
        "recorded by the aggregation output and may be affected by repeated "
        "historical replay events."
    )


# ---------------------------------------------------------
# Data and methodology
# ---------------------------------------------------------
elif page == "Data & Methodology":
    st.title("Data & Methodology")

    st.markdown("#### Batch analytics")
    st.write(
        "Historical OpenAQ measurements are processed into cleaned "
        "Parquet datasets and aggregated into station-day AQI records."
    )
    st.code(
        "OpenAQ archive → PySpark cleaning → Silver Parquet "
        "→ Gold aggregates → AQI dataset → Dashboard",
        language="text",
    )

    st.markdown("#### Streaming analytics")
    st.write(
        "Historical measurements are replayed into Kafka. Spark "
        "Structured Streaming reads the topic, groups events into "
        "five-minute event-time windows, calculates aggregates, and "
        "flags records that exceed configured thresholds."
    )
    st.code(
        "Historical replay → Kafka topic → Spark Structured Streaming "
        "→ 5-minute windows → Threshold alerts → Parquet → Dashboard",
        language="text",
    )

    st.markdown("#### Interpretation notes")
    st.info(
        """
        - The streaming pipeline uses historical replay data; it is not a
          live sensor ingestion system.
        - Anomaly thresholds are project-specific demonstration thresholds,
          not official regulatory limits.
        - AQI values shown are those in the existing `aqi_final` dataset.
          The dashboard does not recalculate or independently certify AQI.
        - CPCB pollutant averaging periods differ by pollutant. Daily
          averages for all pollutants should not be described as a complete
          official CPCB AQI calculation.
        - The dashboard reads existing Parquet outputs; it does not restart
          Spark jobs or reset streaming checkpoints.
        """
    )

    st.markdown("#### Dataset status")
    status_rows = [
        {
            "Dataset": "Historical AQI",
            "Path": str(AQI_PATH),
            "Available": AQI_PATH.exists(),
            "Rows loaded": len(aqi_df),
        },
        {
            "Dataset": "Streaming output",
            "Path": str(STREAM_PATH),
            "Available": STREAM_PATH.exists(),
            "Rows loaded": len(stream_df),
        },
    ]
    st.dataframe(pd.DataFrame(status_rows), use_container_width=True, hide_index=True)

    st.markdown("#### Current loaded columns")
    with st.expander("Historical AQI schema"):
        st.write(aqi_df.columns.tolist() if not aqi_df.empty else "No data loaded.")
    with st.expander("Streaming schema"):
        st.write(
            stream_df.columns.tolist() if not stream_df.empty else "No data loaded."
        )


# ---------------------------------------------------------
# Footer
# ---------------------------------------------------------
st.divider()
st.caption(
    "AirStream · Big Data Computing mini-project · "
    "Historical batch analytics and replay-based streaming monitoring"
)
