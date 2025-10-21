"""Streamlit dashboard streaming synthetic traffic metrics from HDFS via WebHDFS."""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Sequence, Tuple

import pandas as pd
import requests
import streamlit as st
from requests import RequestException
from urllib.parse import quote
import matplotlib.pyplot as plt

WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://localhost:9870").rstrip("/")
HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "/data/gold/synthetic").rstrip("/") or "/"
HDFS_USER = os.getenv("HDFS_USER", "hdfs")
REFRESH_INTERVAL_SECONDS = float(os.getenv("STREAM_REFRESH_SECONDS", "2"))
DEFAULT_WINDOW_MINUTES = int(os.getenv("STREAM_WINDOW_MINUTES", "15"))
HISTORY_MINUTES = int(os.getenv("STREAM_HISTORY_MINUTES", "1440"))
MAX_FILES = int(os.getenv("STREAM_MAX_FILES", "12"))

VEHICLE_SHARE_COLUMNS: Sequence[str] = (
    "pedal_cycle_count",
    "two_wheeled_motor_vehicle_count",
    "car_and_taxi_count",
    "bus_and_coach_count",
    "light_goods_vehicle_count",
    "all_heavy_goods_vehicle_count",
)

VEHICLE_BREAKDOWN_COLUMNS: Sequence[str] = (
    "pedal_cycle_count",
    "two_wheeled_motor_vehicle_count",
    "car_and_taxi_count",
    "bus_and_coach_count",
    "light_goods_vehicle_count",
    "heavy_goods_vehicle_2_rigid_axles_count",
    "heavy_goods_vehicle_3_rigid_axles_count",
    "heavy_goods_vehicle_4_plus_rigid_axles_count",
    "heavy_goods_vehicle_3_or_4_articulated_axles_count",
    "heavy_goods_vehicle_5_articulated_axles_count",
    "heavy_goods_vehicle_6_articulated_axles_count",
)

PASTEL_PALETTE = [
    "#A1C9F4",
    "#FFB6C1",
    "#C7CEEA",
    "#FFDAC1",
    "#B5EAD7",
    "#E2F0CB",
    "#F6A6FF",
    "#FFD1DC",
    "#BFC0F0",
    "#F5C0C0",
]

VEHICLE_LABELS = {
    "pedal_cycle_count": "Pedal cycles",
    "two_wheeled_motor_vehicle_count": "Motorcycles",
    "car_and_taxi_count": "Cars & taxis",
    "bus_and_coach_count": "Buses & coaches",
    "light_goods_vehicle_count": "Light goods",
    "all_heavy_goods_vehicle_count": "All HGVs",
    "heavy_goods_vehicle_2_rigid_axles_count": "HGV 2 rigid axles",
    "heavy_goods_vehicle_3_rigid_axles_count": "HGV 3 rigid axles",
    "heavy_goods_vehicle_4_plus_rigid_axles_count": "HGV ≥4 rigid axles",
    "heavy_goods_vehicle_3_or_4_articulated_axles_count": "HGV 3-4 articulated",
    "heavy_goods_vehicle_5_articulated_axles_count": "HGV 5 articulated",
    "heavy_goods_vehicle_6_articulated_axles_count": "HGV 6 articulated",
}


def _webhdfs_get(path: str, operation: str, timeout: float = 10.0) -> requests.Response:
    params = {"op": operation, "user.name": HDFS_USER}
    url = f"{WEBHDFS_URL}/webhdfs/v1{quote(path, safe='/')}"
    return requests.get(url, params=params, timeout=timeout)


def list_status(path: str) -> List[Dict[str, object]]:
    try:
        resp = _webhdfs_get(path, "LISTSTATUS")
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        payload = resp.json()
    except (RequestException, ValueError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"LISTSTATUS failed for {path}: {exc}") from exc
    statuses = payload.get("FileStatuses", {}).get("FileStatus", [])
    return statuses if isinstance(statuses, list) else []


def read_hdfs_file(path: str) -> List[Dict[str, object]]:
    try:
        resp = _webhdfs_get(path, "OPEN", timeout=30.0)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
    except RequestException as exc:
        raise RuntimeError(f"OPEN failed for {path}: {exc}") from exc

    records: List[Dict[str, object]] = []
    for raw_line in resp.text.splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            records.append(json.loads(raw_line))
        except json.JSONDecodeError:
            continue
    return records


def discover_recent_files() -> List[Dict[str, object]]:
    partitions = list_status(HDFS_BASE_PATH)
    entries: List[Dict[str, object]] = []
    for partition in partitions:
        if partition.get("type") != "DIRECTORY":
            continue
        suffix = partition.get("pathSuffix")
        if not suffix:
            continue
        partition_path = f"{HDFS_BASE_PATH}/{suffix}" if HDFS_BASE_PATH != "/" else f"/{suffix}"
        try:
            files = list_status(partition_path)
        except RuntimeError:
            continue
        for file_status in files:
            if file_status.get("type") != "FILE":
                continue
            name = file_status.get("pathSuffix")
            if not name:
                continue
            entries.append(
                {
                    "path": f"{partition_path}/{name}",
                    "length": int(file_status.get("length", 0)),
                    "mod": int(file_status.get("modificationTime", 0)),
                }
            )
    entries.sort(key=lambda item: item["mod"])
    return entries[-MAX_FILES:]


def fetch_new_records(processed: Dict[str, int]) -> List[Dict[str, object]]:
    pending: List[Dict[str, object]] = []
    try:
        recent_files = discover_recent_files()
    except RuntimeError as exc:
        raise RuntimeError(str(exc)) from exc

    for entry in recent_files:
        path = entry["path"]
        length = entry["length"]
        if length == 0:
            continue
        known = processed.get(path)
        if known is not None and known == length:
            continue
        records = read_hdfs_file(path)
        if not records:
            continue
        processed[path] = length
        pending.extend(records)
    return pending


def normalise_records(rows: List[Dict[str, object]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()

    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame

    ts_col = None
    for candidate in ("event_timestamp", "timestamp", "observation_time"):
        if candidate in frame.columns:
            ts_col = candidate
            break
    if ts_col is None:
        frame["event_timestamp"] = datetime.now(timezone.utc)
    else:
        frame["event_timestamp"] = pd.to_datetime(frame[ts_col], utc=True, errors="coerce")

    if "region_name" not in frame.columns:
        frame["region_name"] = "Unknown"

    total_col = None
    for candidate in ("all_motor_vehicle_count", "all_motor_vehicles", "total_vehicles"):
        if candidate in frame.columns:
            total_col = candidate
            break
    frame["total_vehicles"] = (
        pd.to_numeric(frame[total_col], errors="coerce").fillna(0).astype(int)
        if total_col is not None
        else pd.Series(0, index=frame.index, dtype="int64")
    )

    density_col = None
    for candidate in ("vehicles_per_kilometer", "vehicles_per_km", "density"):
        if candidate in frame.columns:
            density_col = candidate
            break
    frame["vehicles_per_km"] = (
        pd.to_numeric(frame[density_col], errors="coerce").fillna(0.0)
        if density_col is not None
        else pd.Series(0.0, index=frame.index, dtype="float64")
    )

    heavy_col = None
    for candidate in ("all_heavy_goods_vehicle_count", "all_hgvs", "heavy_vehicles"):
        if candidate in frame.columns:
            heavy_col = candidate
            break
    frame["heavy_vehicles"] = (
        pd.to_numeric(frame[heavy_col], errors="coerce").fillna(0).astype(int)
        if heavy_col is not None
        else frame.filter(like="heavy_goods_vehicle_").apply(pd.to_numeric, errors="coerce").fillna(0).astype(int).sum(axis=1)
    )

    if float(frame["vehicles_per_km"].abs().sum()) == 0.0 and "link_length_kilometers" in frame.columns:
        link_lengths = pd.to_numeric(frame["link_length_kilometers"], errors="coerce").replace(0, float("nan"))
        derived_density = frame["total_vehicles"] / link_lengths
        frame["vehicles_per_km"] = derived_density.fillna(0.0)

    return frame


def filter_window(df: pd.DataFrame, minutes_back: int) -> pd.DataFrame:
    if df.empty or minutes_back <= 0:
        return df
    latest = df["event_timestamp"].max()
    if pd.isna(latest):
        return df
    cutoff = latest - timedelta(minutes=minutes_back)
    return df[df["event_timestamp"] >= cutoff]


def prune_history(df: pd.DataFrame, minutes_back: int) -> pd.DataFrame:
    if df.empty or minutes_back <= 0:
        return df
    latest = df["event_timestamp"].max()
    if pd.isna(latest):
        return df
    cutoff = latest - timedelta(minutes=minutes_back)
    return df[df["event_timestamp"] >= cutoff]


def compute_region_metrics(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    if df.empty:
        empty = pd.Series(dtype=float)
        return empty, empty
    totals = df.groupby("region_name")["total_vehicles"].sum().sort_values(ascending=False)
    densities = df.groupby("region_name")["vehicles_per_km"].mean().sort_values(ascending=False)
    return totals, densities


def compute_time_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    return (
        df.set_index("event_timestamp")["total_vehicles"]
        .resample("1Min")
        .sum()
        .fillna(0)
    )


def pastel_colors(count: int) -> List[str]:
    if count <= 0:
        return []
    repeats = (count // len(PASTEL_PALETTE)) + 1
    palette = (PASTEL_PALETTE * repeats)[:count]
    return palette


def render_pie_chart(data: pd.Series, title: str) -> None:
    if data.empty or data.sum() == 0:
        st.info(f"No data available for {title.lower()}.")
        return
    fig, ax = plt.subplots()
    colors = pastel_colors(len(data))
    ax.pie(data, labels=data.index, autopct="%1.1f%%", startangle=120, colors=colors)
    ax.axis("equal")
    ax.set_title(title)
    st.pyplot(fig, clear_figure=True)
    plt.close(fig)


def aggregate_vehicle_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.Series:
    available = [col for col in columns if col in df.columns]
    if not available:
        return pd.Series(dtype=float)
    totals = df[available].apply(pd.to_numeric, errors="coerce").fillna(0).sum()
    totals.index = [VEHICLE_LABELS.get(col, col) for col in totals.index]
    return totals


def main() -> None:
    st.set_page_config(page_title="TrafficFlow Live Monitor", layout="wide")
    st.title("TrafficFlow Live Monitor")
    st.caption(f"Watching HDFS path {HDFS_BASE_PATH}")

    window_minutes = DEFAULT_WINDOW_MINUTES
    refresh_seconds = REFRESH_INTERVAL_SECONDS
    refresh_timestamp = datetime.now(timezone.utc)
    st.caption(
        f"Window: last {window_minutes} minutes | Auto refresh every {refresh_seconds:.1f} seconds"
    )

    if "processed_files" not in st.session_state:
        st.session_state.processed_files = {}
    if "records" not in st.session_state:
        st.session_state.records = pd.DataFrame()

    error_message = None
    new_payload: List[Dict[str, object]] = []
    try:
        new_payload = fetch_new_records(st.session_state.processed_files)
    except RuntimeError as exc:
        error_message = str(exc)

    if new_payload:
        latest_df = normalise_records(new_payload)
        if not latest_df.empty:
            combined = pd.concat([st.session_state.records, latest_df], ignore_index=True)
            combined.sort_values("event_timestamp", inplace=True)
            combined = prune_history(combined, HISTORY_MINUTES)
            st.session_state.records = combined

    live_df = st.session_state.records.copy()
    live_df = filter_window(live_df, window_minutes)

    if error_message:
        st.error(error_message)

    if live_df.empty:
        st.warning(
            "No data available in the selected window. Ensure the producer is running and writing to HDFS."
        )
        st.caption(
            f"Last refresh attempt: {refresh_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')}"
        )
        time.sleep(refresh_seconds)
        st.rerun()

    metrics_row = st.columns(3)
    metrics_row[0].metric("Records (window)", f"{len(live_df):,}")
    metrics_row[1].metric("Vehicles (window)", f"{live_df['total_vehicles'].sum():,}")
    metrics_row[2].metric("Mean vehicles/km", f"{live_df['vehicles_per_km'].mean():.1f}")

    series = compute_time_series(live_df)
    region_totals, region_density = compute_region_metrics(live_df)

    chart_row = st.columns((2, 1))
    chart_row[0].line_chart(series, height=300)
    chart_row[0].caption("Vehicles per minute (resampled)")
    chart_row[1].bar_chart(region_totals, height=300)
    chart_row[1].caption("Vehicles by region (sum)")

    secondary_row = st.columns(1)
    secondary_row[0].bar_chart(region_density, height=300)
    secondary_row[0].caption("Average vehicles/km by region")

    pie_row = st.columns(2)
    with pie_row[0]:
        render_pie_chart(region_totals, "Region share of vehicles")
    with pie_row[1]:
        vehicle_share = aggregate_vehicle_columns(live_df, VEHICLE_SHARE_COLUMNS).sort_values(
            ascending=False
        )
        render_pie_chart(vehicle_share, "Vehicle type share")

    region_options = sorted(live_df["region_name"].dropna().unique().tolist())
    if region_options:
        selected_region = st.selectbox("Region focus", region_options)
        region_focus_df = live_df[live_df["region_name"] == selected_region]
        region_breakdown = aggregate_vehicle_columns(
            region_focus_df, VEHICLE_BREAKDOWN_COLUMNS
        ).sort_values(ascending=False)
        if region_breakdown.empty or region_breakdown.sum() == 0:
            st.info("No vehicle breakdown data for the selected region in this window.")
        else:
            st.bar_chart(region_breakdown, height=320)

    st.subheader("Latest records")
    table_columns = [
        "event_timestamp",
        "region_name",
        "local_authority_name",
        "road_name",
        "road_type",
        "total_vehicles",
        "heavy_vehicles",
        "vehicles_per_km",
    ]
    table_columns = [col for col in table_columns if col in live_df.columns]
    st.dataframe(live_df[table_columns].tail(100), width="stretch")

    st.caption(
        f"Auto refresh every {refresh_seconds:.1f} seconds (fixed). Data retained ~{HISTORY_MINUTES} minutes."
    )
    st.caption(
        f"Last refresh: {refresh_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')} | Processed files: {len(st.session_state.processed_files)} | Cached records: {len(st.session_state.records)}"
    )
    time.sleep(refresh_seconds)
    st.rerun()


if __name__ == "__main__":
    main()
