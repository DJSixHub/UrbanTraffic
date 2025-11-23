"""Streamlit dashboard streaming synthetic traffic metrics from HDFS via WebHDFS."""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence, Tuple

import pandas as pd
import requests
import streamlit as st
from requests import RequestException
from urllib.parse import quote
import altair as alt
import plotly.express as px

WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://localhost:9870").rstrip("/")
HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "/data/gold/management/primary").rstrip("/") or "/"
HDFS_USER = os.getenv("HDFS_USER", "hdfs")
REFRESH_INTERVAL_SECONDS = float(os.getenv("STREAM_REFRESH_SECONDS", "2"))
DEFAULT_WINDOW_MINUTES = int(os.getenv("STREAM_WINDOW_MINUTES", "15"))
HISTORY_MINUTES = int(os.getenv("STREAM_HISTORY_MINUTES", "1440"))
MAX_FILES = int(os.getenv("STREAM_MAX_FILES", "12"))
PROFILE_STATUS_PATH = os.getenv("PROFILE_STATUS_PATH", "").strip()
PROFILE_OVERRIDE_PATH = os.getenv("PROFILE_OVERRIDE_PATH", "").strip()

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

VEHICLE_LABELS = {
    "pedal_cycle_count": "Bicicletas",
    "two_wheeled_motor_vehicle_count": "Motocicletas",
    "car_and_taxi_count": "Autos y taxis",
    "bus_and_coach_count": "Autobuses y autocares",
    "light_goods_vehicle_count": "Vehículos ligeros de carga",
    "all_heavy_goods_vehicle_count": "Vehículos pesados totales",
    "heavy_goods_vehicle_2_rigid_axles_count": "Pesados 2 ejes rígidos",
    "heavy_goods_vehicle_3_rigid_axles_count": "Pesados 3 ejes rígidos",
    "heavy_goods_vehicle_4_plus_rigid_axles_count": "Pesados ≥4 ejes rígidos",
    "heavy_goods_vehicle_3_or_4_articulated_axles_count": "Pesados 3-4 articulados",
    "heavy_goods_vehicle_5_articulated_axles_count": "Pesados 5 articulados",
    "heavy_goods_vehicle_6_articulated_axles_count": "Pesados 6 articulados",
}

alt.data_transformers.disable_max_rows()


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
    entries: List[Dict[str, object]] = []

    def _walk(base_path: str, depth: int) -> None:
        try:
            statuses = list_status(base_path)
        except RuntimeError:
            return
        for status in statuses:
            suffix = status.get("pathSuffix")
            if not suffix:
                continue
            full_path = f"{base_path}/{suffix}" if base_path != "/" else f"/{suffix}"
            if status.get("type") == "FILE":
                entries.append(
                    {
                        "path": full_path,
                        "length": int(status.get("length", 0)),
                        "mod": int(status.get("modificationTime", 0)),
                    }
                )
            elif depth < 3 and status.get("type") == "DIRECTORY":
                _walk(full_path, depth + 1)

    _walk(HDFS_BASE_PATH, 0)
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
    if ts_col is not None:
        frame["event_timestamp"] = pd.to_datetime(frame[ts_col], utc=True, errors="coerce")
    elif "batch_timestamp" in frame.columns:
        frame["event_timestamp"] = pd.to_datetime(frame["batch_timestamp"], unit="s", utc=True, errors="coerce")
    else:
        frame["event_timestamp"] = datetime.now(timezone.utc)

    if "region_name" not in frame.columns:
        frame["region_name"] = "Unknown"

    total_col = None
    for candidate in ("all_motor_vehicle_count", "all_motor_vehicles", "total_vehicles"):
        if candidate in frame.columns:
            total_col = candidate
            break
    if total_col is not None:
        frame["total_vehicles"] = pd.to_numeric(frame[total_col], errors="coerce").fillna(0).astype(int)
    else:
        frame["total_vehicles"] = pd.Series(0, index=frame.index, dtype="int64")

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

    if float(frame["vehicles_per_km"].abs().sum()) == 0.0:
        if "link_length_kilometers" in frame.columns:
            link_lengths = pd.to_numeric(frame["link_length_kilometers"], errors="coerce").replace(0, float("nan"))
            derived_density = frame["total_vehicles"] / link_lengths
            frame["vehicles_per_km"] = derived_density.fillna(0.0)
        elif "avg_vehicles" in frame.columns:
            frame["vehicles_per_km"] = pd.to_numeric(frame["avg_vehicles"], errors="coerce").fillna(0.0)

    aggregated_vehicle_map = {
        "pedal_cycle_total": "pedal_cycle_count",
        "two_wheeled_total": "two_wheeled_motor_vehicle_count",
        "car_and_taxi_total": "car_and_taxi_count",
        "bus_and_coach_total": "bus_and_coach_count",
        "light_goods_total": "light_goods_vehicle_count",
        "hgv2_total": "heavy_goods_vehicle_2_rigid_axles_count",
        "hgv3_total": "heavy_goods_vehicle_3_rigid_axles_count",
        "hgv4_total": "heavy_goods_vehicle_4_plus_rigid_axles_count",
        "hgv34_total": "heavy_goods_vehicle_3_or_4_articulated_axles_count",
        "hgv5_total": "heavy_goods_vehicle_5_articulated_axles_count",
        "hgv6_total": "heavy_goods_vehicle_6_articulated_axles_count",
    }

    for source_col, target_col in aggregated_vehicle_map.items():
        if source_col in frame.columns:
            frame[target_col] = pd.to_numeric(frame[source_col], errors="coerce").fillna(0).astype(int)

    if "role" in frame.columns:
        frame["role"] = frame["role"].fillna("primary").astype(str)
    else:
        frame["role"] = "primary"

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


def render_pie_chart(data: pd.Series, title: str) -> None:
    if data.empty or data.sum() == 0:
        st.info(f"No hay datos disponibles para {title.lower()}.")
        return
    pie_df = data.reset_index()
    pie_df.columns = ["Categoría", "Valor"]
    fig = px.pie(
        pie_df,
        names="Categoría",
        values="Valor",
        title=title,
    )
    fig.update_traces(hovertemplate="%{label}: %{value:,} vehículos (%{percent:.1%})")
    st.plotly_chart(fig, use_container_width=True)


def render_line_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    x_title: str,
    y_title: str,
) -> None:
    if df.empty:
        st.info(f"No hay datos disponibles para {title.lower()}.")
        return
    chart = (
        alt.Chart(df)
        .mark_line(point=True)
        .encode(
            x=alt.X(x_col, title=x_title, type="temporal"),
            y=alt.Y(y_col, title=y_title, type="quantitative"),
            tooltip=[
                alt.Tooltip(x_col, title=x_title, type="temporal"),
                alt.Tooltip(y_col, title=y_title, type="quantitative"),
            ],
        )
        .properties(title=title, height=320)
    )
    st.altair_chart(chart, use_container_width=True)


def render_bar_chart(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    title: str,
    x_title: str,
    y_title: str,
    sort_desc: bool = True,
) -> None:
    if df.empty:
        st.info(f"No hay datos disponibles para {title.lower()}.")
        return
    order = alt.SortField(field=y_col, order="descending" if sort_desc else "ascending")
    chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x=alt.X(x_col, title=x_title, sort=order),
            y=alt.Y(y_col, title=y_title, type="quantitative"),
            tooltip=[
                alt.Tooltip(x_col, title=x_title),
                alt.Tooltip(y_col, title=y_title, type="quantitative"),
            ],
        )
        .properties(title=title, height=320)
    )
    st.altair_chart(chart, use_container_width=True)


def aggregate_vehicle_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.Series:
    available = [col for col in columns if col in df.columns]
    if not available:
        return pd.Series(dtype=float)
    totals = df[available].apply(pd.to_numeric, errors="coerce").fillna(0).sum()
    totals.index = [VEHICLE_LABELS.get(col, col) for col in totals.index]
    return totals


def _load_profile_status() -> Optional[Dict[str, object]]:
    path = PROFILE_STATUS_PATH
    if path and os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            if isinstance(payload, dict):
                return payload
        except (OSError, json.JSONDecodeError):
            pass
    if PROFILE_OVERRIDE_PATH and os.path.exists(PROFILE_OVERRIDE_PATH):
        return {
            "source": "override",
            "profile_path": PROFILE_OVERRIDE_PATH,
        }
    return None


def _render_profile_banner() -> None:
    status = _load_profile_status()
    if not status:
        st.caption("Perfil de generación: sin información disponible.")
        return
    source = str(status.get("source", "")).lower()
    meta = status.get("meta") if isinstance(status.get("meta"), dict) else {}
    dataset_hint = meta.get("input_path") or status.get("profile_path")
    if source == "override":
        hint = f"{dataset_hint}" if dataset_hint else "dataset raw detectado"
        st.success(f"Perfil activo derivado de CSV ({hint}).")
    elif source == "primary":
        st.info("Perfil personalizado preexistente en uso.")
    else:
        st.warning("Perfil por defecto embebido en uso.")


def main() -> None:
    st.set_page_config(page_title="Monitor de tráfico TrafficFlow", layout="wide")
    st.title("Monitor de tráfico en vivo")
    _render_profile_banner()
    st.caption(f"Monitoreando ruta HDFS {HDFS_BASE_PATH}")

    window_minutes = DEFAULT_WINDOW_MINUTES
    refresh_seconds = REFRESH_INTERVAL_SECONDS
    refresh_timestamp = datetime.now(timezone.utc)
    st.caption(
        f"Ventana: últimos {window_minutes} minutos | Autoactualización cada {refresh_seconds:.1f} segundos"
    )

    if "processed_files" not in st.session_state:
        st.session_state.processed_files = {}
    if "records" not in st.session_state:
        st.session_state.records = pd.DataFrame()
    if "increment_history" not in st.session_state:
        st.session_state.increment_history = pd.DataFrame(
            columns=["timestamp", "vehicles_new", "vehicles_avg_region"]
        )

    error_message = None
    new_payload: List[Dict[str, object]] = []
    try:
        new_payload = fetch_new_records(st.session_state.processed_files)
    except RuntimeError as exc:
        error_message = str(exc)

    vehicles_added = 0.0
    avg_per_region = float("nan")
    if new_payload:
        latest_df = normalise_records(new_payload)
        if not latest_df.empty:
            combined = pd.concat([st.session_state.records, latest_df], ignore_index=True)
            combined.sort_values("event_timestamp", inplace=True)
            combined = prune_history(combined, HISTORY_MINUTES)
            st.session_state.records = combined

            if "role" in st.session_state.records.columns:
                st.session_state.records["role"] = (
                    st.session_state.records["role"].fillna("primary").astype(str)
                )
            else:
                st.session_state.records["role"] = "primary"

            latest_primary = (
                latest_df[latest_df["role"] != "authority"].copy()
                if "role" in latest_df.columns
                else latest_df
            )
            vehicles_added = float(
                pd.to_numeric(latest_primary["total_vehicles"], errors="coerce").fillna(0).sum()
            )
            region_count = (
                latest_primary["region_name"].dropna().nunique()
                if "region_name" in latest_primary.columns
                else 0
            )
            if region_count > 0 and vehicles_added > 0:
                avg_per_region = vehicles_added / region_count

    if vehicles_added > 0:
        history_row = pd.DataFrame(
            {
                "timestamp": [refresh_timestamp],
                "vehicles_new": [vehicles_added],
                "vehicles_avg_region": [avg_per_region],
            }
        )
        st.session_state.increment_history = pd.concat(
            [st.session_state.increment_history, history_row], ignore_index=True
        ).tail(500)

    if "role" in st.session_state.records.columns:
        st.session_state.records["role"] = (
            st.session_state.records["role"].fillna("primary").astype(str)
        )
    else:
        st.session_state.records["role"] = "primary"

    live_df = st.session_state.records.copy()
    live_df = filter_window(live_df, window_minutes)

    if error_message:
        st.error(error_message)

    if live_df.empty:
        st.warning(
            "No hay datos en la ventana seleccionada. Verifica que el productor esté escribiendo en HDFS."
        )
        st.caption(
            f"Último intento de actualización: {refresh_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')}"
        )
        time.sleep(refresh_seconds)
        st.rerun()

    if "role" in live_df.columns:
        authority_df = live_df[live_df["role"] == "authority"].copy()
        primary_df = live_df[live_df["role"] != "authority"].copy()
    else:
        authority_df = pd.DataFrame(columns=live_df.columns)
        primary_df = live_df.copy()

    if primary_df.empty:
        primary_df = live_df.copy()

    effective_df = primary_df

    metrics_row = st.columns(3)
    metrics_row[0].metric("Registros (ventana)", f"{len(effective_df):,}")
    metrics_row[1].metric("Vehículos (ventana)", f"{effective_df['total_vehicles'].sum():,}")
    metrics_row[2].metric("Promedio vehículos/km", f"{effective_df['vehicles_per_km'].mean():.1f}")

    region_totals, region_density = compute_region_metrics(effective_df)

    eventos_df = st.session_state.increment_history.rename(
        columns={
            "timestamp": "Marca temporal",
            "vehicles_new": "Vehículos nuevos",
        }
    ).sort_values("Marca temporal")
    eventos_df = eventos_df[eventos_df["Vehículos nuevos"] > 0]

    avg_region_df = (
        st.session_state.increment_history[
            ["timestamp", "vehicles_avg_region"]
        ]
        .rename(
            columns={
                "timestamp": "Marca temporal",
                "vehicles_avg_region": "Promedio vehículos por región",
            }
        )
        .dropna(subset=["Promedio vehículos por región"])
        .sort_values("Marca temporal")
    )
    avg_region_df = avg_region_df[avg_region_df["Promedio vehículos por región"] > 0]

    time_cols = st.columns(2)
    with time_cols[0]:
        render_line_chart(
            eventos_df,
            "Marca temporal",
            "Vehículos nuevos",
            "Vehículos nuevos por registro",
            "Marca temporal",
            "Vehículos nuevos",
        )
    with time_cols[1]:
        render_line_chart(
            avg_region_df,
            "Marca temporal",
            "Promedio vehículos por región",
            "Promedio de vehículos por región",
            "Marca temporal",
            "Promedio vehículos por región",
        )

    region_totals_df = (
        region_totals.reset_index().rename(columns={"region_name": "Región", 0: "Vehículos totales"})
        if not region_totals.empty
        else pd.DataFrame(columns=["Región", "Vehículos totales"])
    )
    if "total_vehicles" in region_totals_df.columns:
        region_totals_df = region_totals_df.rename(columns={"total_vehicles": "Vehículos totales"})

    render_bar_chart(
        region_totals_df,
        "Región",
        "Vehículos totales",
        "Vehículos totales por región",
        "Región",
        "Vehículos totales",
    )

    region_density_df = (
        region_density.reset_index().rename(columns={"region_name": "Región", 0: "Vehículos por kilómetro"})
        if not region_density.empty
        else pd.DataFrame(columns=["Región", "Vehículos por kilómetro"])
    )
    if "vehicles_per_km" in region_density_df.columns:
        region_density_df = region_density_df.rename(
            columns={"vehicles_per_km": "Vehículos por kilómetro"}
        )

    render_bar_chart(
        region_density_df,
        "Región",
        "Vehículos por kilómetro",
        "Promedio de vehículos por kilómetro",
        "Región",
        "Vehículos por kilómetro",
        sort_desc=True,
    )

    pie_row = st.columns(2)
    with pie_row[0]:
        render_pie_chart(region_totals, "Participación de vehículos por región")
    with pie_row[1]:
        vehicle_share = aggregate_vehicle_columns(effective_df, VEHICLE_SHARE_COLUMNS).sort_values(
            ascending=False
        )
        render_pie_chart(vehicle_share, "Distribución por tipo de vehículo")

    region_options = sorted(effective_df["region_name"].dropna().unique().tolist())
    if region_options:
        st.markdown("### Exploración jerárquica")
        selected_region = st.selectbox("Región a analizar", region_options, key="region_selector")
        region_window_df = effective_df[effective_df["region_name"] == selected_region]
        region_vehicle_breakdown = aggregate_vehicle_columns(
            region_window_df, VEHICLE_BREAKDOWN_COLUMNS
        ).sort_values(ascending=False)

        has_local_authority = not authority_df.empty and "local_authority_name" in authority_df.columns

        region_authority_totals = (
            authority_df[authority_df["region_name"] == selected_region]
            .dropna(subset=["local_authority_name"])
            .groupby("local_authority_name")["total_vehicles"]
            .sum()
            .sort_values(ascending=False)
            if has_local_authority
            else pd.Series(dtype=float)
        )

        region_cols = st.columns(2)
        with region_cols[0]:
            if region_vehicle_breakdown.empty or region_vehicle_breakdown.sum() == 0:
                st.info("No hay desglose de vehículos para la región seleccionada en esta ventana.")
            else:
                breakdown_df = region_vehicle_breakdown.reset_index()
                breakdown_df.columns = ["Tipo de vehículo", "Total"]
                render_bar_chart(
                    breakdown_df,
                    "Tipo de vehículo",
                    "Total",
                    f"Desglose de vehículos en {selected_region}",
                    "Tipo de vehículo",
                    "Total",
                )

        with region_cols[1]:
            if region_authority_totals.empty or region_authority_totals.sum() == 0:
                st.info("No hay tráfico registrado por autoridad local en la región seleccionada.")
            else:
                render_pie_chart(
                    region_authority_totals,
                    f"Participación de autoridades locales en {selected_region}",
                )

        authority_options = region_authority_totals.index.tolist()
        if has_local_authority and authority_options:
            authority_key = f"authority_selector_{selected_region.lower().replace(' ', '_')}"
            selected_authority = st.selectbox(
                "Autoridad local a analizar",
                authority_options,
                key=authority_key,
            )
            authority_window_df = authority_df[
                (authority_df["region_name"] == selected_region)
                & (authority_df["local_authority_name"] == selected_authority)
            ]
            authority_vehicle_breakdown = aggregate_vehicle_columns(
                authority_window_df, VEHICLE_BREAKDOWN_COLUMNS
            ).sort_values(ascending=False)

            authority_history_df = (
                st.session_state.records[
                    (st.session_state.records["role"] == "authority")
                    & (st.session_state.records["region_name"] == selected_region)
                    & (st.session_state.records["local_authority_name"] == selected_authority)
                ]
                if {
                    "region_name",
                    "local_authority_name",
                    "total_vehicles",
                    "role",
                }.issubset(st.session_state.records.columns)
                else pd.DataFrame()
            )

            authority_cols = st.columns(2)
            with authority_cols[0]:
                if authority_vehicle_breakdown.empty or authority_vehicle_breakdown.sum() == 0:
                    st.info("Sin desglose por tipo de vehículo para la autoridad seleccionada.")
                else:
                    breakdown_df = authority_vehicle_breakdown.reset_index()
                    breakdown_df.columns = ["Tipo de vehículo", "Total"]
                    render_bar_chart(
                        breakdown_df,
                        "Tipo de vehículo",
                        "Total",
                        f"Desglose en {selected_authority}",
                        "Tipo de vehículo",
                        "Total",
                    )

            has_road_details = "road_name" in authority_history_df.columns
            road_counts = (
                authority_history_df.dropna(subset=["road_name"])
                .groupby("road_name")["total_vehicles"]
                .sum()
                .sort_values(ascending=False)
                .head(12)
                if has_road_details and not authority_history_df.empty
                else pd.Series(dtype=float)
            )

            with authority_cols[1]:
                if road_counts.empty or road_counts.sum() == 0:
                    st.info("No hay información histórica de calles para la autoridad seleccionada.")
                else:
                    render_pie_chart(
                        road_counts,
                        f"Participación de calles en {selected_authority}",
                    )

    st.subheader("Registros más recientes")
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
        f"Autoactualización fija cada {refresh_seconds:.1f} segundos. Datos conservados ~{HISTORY_MINUTES} minutos."
    )
    st.caption(
        f"Última actualización: {refresh_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')} | Archivos procesados: {len(st.session_state.processed_files)} | Registros en caché: {len(st.session_state.records)}"
    )
    time.sleep(refresh_seconds)
    st.rerun()


if __name__ == "__main__":
    main()
