# Panel Streamlit que muestra métricas de tráfico sintetizadas desde HDFS mediante WebHDFS.
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
from requests.exceptions import ChunkedEncodingError
from urllib.parse import quote, urlparse, urlunparse
import altair as alt
import networkx as nx
import plotly.express as px
import plotly.graph_objects as go

WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://localhost:9870").rstrip("/")
WEBHDFS_DATANODE_URL = os.getenv("WEBHDFS_DATANODE_URL", "").rstrip("/")
HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "/data/gold/management/primary").rstrip("/") or "/"
HDFS_USER = os.getenv("HDFS_USER", "hdfs")
REFRESH_INTERVAL_SECONDS = float(os.getenv("STREAM_REFRESH_SECONDS", "2"))
DEFAULT_WINDOW_MINUTES = int(os.getenv("STREAM_WINDOW_MINUTES", "15"))
HISTORY_MINUTES = int(os.getenv("STREAM_HISTORY_MINUTES", "1440"))
MAX_FILES = int(os.getenv("STREAM_MAX_FILES", "12"))
PROFILE_STATUS_PATH = os.getenv("PROFILE_STATUS_PATH", "").strip()
PROFILE_OVERRIDE_PATH = os.getenv("PROFILE_OVERRIDE_PATH", "").strip()
ML_SERVICE_URL = os.getenv("ML_SERVICE_URL", "http://ml-service:8000").strip().rstrip("/")
RESOURCE_SERVICE_URL = os.getenv("RESOURCE_SERVICE_URL", "http://resource-management:8000").strip().rstrip("/")
PREDICTION_STORE_KEY = "prediction_store"
PREDICTIONS_PER_TIMESTAMP_LIMIT = 12
PREDICTION_HISTORY_MAX_MINUTES = max(HISTORY_MINUTES, DEFAULT_WINDOW_MINUTES)

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

# Recupera y almacena en caché la lista de carreteras soportadas por el servicio de ML.
@st.cache_data(ttl=300, show_spinner=False)
def fetch_ml_roads() -> List[str]:
    if not ML_SERVICE_URL:
        return []
    try:
        response = requests.get(f"{ML_SERVICE_URL}/roads", timeout=5.0)
        response.raise_for_status()
        payload = response.json()
    except (RequestException, ValueError, json.JSONDecodeError):
        return []
    roads = payload.get("roads")
    if isinstance(roads, list):
        return [str(road) for road in roads if isinstance(road, str)]
    return []

# Solicita una predicción puntual al servicio de ML y la cachea brevemente.
@st.cache_data(ttl=5, show_spinner=False)
def fetch_ml_prediction(
    road_id: str,
    hour_of_day: int,
    direction: Optional[str],
    scaling_factor: float,
) -> Optional[Dict[str, object]]:
    if not ML_SERVICE_URL:
        return None
    payload: Dict[str, object] = {
        "road_id": road_id,
        "hour_of_day": int(hour_of_day),
        "scaling_factor": float(max(scaling_factor, 0.0)),
    }
    if direction:
        payload["direction"] = direction
    try:
        response = requests.post(f"{ML_SERVICE_URL}/predict", json=payload, timeout=6.0)
        response.raise_for_status()
        data = response.json()
    except (RequestException, ValueError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None

# Obtiene métricas del servicio de recursos y mantiene un caché corto.
@st.cache_data(ttl=10, show_spinner=False)
def fetch_resource_metrics() -> Optional[Dict[str, object]]:
    if not RESOURCE_SERVICE_URL:
        return None
    try:
        response = requests.get(f"{RESOURCE_SERVICE_URL}/metrics", timeout=5.0)
        response.raise_for_status()
        payload = response.json()
    except (RequestException, ValueError, json.JSONDecodeError):
        try:
            fetch_resource_metrics.clear()
        except Exception:
            pass
        return None
    rows = payload.get("rows")
    if isinstance(rows, list):
        return payload
    return None

# Ejecuta una llamada básica a WebHDFS para la operación indicada.
def _webhdfs_get(
    path: str,
    operation: str,
    timeout: float = 10.0,
    *,
    allow_redirects: bool = True,
) -> requests.Response:
    params = {"op": operation, "user.name": HDFS_USER}
    url = f"{WEBHDFS_URL}/webhdfs/v1{quote(path, safe='/')}"
    return requests.get(url, params=params, timeout=timeout, allow_redirects=allow_redirects)

# Reescribe la URL de redirección de WebHDFS cuando se usa un datanode alternativo.
def _rewrite_redirect_location(location: str) -> str:
    if not location or not WEBHDFS_DATANODE_URL:
        return location
    try:
        override = urlparse(WEBHDFS_DATANODE_URL)
        target = urlparse(location)
    except ValueError:
        return location

    override_netloc = override.netloc or override.path
    if not override_netloc:
        return location

    scheme = override.scheme or target.scheme or "http"
    return urlunparse(
        (
            scheme,
            override_netloc,
            target.path,
            target.params,
            target.query,
            target.fragment,
        )
    )

# Lista los elementos del camino en HDFS gestionando errores de comunicación.
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

# Extrae la cabecera Location de una respuesta de WebHDFS si está disponible.
def _extract_redirect_location(response: requests.Response) -> Optional[str]:
    location = response.headers.get("Location")
    if location:
        return location
    try:
        payload = response.json()
    except (ValueError, json.JSONDecodeError):
        return None
    if isinstance(payload, dict):
        raw_location = payload.get("Location")
        return raw_location if isinstance(raw_location, str) else None
    return None

# Descarga el cuerpo de una redirección manual de WebHDFS y lo devuelve como texto.
def _download_redirect_body(path: str, location: str, follow_url: str) -> str:
    headers: Dict[str, str] = {}
    try:
        parsed = urlparse(location)
    except ValueError:
        parsed = None
    if parsed and parsed.netloc:
        headers["Host"] = parsed.netloc

    try:
        with requests.get(follow_url, timeout=30.0, headers=headers, stream=True) as data_resp:
            data_resp.raise_for_status()
            chunks: List[bytes] = []
            try:
                for chunk in data_resp.iter_content(chunk_size=65536):
                    if chunk:
                        chunks.append(chunk)
            except ChunkedEncodingError as exc:
                partial = getattr(exc, "partial", b"")
                if partial:
                    chunks.append(partial)
                else:
                    raise RuntimeError(f"OPEN redirect failed for {path}: {exc}") from exc
    except RequestException as exc:
        raise RuntimeError(f"OPEN redirect failed for {path}: {exc}") from exc

    body_bytes = b"".join(chunks)
    return body_bytes.decode("utf-8", errors="ignore")

# Lee un archivo JSONL desde HDFS siguiendo redirecciones de WebHDFS si es necesario.
def read_hdfs_file(path: str) -> List[Dict[str, object]]:
    manual_redirect = bool(WEBHDFS_DATANODE_URL)
    try:
        resp = _webhdfs_get(path, "OPEN", timeout=30.0, allow_redirects=not manual_redirect)
        if resp.status_code == 404:
            return []

        if resp.is_redirect and manual_redirect:
            location = _extract_redirect_location(resp)
            if not location:
                raise RuntimeError(f"OPEN redirect missing location for {path}")
            follow_url = _rewrite_redirect_location(location)
            body_text = _download_redirect_body(path, location, follow_url)
        else:
            resp.raise_for_status()
            body_text = resp.text
    except RequestException as exc:
        raise RuntimeError(f"OPEN failed for {path}: {exc}") from exc

    records: List[Dict[str, object]] = []
    for raw_line in body_text.splitlines():
        raw_line = raw_line.strip()
        if not raw_line:
            continue
        try:
            records.append(json.loads(raw_line))
        except json.JSONDecodeError:
            continue
    return records

# Descubre los archivos más recientes en el directorio base configurado en HDFS.
def discover_recent_files() -> List[Dict[str, object]]:
    entries: List[Dict[str, object]] = []

    # Recorre recursivamente directorios en HDFS acumulando archivos recientes.
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

# Carga los registros nuevos desde HDFS evitando reprocesar archivos ya leídos.
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

# Normaliza los registros brutos en un DataFrame con columnas estándar y tipos seguros.
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

    if "total_link_length_km" in frame.columns:
        frame["total_link_length_km"] = pd.to_numeric(
            frame["total_link_length_km"], errors="coerce"
        ).fillna(0.0)

    if "link_length_kilometers" in frame.columns:
        frame["link_length_kilometers"] = pd.to_numeric(
            frame["link_length_kilometers"], errors="coerce"
        ).fillna(0.0)

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
        if "total_link_length_km" in frame.columns:
            totals = frame["total_link_length_km"].replace(0, pd.NA)
            frame["vehicles_per_km"] = (
                frame["total_vehicles"] / totals
            ).fillna(0.0)
        elif "link_length_kilometers" in frame.columns:
            link_lengths = frame["link_length_kilometers"].replace(0, pd.NA)
            frame["vehicles_per_km"] = (
                frame["total_vehicles"] / link_lengths
            ).fillna(0.0)
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
        "pedal_cycles": "pedal_cycle_count",
        "two_wheeled_motor_vehicles": "two_wheeled_motor_vehicle_count",
        "cars_and_taxis": "car_and_taxi_count",
        "buses_and_coaches": "bus_and_coach_count",
        "light_goods_vehicles": "light_goods_vehicle_count",
        "hgv_2_rigid_axles": "heavy_goods_vehicle_2_rigid_axles_count",
        "hgv_3_rigid_axles": "heavy_goods_vehicle_3_rigid_axles_count",
        "hgv_4_plus_rigid_axles": "heavy_goods_vehicle_4_plus_rigid_axles_count",
        "hgv_3_or_4_articulated_axles": "heavy_goods_vehicle_3_or_4_articulated_axles_count",
        "hgv_5_articulated_axles": "heavy_goods_vehicle_5_articulated_axles_count",
        "hgv_6_articulated_axles": "heavy_goods_vehicle_6_articulated_axles_count",
    }

    for source_col, target_col in aggregated_vehicle_map.items():
        if source_col in frame.columns:
            frame[target_col] = pd.to_numeric(frame[source_col], errors="coerce").fillna(0).astype(int)

    if "role" in frame.columns:
        frame["role"] = frame["role"].fillna("primary").astype(str)
    else:
        frame["role"] = "primary"

    frame["vehicles_per_km"] = pd.to_numeric(frame["vehicles_per_km"], errors="coerce").fillna(0.0)

    return frame

# Limita el DataFrame a los últimos minutos indicados tomando el timestamp más reciente como referencia.
def filter_window(df: pd.DataFrame, minutes_back: int) -> pd.DataFrame:
    if df.empty or minutes_back <= 0:
        return df
    latest = df["event_timestamp"].max()
    if pd.isna(latest):
        return df
    cutoff = latest - timedelta(minutes=minutes_back)
    return df[df["event_timestamp"] >= cutoff]

# Recorta el historial conservando únicamente los registros dentro de la ventana indicada.
def prune_history(df: pd.DataFrame, minutes_back: int) -> pd.DataFrame:
    if df.empty or minutes_back <= 0:
        return df
    latest = df["event_timestamp"].max()
    if pd.isna(latest):
        return df
    cutoff = latest - timedelta(minutes=minutes_back)
    return df[df["event_timestamp"] >= cutoff]

# Calcula totales y densidades por región para la vista de métricas agregadas.
def compute_region_metrics(df: pd.DataFrame) -> Tuple[pd.Series, pd.Series]:
    if df.empty:
        empty = pd.Series(dtype=float)
        return empty, empty
    totals = df.groupby("region_name")["total_vehicles"].sum().sort_values(ascending=False)
    if "total_link_length_km" in df.columns:
        length_series = df.groupby("region_name")["total_link_length_km"].max()
        densities = (totals / length_series.replace(0, pd.NA)).fillna(0).sort_values(
            ascending=False
        )
    else:
        densities = df.groupby("region_name")["vehicles_per_km"].mean().sort_values(
            ascending=False
        )
    return totals, densities

# Construye una serie temporal re-muestreada por minuto con el total de vehículos observados.
def compute_time_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    return (
        df.set_index("event_timestamp")["total_vehicles"]
        .resample("1Min")
        .sum()
        .fillna(0)
    )

# Dibuja un gráfico circular con la distribución de valores proporcionada.
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
    st.plotly_chart(fig, width="stretch")

# Genera un gráfico de líneas con marcadores para visualizar series temporales.
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
    st.altair_chart(chart, width="stretch")

# Produce un gráfico de barras ordenado para comparar métricas categóricas.
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
    st.altair_chart(chart, width="stretch")

# Suma las columnas de vehículos indicadas y devuelve los totales etiquetados.
def aggregate_vehicle_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.Series:
    available = [col for col in columns if col in df.columns]
    if not available:
        return pd.Series(dtype=float)
    totals = df[available].apply(pd.to_numeric, errors="coerce").fillna(0).sum()
    totals.index = [VEHICLE_LABELS.get(col, col) for col in totals.index]
    return totals

# Convierte una cantidad de segundos en un texto legible para la interfaz.
def _format_lag_seconds(value: Optional[object]) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/D"
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return "N/D"
    if seconds < 0:
        return "N/D"
    if seconds < 1:
        return "<1s"
    minutes, remaining = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    parts: List[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{remaining}s")
    return " ".join(parts)

# Formatea un tamaño en bytes a una cadena humana con sufijos.
def _format_bytes(value: Optional[object]) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "N/D"
    try:
        size = float(value)
    except (TypeError, ValueError):
        return "N/D"
    if size < 0:
        return "N/D"
    suffixes = ("B", "KB", "MB", "GB", "TB", "PB")
    index = 0
    while size >= 1024.0 and index < len(suffixes) - 1:
        size /= 1024.0
        index += 1
    return f"{size:.1f} {suffixes[index]}"

# Genera un texto con el uso de recursos combinando valores absolutos y porcentaje.
def _format_usage(used: Optional[object], total: Optional[object], percent: Optional[object]) -> str:
    try:
        used_value = float(used)
        total_value = float(total)
    except (TypeError, ValueError):
        return "N/D"
    if total_value <= 0:
        return "N/D"
    try:
        pct_value = float(percent)
    except (TypeError, ValueError):
        pct_value = (used_value / total_value) * 100.0
    if pd.isna(pct_value):
        pct_value = (used_value / total_value) * 100.0
    return f"{_format_bytes(used_value)} / {_format_bytes(total_value)} ({pct_value:.1f}%)"

# Convierte una lista de métricas en una cadena legible separada por barras.
def _stringify_metrics(value: Optional[object]) -> str:
    if not isinstance(value, list):
        return "N/D"
    parts: List[str] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        name = item.get("name")
        if not name:
            continue
        human = item.get("human")
        if isinstance(human, str) and human:
            display = human
        else:
            raw = item.get("value")
            if raw is None or (isinstance(raw, float) and pd.isna(raw)):
                display = "N/D"
            else:
                display = str(raw)
        parts.append(f"{name}: {display}")
    return " | ".join(parts) if parts else "N/D"


# Agrega utilidades para condensar las métricas del servicio de recursos.
def _collect_resource_rows(payload: Dict[str, object]) -> List[Dict[str, object]]:
    collected: Dict[str, Dict[str, object]] = {}

    def _consume(items: Optional[Sequence[object]]) -> None:
        if not isinstance(items, Sequence):
            return
        for item in items:
            if not isinstance(item, dict):
                continue
            component_raw = item.get("component")
            key = str(component_raw) if component_raw else str(id(item))
            collected[key] = item

    sections = payload.get("sections")
    if isinstance(sections, list):
        for section in sections:
            if isinstance(section, dict):
                _consume(section.get("rows"))
    _consume(payload.get("rows"))
    return list(collected.values())


def _metrics_to_lookup(metrics: Optional[object]) -> Dict[str, Dict[str, object]]:
    lookup: Dict[str, Dict[str, object]] = {}
    if not isinstance(metrics, list):
        return lookup
    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        name = metric.get("name")
        if not isinstance(name, str):
            continue
        lookup[name.strip().lower()] = metric
    return lookup


def _find_metric(metrics: Dict[str, Dict[str, object]], names: Sequence[str]) -> Optional[Dict[str, object]]:
    for name in names:
        candidate = metrics.get(name.lower())
        if candidate:
            return candidate
    return None


def _metric_display(metric: Optional[Dict[str, object]]) -> Optional[str]:
    if not isinstance(metric, dict):
        return None
    human = metric.get("human")
    if isinstance(human, str) and human:
        return human
    value = metric.get("value")
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    unit = metric.get("unit")
    if unit == "bytes":
        try:
            return _format_bytes(float(value))
        except (TypeError, ValueError):
            return None
    if unit == "percent":
        try:
            return f"{float(value):.1f}%"
        except (TypeError, ValueError):
            return None
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _prepare_resource_table(rows: Sequence[Dict[str, object]]) -> List[Dict[str, str]]:
    table: List[Dict[str, str]] = []
    for row in rows:
        component = str(row.get("component") or "desconocido")
        status = str(row.get("status") or "unknown")
        category = str(row.get("category") or "")
        if category in {"storage", "system"}:
            continue
        metrics_lookup = _metrics_to_lookup(row.get("metrics"))

        latency = row.get("human_lag")
        if not isinstance(latency, str) or not latency:
            latency = _format_lag_seconds(row.get("lag_seconds"))

        ram_display: Optional[str] = None
        storage_display: Optional[str] = None
        cpu_display: Optional[str] = None

        ram_display = _metric_display(_find_metric(metrics_lookup, ("ram usada", "ram", "memoria")))
        storage_display = _metric_display(_find_metric(metrics_lookup, ("almacenamiento", "tamano", "uso")))
        cpu_display = _metric_display(_find_metric(metrics_lookup, ("cpu", "porcentaje")))

        if storage_display is None:
            storage_display = _metric_display(_find_metric(metrics_lookup, ("uso", "tamano")))
        if cpu_display is None:
            usage_percent = row.get("usage_percent")
            if usage_percent is not None and not (isinstance(usage_percent, float) and pd.isna(usage_percent)):
                try:
                    cpu_display = f"{float(usage_percent):.1f}%"
                except (TypeError, ValueError):
                    cpu_display = None

        table.append(
            {
                "Componente": component,
                "Estado": status,
                "Latencia": latency if isinstance(latency, str) and latency else "N/D",
                "RAM usada": ram_display or "N/D",
                "Almacenamiento": storage_display or "N/D",
                "CPU": cpu_display or "N/D",
            }
        )

    table.sort(key=lambda item: item["Componente"].lower())
    return table

# Combina observaciones recientes y predicciones del servicio ML para proyectar minutos futuros.
def build_prediction_extension(
    observed_df: pd.DataFrame,
    prediction: Optional[Dict[str, object]],
    horizon_minutes: int = 5,
) -> pd.DataFrame:
    if prediction is None or observed_df.empty:
        return pd.DataFrame(columns=["Marca temporal", "Serie", "Valor"])

    expected_per_minute = float(prediction.get("expected_flow_per_minute", 0.0))
    if expected_per_minute < 0:
        expected_per_minute = 0.0

    last_points = observed_df["Valor"].tail(5)
    if last_points.empty:
        base_value = expected_per_minute
        trend = 0.0
    else:
        base_value = float(last_points.iloc[-1])
        if len(last_points) >= 2:
            trend = (last_points.iloc[-1] - last_points.iloc[0]) / max(len(last_points) - 1, 1)
        else:
            trend = 0.0

    last_timestamp = observed_df["Marca temporal"].max()
    if pd.isna(last_timestamp):
        return pd.DataFrame(columns=["Marca temporal", "Serie", "Valor"])
    last_timestamp = pd.to_datetime(last_timestamp)

    timestamps: List[pd.Timestamp] = []
    values: List[float] = []
    for minute in range(1, horizon_minutes + 1):
        future_ts = last_timestamp + timedelta(minutes=minute)
        projected = base_value + trend * minute
        blend_ratio = minute / float(horizon_minutes)
        blended_value = (1 - blend_ratio) * projected + blend_ratio * expected_per_minute
        timestamps.append(future_ts)
        values.append(max(blended_value, 0.0))

    return pd.DataFrame(
        {
            "Marca temporal": timestamps,
            "Serie": ["Predicción"] * len(timestamps),
            "Valor": values,
        }
    )

# Garantiza que exista el almacén de predicciones en el estado de sesión.
def _ensure_prediction_store() -> Dict[str, Dict[str, object]]:
    return st.session_state.setdefault(PREDICTION_STORE_KEY, {})

# Elimina predicciones antiguas y limita la cantidad almacenada por timestamp.
def _prune_prediction_store(store: Dict[str, Dict[str, object]]) -> None:
    if not store:
        return
    cutoff_base = datetime.now(timezone.utc) - timedelta(minutes=PREDICTION_HISTORY_MAX_MINUTES)
    cutoff = pd.Timestamp(cutoff_base)
    for key, entry in list(store.items()):
        ts = pd.to_datetime(key, utc=True, errors="coerce")
        if pd.isna(ts) or ts < cutoff:
            store.pop(key, None)
            continue
        values = entry.get("values") or []
        generated_at = entry.get("generated_at") or []
        if len(values) > PREDICTIONS_PER_TIMESTAMP_LIMIT:
            values = values[-PREDICTIONS_PER_TIMESTAMP_LIMIT:]
            entry["values"] = values
            if generated_at:
                entry["generated_at"] = generated_at[-PREDICTIONS_PER_TIMESTAMP_LIMIT:]
        if values:
            entry["latest"] = float(values[-1])
        else:
            store.pop(key, None)

# Actualiza el historial persistente de predicciones y lo devuelve como DataFrame ordenado.
def update_prediction_history(predicted_df: pd.DataFrame) -> pd.DataFrame:
    store = _ensure_prediction_store()
    if predicted_df is not None and not predicted_df.empty:
        generated_at = datetime.now(timezone.utc).isoformat()
        for _, row in predicted_df.iterrows():
            timestamp_value = row.get("Marca temporal")
            predicted_value = row.get("Valor")
            ts = pd.to_datetime(timestamp_value, utc=True, errors="coerce")
            if pd.isna(ts) or pd.isna(predicted_value):
                continue
            key = ts.isoformat()
            entry = store.setdefault(key, {"values": [], "generated_at": []})
            entry["values"].append(float(predicted_value))
            entry["generated_at"].append(generated_at)
            if len(entry["values"]) > PREDICTIONS_PER_TIMESTAMP_LIMIT:
                entry["values"] = entry["values"][-PREDICTIONS_PER_TIMESTAMP_LIMIT:]
                entry["generated_at"] = entry["generated_at"][-PREDICTIONS_PER_TIMESTAMP_LIMIT:]
            entry["latest"] = float(entry["values"][-1])
            entry["latest_generated_at"] = generated_at
    _prune_prediction_store(store)

    records: List[Dict[str, object]] = []
    for key, entry in store.items():
        latest_value = entry.get("latest")
        if latest_value is None:
            continue
        ts = pd.to_datetime(key, utc=True, errors="coerce")
        if pd.isna(ts):
            continue
        records.append(
            {
                "Marca temporal": ts,
                "Serie": "Predicción",
                "Valor": float(latest_value),
            }
        )

    history_df = pd.DataFrame(records)
    if history_df.empty:
        return pd.DataFrame(columns=["Marca temporal", "Serie", "Valor"])
    return history_df.sort_values("Marca temporal")

# Calcula el peor error por timestamp comparando observaciones contra predicciones previas.
def compute_worst_prediction_points(observed_df: pd.DataFrame) -> pd.DataFrame:
    store = st.session_state.get(PREDICTION_STORE_KEY)
    if not store or observed_df.empty:
        return pd.DataFrame(columns=["Marca temporal", "Peor predicción", "Observado", "Error absoluto"])

    records: List[Dict[str, object]] = []
    for _, row in observed_df.iterrows():
        timestamp_value = row.get("Marca temporal")
        observed_value = row.get("Valor")
        ts = pd.to_datetime(timestamp_value, utc=True, errors="coerce")
        if pd.isna(ts) or pd.isna(observed_value):
            continue
        key = ts.isoformat()
        entry = store.get(key)
        if not entry:
            continue
        values = entry.get("values") or []
        if not values:
            continue
        abs_errors = [abs(float(observed_value) - float(pred)) for pred in values]
        worst_idx = max(range(len(abs_errors)), key=lambda idx: abs_errors[idx])
        worst_value = float(values[worst_idx])
        worst_error = float(abs_errors[worst_idx])
        records.append(
            {
                "Marca temporal": ts,
                "Peor predicción": worst_value,
                "Observado": float(observed_value),
                "Error absoluto": worst_error,
                "Valor": worst_value,
            }
        )

    worst_df = pd.DataFrame(records)
    if worst_df.empty:
        return worst_df
    return worst_df.sort_values("Marca temporal")

# Deriva un color de gradiente rojo-verde a partir de un valor normalizado.
def _color_from_ratio(ratio: float) -> str:
    value = max(0.0, min(1.0, float(ratio)))
    green = (39, 174, 96)
    red = (192, 57, 43)
    r = int(green[0] + (red[0] - green[0]) * value)
    g = int(green[1] + (red[1] - green[1]) * value)
    b = int(green[2] + (red[2] - green[2]) * value)
    return f"rgb({r},{g},{b})"

# Calcula y memoriza la disposición de nodos para el grafo de una autoridad específica.
def _ensure_authority_layout(cache_key: str, graph: nx.Graph) -> Dict[str, Tuple[float, float]]:
    if "graph_layouts" not in st.session_state:
        st.session_state.graph_layouts = {}
    layout_entry = st.session_state.graph_layouts.get(cache_key)
    current_nodes = set(graph.nodes())
    if layout_entry is not None:
        cached_nodes = set(layout_entry.keys())
        if cached_nodes == current_nodes:
            return layout_entry

    raw_layout = nx.spring_layout(graph, seed=42, weight="weight")
    layout = {node: (float(coords[0]), float(coords[1])) for node, coords in raw_layout.items()}
    st.session_state.graph_layouts[cache_key] = layout
    return layout

# Representa gráficamente la red vial de una autoridad con colores según carga vehicular.
def render_authority_graph(
    region_name: str,
    authority_name: str,
    road_history_df: pd.DataFrame,
) -> None:
    if road_history_df.empty:
        st.info("Sin datos de calles para la autoridad seleccionada.")
        return

    if "road_id" not in road_history_df.columns:
        st.info("La capa gold aún no expone identificadores de carretera para este conjunto.")
        return

    required_nodes = {"start_junction_name", "end_junction_name"}
    if not required_nodes.issubset(road_history_df.columns):
        st.info("Aún no hay información de intersecciones para trazar el grafo.")
        return

    latest = (
        road_history_df.sort_values("event_timestamp")
        .groupby("road_id", dropna=False)
        .tail(1)
        .copy()
    )
    latest = latest.dropna(subset=["road_name"])
    if latest.empty:
        st.info("Aún no hay calles con actividad reciente en esta autoridad.")
        return

    latest["start_junction_name"] = latest["start_junction_name"].fillna("").replace("", pd.NA)
    latest["end_junction_name"] = latest["end_junction_name"].fillna("").replace("", pd.NA)
    latest["start_junction_name"] = latest["start_junction_name"].fillna(
        latest["road_name"].astype(str) + " (inicio)"
    )
    latest["end_junction_name"] = latest["end_junction_name"].fillna(
        latest["road_name"].astype(str) + " (fin)"
    )
    latest.loc[
        latest["start_junction_name"] == latest["end_junction_name"],
        "end_junction_name",
    ] = latest["end_junction_name"].astype(str) + " (fin)"

    graph = nx.Graph()
    length_values: Dict[Tuple[str, str], float] = {}
    for _, row in latest.iterrows():
        start_node = str(row["start_junction_name"])
        end_node = str(row["end_junction_name"])
        length_km = float(row.get("link_length_kilometers") or row.get("total_link_length_km") or 0.0)
        if length_km <= 0.0:
            length_km = 0.1
        weight = 1.0 / max(length_km, 0.1)
        graph.add_node(start_node)
        graph.add_node(end_node)
        graph.add_edge(start_node, end_node, road_id=row["road_id"], weight=weight)
        length_values[(start_node, end_node)] = length_km
        length_values[(end_node, start_node)] = length_km

    if graph.number_of_edges() == 0:
        st.info("No hay calles conectadas suficientes para dibujar el grafo.")
        return

    cache_key = f"{region_name}::{authority_name}"
    layout = _ensure_authority_layout(cache_key, graph)

    totals = latest["total_vehicles"].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    min_total = float(totals.min()) if not totals.empty else 0.0
    max_total = float(totals.max()) if not totals.empty else 0.0

    fig = go.Figure()

    mid_x_coords: List[float] = []
    mid_y_coords: List[float] = []
    mid_hover_texts: List[str] = []
    mid_colors: List[str] = []

    for _, row in latest.iterrows():
        start_node = str(row["start_junction_name"])
        end_node = str(row["end_junction_name"])
        start_pos = layout.get(start_node)
        end_pos = layout.get(end_node)
        if not start_pos or not end_pos:
            continue
        total = float(row.get("total_vehicles", 0.0))
        if max_total - min_total <= 0:
            ratio = 0.5
        else:
            ratio = (total - min_total) / (max_total - min_total)
        color = _color_from_ratio(ratio)
        width = 2.0 + (4.0 * ratio)
        vehicle_details: List[str] = []
        for column in VEHICLE_BREAKDOWN_COLUMNS:
            if column in latest.columns:
                value = row.get(column)
                if pd.notna(value) and float(value) > 0:
                    vehicle_details.append(
                        f"{VEHICLE_LABELS.get(column, column)}: {int(float(value)):,}"
                    )
        edge_length = length_values.get((start_node, end_node), 0.0)
        hover_lines = [
            f"Calle: {row.get('road_name', 'Desconocida')}",
            f"Longitud: {edge_length:.2f} km",
            f"Vehículos totales: {int(total):,}",
        ]
        if vehicle_details:
            hover_lines.append("Detalle: " + ", ".join(vehicle_details))
        hover_text = "<br>".join(hover_lines)

        fig.add_trace(
            go.Scatter(
                x=[start_pos[0], end_pos[0]],
                y=[start_pos[1], end_pos[1]],
                mode="lines",
                line=dict(color=color, width=width),
                hoverinfo="skip",
                name=str(row.get("road_name", "")),
                showlegend=False,
            )
        )

        mid_x_coords.append((start_pos[0] + end_pos[0]) / 2.0)
        mid_y_coords.append((start_pos[1] + end_pos[1]) / 2.0)
        mid_hover_texts.append(hover_text)
        mid_colors.append(color)

    node_x: List[float] = []
    node_y: List[float] = []
    node_text: List[str] = []
    for node, (x_coord, y_coord) in layout.items():
        node_x.append(x_coord)
        node_y.append(y_coord)
        node_text.append(node)

    fig.add_trace(
        go.Scatter(
            x=node_x,
            y=node_y,
            mode="markers+text",
            marker=dict(size=14, color="#1f77b4"),
            text=node_text,
            textposition="top center",
            hoverinfo="text",
            showlegend=False,
        )
    )

    if mid_x_coords:
        fig.add_trace(
            go.Scatter(
                x=mid_x_coords,
                y=mid_y_coords,
                mode="markers",
                marker=dict(size=18, color=mid_colors, opacity=0.0),
                hoverinfo="text",
                hovertext=mid_hover_texts,
                showlegend=False,
            )
        )

    fig.update_layout(
        title=f"Grafo de carreteras en {authority_name}",
        margin=dict(l=10, r=10, t=40, b=10),
        plot_bgcolor="white",
        paper_bgcolor="white",
        xaxis=dict(showgrid=False, zeroline=False, visible=False),
        yaxis=dict(showgrid=False, zeroline=False, visible=False),
        dragmode=False,
    )

    st.plotly_chart(fig, use_container_width=True)

# Recupera el estado del perfil de generación a partir de archivos locales configurados.
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

# Muestra una franja informativa con el origen del perfil de generación activo.
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

# Orquesta el flujo principal del dashboard actualizando datos y renderizando paneles.
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
        refresh_notice = st.empty()
        refresh_notice.info("Actualizando datos...")
        time.sleep(refresh_seconds)
        refresh_notice.empty()
        st.rerun()

    if "role" in live_df.columns:
        authority_df = live_df[live_df["role"] == "authority"].copy()
        primary_df = live_df[live_df["role"] == "primary"].copy()
        road_df = live_df[live_df["role"] == "road"].copy()
    else:
        authority_df = pd.DataFrame(columns=live_df.columns)
        primary_df = live_df.copy()
        road_df = pd.DataFrame(columns=live_df.columns)

    if primary_df.empty:
        primary_df = live_df.copy()

    effective_df = primary_df

    if "role" in st.session_state.records.columns:
        history_primary_df = st.session_state.records[
            st.session_state.records["role"] != "authority"
        ].copy()
    else:
        history_primary_df = st.session_state.records.copy()
    if history_primary_df.empty:
        history_primary_df = effective_df.copy()

    metrics_row = st.columns(3)
    metrics_row[0].metric("Registros (ventana)", f"{len(effective_df):,}")
    metrics_row[1].metric("Vehículos (ventana)", f"{effective_df['total_vehicles'].sum():,}")
    density_metric = effective_df["vehicles_per_km"].mean()
    if "total_link_length_km" in effective_df.columns:
        length_sum = (
            effective_df.groupby("region_name")["total_link_length_km"].max()
            .fillna(0.0)
            .sum()
        )
        if length_sum > 0:
            total_vehicles_window = pd.to_numeric(
                effective_df["total_vehicles"], errors="coerce"
            ).fillna(0.0).sum()
            density_metric = total_vehicles_window / length_sum
    density_value = float(density_metric) if pd.notna(density_metric) else 0.0
    metrics_row[2].metric("Promedio vehículos/km", f"{density_value:.2f}")

    region_totals, region_density = compute_region_metrics(effective_df)

    observed_series = compute_time_series(history_primary_df)
    observed_df = (
        observed_series.reset_index()
        .rename(columns={"event_timestamp": "Marca temporal", 0: "Vehículos nuevos"})
    )
    if "Vehículos nuevos" not in observed_df.columns:
        observed_df = observed_df.rename(columns={observed_df.columns[-1]: "Vehículos nuevos"})
    observed_df["Marca temporal"] = pd.to_datetime(
        observed_df["Marca temporal"], utc=True, errors="coerce"
    )
    observed_df["Valor"] = pd.to_numeric(observed_df["Vehículos nuevos"], errors="coerce")
    observed_df = (
        observed_df.dropna(subset=["Marca temporal", "Valor"])
        .sort_values("Marca temporal")
    )
    if HISTORY_MINUTES > 0:
        observed_df = observed_df.tail(int(HISTORY_MINUTES))
    observed_df["Serie"] = "Observado"

    prediction_payload: Optional[Dict[str, object]] = None
    predicted_series_df = pd.DataFrame(columns=["Marca temporal", "Serie", "Valor"])
    selected_road: Optional[str] = None
    direction_hint: Optional[str] = None

    if ML_SERVICE_URL:
        road_candidates = fetch_ml_roads()
        if not road_df.empty and "road_id" in road_df.columns:
            recent_roads = (
                road_df.dropna(subset=["road_id"])
                .groupby("road_id")["total_vehicles"]
                .sum()
                .sort_values(ascending=False)
            )
            if not recent_roads.empty:
                selected_road = str(recent_roads.index[0])
                if "direction" in road_df.columns:
                    recent_direction = (
                        road_df[road_df["road_id"] == selected_road]
                        .dropna(subset=["direction"])
                        .sort_values("event_timestamp")
                    )
                    if not recent_direction.empty:
                        direction_hint = str(recent_direction.iloc[-1]["direction"])
        if not selected_road and road_candidates:
            selected_road = road_candidates[0]
        if selected_road and road_candidates and selected_road not in road_candidates:
            selected_road = road_candidates[0]

        if selected_road:
            last_timestamp = observed_df["Marca temporal"].max()
            hour_value = datetime.now(timezone.utc).hour
            if pd.notna(last_timestamp):
                try:
                    hour_value = int(pd.to_datetime(last_timestamp, utc=True).hour)
                except (TypeError, ValueError):
                    hour_value = datetime.now(timezone.utc).hour

            scaling_factor = 1.0
            recent_values = observed_df["Valor"].tail(12)
            if not recent_values.empty:
                rolling_avg = float(recent_values.mean())
                last_value = float(recent_values.iloc[-1])
                if rolling_avg > 0:
                    scaling_factor = max(0.25, min(2.5, last_value / rolling_avg))

            prediction_payload = fetch_ml_prediction(
                selected_road,
                int(hour_value),
                direction_hint,
                scaling_factor,
            )
            if prediction_payload:
                predicted_series_df = build_prediction_extension(observed_df, prediction_payload)

    persisted_prediction_df = update_prediction_history(predicted_series_df)
    if not persisted_prediction_df.empty:
        persisted_prediction_df = persisted_prediction_df.drop_duplicates(subset=["Marca temporal"], keep="last")
        history_limit = HISTORY_MINUTES if HISTORY_MINUTES > 0 else 240
        persisted_prediction_df = persisted_prediction_df.tail(history_limit)
    worst_predictions_df = compute_worst_prediction_points(observed_df)
    if not worst_predictions_df.empty:
        worst_predictions_df = worst_predictions_df.drop_duplicates(subset=["Marca temporal"], keep="last")
        history_limit = HISTORY_MINUTES if HISTORY_MINUTES > 0 else 240
        worst_predictions_df = worst_predictions_df.tail(history_limit)

    time_cols = st.columns((3, 2))

    with time_cols[0]:
        chart_frames: List[pd.DataFrame] = []
        if not observed_df.empty:
            chart_frames.append(observed_df[["Marca temporal", "Serie", "Valor"]])
        if not persisted_prediction_df.empty:
            chart_frames.append(persisted_prediction_df)

        if chart_frames:
            chart_df = pd.concat(chart_frames, ignore_index=True)
            chart_df = chart_df.dropna(subset=["Marca temporal", "Valor"])
            chart_df["Valor"] = pd.to_numeric(chart_df["Valor"], errors="coerce")
            chart_df = chart_df.dropna(subset=["Valor"])
            if not chart_df.empty:
                series_values = chart_df["Serie"].unique().tolist()
                ordered_series = [value for value in ("Observado", "Predicción") if value in series_values]
                color_map = {"Observado": "#1f77b4", "Predicción": "#ff7f0e"}
                range_colors = [color_map[value] for value in ordered_series]
                color_encoding = alt.Color(
                    "Serie:N",
                    title="Serie",
                    scale=alt.Scale(domain=ordered_series, range=range_colors),
                )
                chart = (
                    alt.Chart(chart_df)
                    .mark_line(point=True)
                    .encode(
                        x=alt.X("Marca temporal:T", title="Marca temporal"),
                        y=alt.Y("Valor:Q", title="Vehículos nuevos"),
                        color=color_encoding,
                        strokeDash=alt.condition(
                            alt.datum.Serie == "Predicción",
                            alt.value([6, 3]),
                            alt.value([1, 0]),
                        ),
                        tooltip=[
                            alt.Tooltip("Marca temporal:T", title="Marca temporal"),
                            alt.Tooltip("Valor:Q", title="Vehículos nuevos", format=".2f"),
                            alt.Tooltip("Serie:N", title="Serie"),
                        ],
                    )
                    .properties(height=320, title="Vehículos por actualización")
                )
                if not worst_predictions_df.empty:
                    worst_layer = (
                        alt.Chart(worst_predictions_df)
                        .mark_point(color="#d62728", size=120, filled=True)
                        .encode(
                            x=alt.X("Marca temporal:T", title="Marca temporal"),
                            y=alt.Y("Valor:Q", title="Vehículos nuevos"),
                            tooltip=[
                                alt.Tooltip("Marca temporal:T", title="Marca temporal"),
                                alt.Tooltip("Peor predicción:Q", title="Predicción (peor)", format=".2f"),
                                alt.Tooltip("Observado:Q", title="Observado", format=".2f"),
                                alt.Tooltip("Error absoluto:Q", title="Error absoluto", format=".2f"),
                            ],
                        )
                    )
                    chart = chart + worst_layer
                st.altair_chart(chart, use_container_width=True)

    with time_cols[1]:
        st.markdown("#### Densidad promedio por región")
        density_chart_df = pd.DataFrame()
        if not effective_df.empty and {
            "event_timestamp",
            "region_name",
            "vehicles_per_km",
        }.issubset(effective_df.columns):
            region_density_ts = effective_df[
                ["event_timestamp", "region_name", "vehicles_per_km"]
            ].dropna(subset=["event_timestamp", "region_name"])
            if not region_density_ts.empty:
                region_density_ts["Marca temporal"] = pd.to_datetime(
                    region_density_ts["event_timestamp"], utc=True, errors="coerce"
                ).dt.floor("T")
                density_chart_df = (
                    region_density_ts.groupby(["Marca temporal", "region_name"], dropna=False)[
                        "vehicles_per_km"
                    ]
                    .mean()
                    .reset_index()
                    .rename(columns={"region_name": "Región", "vehicles_per_km": "Vehículos por km"})
                    .sort_values("Marca temporal")
                    .tail(240)
                )
        if density_chart_df.empty:
            st.info("No hay datos disponibles para densidad por región en esta ventana.")
        else:
            density_chart = (
                alt.Chart(density_chart_df)
                .mark_line(point=True)
                .encode(
                    x=alt.X("Marca temporal:T", title="Marca temporal"),
                    y=alt.Y("Vehículos por km:Q", title="Vehículos por km"),
                    color=alt.Color("Región:N", title="Región"),
                    tooltip=[
                        alt.Tooltip("Marca temporal:T", title="Marca temporal"),
                        alt.Tooltip("Región:N", title="Región"),
                        alt.Tooltip("Vehículos por km:Q", title="Vehículos por km", format=".2f"),
                    ],
                )
                .properties(height=320)
            )
            st.altair_chart(density_chart, use_container_width=True)

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

            road_history_df = (
                st.session_state.records[
                    (st.session_state.records["role"] == "road")
                    & (st.session_state.records["region_name"] == selected_region)
                    & (
                        st.session_state.records["local_authority_name"]
                        == selected_authority
                    )
                ]
                if {
                    "region_name",
                    "local_authority_name",
                    "road_name",
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
                render_authority_graph(
                    selected_region,
                    selected_authority,
                    road_history_df,
                )

            has_road_details = not road_history_df.empty and "road_name" in road_history_df.columns
            road_counts = (
                road_history_df.dropna(subset=["road_name"])
                .groupby("road_name")["total_vehicles"]
                .sum()
                .sort_values(ascending=False)
                .head(12)
                if has_road_details
                else pd.Series(dtype=float)
            )

            road_density = (
                road_history_df.dropna(subset=["road_name"])
                .groupby("road_name")["vehicles_per_km"]
                .mean()
                .sort_values(ascending=False)
                .head(12)
                if has_road_details and "vehicles_per_km" in road_history_df.columns
                else pd.Series(dtype=float)
            )

            with authority_cols[1]:
                if road_counts.empty or road_counts.sum() == 0:
                    st.info("No hay información histórica de calles para la autoridad seleccionada.")
                else:
                    totals_df = road_counts.reset_index()
                    totals_df.columns = ["Calle", "Vehículos totales"]
                    render_bar_chart(
                        totals_df,
                        "Calle",
                        "Vehículos totales",
                        f"Calles con más tráfico en {selected_authority}",
                        "Calle",
                        "Vehículos totales",
                    )
                    if not road_density.empty:
                        density_df = road_density.reset_index()
                        density_df.columns = ["Calle", "Vehículos por kilómetro"]
                        render_bar_chart(
                            density_df,
                            "Calle",
                            "Vehículos por kilómetro",
                            f"Densidad por kilómetro en {selected_authority}",
                            "Calle",
                            "Vehículos por kilómetro",
                        )

    st.subheader("Registros más recientes")
    table_columns = [
        "event_timestamp",
        "region_name",
        "local_authority_name",
        "road_name",
        "road_type",
        "total_link_length_km",
        "link_length_kilometers",
        "total_vehicles",
        "heavy_vehicles",
        "vehicles_per_km",
    ]
    table_columns = [col for col in table_columns if col in live_df.columns]
    st.dataframe(live_df[table_columns].tail(100), width="stretch")

    st.subheader("Estado del clúster")
    cluster_container = st.container()
    if not RESOURCE_SERVICE_URL:
        cluster_container.info("Configura la variable de entorno RESOURCE_SERVICE_URL para habilitar el monitoreo de recursos.")
    else:
        resource_payload = fetch_resource_metrics()
        if not resource_payload:
            with cluster_container:
                st.info("No se pudo recuperar el estado de recursos del servicio correspondiente.")
        else:
            resource_rows = _collect_resource_rows(resource_payload)
            if not resource_rows:
                with cluster_container:
                    st.info("El servicio de recursos no reportó filas en esta captura.")
            else:
                table_rows = _prepare_resource_table(resource_rows)
                if not table_rows:
                    with cluster_container:
                        st.info("El servicio de recursos no reportó métricas compatibles.")
                else:
                    resource_df = pd.DataFrame(table_rows)
                    with cluster_container:
                        st.dataframe(resource_df, width="stretch")
            collected_at = resource_payload.get("collected_at")
            if collected_at:
                cluster_container.caption(f"Captura de recursos: {collected_at}")

    st.caption(
        f"Autoactualización fija cada {refresh_seconds:.1f} segundos. Datos conservados ~{HISTORY_MINUTES} minutos."
    )
    st.caption(
        f"Última actualización: {refresh_timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')} | Archivos procesados: {len(st.session_state.processed_files)} | Registros en caché: {len(st.session_state.records)}"
    )
    refresh_notice = st.empty()
    refresh_notice.info("Actualizando datos...")
    time.sleep(refresh_seconds)
    refresh_notice.empty()
    st.rerun()


if __name__ == "__main__":
    main()
