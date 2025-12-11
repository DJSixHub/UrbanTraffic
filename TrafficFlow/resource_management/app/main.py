from __future__ import annotations

import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import docker
from docker.errors import DockerException

from fastapi import FastAPI

DATA_ROOT = Path(os.getenv("DATA_ROOT", "/data"))
PIPELINE_STATUS_ROOT = Path(os.getenv("PIPELINE_STATUS_ROOT", DATA_ROOT / "pipeline_status"))
MANAGEMENT_STATUS_ROOT = Path(os.getenv("MANAGEMENT_STATUS_ROOT", DATA_ROOT / "management_status"))
DOCKER_BASE_URL = os.getenv("DOCKER_HOST", "unix:///var/run/docker.sock")
DOCKER_TIMEOUT = float(os.getenv("DOCKER_TIMEOUT", "2.5"))
CONTAINER_STATS_MAX_WORKERS = max(int(os.getenv("CONTAINER_STATS_MAX_WORKERS", "48")), 1)

app = FastAPI(title="TrafficFlow Resource Service", version="0.1.0")

CATEGORY_TITLES: Dict[str, str] = {
    "pipeline": "Pipelines",
    "management_status": "Gestion",
    "docker": "Contenedores",
}

SECTION_ORDER: Dict[str, int] = {
    "pipeline": 0,
    "management_status": 1,
    "docker": 2,
}


# Lee un archivo JSON y maneja errores de acceso o parseo.
def _load_json(path: Path) -> Optional[Dict[str, object]]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None


# Convierte un número de bytes en una cadena legible.
def _format_bytes(value: float) -> str:
    size = float(max(value, 0.0))
    suffixes = ("B", "KB", "MB", "GB", "TB", "PB")
    for suffix in suffixes:
        if size < 1024.0 or suffix == suffixes[-1]:
            return f"{size:.1f}{suffix}"
        size /= 1024.0
    return f"{size:.1f}B"


# Traduce segundos a un formato humano amigable.
def _humanize_seconds(value: Optional[object]) -> Optional[str]:
    if value is None:
        return None
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(seconds) or seconds < 0:
        return None
    if seconds < 1:
        return "<1s"
    seconds_int = int(seconds)
    minutes, remaining = divmod(seconds_int, 60)
    hours, minutes = divmod(minutes, 60)
    days, hours = divmod(hours, 24)
    parts: List[str] = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{remaining}s")
    return " ".join(parts)


# Construye un diccionario estándar para describir una métrica.
def _make_metric(name: str, value: Optional[object], *, human: Optional[str] = None, unit: Optional[str] = None) -> Dict[str, object]:
    metric: Dict[str, object] = {"name": name, "value": value}
    if human is not None:
        metric["human"] = human
    if unit is not None:
        metric["unit"] = unit
    return metric


# Decide la representación textual que debe mostrarse para la métrica.
def _format_metric_display(metric: Dict[str, object]) -> str:
    human = metric.get("human")
    if isinstance(human, str) and human:
        return human
    value = metric.get("value")
    if value is None:
        return "N/D"
    if isinstance(value, float):
        if math.isnan(value):
            return "N/D"
        if value.is_integer():
            return str(int(value))
        return f"{value:.2f}"
    return str(value)


# Crea un resumen compacto a partir de una lista de métricas.
def _render_metric_summary(metrics: List[Dict[str, object]]) -> str:
    parts: List[str] = []
    for metric in metrics:
        name = metric.get("name")
        if not name:
            continue
        display = _format_metric_display(metric)
        parts.append(f"{name}: {display}")
    return " | ".join(parts)


# Recolecta métricas sobre los pipelines basándose en sus archivos de estado.
def _summarise_pipeline_status(now: float) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    if not PIPELINE_STATUS_ROOT.exists():
        return rows
    for status_file in sorted(PIPELINE_STATUS_ROOT.glob("*.json")):
        payload = _load_json(status_file)
        if not isinstance(payload, dict):
            continue
        last_flush = float(payload.get("last_flush_time") or 0.0)
        lag_seconds = float(now - last_flush) if last_flush > 0 else None
        status = "ok"
        if lag_seconds is None:
            status = "unknown"
        elif lag_seconds > 180:
            status = "delayed"
        component = status_file.stem
        human_lag = _humanize_seconds(lag_seconds)
        metrics = [
            _make_metric("Lag", lag_seconds, human=human_lag, unit="seconds"),
            _make_metric("Eventos totales", payload.get("total_events")),
            _make_metric("Registros gold", payload.get("gold_records")),
        ]
        summary = _render_metric_summary(metrics)
        updated_at = (
            datetime.fromtimestamp(last_flush, tz=timezone.utc).isoformat()
            if last_flush > 0
            else None
        )
        rows.append(
            {
                "component": component,
                "category": "pipeline",
                "status": status,
                "lag_seconds": lag_seconds,
                "total_events": payload.get("total_events"),
                "gold_records": payload.get("gold_records"),
                "metrics": metrics,
                "summary": summary,
                "human_lag": human_lag,
                "updated_at": updated_at,
                "notes": f"Ultimo flush hace {human_lag}" if human_lag else "Sin timestamp",
            }
        )
    return rows


# Resume la frescura y tamaño de los archivos de estado de gestión.
def _summarise_management_status(now: float) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    if not MANAGEMENT_STATUS_ROOT.exists():
        return rows
    for status_file in sorted(MANAGEMENT_STATUS_ROOT.glob("*.json")):
        try:
            stats = status_file.stat()
        except OSError:
            continue
        lag_seconds = float(now - stats.st_mtime)
        human_lag = _humanize_seconds(lag_seconds)
        metrics = [
            _make_metric("Tamano", stats.st_size, human=_format_bytes(float(stats.st_size)), unit="bytes"),
            _make_metric("Actualizado", lag_seconds, human=human_lag, unit="seconds"),
        ]
        rows.append(
            {
                "component": f"management_status:{status_file.stem}",
                "category": "management_status",
                "status": "ok" if lag_seconds < 120 else "stale",
                "lag_seconds": lag_seconds,
                "record_count": None,
                "size_bytes": stats.st_size,
                "metrics": metrics,
                "summary": _render_metric_summary(metrics),
                "human_lag": human_lag,
                "updated_at": datetime.fromtimestamp(stats.st_mtime, tz=timezone.utc).isoformat(),
                "notes": f"Actualizado hace {human_lag}" if human_lag else "Sin informacion reciente",
            }
        )
    return rows


# Obtiene métricas del sistema operativo sobre memoria y disco.
def _docker_client() -> Optional[docker.DockerClient]:
    if not DOCKER_BASE_URL:
        return None
    try:
        return docker.DockerClient(base_url=DOCKER_BASE_URL, timeout=DOCKER_TIMEOUT)
    except DockerException:
        return None


def _parse_docker_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    candidate = value.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        return None


def _safe_cpu_percent(stats: Dict[str, object]) -> Optional[float]:
    cpu_stats = stats.get("cpu_stats") if isinstance(stats.get("cpu_stats"), dict) else {}
    cpu_usage = cpu_stats.get("cpu_usage") if isinstance(cpu_stats.get("cpu_usage"), dict) else {}
    cpu_total = cpu_usage.get("total_usage")
    system_total = cpu_stats.get("system_cpu_usage")
    if cpu_total is None or system_total is None:
        return None

    precpu_stats = stats.get("precpu_stats") if isinstance(stats.get("precpu_stats"), dict) else {}
    precpu_usage = (
        precpu_stats.get("cpu_usage") if isinstance(precpu_stats.get("cpu_usage"), dict) else {}
    )
    pre_total = precpu_usage.get("total_usage")
    pre_system_total = precpu_stats.get("system_cpu_usage")

    online_cpus = cpu_stats.get("online_cpus")
    if not online_cpus and isinstance(cpu_usage.get("percpu_usage"), Sequence):
        cpu_count = len(cpu_usage["percpu_usage"])
        online_cpus = cpu_count if cpu_count > 0 else None
    if not online_cpus:
        online_cpus = 1

    if (
        pre_total is not None
        and pre_system_total is not None
        and float(system_total) > float(pre_system_total)
        and float(cpu_total) >= float(pre_total)
    ):
        cpu_delta = float(cpu_total) - float(pre_total)
        system_delta = float(system_total) - float(pre_system_total)
        if cpu_delta > 0 and system_delta > 0:
            return (cpu_delta / system_delta) * float(online_cpus) * 100.0

    system_total_float = float(system_total)
    if system_total_float <= 0:
        return None
    return (float(cpu_total) / system_total_float) * float(online_cpus) * 100.0


def _summarise_container_metrics(now: float) -> List[Dict[str, object]]:
    client = _docker_client()
    if client is None:
        return []
    rows: List[Dict[str, object]] = []
    size_lookup: Dict[str, Dict[str, Optional[float]]] = {}
    try:
        usage_snapshot = client.api.df()
    except DockerException:
        usage_snapshot = {}
    containers_snapshot = usage_snapshot.get("Containers") if isinstance(usage_snapshot, dict) else None
    if isinstance(containers_snapshot, list):
        for entry in containers_snapshot:
            if not isinstance(entry, dict):
                continue
            container_id = entry.get("Id")
            if not isinstance(container_id, str):
                continue
            rw_size = entry.get("SizeRw")
            root_size = entry.get("SizeRootFs")
            size_lookup[container_id] = {
                "rw": float(rw_size) if isinstance(rw_size, (int, float)) else None,
                "root": float(root_size) if isinstance(root_size, (int, float)) else None,
            }

    def _collect_stats(container: docker.models.containers.Container) -> Dict[str, object]:
        stats: Dict[str, object] = {}
        attrs: Dict[str, object] = {}
        try:
            stats = container.stats(stream=False)
        except DockerException:
            stats = {}
        try:
            container.reload()
        except DockerException:
            pass
        try:
            attrs_raw = container.attrs
            attrs = attrs_raw if isinstance(attrs_raw, dict) else {}
        except DockerException:
            attrs = {}
        return {
            "container": container,
            "stats": stats,
            "attrs": attrs,
        }

    try:
        try:
            containers = client.containers.list(all=True)
        except DockerException:
            return []

        details: List[Dict[str, object]] = []
        if containers:
            worker_count = min(CONTAINER_STATS_MAX_WORKERS, max(len(containers), 1))
            with ThreadPoolExecutor(max_workers=worker_count) as pool:
                details = list(pool.map(_collect_stats, containers))

        for entry in details:
            container = entry.get("container")
            if not isinstance(container, docker.models.containers.Container):
                continue
            stats = entry.get("stats") if isinstance(entry.get("stats"), dict) else {}
            attrs = entry.get("attrs") if isinstance(entry.get("attrs"), dict) else {}

            name = container.name or container.short_id
            status = container.status or "unknown"

            mem_stats = stats.get("memory_stats") if isinstance(stats.get("memory_stats"), dict) else {}
            raw_mem = float(mem_stats.get("usage", 0.0))
            cache_value = 0.0
            if isinstance(mem_stats.get("stats"), dict):
                cache_value = float(mem_stats["stats"].get("cache", 0.0))
            mem_usage = max(raw_mem - cache_value, 0.0)
            mem_limit = float(mem_stats.get("limit", 0.0))
            cpu_percent = _safe_cpu_percent(stats)

            blkio_stats = stats.get("blkio_stats") if isinstance(stats.get("blkio_stats"), dict) else {}
            storage_bytes = None
            io_recursive = blkio_stats.get("io_service_bytes_recursive")
            if isinstance(io_recursive, list) and io_recursive:
                storage_bytes = sum(float(item.get("value", 0.0)) for item in io_recursive if isinstance(item, dict))

            size_rw = attrs.get("SizeRw") if isinstance(attrs.get("SizeRw"), (int, float)) else None
            state = attrs.get("State") if isinstance(attrs.get("State"), dict) else {}
            started_at = state.get("StartedAt")
            if size_rw is None:
                snapshot_sizes = size_lookup.get(container.id)
                if snapshot_sizes:
                    size_rw = snapshot_sizes.get("rw")
                    if storage_bytes is None:
                        storage_bytes = snapshot_sizes.get("rw")
                    if storage_bytes is None:
                        storage_bytes = snapshot_sizes.get("root")
            latency_seconds = None
            started_dt = _parse_docker_timestamp(started_at)
            if started_dt is not None:
                latency_seconds = max(now - started_dt.timestamp(), 0.0)

            metrics = [
                _make_metric("RAM usada", mem_usage, human=_format_bytes(mem_usage), unit="bytes"),
            ]
            if mem_limit > 0:
                metrics.append(
                    _make_metric(
                        "RAM limite",
                        mem_limit,
                        human=_format_bytes(mem_limit),
                        unit="bytes",
                    )
                )
                metrics.append(
                    _make_metric(
                        "RAM %",
                        (mem_usage / mem_limit) * 100.0 if mem_limit > 0 else None,
                        human=f"{(mem_usage / mem_limit) * 100.0:.1f}%" if mem_limit > 0 else None,
                        unit="percent",
                    )
                )
            if storage_bytes is None and size_rw is not None:
                storage_bytes = float(size_rw)
            if storage_bytes is not None:
                metrics.append(
                    _make_metric(
                        "Almacenamiento",
                        storage_bytes,
                        human=_format_bytes(storage_bytes),
                        unit="bytes",
                    )
                )
            if cpu_percent is not None:
                metrics.append(
                    _make_metric(
                        "CPU",
                        cpu_percent,
                        human=f"{cpu_percent:.1f}%",
                        unit="percent",
                    )
                )

            rows.append(
                {
                    "component": name,
                    "category": "docker",
                    "status": status,
                    "lag_seconds": latency_seconds,
                    "human_lag": _humanize_seconds(latency_seconds),
                    "metrics": metrics,
                    "summary": _render_metric_summary(metrics),
                    "notes": container.image.tags[0] if container.image.tags else container.image.short_id,
                }
            )
    finally:
        try:
            client.close()
        except Exception:
            pass
    return rows


# Expone un endpoint simple para verificar que el servicio está activo.
@app.get("/health")
def health() -> Dict[str, object]:
    now = datetime.now(timezone.utc)
    return {
        "status": "ok",
        "timestamp": now.isoformat(),
        "data_root": str(DATA_ROOT),
    }


# Genera métricas agregadas de pipeline, gestión y sistema.
@app.get("/metrics")
def metrics() -> Dict[str, object]:
    now = time.time()
    rows: List[Dict[str, object]] = []
    rows.extend(_summarise_pipeline_status(now))
    rows.extend(_summarise_management_status(now))
    rows.extend(_summarise_container_metrics(now))
    collected = datetime.fromtimestamp(now, tz=timezone.utc).isoformat()
    return {
        "collected_at": collected,
        "rows": rows,
        "sections": [],
    }
