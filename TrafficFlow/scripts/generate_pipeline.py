"""Generate per-road producer profiles and a full docker compose stack."""
from __future__ import annotations

import argparse
import base64
import json
import math
import os
import sys
import uuid
from datetime import datetime, timezone
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_DATASET = PROJECT_ROOT / "data" / "raw" / "clean_data.csv"
DEFAULT_PROFILES = PROJECT_ROOT / "data" / "generated_profiles" / "distributions.json"
DEFAULT_FALLBACK = PROJECT_ROOT / "data" / "generated_profiles" / "fallback_distributions.json"
DEFAULT_COMPOSE = PROJECT_ROOT / "docker-compose.yaml"
KAFKA_CLUSTER_ID_FILE = PROJECT_ROOT / "data" / "kafka_cluster_id"

BASE_COMPOSE = {
    "services": {
        "namenode": {
            "image": "bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8",
            "container_name": "tf-namenode",
            "environment": [
                "CLUSTER_NAME=trafficflow",
                "CORE_CONF_fs_defaultFS=hdfs://namenode:8020",
                "HDFS_CONF_dfs_replication=3",
            ],
            "ports": ["9870:9870", "9000:9000"],
            "volumes": ["namenode:/hadoop/dfs/name", "./data:/data"],
            "networks": ["hadoop"],
        },
        "hdfs-bootstrap": {
            "image": "bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8",
            "container_name": "tf-hdfs-bootstrap",
            "depends_on": ["namenode"],
            "environment": ["CORE_CONF_fs_defaultFS=hdfs://namenode:8020"],
            "command": ["/bin/bash", "/bootstrap/bootstrap_hdfs.sh"],
            "volumes": ["./scripts/bootstrap_hdfs.sh:/bootstrap/bootstrap_hdfs.sh:ro"],
            "restart": "no",
            "networks": ["hadoop"],
        },
    },
    "networks": {"hadoop": {"driver": "bridge"}},
    "volumes": {"namenode": {}},
}


KAFKA_CLUSTER_NODES: Tuple[Dict[str, object], ...] = (
    {
        "id": 1,
        "service": "kafka-primary",
        "container": "tf-kafka-primary",
        "host": "tf-kafka-primary",
        "port_mapping": "9092:9092",
    },
    {
        "id": 2,
        "service": "kafka-secondary",
        "container": "tf-kafka-secondary",
        "host": "tf-kafka-secondary",
        "port_mapping": None,
    },
    {
        "id": 3,
        "service": "kafka-tertiary",
        "container": "tf-kafka-tertiary",
        "host": "tf-kafka-tertiary",
        "port_mapping": None,
    },
)

KAFKA_CONTROLLER_QUORUM = ",".join(
    f"{node['id']}@{node['host']}:9093" for node in KAFKA_CLUSTER_NODES
)

KAFKA_CONTROLLER_BOOTSTRAP = ",".join(
    f"{node['host']}:9093" for node in KAFKA_CLUSTER_NODES
)

KAFKA_BOOTSTRAP_TARGETS = ",".join(
    f"{node['host']}:9092" for node in KAFKA_CLUSTER_NODES
)

ALLOWED_CLUSTER_CHARS = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")


def generate_cluster_id() -> str:
    raw = uuid.uuid4().bytes
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def load_or_create_cluster_id(path: Path) -> str:
    if path.exists():
        try:
            current = path.read_text(encoding="utf-8").strip()
        except OSError:
            current = ""
        if current and set(current).issubset(ALLOWED_CLUSTER_CHARS) and 16 <= len(current) <= 22:
            return current

    cluster_id = generate_cluster_id()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(cluster_id, encoding="utf-8")
    except OSError:
        pass
    return cluster_id


def build_hdfs_datanode_service(hostname: str, volume_name: str, expose_port: bool) -> Dict[str, object]:
    environment = [
        "CORE_CONF_fs_defaultFS=hdfs://namenode:8020",
        "SERVICE_PRECONDITION=namenode:9870",
        f"HDFS_CONF_dfs_datanode_hostname={hostname}",
        f"HDFS_CONF_dfs_datanode_http_address={hostname}:9864",
        "HDFS_CONF_dfs_replication=3",
    ]
    service: Dict[str, object] = {
        "image": "bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8",
        "container_name": hostname,
        "depends_on": ["namenode"],
        "environment": environment,
        "volumes": [f"{volume_name}:/hadoop/dfs/data", "./data:/data"],
        "networks": ["hadoop"],
    }
    if expose_port:
        service["ports"] = ["9864:9864"]
    return service


def build_kafka_cluster_services(cluster_id: str) -> Dict[str, object]:
    services: Dict[str, object] = {}
    for node in KAFKA_CLUSTER_NODES:
        environment = [
            f"KAFKA_CFG_NODE_ID={node['id']}",
            "KAFKA_CFG_PROCESS_ROLES=broker,controller",
            f"KAFKA_CFG_CONTROLLER_QUORUM_VOTERS={KAFKA_CONTROLLER_QUORUM}",
            f"KAFKA_CFG_CONTROLLER_QUORUM_BOOTSTRAP_SERVERS={KAFKA_CONTROLLER_BOOTSTRAP}",
            "KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "KAFKA_CFG_INTER_BROKER_LISTENER_NAME=PLAINTEXT_INTERNAL",
            "KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT_INTERNAL:PLAINTEXT",
            "KAFKA_CFG_LISTENERS=PLAINTEXT_INTERNAL://:9092,CONTROLLER://:9093",
            f"KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT_INTERNAL://{node['host']}:9092",
            "KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true",
            "KAFKA_CFG_LOG_DIRS=/bitnami/kafka/data",
            "KAFKA_CFG_DEFAULT_REPLICATION_FACTOR=3",
            "KAFKA_CFG_NUM_PARTITIONS=3",
            "KAFKA_CFG_MIN_INSYNC_REPLICAS=2",
            "KAFKA_CFG_OFFSETS_TOPIC_REPLICATION_FACTOR=3",
            "KAFKA_CFG_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=3",
            "KAFKA_CFG_TRANSACTION_STATE_LOG_MIN_ISR=2",
            f"KAFKA_KRAFT_CLUSTER_ID={cluster_id}",
            "ALLOW_PLAINTEXT_LISTENER=yes",
        ]
        service: Dict[str, object] = {
            "image": "bitnamilegacy/kafka:latest",
            "container_name": node["container"],
            "environment": environment,
            "volumes": [f"kafka-data-{node['id']}:/bitnami/kafka"],
            "networks": ["hadoop"],
            "restart": "unless-stopped",
        }
        port_mapping = node.get("port_mapping")
        if port_mapping:
            service["ports"] = [port_mapping]
        services[node["service"]] = service
    return services


@dataclass(frozen=True)
class RegionEntry:
    region_id: str
    region_name: str
    baseline_rate: float
    road_count: int


def slugify(value: str) -> str:
    value = value.strip().lower()
    cleaned = []
    for char in value:
        if char.isalnum():
            cleaned.append(char)
        elif cleaned and cleaned[-1] != "-":
            cleaned.append("-")
    slug = "".join(cleaned).strip("-")
    return slug or "unknown"


def load_fallback_profile(path: Path) -> Dict[str, object]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SystemExit(f"Fallback profile not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Fallback profile is not valid JSON: {path}") from exc


VEHICLE_CATEGORY_COLUMNS: Tuple[Tuple[str, str], ...] = (
    ("pedal_cycle_count", "pedal_cycle"),
    ("two_wheeled_motor_vehicle_count", "two_wheeled_motor_vehicle"),
    ("car_and_taxi_count", "car_and_taxi"),
    ("bus_and_coach_count", "bus_and_coach"),
    ("light_goods_vehicle_count", "light_goods_vehicle"),
    ("heavy_goods_vehicle_2_rigid_axles_count", "heavy_goods_vehicle_2_rigid_axles"),
    ("heavy_goods_vehicle_3_rigid_axles_count", "heavy_goods_vehicle_3_rigid_axles"),
    ("heavy_goods_vehicle_4_plus_rigid_axles_count", "heavy_goods_vehicle_4_plus_rigid_axles"),
    ("heavy_goods_vehicle_3_or_4_articulated_axles_count", "heavy_goods_vehicle_3_or_4_articulated_axles"),
    ("heavy_goods_vehicle_5_articulated_axles_count", "heavy_goods_vehicle_5_articulated_axles"),
    ("heavy_goods_vehicle_6_articulated_axles_count", "heavy_goods_vehicle_6_articulated_axles"),
)


def build_profiles_via_pandas(
    dataset_path: Path,
    min_observations: int,
) -> Optional[Dict[str, object]]:
    try:
        import pandas as pd
    except ImportError:
        print("[generate_pipeline] pandas no está instalado; usando fallback de perfiles")
        return None

    try:
        df = pd.read_csv(dataset_path, low_memory=False)
    except FileNotFoundError:
        return None
    except Exception as exc:  # noqa: BLE001 - log and fallback
        print(f"[generate_pipeline] No se pudo leer el dataset con pandas ({exc}); usando fallback")
        return None

    required_columns = {"road_name", "all_motor_vehicle_count", "hour_of_day", "observation_date"}
    missing_columns = sorted(required_columns.difference(df.columns))
    if missing_columns:
        print(
            "[generate_pipeline] Faltan columnas requeridas para el constructor pandas: "
            + ", ".join(missing_columns)
        )
        return None

    df = df.copy()
    df = df.dropna(subset=["road_name", "all_motor_vehicle_count", "hour_of_day"])
    if df.empty:
        return None

    df["road_name"] = df["road_name"].astype(str).str.strip()
    df["all_motor_vehicle_count"] = pd.to_numeric(df["all_motor_vehicle_count"], errors="coerce").fillna(0.0)
    df = df[df["road_name"] != ""]

    df["hour_int"] = pd.to_numeric(df["hour_of_day"], errors="coerce").astype("Int64")
    df["observation_date"] = pd.to_datetime(df["observation_date"], errors="coerce").dt.date
    df = df.dropna(subset=["hour_int", "observation_date"])
    if df.empty:
        return None
    df["hour_int"] = df["hour_int"].astype(int)

    def _std_pop(series: "pd.Series") -> float:
        value = float(series.std(ddof=0))
        return 0.0 if math.isnan(value) else value

    aggregations: Dict[str, Tuple[str, object]] = {
        "observation_count": ("road_name", "size"),
        "days_observed": ("observation_date", lambda s: int(pd.Series(s).dropna().nunique())),
    }

    if "road_type" in df.columns:
        aggregations["road_type"] = ("road_type", "first")
    if "region_name" in df.columns:
        aggregations["region_name"] = ("region_name", "first")
    if "local_authority_name" in df.columns:
        aggregations["local_authority_name"] = ("local_authority_name", "first")
    if "start_junction_road_name" in df.columns:
        aggregations["start_junction"] = ("start_junction_road_name", "first")
    if "end_junction_road_name" in df.columns:
        aggregations["end_junction"] = ("end_junction_road_name", "first")
    if "latitude" in df.columns:
        aggregations["latitude_mean"] = ("latitude", "mean")
        aggregations["latitude_std"] = ("latitude", _std_pop)
    if "longitude" in df.columns:
        aggregations["longitude_mean"] = ("longitude", "mean")
        aggregations["longitude_std"] = ("longitude", _std_pop)
    if "british_national_grid_easting" in df.columns:
        aggregations["easting_mean"] = ("british_national_grid_easting", "mean")
        aggregations["easting_std"] = ("british_national_grid_easting", _std_pop)
    if "british_national_grid_northing" in df.columns:
        aggregations["northing_mean"] = ("british_national_grid_northing", "mean")
        aggregations["northing_std"] = ("british_national_grid_northing", _std_pop)
    if "link_length_kilometers" in df.columns:
        aggregations["link_length_km_mean"] = ("link_length_kilometers", "mean")
        aggregations["link_length_km_std"] = ("link_length_kilometers", _std_pop)
    if "link_length_miles" in df.columns:
        aggregations["link_length_miles_mean"] = ("link_length_miles", "mean")
        aggregations["link_length_miles_std"] = ("link_length_miles", _std_pop)

    grouped = df.groupby("road_name")
    base_stats = grouped.agg(**aggregations).reset_index()

    direction_distribution: Dict[str, Dict[str, float]] = {}
    if "travel_direction" in df.columns:
        df["travel_direction"] = df["travel_direction"].fillna("Unknown").astype(str).str.strip()
        direction_totals = (
            df.groupby(["road_name", "travel_direction"])["all_motor_vehicle_count"].sum().reset_index()
        )
        for road_name, subset in direction_totals.groupby("road_name"):
            total = float(subset["all_motor_vehicle_count"].sum())
            if total <= 0.0:
                continue
            mapping = {
                str(row["travel_direction"]) or "Unknown": float(row["all_motor_vehicle_count"]) / total
                for _, row in subset.iterrows()
                if float(row["all_motor_vehicle_count"]) > 0.0
            }
            if mapping:
                direction_distribution[road_name] = mapping

    hourly_totals = (
        df.groupby(["road_name", "observation_date", "hour_int"])["all_motor_vehicle_count"].sum().reset_index()
    )
    hourly_group = hourly_totals.groupby(["road_name", "hour_int"])["all_motor_vehicle_count"]
    hourly_mean = hourly_group.mean()
    hourly_std = hourly_group.apply(lambda s: 0.0 if s.empty else _std_pop(s))
    hourly_count = hourly_group.count()

    hourly_profiles: Dict[str, Dict[int, Dict[str, float]]] = {}
    for (road_name, hour), mean_value in hourly_mean.items():
        hour_entry = {
            "mean_per_minute": float(mean_value) / 60.0,
            "std_per_minute": float(hourly_std.get((road_name, hour), 0.0)) / 60.0,
            "observations": int(hourly_count.get((road_name, hour), 0)),
        }
        hourly_profiles.setdefault(road_name, {})[int(hour)] = hour_entry

    vehicle_columns = [column for column, _ in VEHICLE_CATEGORY_COLUMNS if column in df.columns]
    vehicle_distribution: Dict[str, Dict[str, float]] = {}
    if vehicle_columns:
        vehicle_totals = grouped[vehicle_columns].sum().fillna(0)
        for road_name, row in vehicle_totals.iterrows():
            total = float(row.sum())
            if total <= 0.0:
                continue
            mapping = {}
            for column, alias in VEHICLE_CATEGORY_COLUMNS:
                if column not in vehicle_columns:
                    continue
                value = float(row[column])
                if value > 0.0:
                    mapping[alias] = value / total
            if mapping:
                vehicle_distribution[road_name] = mapping

    hourly_baseline: Dict[str, float] = {}
    for road_name, hours in hourly_profiles.items():
        if not hours:
            continue
        mean_rate = sum(entry["mean_per_minute"] for entry in hours.values()) / max(len(hours), 1)
        hourly_baseline[road_name] = mean_rate

    roads_payload: Dict[str, Dict[str, object]] = {}
    road_index: List[Dict[str, object]] = []

    for _, row in base_stats.iterrows():
        road_name = str(row["road_name"])
        observation_count = int(row["observation_count"]) if "observation_count" in row else 0
        if observation_count < int(min_observations):
            continue

        hours = hourly_profiles.get(road_name)
        vehicles = vehicle_distribution.get(road_name)
        if not hours or not vehicles:
            continue

        direction = direction_distribution.get(road_name, {})
        road_id = f"road:{slugify(road_name)}"
        location = {
            "latitude_mean": float(row.get("latitude_mean", 0.0) or 0.0),
            "latitude_std": float(row.get("latitude_std", 0.0) or 0.0) or 0.001,
            "longitude_mean": float(row.get("longitude_mean", 0.0) or 0.0),
            "longitude_std": float(row.get("longitude_std", 0.0) or 0.0) or 0.001,
            "easting_mean": float(row.get("easting_mean", 0.0) or 0.0),
            "easting_std": float(row.get("easting_std", 0.0) or 0.0) or 1.0,
            "northing_mean": float(row.get("northing_mean", 0.0) or 0.0),
            "northing_std": float(row.get("northing_std", 0.0) or 0.0) or 1.0,
        }
        link_length = {
            "kilometers_mean": float(row.get("link_length_km_mean", 0.0) or 0.0),
            "kilometers_std": float(row.get("link_length_km_std", 0.0) or 0.0) or 0.001,
            "miles_mean": float(row.get("link_length_miles_mean", 0.0) or 0.0),
            "miles_std": float(row.get("link_length_miles_std", 0.0) or 0.0) or 0.001,
        }

        days_observed = int(row.get("days_observed", 0))
        hourly_profile = {str(hour): data for hour, data in sorted(hours.items()) if data["observations"] > 0}
        if not hourly_profile:
            continue

        baseline_rate = float(hourly_baseline.get(road_name, 0.0))
        road_profile = {
            "road_id": road_id,
            "road_name": road_name,
            "region_name": row.get("region_name"),
            "local_authority_name": row.get("local_authority_name"),
            "road_type": row.get("road_type"),
            "start_junction": row.get("start_junction"),
            "end_junction": row.get("end_junction"),
            "location": location,
            "link_length": link_length,
            "hourly_profile": hourly_profile,
            "direction_distribution": direction,
            "vehicle_distribution": vehicles,
            "baseline_rate_per_minute": baseline_rate,
            "observation_count": observation_count,
            "days_observed": days_observed,
        }
        roads_payload[road_id] = road_profile
        road_index.append(
            {
                "id": road_id,
                "road_name": road_name,
                "region_name": row.get("region_name"),
                "local_authority_name": row.get("local_authority_name"),
                "baseline_rate_per_minute": baseline_rate,
            }
        )

    road_index.sort(key=lambda item: item.get("road_name") or "")

    # Preparar agregaciones por región
    df["region_name"] = df["region_name"].fillna("Unknown Region").astype(str).str.strip()
    df.loc[df["region_name"] == "", "region_name"] = "Unknown Region"
    df["local_authority_name"] = (
        df["local_authority_name"].fillna("Unknown Authority").astype(str).str.strip()
    )
    df.loc[df["local_authority_name"] == "", "local_authority_name"] = "Unknown Authority"
    df["travel_direction"] = df["travel_direction"].fillna("Unknown").astype(str).str.strip()

    # Mapear carreteras por región para incrustarlas en cada perfil regional
    roads_by_region: Dict[str, Dict[str, Dict[str, object]]] = defaultdict(dict)
    road_lookup: Dict[Tuple[str, str], str] = {}
    for road_id, payload in roads_payload.items():
        region = str(payload.get("region_name") or "Unknown Region")
        road_name = str(payload.get("road_name") or road_id)
        roads_by_region[region][road_id] = payload
        road_lookup[(region, road_name)] = road_id

    # Totales por región para distribuciones
    vehicle_totals_df = (
        df.groupby("region_name")[vehicle_columns].sum().fillna(0) if vehicle_columns else pd.DataFrame()
    )
    direction_totals_df = (
        df.groupby(["region_name", "travel_direction"])["all_motor_vehicle_count"]
        .sum()
        .reset_index()
    )
    authority_totals_df = (
        df.groupby(["region_name", "local_authority_name"])["all_motor_vehicle_count"]
        .sum()
        .reset_index()
    )
    road_totals_df = (
        df.groupby(["region_name", "road_name"])["all_motor_vehicle_count"]
        .sum()
        .reset_index()
    )
    observation_counts_series = df.groupby("region_name").size()
    days_observed_series = df.groupby("region_name")["observation_date"].nunique()

    region_hourly_totals = (
        df.groupby(["region_name", "observation_date", "hour_int"])["all_motor_vehicle_count"]
        .sum()
        .reset_index()
    )
    region_hourly_stats = (
        region_hourly_totals.groupby(["region_name", "hour_int"])["all_motor_vehicle_count"]
        .agg([
            ("mean_hour_total", "mean"),
            ("std_hour_total", lambda s: float(s.std(ddof=0)) if len(s) > 0 else 0.0),
            ("measurement_count", "count"),
        ])
        .reset_index()
    )

    region_hourly_profiles: Dict[str, Dict[int, Dict[str, float]]] = defaultdict(dict)
    for _, row in region_hourly_stats.iterrows():
        region_name = str(row["region_name"])
        hour = int(row["hour_int"])
        mean_total = float(row["mean_hour_total"] or 0.0)
        std_total = float(row["std_hour_total"] or 0.0)
        measurements = int(row["measurement_count"] or 0)
        region_hourly_profiles[region_name][hour] = {
            "mean_per_minute": mean_total / 60.0,
            "std_per_minute": abs(std_total) / 60.0,
            "observations": measurements,
        }

    def _normalize_series(series: pd.Series) -> Dict[str, float]:
        if series.empty:
            return {}
        series = series[series > 0]
        total = float(series.sum())
        if total <= 0.0:
            return {}
        return {str(index): float(value) / total for index, value in series.items() if float(value) > 0.0}

    regions_payload: Dict[str, Dict[str, object]] = {}
    region_index: List[Dict[str, object]] = []

    for region_name, region_roads in roads_by_region.items():
        region_slug = slugify(region_name)
        region_id = f"region:{region_slug}"
        road_count = len(region_roads)
        hourly_profile_raw = region_hourly_profiles.get(region_name, {})
        if hourly_profile_raw:
            baseline_rate = sum(entry["mean_per_minute"] for entry in hourly_profile_raw.values()) / max(
                len(hourly_profile_raw), 1
            )
        else:
            baseline_rate = sum(
                float(road.get("baseline_rate_per_minute", 0.0)) for road in region_roads.values()
            )
        if baseline_rate <= 0.0:
            baseline_rate = max(
                sum(float(road.get("baseline_rate_per_minute", 0.0)) for road in region_roads.values()),
                0.1,
            )

        vehicle_series = (
            vehicle_totals_df.loc[region_name] if not vehicle_totals_df.empty and region_name in vehicle_totals_df.index else pd.Series(dtype=float)
        )
        vehicle_distribution_region = _normalize_series(vehicle_series)

        direction_series = direction_totals_df[direction_totals_df["region_name"] == region_name]
        direction_distribution_region = _normalize_series(
            direction_series.set_index("travel_direction")["all_motor_vehicle_count"]
            if not direction_series.empty
            else pd.Series(dtype=float)
        )

        authority_series = authority_totals_df[authority_totals_df["region_name"] == region_name]
        authority_distribution = _normalize_series(
            authority_series.set_index("local_authority_name")["all_motor_vehicle_count"]
            if not authority_series.empty
            else pd.Series(dtype=float)
        )

        road_series = road_totals_df[road_totals_df["region_name"] == region_name]
        road_distribution: Dict[str, float] = {}
        if not road_series.empty:
            road_distribution_series = _normalize_series(road_series.set_index("road_name")["all_motor_vehicle_count"])
            for road_name_key, weight in road_distribution_series.items():
                road_id = road_lookup.get((region_name, road_name_key))
                if road_id:
                    road_distribution[road_id] = weight

        if not road_distribution:
            total_baseline = sum(float(road.get("baseline_rate_per_minute", 0.0)) for road in region_roads.values())
            if total_baseline > 0.0:
                for road_id, road in region_roads.items():
                    weight = float(road.get("baseline_rate_per_minute", 0.0))
                    if weight > 0.0:
                        road_distribution[road_id] = weight / total_baseline

        observation_count = sum(int(road.get("observation_count", 0)) for road in region_roads.values())
        days_observed = int(days_observed_series.get(region_name, 0))

        region_profile = {
            "region_id": region_id,
            "region_name": region_name,
            "road_count": road_count,
            "baseline_rate_per_minute": baseline_rate,
            "hourly_profile": {str(hour): value for hour, value in sorted(hourly_profile_raw.items())},
            "vehicle_distribution": vehicle_distribution_region,
            "direction_distribution": direction_distribution_region,
            "authority_distribution": authority_distribution,
            "road_distribution": road_distribution,
            "observation_count": observation_count,
            "days_observed": days_observed,
            "roads": region_roads,
        }

        regions_payload[region_id] = region_profile
        region_index.append(
            {
                "id": region_id,
                "region_name": region_name,
                "road_count": road_count,
                "baseline_rate_per_minute": baseline_rate,
            }
        )

    region_index.sort(key=lambda item: item.get("region_name") or "")

    profiles = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "input_path": str(dataset_path),
            "min_observations": int(min_observations),
            "road_count": len(roads_payload),
            "region_count": len(regions_payload),
            "source": "pandas-local",
        },
        "road_index": road_index,
        "roads": roads_payload,
        "region_index": region_index,
        "regions": regions_payload,
    }

    if not region_index:
        return None

    print(
        f"Perfiles generados con pandas (regiones={profiles['meta']['region_count']}, carreteras={profiles['meta']['road_count']})"
    )
    return profiles


def ensure_profiles(
    dataset_path: Path,
    output_path: Path,
    fallback_path: Path,
    min_observations: int,
    skip_profiles: bool = False,
    force_fallback: bool = False,
) -> Dict[str, object]:
    profiles: Optional[Dict[str, object]] = None
    dataset_exists = dataset_path.exists()
    if not dataset_exists:
        print(f"[generate_pipeline] Dataset {dataset_path} no encontrado; usando fallback")
    elif force_fallback:
        print("[generate_pipeline] Se forzó el uso del perfil de respaldo (--force-fallback)")
    elif skip_profiles:
        print("[generate_pipeline] Construcción de perfiles omitida (--skip-profiles); usando fallback")
    else:
        profiles = build_profiles_via_pandas(dataset_path, min_observations)
        if profiles is None:
            print("[generate_pipeline] No se pudieron generar perfiles locales; usando fallback")

    if profiles is None:
        profiles = load_fallback_profile(fallback_path)
        print(f"Perfiles copiados desde {fallback_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(profiles, indent=2), encoding="utf-8")
    return profiles


def extract_regions(profiles: Dict[str, object]) -> List[RegionEntry]:
    index = profiles.get("region_index")
    if not isinstance(index, list):
        return []
    regions: List[RegionEntry] = []
    for entry in index:
        if not isinstance(entry, dict):
            continue
        region_id = str(entry.get("id") or "").strip()
        region_name = str(entry.get("region_name") or region_id or "Unknown Region").strip()
        baseline_value = entry.get("baseline_rate_per_minute")
        road_count_value = entry.get("road_count")
        try:
            baseline_rate = float(baseline_value)
        except (TypeError, ValueError):
            baseline_rate = 0.0
        try:
            road_count = int(road_count_value)
        except (TypeError, ValueError):
            road_count = 0
        regions.append(
            RegionEntry(
                region_id=region_id or f"region:{slugify(region_name)}",
                region_name=region_name,
                baseline_rate=max(baseline_rate, 0.0),
                road_count=max(road_count, 0),
            )
        )
    return regions


def build_generator_service(
    region: RegionEntry,
    bootstrap_servers: str,
    kafka_dependencies: Sequence[str],
) -> Dict[str, object]:
    region_slug = slugify(region.region_name)
    return {
        "build": {"context": "./producer_service"},
        "container_name": f"tf-generator-{region_slug}",
        "environment": {
            "PROFILES_PATH": "/opt/producer/generated/distributions.json",
            "PROFILE_OVERRIDE_PATH": "/opt/producer/generated/distributions.json",
            "PROFILE_STATUS_PATH": f"/opt/producer/generated/status/{region_slug}.json",
            "PRODUCER_REGION_ID": region.region_id,
            "PRODUCER_REGION_NAME": region.region_name,
            "PRODUCER_ROAD_COUNT": str(max(region.road_count, 0)),
            "PRODUCER_SINK": "file,kafka",
            "OUTPUT_PATH": f"/opt/producer/output/{region_slug}.jsonl",
            "KAFKA_BOOTSTRAP_SERVERS": bootstrap_servers,
            "KAFKA_TOPIC": f"traffic.raw.{region_slug}",
            "ROTATE_RECORDS": "500",
            "PRODUCER_SPOOL_PATH": "/opt/producer/spool",
            "RATE_PER_MINUTE": str(
                max(
                    int(math.ceil(max(region.baseline_rate, 0.1) * 60)),
                    60,
                )
            ),
        },
        "volumes": [
            "./data/generated_profiles:/opt/producer/generated:ro",
            "./data/synthetic:/opt/producer/output",
            f"./data/producer_spool/{region_slug}:/opt/producer/spool",
        ],
        "depends_on": {
            dependency: {"condition": "service_started"} for dependency in kafka_dependencies
        },
        "mem_limit": "96m",
        "cpus": "0.15",
        "networks": ["hadoop"],
        "restart": "unless-stopped",
    }


def build_pipeline_service(
    regions: Sequence[RegionEntry],
    bootstrap_servers: str,
    status_filename: str,
    container_name: str,
    kafka_dependencies: Sequence[str],
) -> Dict[str, object]:
    topics = ",".join(
        sorted({f"traffic.raw.{slugify(region.region_name)}" for region in regions})
    )
    return {
        "build": {"context": "./pipeline_service"},
        "container_name": container_name,
        "environment": {
            "KAFKA_BOOTSTRAP_SERVERS": bootstrap_servers,
            "KAFKA_TOPICS": topics,
            "KAFKA_GROUP_ID": "trafficflow-pipeline",
            "SILVER_BASE_PATH": "/data/silver/regions",
            "GOLD_OUTPUT_PATH": "/data/gold/management/primary",
            "STATUS_PATH": f"/opt/pipeline/status/{status_filename}",
            "WEBHDFS_URL": "http://namenode:9870",
            "HDFS_USER": "hdfs",
            "FLUSH_INTERVAL_SECONDS": "20",
            "BATCH_SIZE": "500",
        },
        "volumes": [
            "./data/pipeline_status:/opt/pipeline/status",
        ],
        "depends_on": {
            **{dependency: {"condition": "service_started"} for dependency in kafka_dependencies},
            "namenode": {"condition": "service_started"},
            "hdfs-bootstrap": {"condition": "service_completed_successfully"},
        },
        "mem_limit": "320m",
        "cpus": "0.45",
        "networks": ["hadoop"],
        "restart": "unless-stopped",
    }


def build_dashboard_service(primary_pipeline_service: str) -> Dict[str, object]:
    return {
        "build": {"context": "./dashboard"},
        "container_name": "tf-dashboard",
        "environment": {
            "WEBHDFS_URL": "http://namenode:9870",
            "HDFS_USER": "hdfs",
            "HDFS_BASE_PATH": "/data/gold/management/primary",
            "STREAM_WINDOW_MINUTES": "15",
            "STREAM_REFRESH_SECONDS": "5",
            "STREAM_HISTORY_MINUTES": "180",
            "PROFILE_STATUS_PATH": "/opt/dashboard/generated_profiles/profile_status.json",
            "PROFILE_OVERRIDE_PATH": "/opt/dashboard/generated_profiles/distributions.json",
        },
        "depends_on": {
            primary_pipeline_service: {"condition": "service_started"},
            "namenode": {"condition": "service_started"},
            "hdfs-bootstrap": {"condition": "service_completed_successfully"},
        },
        "ports": ["8501:8501"],
        "volumes": ["./data/generated_profiles:/opt/dashboard/generated_profiles:ro"],
        "networks": ["hadoop"],
        "restart": "unless-stopped",
    }


def build_dynamic_services(
    regions: Sequence[RegionEntry],
    cluster_id: str,
) -> Dict[str, object]:
    services: Dict[str, object] = build_kafka_cluster_services(cluster_id)
    kafka_dependencies = [node["service"] for node in KAFKA_CLUSTER_NODES]
    bootstrap_servers = KAFKA_BOOTSTRAP_TARGETS
    ordered_regions = sorted(regions, key=lambda item: (item.region_name or item.region_id))
    for region in ordered_regions:
        services[f"generator-{slugify(region.region_id or region.region_name)}"] = build_generator_service(
            region,
            bootstrap_servers=bootstrap_servers,
            kafka_dependencies=kafka_dependencies,
        )

    services["pipeline-primary"] = build_pipeline_service(
        ordered_regions,
        bootstrap_servers=bootstrap_servers,
        status_filename="pipeline-primary.json",
        container_name="tf-pipeline-primary",
        kafka_dependencies=kafka_dependencies,
    )
    services["pipeline-backup"] = build_pipeline_service(
        ordered_regions,
        bootstrap_servers=bootstrap_servers,
        status_filename="pipeline-backup.json",
        container_name="tf-pipeline-backup",
        kafka_dependencies=kafka_dependencies,
    )
    services["dashboard"] = build_dashboard_service("pipeline-primary")

    return services


def build_full_compose(dynamic_services: Dict[str, object]) -> Dict[str, object]:
    compose = deepcopy(BASE_COMPOSE)
    compose_services = compose.setdefault("services", {})
    compose_volumes = compose.setdefault("volumes", {})
    datanode_specs = [
        ("datanode", "tf-datanode", "datanode", True),
        ("datanode-2", "tf-datanode-2", "datanode-2", False),
        ("datanode-3", "tf-datanode-3", "datanode-3", False),
    ]
    for service_name, hostname, volume_name, expose_port in datanode_specs:
        compose_services[service_name] = build_hdfs_datanode_service(
            hostname=hostname,
            volume_name=volume_name,
            expose_port=expose_port,
        )
        compose_volumes.setdefault(volume_name, {})
    compose_services.update(dynamic_services)
    for node in KAFKA_CLUSTER_NODES:
        compose_volumes.setdefault(f"kafka-data-{node['id']}", {})
    return compose


def write_compose(compose: Dict[str, object], output_path: Path) -> None:
    output_path.write_text(json.dumps(compose, indent=2), encoding="utf-8")


def parse_cli() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate profiles and compose stack for TrafficFlow")
    parser.add_argument("--dataset", type=Path, default=DEFAULT_DATASET, help="Input CSV dataset path")
    parser.add_argument("--profiles", type=Path, default=DEFAULT_PROFILES, help="Destination JSON for generated profiles")
    parser.add_argument("--fallback", type=Path, default=DEFAULT_FALLBACK, help="Fallback profile JSON path")
    parser.add_argument("--compose", type=Path, default=DEFAULT_COMPOSE, help="Output docker compose file")
    parser.add_argument("--min-observations", type=int, default=50, help="Minimum observations per road when building profiles")
    parser.add_argument(
        "--skip-profiles",
        dest="skip_profiles",
        action="store_true",
        help="Omit the local profile generation step and reutiliza el JSON de respaldo",
    )
    parser.add_argument(
        "--force-fallback",
        action="store_true",
        help="Forzar el uso del perfil de respaldo incluso si el dataset está disponible",
    )
    return parser.parse_args()


def ensure_runtime_directories(regions: Sequence[RegionEntry]) -> None:
    base_directories = (
        PROJECT_ROOT / "data" / "pipeline_status",
        PROJECT_ROOT / "data" / "producer_spool",
        PROJECT_ROOT / "data" / "synthetic",
    )
    for directory in base_directories:
        directory.mkdir(parents=True, exist_ok=True)

    producer_spool_root = PROJECT_ROOT / "data" / "producer_spool"
    for region in regions:
        slug = slugify(region.region_name)
        (producer_spool_root / slug).mkdir(parents=True, exist_ok=True)


def main() -> None:
    args = parse_cli()
    profiles = ensure_profiles(
        dataset_path=args.dataset,
        output_path=args.profiles,
        fallback_path=args.fallback,
        min_observations=args.min_observations,
        skip_profiles=args.skip_profiles,
        force_fallback=args.force_fallback,
    )
    regions = extract_regions(profiles)
    if not regions:
        raise SystemExit("No se encontraron regiones válidas en el perfil generado")
    cluster_id = load_or_create_cluster_id(KAFKA_CLUSTER_ID_FILE)
    dynamic_services = build_dynamic_services(regions, cluster_id)
    compose = build_full_compose(dynamic_services)
    write_compose(compose, args.compose)
    legacy_compose = PROJECT_ROOT / "docker-compose.generated.yml"
    if legacy_compose.exists() and legacy_compose != args.compose:
        try:
            legacy_compose.unlink()
        except OSError:
            pass
    ensure_runtime_directories(regions)
    print(f"Perfiles listos para {len(regions)} regiones")
    print(f"Compose generado en {args.compose}")


if __name__ == "__main__":
    main()
