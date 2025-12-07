from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import re
import signal
import time
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable

LOG = logging.getLogger("producer")
STOP_REQUESTED = False
SINK_CHOICES = {"file", "kafka"}
SINK_ALIASES = {
    "all": SINK_CHOICES,
    "stdout": {"file"},
    "filesystem": {"file"},
    "local": {"file"},
    "stream": {"kafka"},
    "broker": {"kafka"},
    "queue": {"kafka"},
}

VEHICLE_CATEGORY_TO_COLUMN: Dict[str, str] = {
    "pedal_cycle": "pedal_cycle_count",
    "two_wheeled_motor_vehicle": "two_wheeled_motor_vehicle_count",
    "car_and_taxi": "car_and_taxi_count",
    "bus_and_coach": "bus_and_coach_count",
    "light_goods_vehicle": "light_goods_vehicle_count",
    "heavy_goods_vehicle_2_rigid_axles": "heavy_goods_vehicle_2_rigid_axles_count",
    "heavy_goods_vehicle_3_rigid_axles": "heavy_goods_vehicle_3_rigid_axles_count",
    "heavy_goods_vehicle_4_plus_rigid_axles": "heavy_goods_vehicle_4_plus_rigid_axles_count",
    "heavy_goods_vehicle_3_or_4_articulated_axles": "heavy_goods_vehicle_3_or_4_articulated_axles_count",
    "heavy_goods_vehicle_5_articulated_axles": "heavy_goods_vehicle_5_articulated_axles_count",
    "heavy_goods_vehicle_6_articulated_axles": "heavy_goods_vehicle_6_articulated_axles_count",
}

HEAVY_VEHICLE_CATEGORIES = {
    "heavy_goods_vehicle_2_rigid_axles",
    "heavy_goods_vehicle_3_rigid_axles",
    "heavy_goods_vehicle_4_plus_rigid_axles",
    "heavy_goods_vehicle_3_or_4_articulated_axles",
    "heavy_goods_vehicle_5_articulated_axles",
    "heavy_goods_vehicle_6_articulated_axles",
}


# Construye un perfil horario básico con medias y desviaciones uniformes.
def _default_hourly_profile(mean: float, std: float, observations: int) -> Dict[str, Dict[str, float]]:
    return {
        str(hour): {
            "mean_per_minute": mean,
            "std_per_minute": std,
            "observations": observations,
        }
        for hour in range(24)
    }


_DEFAULT_ROAD_PROFILE_PAYLOAD = {
    "road_id": "road:demo",
    "road_name": "Demo Road",
    "region_name": "Demo Region",
    "local_authority_name": "Demo Authority",
    "road_type": "Major",
    "start_junction": "DR-J1",
    "end_junction": "DR-J2",
    "location": {
        "latitude_mean": 51.5,
        "latitude_std": 0.01,
        "longitude_mean": -0.1,
        "longitude_std": 0.01,
        "easting_mean": 530000.0,
        "easting_std": 50.0,
        "northing_mean": 180000.0,
        "northing_std": 50.0,
    },
    "link_length": {
        "kilometers_mean": 1.0,
        "kilometers_std": 0.05,
        "miles_mean": 0.62,
        "miles_std": 0.05,
    },
    "hourly_profile": _default_hourly_profile(6.0, 1.0, 8),
    "direction_distribution": {"Unknown": 1.0},
    "vehicle_distribution": {
        "car_and_taxi": 0.75,
        "light_goods_vehicle": 0.12,
        "pedal_cycle": 0.04,
        "bus_and_coach": 0.02,
        "two_wheeled_motor_vehicle": 0.03,
        "heavy_goods_vehicle_2_rigid_axles": 0.02,
        "heavy_goods_vehicle_3_rigid_axles": 0.01,
        "heavy_goods_vehicle_4_plus_rigid_axles": 0.01,
    },
    "baseline_rate_per_minute": 6.0,
    "observation_count": 24,
    "days_observed": 1,
}

DEFAULT_PROFILES = {
    "meta": {
        "generated_at": None,
        "source": "embedded defaults",
        "road_count": 1,
        "region_count": 1,
    },
    "road_index": [
        {
            "id": "road:demo",
            "road_name": "Demo Road",
            "region_name": "Demo Region",
            "local_authority_name": "Demo Authority",
            "baseline_rate_per_minute": 6.0,
        }
    ],
    "roads": {
        "road:demo": _DEFAULT_ROAD_PROFILE_PAYLOAD,
    },
    "region_index": [
        {
            "id": "region:demo",
            "region_name": "Demo Region",
            "road_count": 1,
            "baseline_rate_per_minute": 6.0,
        }
    ],
    "regions": {
        "region:demo": {
            "region_id": "region:demo",
            "region_name": "Demo Region",
            "road_count": 1,
            "baseline_rate_per_minute": 6.0,
            "hourly_profile": _default_hourly_profile(6.0, 1.0, 8),
            "vehicle_distribution": {
                "car_and_taxi": 0.75,
                "light_goods_vehicle": 0.12,
                "pedal_cycle": 0.04,
                "bus_and_coach": 0.02,
                "two_wheeled_motor_vehicle": 0.03,
                "heavy_goods_vehicle_2_rigid_axles": 0.02,
                "heavy_goods_vehicle_3_rigid_axles": 0.01,
                "heavy_goods_vehicle_4_plus_rigid_axles": 0.01,
            },
            "direction_distribution": {"Unknown": 1.0},
            "authority_distribution": {"Demo Authority": 1.0},
            "road_distribution": {"road:demo": 1.0},
            "observation_count": 24,
            "days_observed": 1,
            "roads": {
                "road:demo": _DEFAULT_ROAD_PROFILE_PAYLOAD,
            },
        }
    },
}


@dataclass
# Describe métricas resumidas por hora para una carretera.
class RoadHourProfile:
    mean_per_minute: float
    std_per_minute: float
    observations: int


@dataclass
# Modela los atributos completos necesarios para simular una carretera.
class RoadProfile:
    id: str
    name: str
    region_name: str
    local_authority_name: str
    road_type: Optional[str]
    start_junction: Optional[str]
    end_junction: Optional[str]
    direction_distribution: Dict[str, float]
    vehicle_distribution: Dict[str, float]
    hourly_profile: Dict[int, RoadHourProfile]
    baseline_rate_per_minute: float
    latitude_mean: float
    latitude_std: float
    longitude_mean: float
    longitude_std: float
    easting_mean: float
    easting_std: float
    northing_mean: float
    northing_std: float
    link_length_km_mean: float
    link_length_km_std: float
    link_length_miles_mean: float
    link_length_miles_std: float


@dataclass
# Contiene parámetros agregados para simular una región completa.
class RegionProfile:
    id: str
    name: str
    baseline_rate_per_minute: float
    hourly_profile: Dict[int, RoadHourProfile]
    direction_distribution: Dict[str, float]
    vehicle_distribution: Dict[str, float]
    authority_distribution: Dict[str, float]
    road_distribution: Dict[str, float]
    observation_count: int
    days_observed: int
    roads: Dict[str, RoadProfile]


@dataclass
# Facilita el acceso a perfiles de carreteras y regiones.
class Profiles:
    data: Dict[str, object]

    # Devuelve la colección de carreteras disponibles en el perfil.
    def list_roads(self) -> List[Dict[str, object]]:
        index = self.data.get("road_index")
        if isinstance(index, list):
            return index
        roads = self.data.get("roads")
        if isinstance(roads, dict):
            result: List[Dict[str, object]] = []
            for road_id, payload in roads.items():
                if isinstance(payload, dict):
                    result.append(
                        {
                            "id": road_id,
                            "road_name": payload.get("road_name"),
                            "region_name": payload.get("region_name"),
                            "local_authority_name": payload.get("local_authority_name"),
                            "baseline_rate_per_minute": payload.get("baseline_rate_per_minute", 0.0),
                        }
                    )
            result.sort(key=lambda item: item.get("road_name") or "")
            return result
        return []

    # Recupera la definición de una carretera específica.
    def get_road(self, road_id: str) -> Optional[Dict[str, object]]:
        roads = self.data.get("roads")
        if isinstance(roads, dict):
            payload = roads.get(road_id)
            return payload if isinstance(payload, dict) else None
        return None

    # Devuelve la lista de regiones incluidas en el perfil.
    def list_regions(self) -> List[Dict[str, object]]:
        index = self.data.get("region_index")
        if isinstance(index, list):
            return index
        regions = self.data.get("regions")
        if isinstance(regions, dict):
            result: List[Dict[str, object]] = []
            for region_id, payload in regions.items():
                if isinstance(payload, dict):
                    entry = {
                        "id": region_id,
                        "region_name": payload.get("region_name"),
                        "road_count": payload.get("road_count"),
                        "baseline_rate_per_minute": payload.get("baseline_rate_per_minute", 0.0),
                    }
                    result.append(entry)
            result.sort(key=lambda item: item.get("region_name") or "")
            return result
        return []

    # Recupera los detalles de una región concreta.
    def get_region(self, region_id: str) -> Optional[Dict[str, object]]:
        regions = self.data.get("regions")
        if isinstance(regions, dict):
            payload = regions.get(region_id)
            return payload if isinstance(payload, dict) else None
        return None


# Configura y parsea los argumentos CLI del generador.
def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Per-road synthetic traffic producer")
    parser.add_argument(
        "--profile-path",
        default=os.environ.get("PROFILES_PATH", "/opt/producer/profiles/distributions.json"),
        help="Path to JSON file with per-road profiles",
    )
    parser.add_argument(
        "--profile-override-path",
        default=os.environ.get("PROFILE_OVERRIDE_PATH"),
        help="Optional override profile generated at runtime (JSON)",
    )
    parser.add_argument(
        "--profile-status-path",
        default=os.environ.get("PROFILE_STATUS_PATH"),
        help="Optional path where the producer writes the resolved profile status (JSON)",
    )
    parser.add_argument(
        "--output-path",
        default=os.environ.get("OUTPUT_PATH", "/opt/producer/output/traffic_stream.jsonl"),
        help="Destination file for newline-delimited JSON output",
    )
    parser.add_argument(
        "--spool-path",
        default=os.environ.get("PRODUCER_SPOOL_PATH", "/opt/producer/spool"),
        help="Directory used to persist events when Kafka is unavailable",
    )
    parser.add_argument(
        "--sink",
        action="append",
        help="Output sink(s) (file, kafka, all). Can be repeated or comma separated",
    )
    parser.add_argument(
        "--kafka-bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        help="Comma-separated list of Kafka bootstrap servers",
    )
    parser.add_argument(
        "--kafka-topic",
        default=os.environ.get("KAFKA_TOPIC"),
        help="Kafka topic where synthetic events are published",
    )
    parser.add_argument(
        "--rotate-records",
        type=int,
        default=int(os.environ.get("ROTATE_RECORDS", "500")),
        help="Number of records emitted before flushing and reopening sinks",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=int(os.environ["MAX_RECORDS"]) if os.environ.get("MAX_RECORDS") else None,
        help="Optional cap used for dry runs or diagnostics",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity",
    )
    parser.add_argument(
        "--preview-records",
        type=int,
        default=int(os.environ.get("PREVIEW_RECORDS", 3)),
        help="Number of generated records echoed to stdout for sanity checks",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=int(os.environ["RANDOM_SEED"]) if os.environ.get("RANDOM_SEED") else None,
        help="Optional random seed for repeatability",
    )
    parser.add_argument(
        "--region-id",
        default=os.environ.get("PRODUCER_REGION_ID") or os.environ.get("PRODUCER_ROAD_ID"),
        help="Explicit region identifier to simulate",
    )
    parser.add_argument(
        "--region-index",
        type=int,
        default=int(os.environ["PRODUCER_REGION_INDEX"]) if os.environ.get("PRODUCER_REGION_INDEX") else (
            int(os.environ["PRODUCER_ROAD_INDEX"]) if os.environ.get("PRODUCER_ROAD_INDEX") else None
        ),
        help="Zero-based region index to simulate when no explicit identifier is provided",
    )
    parser.add_argument(
        "--instance-offset",
        type=int,
        default=int(os.environ.get("PRODUCER_INSTANCE_OFFSET", "0")),
        help="Optional offset applied to the inferred instance index",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


# Determina los destinos de salida efectivos según CLI y entorno.
def resolve_sink_targets(cli_values: Optional[List[str]], env_value: Optional[str]) -> List[str]:
    sources: List[str] = []
    if cli_values:
        sources.extend(cli_values)
    elif env_value:
        sources.append(env_value)
    else:
        sources.append("file")

    resolved: List[str] = []
    for source in sources:
        for token in re.split(r"[\s,+]+", source.strip()):
            if not token:
                continue
            key = token.lower()
            if key in SINK_ALIASES:
                for alias in SINK_ALIASES[key]:
                    if alias not in resolved:
                        resolved.append(alias)
                continue
            if key not in SINK_CHOICES:
                raise ValueError(f"Unknown sink target '{token}'")
            if key not in resolved:
                resolved.append(key)
    return resolved


# Carga perfiles desde disco y aplica overrides opcionales.
def load_profiles(path: str, override_path: Optional[str] = None) -> Tuple[Profiles, str, Optional[Path]]:
    base = deepcopy(DEFAULT_PROFILES)
    resolved_label = "defaults"
    resolved_path: Optional[Path] = None
    candidates: List[Tuple[str, Path]] = []
    if override_path:
        candidates.append(("override", Path(override_path)))
    candidates.append(("primary", Path(path)))

    for label, candidate in candidates:
        if not candidate.exists():
            if label == "primary":
                LOG.warning("Profile file %s not found; using embedded defaults", candidate)
            else:
                LOG.info("Override profile %s not present; skipping", candidate)
            continue
        try:
            with candidate.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (json.JSONDecodeError, OSError) as exc:
            LOG.warning("Failed to load %s profiles from %s (%s)", label, candidate, exc)
            continue
        if isinstance(data, dict):
            base.update(data)
            resolved_label = label
            resolved_path = candidate
            LOG.info("Loaded %s profiles from %s", label, candidate)
            break
        LOG.warning("Profile file %s did not contain a JSON object; ignoring", candidate)

    return Profiles(base), resolved_label, resolved_path


# Registra en disco la procedencia del perfil resuelto.
def write_profile_status(
    status_path: Optional[str],
    source_label: str,
    profile_path: Optional[Path],
    profiles: Profiles,
    region: Optional[RegionProfile],
) -> None:
    if not status_path:
        return
    payload: Dict[str, object] = {
        "source": source_label,
        "profile_path": str(profile_path) if profile_path else None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "meta": profiles.data.get("meta"),
    }
    if region is not None:
        payload["assigned_region"] = {
            "id": region.id,
            "name": region.name,
            "road_count": len(region.roads),
            "baseline_rate_per_minute": region.baseline_rate_per_minute,
        }
    try:
        target = Path(status_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
    except OSError as exc:
        LOG.warning("Failed to persist profile status to %s (%s)", status_path, exc)


# Convierte valores en flotantes aplicando un valor por defecto.
def _safe_float(value: object, default: float) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(result) or math.isinf(result):
        return default
    return result


# Reescala un diccionario de pesos para que sumen uno.
def _normalize_distribution(raw: Dict[str, object]) -> Dict[str, float]:
    values: Dict[str, float] = {}
    for key, value in raw.items():
        weight = _safe_float(value, 0.0)
        if weight > 0.0:
            values[str(key)] = weight
    total = sum(values.values())
    if total <= 0.0:
        return {}
    return {key: weight / total for key, weight in values.items()}


# Selecciona una clave ponderada usando un generador aleatorio dado.
def _weighted_choice(mapping: Dict[str, float], rng: random.Random, fallback: str) -> str:
    if not mapping:
        return fallback
    items = list(mapping.items())
    total = sum(max(value, 0.0) for _, value in items)
    if total <= 0.0:
        return fallback
    target = rng.random() * total
    cumulative = 0.0
    for key, value in items:
        value = max(value, 0.0)
        cumulative += value
        if cumulative >= target:
            return key
    return items[-1][0]


# Normaliza cadenas para generar identificadores seguros.
def _slugify(value: str) -> str:
    text = value.strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = re.sub(r"-+", "-", text).strip("-")
    return text or "value"


# Transforma datos crudos en perfiles horarios tipados.
def _build_hourly_profile(raw: Dict[str, object], baseline: float) -> Dict[int, RoadHourProfile]:
    profile: Dict[int, RoadHourProfile] = {}
    for key, value in raw.items():
        try:
            hour = int(key)
        except (TypeError, ValueError):
            continue
        if not isinstance(value, dict):
            continue
        mean = _safe_float(value.get("mean_per_minute"), baseline)
        std = abs(_safe_float(value.get("std_per_minute"), max(baseline * 0.1, 0.01)))
        observations = int(value.get("observations") or 0)
        profile[hour % 24] = RoadHourProfile(mean_per_minute=mean, std_per_minute=std, observations=observations)
    if not profile:
        fallback_std = max(baseline * 0.1, 0.01)
        profile = {hour: RoadHourProfile(baseline, fallback_std, 0) for hour in range(24)}
    return profile


# Construye la estructura completa de una carretera a partir de JSON.
def build_road_profile(road_id: str, raw: Dict[str, object]) -> RoadProfile:
    location = raw.get("location") if isinstance(raw.get("location"), dict) else {}
    link_length = raw.get("link_length") if isinstance(raw.get("link_length"), dict) else {}
    baseline = _safe_float(raw.get("baseline_rate_per_minute"), 0.5)
    hourly_raw = raw.get("hourly_profile") if isinstance(raw.get("hourly_profile"), dict) else {}
    hourly_profile = _build_hourly_profile(hourly_raw, baseline)

    direction_distribution = _normalize_distribution(
        raw.get("direction_distribution") if isinstance(raw.get("direction_distribution"), dict) else {}
    )
    vehicle_distribution = _normalize_distribution(
        raw.get("vehicle_distribution") if isinstance(raw.get("vehicle_distribution"), dict) else {}
    )

    return RoadProfile(
        id=road_id,
        name=str(raw.get("road_name") or road_id),
        region_name=str(raw.get("region_name") or "Unknown"),
        local_authority_name=str(raw.get("local_authority_name") or "Unknown Authority"),
        road_type=raw.get("road_type"),
        start_junction=raw.get("start_junction"),
        end_junction=raw.get("end_junction"),
        direction_distribution=direction_distribution if direction_distribution else {"Unknown": 1.0},
        vehicle_distribution=vehicle_distribution if vehicle_distribution else {"car_and_taxi": 1.0},
        hourly_profile=hourly_profile,
        baseline_rate_per_minute=baseline,
        latitude_mean=_safe_float((location or {}).get("latitude_mean"), 0.0),
        latitude_std=max(_safe_float((location or {}).get("latitude_std"), 0.0005), 0.0005),
        longitude_mean=_safe_float((location or {}).get("longitude_mean"), 0.0),
        longitude_std=max(_safe_float((location or {}).get("longitude_std"), 0.0005), 0.0005),
        easting_mean=_safe_float((location or {}).get("easting_mean"), 0.0),
        easting_std=max(_safe_float((location or {}).get("easting_std"), 1.0), 1.0),
        northing_mean=_safe_float((location or {}).get("northing_mean"), 0.0),
        northing_std=max(_safe_float((location or {}).get("northing_std"), 1.0), 1.0),
        link_length_km_mean=max(_safe_float((link_length or {}).get("kilometers_mean"), 1.0), 0.05),
        link_length_km_std=max(_safe_float((link_length or {}).get("kilometers_std"), 0.05), 0.01),
        link_length_miles_mean=max(_safe_float((link_length or {}).get("miles_mean"), 0.62), 0.03),
        link_length_miles_std=max(_safe_float((link_length or {}).get("miles_std"), 0.05), 0.01),
    )


# Ensambla los metadatos de una región y sus carreteras.
def build_region_profile(region_id: str, raw: Dict[str, object]) -> RegionProfile:
    baseline = _safe_float(raw.get("baseline_rate_per_minute"), 1.0)
    hourly_raw = raw.get("hourly_profile") if isinstance(raw.get("hourly_profile"), dict) else {}
    hourly_profile = _build_hourly_profile(hourly_raw, baseline)

    direction_distribution = _normalize_distribution(
        raw.get("direction_distribution") if isinstance(raw.get("direction_distribution"), dict) else {}
    ) or {"Unknown": 1.0}
    vehicle_distribution = _normalize_distribution(
        raw.get("vehicle_distribution") if isinstance(raw.get("vehicle_distribution"), dict) else {}
    ) or {"car_and_taxi": 1.0}
    authority_distribution = _normalize_distribution(
        raw.get("authority_distribution") if isinstance(raw.get("authority_distribution"), dict) else {}
    )
    road_distribution = _normalize_distribution(
        raw.get("road_distribution") if isinstance(raw.get("road_distribution"), dict) else {}
    )

    roads_raw = raw.get("roads") if isinstance(raw.get("roads"), dict) else {}
    road_profiles: Dict[str, RoadProfile] = {}
    for road_key, road_payload in roads_raw.items():
        if isinstance(road_payload, dict):
            road_profiles[road_key] = build_road_profile(road_key, road_payload)

    if not road_distribution and road_profiles:
        total_baseline = sum(profile.baseline_rate_per_minute for profile in road_profiles.values())
        if total_baseline > 0.0:
            road_distribution = {
                road_id: profile.baseline_rate_per_minute / total_baseline
                for road_id, profile in road_profiles.items()
                if profile.baseline_rate_per_minute > 0.0
            }

    observation_count = int(raw.get("observation_count") or 0)
    days_observed = int(raw.get("days_observed") or 0)
    region_name = str(raw.get("region_name") or region_id)

    return RegionProfile(
        id=region_id,
        name=region_name,
        baseline_rate_per_minute=max(baseline, 0.05),
        hourly_profile=hourly_profile,
        direction_distribution=direction_distribution,
        vehicle_distribution=vehicle_distribution,
        authority_distribution=authority_distribution,
        road_distribution=road_distribution,
        observation_count=observation_count,
        days_observed=days_observed,
        roads=road_profiles,
    )


# Calcula el índice de instancia a utilizar según el entorno.
def resolve_instance_index(explicit: Optional[int]) -> Optional[int]:
    if explicit is not None:
        return max(explicit, 0)
    for env_name in (
        "PRODUCER_INSTANCE_INDEX",
        "INSTANCE_INDEX",
        "COMPOSE_PROJECT",
        "TASK_INDEX",
    ):
        value = os.environ.get(env_name)
        if value is None:
            continue
        try:
            return max(int(value), 0)
        except ValueError:
            continue
    hostname = os.environ.get("HOSTNAME", "")
    match = re.search(r"-(\d+)$", hostname)
    if match:
        try:
            return max(int(match.group(1)) - 1, 0)
        except ValueError:
            return None
    return None


# Elige la región a simular considerando filtros y balances.
def select_region_profile(
    profiles: Profiles,
    region_id: Optional[str],
    region_index: Optional[int],
    instance_offset: int,
) -> RegionProfile:
    regions = profiles.list_regions()
    if not regions:
        raise ValueError("Profile file does not define any regions")

    if region_id:
        raw = profiles.get_region(region_id)
        if raw is None:
            raise ValueError(f"Region '{region_id}' not found in profiles")
        return build_region_profile(region_id, raw)

    inferred_index = resolve_instance_index(region_index)
    if inferred_index is None:
        inferred_index = 0
    index = (inferred_index + max(instance_offset, 0)) % len(regions)
    entry = regions[index]
    selected_id = entry.get("id")
    if not selected_id:
        raise ValueError("Selected region entry does not contain an identifier")
    raw = profiles.get_region(selected_id)
    if raw is None:
        raise ValueError(f"Region '{selected_id}' not found in profiles")
    return build_region_profile(selected_id, raw)


# Gestiona la escritura continua en archivos JSONL.
class JsonlWriter:
    # Configura la ruta del archivo y el manejador.
    def __init__(self, path: str) -> None:
        self._path = Path(path)
        self._file: Optional[object] = None

    # Abre el archivo y habilita el contexto de escritura.
    def __enter__(self) -> "JsonlWriter":
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self._path.open("a", encoding="utf-8")
        LOG.info("Writing synthetic stream to %s", self._path)
        return self

    # Asegura el cierre del archivo cuando termina el contexto.
    def __exit__(self, exc_type, exc, tb) -> None:
        if self._file:
            self._file.flush()
            self._file.close()
            self._file = None

    # Escribe el evento serializado en el archivo abierto.
    def write(self, payload: Dict[str, object]) -> None:
        if self._file is None:
            raise RuntimeError("Writer is not opened")
        self._file.write(json.dumps(payload, separators=(",", ":")) + "\n")
        self._file.flush()


# Administra una cola en disco para reintentos de envío.
class DiskSpool:
    # Inicializa la carpeta de spool y los archivos auxiliares.
    def __init__(self, root_path: str) -> None:
        self.root = Path(root_path)
        self.queue_file = self.root / "queue.jsonl"
        self.root.mkdir(parents=True, exist_ok=True)

    # Añade un evento a la cola persistente.
    def append(self, payload: Dict[str, object]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        with self.queue_file.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, separators=(",", ":")) + "\n")

    # Reprocesa los eventos pendientes intentando reenviarlos.
    def flush(self, sender: Callable[[Dict[str, object]], None], batch_size: int = 500) -> bool:
        if not self.queue_file.exists():
            return True
        try:
            lines = self.queue_file.read_text(encoding="utf-8").splitlines()
        except OSError:
            return False
        if not lines:
            try:
                self.queue_file.unlink()
            except FileNotFoundError:
                pass
            return True

        processed = 0
        remainder: List[str] = []
        success = True
        for index, line in enumerate(lines):
            if batch_size and processed >= batch_size:
                remainder.extend(lines[index:])
                break
            processed += 1
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            try:
                sender(payload)
            except Exception:
                remainder.extend(lines[index:])
                success = False
                break

        try:
            if remainder:
                with self.queue_file.open("w", encoding="utf-8") as handle:
                    for entry in remainder:
                        handle.write(entry + "\n")
            else:
                self.queue_file.unlink()
        except OSError:
            success = False
        return success

    # Indica si existen eventos pendientes en la cola.
    def has_pending(self) -> bool:
        return self.queue_file.exists() and self.queue_file.stat().st_size > 0


# Produce eventos hacia Kafka gestionando reconexiones y spool.
class KafkaWriter:
    # Configura parámetros de Kafka y la cola local.
    def __init__(self, bootstrap_servers: str, topic: str, spool_path: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self._producer: Optional[KafkaProducer] = None
        self._bootstrap_targets = [server.strip() for server in bootstrap_servers.split(",") if server.strip()]
        self._spool = DiskSpool(spool_path)
        self._spool_batch = 500

    # Abre el productor y drena la cola antes de publicar.
    def __enter__(self) -> "KafkaWriter":
        self._ensure_producer()
        if self._producer is not None:
            self._drain_backlog()
            LOG.info("Publishing synthetic stream to Kafka topic %s", self.topic)
        else:
            LOG.warning("Kafka no disponible; se usará cola local hasta recuperar la conexión")
        return self

    # Libera recursos de Kafka asegurando flush final.
    def __exit__(self, exc_type, exc, tb) -> None:
        if self._producer is not None:
            try:
                self._drain_backlog()
                self._producer.flush()
            finally:
                self._producer.close()
                self._producer = None

    # Envía el evento a Kafka o lo redirige al spool si falla.
    def write(self, payload: Dict[str, object]) -> None:
        if self._producer is None:
            self._ensure_producer()
        if self._producer is not None:
            self._drain_backlog()
            if self._try_send(payload):
                return
        self._spool.append(payload)

    # Crea el productor de Kafka si aún no existe.
    def _ensure_producer(self) -> None:
        if self._producer is not None:
            return
        if not self._bootstrap_targets:
            raise RuntimeError("Kafka bootstrap servers not configured")
        try:
            self._producer = KafkaProducer(
                bootstrap_servers=self._bootstrap_targets,
                value_serializer=lambda payload: json.dumps(payload, separators=(",", ":")).encode("utf-8"),
                linger_ms=5,
                acks="all",
                retries=0,
                max_in_flight_requests_per_connection=1,
            )
        except (NoBrokersAvailable, KafkaTimeoutError, KafkaError) as exc:
            LOG.warning("Kafka broker no disponible (%s); eventos serán almacenados localmente", exc)
            self._producer = None

    # Intenta publicar un evento en Kafka retornando el resultado.
    def _try_send(self, payload: Dict[str, object]) -> bool:
        if self._producer is None:
            return False
        try:
            future = self._producer.send(self.topic, payload)
            future.get(timeout=10)
            return True
        except (KafkaTimeoutError, KafkaError) as exc:
            LOG.warning("Fallo al publicar en Kafka; evento se envía a cola local (%s)", exc)
            self._close_producer()
            return False

    # Cierra el productor activo y libera recursos.
    def _close_producer(self) -> None:
        if self._producer is not None:
            try:
                self._producer.close()
            finally:
                self._producer = None

    # Reenvía los eventos pendientes del spool a Kafka.
    def _drain_backlog(self) -> None:
        if self._producer is None:
            return
        drained = self._spool.flush(self._ensure_delivery, batch_size=self._spool_batch)
        if not drained and self._spool.has_pending():
            LOG.debug("Persisten eventos pendientes en el spool local (%s)", self._spool.queue_file)

    # Garantiza que un evento termine en Kafka o arroje error.
    def _ensure_delivery(self, payload: Dict[str, object]) -> None:
        if self._producer is None:
            self._ensure_producer()
            if self._producer is None:
                raise KafkaError("Kafka producer not available")
        future = self._producer.send(self.topic, payload)
        future.get(timeout=10)


# Coordina múltiples writers para publicar en paralelo.
class MultiWriter:
    # Recibe la lista de writers a administrar.
    def __init__(self, writers: Sequence[object]) -> None:
        self._writers = list(writers)
        self._active: List[object] = []

    # Entra en el contexto de todos los writers disponibles.
    def __enter__(self) -> "MultiWriter":
        self._active = []
        for writer in self._writers:
            entered = writer.__enter__() if hasattr(writer, "__enter__") else writer
            self._active.append(entered)
        return self

    # Sale de los contextos abiertos y limpia el estado activo.
    def __exit__(self, exc_type, exc, tb) -> None:
        for writer in reversed(self._writers):
            if hasattr(writer, "__exit__"):
                writer.__exit__(exc_type, exc, tb)
        self._active = []

    # Propaga la escritura del evento a cada writer activo.
    def write(self, payload: Dict[str, object]) -> None:
        for writer in self._active:
            writer.write(payload)

    # Reinicia los writers para renovar archivos o conexiones.
    def rotate(self) -> None:
        if not self._writers:
            return
        self.__exit__(None, None, None)
        self.__enter__()


# Genera eventos individuales basados en un perfil de carretera.
class RoadRecordGenerator:
    # Recibe el perfil de carretera y la fuente de aleatoriedad.
    def __init__(self, road: RoadProfile, rng: random.Random) -> None:
        self.road = road
        self.rng = rng

    # Obtiene el perfil horario correspondiente al instante actual.
    def _hour_profile(self, hour: int) -> RoadHourProfile:
        return self.road.hourly_profile.get(hour, RoadHourProfile(
            mean_per_minute=self.road.baseline_rate_per_minute,
            std_per_minute=max(self.road.baseline_rate_per_minute * 0.1, 0.01),
            observations=0,
        ))

    # Calcula una tasa de generación respetando la distribución horaria.
    def _sample_rate(self, hour_profile: RoadHourProfile) -> float:
        mean = max(hour_profile.mean_per_minute, 0.01)
        std = hour_profile.std_per_minute
        if std <= 0.0:
            std = max(mean * 0.1, 0.01)
        rate = self.rng.gauss(mean, std)
        return max(rate, 0.01)

    # Elige una dirección de viaje acorde al perfil.
    def _pick_direction(self) -> str:
        return _weighted_choice(self.road.direction_distribution, self.rng, "Unknown")

    # Selecciona la categoría de vehículo a generar.
    def _pick_vehicle_category(self) -> str:
        return _weighted_choice(self.road.vehicle_distribution, self.rng, "car_and_taxi")

    # Muestra coordenadas geográficas de forma consistente.
    def _sample_location(self) -> Tuple[float, float, float, float]:
        latitude = self.rng.gauss(self.road.latitude_mean, self.road.latitude_std)
        longitude = self.rng.gauss(self.road.longitude_mean, self.road.longitude_std)
        easting = self.rng.gauss(self.road.easting_mean, self.road.easting_std)
        northing = self.rng.gauss(self.road.northing_mean, self.road.northing_std)
        return latitude, longitude, easting, northing

    # Determina la longitud del tramo en kilómetros y millas.
    def _sample_link_length(self) -> Tuple[float, float]:
        km = max(self.rng.gauss(self.road.link_length_km_mean, self.road.link_length_km_std), 0.05)
        miles = max(self.rng.gauss(self.road.link_length_miles_mean, self.road.link_length_miles_std), 0.03)
        return km, miles

    # Construye un evento completo y calcula la tasa resultante.
    def generate_record(
        self,
        now: datetime,
        rate_override: Optional[float] = None,
    ) -> Tuple[Dict[str, object], float]:
        hour_profile = self._hour_profile(now.hour)
        rate = self._sample_rate(hour_profile) if rate_override is None else max(rate_override, 0.01)
        direction = self._pick_direction()
        category = self._pick_vehicle_category()
        latitude, longitude, easting, northing = self._sample_location()
        link_km, link_miles = self._sample_link_length()
        record = self._build_record(
            now,
            direction,
            category,
            latitude,
            longitude,
            easting,
            northing,
            link_km,
            link_miles,
            rate,
        )
        return record, rate

    # Genera un evento con marca temporal actual y su periodo objetivo.
    def sample(self) -> Tuple[Dict[str, object], float, float]:
        now = datetime.now(timezone.utc)
        record, rate = self.generate_record(now)
        delay = max(60.0 / rate, 0.05)
        return record, delay, rate

    # Arma el diccionario de salida con campos normalizados.
    def _build_record(
        self,
        now: datetime,
        direction: str,
        category: str,
        latitude: float,
        longitude: float,
        easting: float,
        northing: float,
        link_km: float,
        link_miles: float,
        rate: float,
    ) -> Dict[str, object]:
        counts = {column: 0 for column in VEHICLE_CATEGORY_TO_COLUMN.values()}
        column = VEHICLE_CATEGORY_TO_COLUMN.get(category)
        if column:
            counts[column] = 1
        is_heavy = category in HEAVY_VEHICLE_CATEGORIES

        start_junction = self.road.start_junction or f"{self.road.name}-START"
        end_junction = self.road.end_junction or f"{self.road.name}-END"

        record: Dict[str, object] = {
            "event_id": uuid.uuid4().hex,
            "event_timestamp": now.isoformat(),
            "count_point_identifier": self.road.id.upper().replace(":", "-"),
            "travel_direction": direction,
            "observation_year": now.year,
            "observation_date": now.date().isoformat(),
            "hour_of_day": f"{now.hour:02d}",
            "minute_of_hour": f"{now.minute:02d}",
            "second_of_minute": f"{now.second:02d}",
            "region_name": self.road.region_name,
            "local_authority_name": self.road.local_authority_name,
            "road_id": self.road.id,
            "road_name": self.road.name,
            "road_type": self.road.road_type or "Unknown",
            "start_junction_road_name": start_junction,
            "end_junction_road_name": end_junction,
            "british_national_grid_easting": int(round(easting)),
            "british_national_grid_northing": int(round(northing)),
            "latitude": round(latitude, 6),
            "longitude": round(longitude, 6),
            "link_length_kilometers": round(link_km, 3),
            "link_length_miles": round(link_miles, 3),
            "vehicle_category": category,
            "vehicle_column": column,
            "rate_per_minute_target": round(rate, 5),
            "source": "synthetic",
        }
        record.update(counts)
        record["all_heavy_goods_vehicle_count"] = 1 if is_heavy else 0
        record["all_motor_vehicle_count"] = 1
        return record


# Genera eventos escogiendo carreteras dentro de una región.
class RegionRecordGenerator:
    # Configura el generador regional y crea subgeneradores por carretera.
    def __init__(self, region: RegionProfile, rng: random.Random) -> None:
        if not region.roads:
            raise ValueError(f"Region '{region.id}' does not contain any road profiles")
        self.region = region
        self.rng = rng
        self.road_generators: Dict[str, RoadRecordGenerator] = {
            road_id: RoadRecordGenerator(road_profile, rng)
            for road_id, road_profile in region.roads.items()
        }

    # Obtiene el perfil horario agregado de la región.
    def _hour_profile(self, hour: int) -> RoadHourProfile:
        profile = self.region.hourly_profile.get(hour)
        if profile is not None:
            return profile
        fallback_std = max(self.region.baseline_rate_per_minute * 0.1, 0.05)
        return RoadHourProfile(
            mean_per_minute=self.region.baseline_rate_per_minute,
            std_per_minute=fallback_std,
            observations=0,
        )

    # Estima la tasa regional para la hora solicitada.
    def _sample_rate(self, hour_profile: RoadHourProfile) -> float:
        mean = max(hour_profile.mean_per_minute, 0.1)
        std = hour_profile.std_per_minute
        if std <= 0.0:
            std = max(mean * 0.1, 0.05)
        rate = self.rng.gauss(mean, std)
        return max(rate, 0.1)

    # Calcula los pesos relativos de cada carretera de la región.
    def _road_weights(self, hour: int) -> Dict[str, float]:
        weights: Dict[str, float] = {}
        for road_id, road_profile in self.region.roads.items():
            hour_entry = road_profile.hourly_profile.get(hour)
            base_weight = hour_entry.mean_per_minute if hour_entry else road_profile.baseline_rate_per_minute
            base_weight = max(base_weight, 0.001)
            distribution_weight = max(self.region.road_distribution.get(road_id, 1.0), 0.001)
            weights[road_id] = base_weight * distribution_weight
        if not weights:
            return {road_id: 1.0 for road_id in self.region.roads.keys()}
        return weights

    # Genera un evento regional y devuelve la cadencia objetivo.
    def sample(self) -> Tuple[Dict[str, object], float, float]:
        now = datetime.now(timezone.utc)
        hour_profile = self._hour_profile(now.hour)
        region_rate = self._sample_rate(hour_profile)

        weights = self._road_weights(now.hour)
        fallback_id = next(iter(weights))
        selected_road_id = _weighted_choice(weights, self.rng, fallback_id)
        total_weight = sum(weights.values())
        probability = (
            weights.get(selected_road_id, 0.0) / total_weight
            if total_weight > 0.0
            else 1.0 / max(len(weights), 1)
        )
        road_rate = max(region_rate * probability, 0.05)

        road_generator = self.road_generators[selected_road_id]
        record, _ = road_generator.generate_record(now, road_rate)
        record.setdefault("region_name", self.region.name)
        record["region_id"] = self.region.id
        delay = max(60.0 / max(region_rate, 0.05), 0.05)
        return record, delay, region_rate


# Controla el bucle principal de generación y escritura.
class ProducerRunner:
    # Recibe los componentes principales del productor y la rotación.
    def __init__(
        self,
        generator: RegionRecordGenerator,
        writer: MultiWriter,
        rotation_size: Optional[int],
    ) -> None:
        self.generator = generator
        self.writer = writer
        self.rotation_size = rotation_size if rotation_size and rotation_size > 0 else None

    # Ejecuta la generación continua respetando límites y previsualizaciones.
    def run(self, max_records: Optional[int], preview_records: int) -> None:
        produced = 0
        preview_remaining = max(preview_records, 0)
        next_emit = time.perf_counter()
        with self.writer as out:
            while not STOP_REQUESTED:
                if max_records is not None and produced >= max_records:
                    break
                now_perf = time.perf_counter()
                if now_perf < next_emit:
                    time.sleep(min(next_emit - now_perf, 0.5))
                    continue
                record, delay, rate = self.generator.sample()
                out.write(record)
                produced += 1
                if preview_remaining > 0:
                    LOG.info("Preview record (%.2f/min): %s", rate, record)
                    preview_remaining -= 1
                if self.rotation_size and produced % self.rotation_size == 0:
                    out.rotate()
                if produced % 500 == 0:
                    LOG.info("Generated %s records (current %.2f/min)", produced, rate)
                next_emit = time.perf_counter() + delay
        LOG.info("Producer stopped after emitting %s records", produced)


# Configura el logger global del productor.
def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


# Marca la solicitud de parada al recibir señales del sistema.
def _handle_stop(signum: int, frame: object) -> None:
    del signum, frame
    global STOP_REQUESTED
    STOP_REQUESTED = True
    LOG.info("Termination signal received; stopping producer loop")


# Construye la lista de writers en función de los sinks elegidos.
def build_writers(
    sinks: Sequence[str],
    output_path: str,
    kafka_bootstrap_servers: str,
    kafka_topic: Optional[str],
    spool_path: str,
) -> List[object]:
    writers: List[object] = []
    if "file" in sinks:
        writers.append(JsonlWriter(output_path))
    if "kafka" in sinks:
        if not kafka_topic:
            raise ValueError("Kafka topic must be provided when using the kafka sink")
        writers.append(KafkaWriter(kafka_bootstrap_servers, kafka_topic, spool_path))
    return writers


# Punto de entrada del generador regional.
def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)

    if args.seed is not None:
        random.seed(args.seed)
    rng = random.Random(args.seed)

    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, _handle_stop)

    profiles, profile_source, profile_path = load_profiles(args.profile_path, args.profile_override_path)
    try:
        region_profile = select_region_profile(profiles, args.region_id, args.region_index, args.instance_offset)
    except ValueError as exc:
        LOG.error("Profile selection failed: %s", exc)
        return 1

    try:
        sink_targets = resolve_sink_targets(args.sink, os.environ.get("PRODUCER_SINK"))
    except ValueError as exc:
        LOG.error("%s", exc)
        return 1

    region_slug = _slugify(region_profile.name)
    kafka_topic = args.kafka_topic or f"traffic.raw.{region_slug}"

    try:
        writers = build_writers(
            sink_targets,
            output_path=args.output_path,
            kafka_bootstrap_servers=args.kafka_bootstrap_servers,
            kafka_topic=kafka_topic,
            spool_path=args.spool_path,
        )
    except (ValueError, KafkaError) as exc:
        LOG.error("Failed to configure output sinks: %s", exc)
        return 1

    if not writers:
        LOG.error("No output sink configured; use --sink to select at least one destination")
        return 1

    writer = MultiWriter(writers)
    generator = RegionRecordGenerator(region_profile, rng)
    runner = ProducerRunner(generator, writer, args.rotate_records)

    write_profile_status(args.profile_status_path, profile_source, profile_path, profiles, region_profile)

    LOG.info(
        "Starting regional generator for %s (roads=%s) using sinks: %s",
        region_profile.name,
        len(region_profile.roads),
        ", ".join(sink_targets),
    )

    try:
        runner.run(args.max_records, args.preview_records)
    except KafkaError as exc:
        LOG.error("Producer halted due to Kafka error: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
