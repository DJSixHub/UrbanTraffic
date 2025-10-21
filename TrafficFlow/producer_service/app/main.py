"""Synthetic traffic data producer container entrypoint."""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import re
import signal
import sys
import tempfile
import time
import uuid
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

try:  # pragma: no cover - optional dependency for runtime container
	from kafka import KafkaProducer  # type: ignore
except ImportError:  # pragma: no cover - handled gracefully when Kafka is unused
	KafkaProducer = None  # type: ignore

from .webhdfs_client import WebHDFSClient, WebHDFSException

LOG = logging.getLogger("producer")
STOP_REQUESTED = False
KM_TO_MILES = 0.621371
SINK_CHOICES = {"file", "kafka", "hdfs"}
SINK_ALIASES = {
	"both": {"file", "kafka"},
	"all": SINK_CHOICES,
}

DEFAULT_NUMERIC_PROFILE = {
	"log_mean": math.log1p(2500.0),
	"log_std": 0.6,
	"p05": 50.0,
	"p95": 90000.0,
}

VEHICLE_COLUMNS: Sequence[str] = (
	"pedal_cycle_count",
	"two_wheeled_motor_vehicle_count",
	"car_and_taxi_count",
	"bus_and_coach_count",
	"light_goods_vehicle_count",
)

HEAVY_COLUMNS: Sequence[str] = (
	"heavy_goods_vehicle_2_rigid_axles_count",
	"heavy_goods_vehicle_3_rigid_axles_count",
	"heavy_goods_vehicle_4_plus_rigid_axles_count",
	"heavy_goods_vehicle_3_or_4_articulated_axles_count",
	"heavy_goods_vehicle_5_articulated_axles_count",
	"heavy_goods_vehicle_6_articulated_axles_count",
)

DEFAULT_PROFILES = {
	"meta": {
		"generated_at": None,
		"source": "embedded defaults",
	},
	"region_distribution": {
		"South West": {"probability": 1.0},
	},
	"local_authority_distribution": {
		"South West": [
			{
				"identifier": "south-west-demo",
				"name": "South West Demo Authority",
				"probability": 1.0,
				"latitude_mean": 50.9,
				"latitude_std": 0.15,
				"longitude_mean": -3.2,
				"longitude_std": 0.15,
				"easting_mean": 220000.0,
				"easting_std": 1500.0,
				"northing_mean": 95000.0,
				"northing_std": 1500.0,
			}
		]
	},
	"road_type_distribution": {
		"Major": 0.45,
		"Minor": 0.3,
		"Motorway": 0.25,
	},
	"travel_direction_distribution": {"E": 0.5, "W": 0.5},
	"hour_distribution": {str(i): 1 / 24 for i in range(24)},
	"day_of_week_distribution": {str(i): 1 / 7 for i in range(1, 8)},
	"link_length_profiles": {
		"Major": {
			"log_mean": math.log1p(3.5),
			"log_std": 0.4,
			"p05": 0.2,
			"p95": 15.0,
		},
		"Minor": {
			"log_mean": math.log1p(1.2),
			"log_std": 0.5,
			"p05": 0.05,
			"p95": 6.0,
		},
		"Motorway": {
			"log_mean": math.log1p(6.0),
			"log_std": 0.35,
			"p05": 0.5,
			"p95": 25.0,
		},
	},
	"density_profiles": {
		"Major": {
			"log_mean": math.log1p(8000.0),
			"log_std": 0.4,
			"p05": 1000.0,
			"p95": 25000.0,
		},
		"Minor": {
			"log_mean": math.log1p(2500.0),
			"log_std": 0.45,
			"p05": 200.0,
			"p95": 9000.0,
		},
		"Motorway": {
			"log_mean": math.log1p(12000.0),
			"log_std": 0.35,
			"p05": 1500.0,
			"p95": 28000.0,
		},
	},
	"heavy_vehicle_share_profile": {
		"Major": {
			"log_mean": math.log1p(0.12),
			"log_std": 0.25,
			"p05": 0.03,
			"p95": 0.28,
		},
		"Minor": {
			"log_mean": math.log1p(0.07),
			"log_std": 0.28,
			"p05": 0.01,
			"p95": 0.2,
		},
		"Motorway": {
			"log_mean": math.log1p(0.18),
			"log_std": 0.22,
			"p05": 0.05,
			"p95": 0.35,
		},
	},
	"vehicle_profiles": {
		"South West": {
			"log_mean": math.log1p(22000.0),
			"log_std": 0.45,
			"p05": 3000.0,
			"p95": 85000.0,
		}
	},
	"vehicle_share_profiles": {
		"__global__": {
			"pedal_cycle_count": {"mean": 0.015, "stddev": 0.01},
			"two_wheeled_motor_vehicle_count": {"mean": 0.02, "stddev": 0.015},
			"car_and_taxi_count": {"mean": 0.72, "stddev": 0.05},
			"bus_and_coach_count": {"mean": 0.02, "stddev": 0.01},
			"light_goods_vehicle_count": {"mean": 0.08, "stddev": 0.03},
		}
	},
	"heavy_breakdown_profiles": {
		"__global__": {
			"heavy_goods_vehicle_2_rigid_axles_count": {"mean": 0.18, "stddev": 0.04},
			"heavy_goods_vehicle_3_rigid_axles_count": {"mean": 0.16, "stddev": 0.04},
			"heavy_goods_vehicle_4_plus_rigid_axles_count": {"mean": 0.14, "stddev": 0.04},
			"heavy_goods_vehicle_3_or_4_articulated_axles_count": {"mean": 0.18, "stddev": 0.04},
			"heavy_goods_vehicle_5_articulated_axles_count": {"mean": 0.17, "stddev": 0.04},
			"heavy_goods_vehicle_6_articulated_axles_count": {"mean": 0.17, "stddev": 0.04},
		}
	},
}


@dataclass
class Profiles:
	data: Dict[str, object]

	@property
	def region_distribution(self) -> Dict[str, Dict[str, float]]:
		return self.data.get("region_distribution", {})  # type: ignore[return-value]

	@property
	def local_authorities(self) -> Dict[str, List[Dict[str, float]]]:
		return self.data.get("local_authority_distribution", {})  # type: ignore[return-value]

	@property
	def road_type_distribution(self) -> Dict[str, float]:
		return self.data.get("road_type_distribution", {})  # type: ignore[return-value]

	@property
	def travel_direction_distribution(self) -> Dict[str, float]:
		return self.data.get("travel_direction_distribution", {})  # type: ignore[return-value]

	@property
	def hour_distribution(self) -> Dict[str, float]:
		return self.data.get("hour_distribution", {})  # type: ignore[return-value]

	@property
	def day_of_week_distribution(self) -> Dict[str, float]:
		return self.data.get("day_of_week_distribution", {})  # type: ignore[return-value]

	@property
	def link_length_profiles(self) -> Dict[str, Dict[str, float]]:
		return self.data.get("link_length_profiles", {})  # type: ignore[return-value]

	@property
	def density_profiles(self) -> Dict[str, Dict[str, float]]:
		return self.data.get("density_profiles", {})  # type: ignore[return-value]

	@property
	def heavy_share_profiles(self) -> Dict[str, Dict[str, float]]:
		return self.data.get("heavy_vehicle_share_profile", {})  # type: ignore[return-value]

	@property
	def vehicle_profiles(self) -> Dict[str, Dict[str, float]]:
		return self.data.get("vehicle_profiles", {})  # type: ignore[return-value]

	@property
	def vehicle_share_profiles(self) -> Dict[str, Dict[str, Dict[str, float]]]:
		return self.data.get("vehicle_share_profiles", {})  # type: ignore[return-value]

	@property
	def heavy_breakdown_profiles(self) -> Dict[str, Dict[str, Dict[str, float]]]:
		return self.data.get("heavy_breakdown_profiles", {})  # type: ignore[return-value]


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Synthetic traffic producer")
	parser.add_argument(
		"--profile-path",
		default=os.environ.get("PROFILES_PATH", "/opt/producer/profiles/distributions.json"),
		help="Path to JSON file with probability distributions",
	)
	parser.add_argument(
		"--output-path",
		default=os.environ.get("OUTPUT_PATH", "/opt/producer/output/traffic_stream.jsonl"),
		help="Destination file for newline-delimited JSON output",
	)
	parser.add_argument(
		"--sink",
		action="append",
		help="Output sink(s) (file, kafka, hdfs, both, all). Can be repeated or comma separated",
	)
	parser.add_argument(
		"--kafka-bootstrap",
		default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
		help="Kafka bootstrap servers (comma separated)",
	)
	parser.add_argument(
		"--kafka-topic",
		default=os.environ.get("KAFKA_TOPIC", "traffic.synthetic"),
		help="Kafka topic where synthetic records will be published",
	)
	parser.add_argument(
		"--webhdfs-url",
		default=os.environ.get("WEBHDFS_URL", "http://namenode:9870"),
		help="Base URL for the WebHDFS endpoint",
	)
	parser.add_argument(
		"--hdfs-user",
		default=os.environ.get("HDFS_USER", "hdfs"),
		help="HDFS username used for WebHDFS operations",
	)
	parser.add_argument(
		"--hdfs-base-path",
		default=os.environ.get("HDFS_BASE_PATH", "/data/gold/synthetic"),
		help="Root directory (in HDFS) where synthetic batches are landed",
	)
	parser.add_argument(
		"--hdfs-partition-template",
		default=os.environ.get("HDFS_PARTITION_TEMPLATE", "dt=%Y%m%d"),
		help="strftime template used for subdirectories inside HDFS base path",
	)
	parser.add_argument(
		"--hdfs-file-template",
		default=os.environ.get(
			"HDFS_FILE_TEMPLATE",
			"traffic_stream_%Y%m%dT%H%M%SZ_{uuid}.jsonl",
		),
		help="strftime template for the generated file name (supports {uuid} placeholder)",
	)
	parser.add_argument(
		"--rate-per-minute",
		type=float,
		default=float(os.environ.get("RATE_PER_MINUTE", 1500.0)),
		help="Target emission rate (records per minute)",
	)
	parser.add_argument(
		"--rotate-records",
		type=int,
		default=int(os.environ.get("ROTATE_RECORDS", "500")),
		help="Number of records emitted before flushing and reopening sinks (useful for HDFS uploads)",
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
	return parser.parse_args(list(argv) if argv is not None else None)


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


def load_profiles(path: str) -> Profiles:
	base = deepcopy(DEFAULT_PROFILES)
	candidate = Path(path)
	if candidate.exists():
		try:
			with candidate.open("r", encoding="utf-8") as handle:
				data = json.load(handle)
			if isinstance(data, dict):
				base.update(data)
				LOG.info("Loaded profiles from %s", candidate)
		except (json.JSONDecodeError, OSError) as exc:
			LOG.warning("Failed to load profiles from %s (%s); using defaults", candidate, exc)
	else:
		LOG.warning("Profile file %s not found; using embedded defaults", candidate)
	return Profiles(base)


def _resolve_probability(raw_value: object) -> float:
	if isinstance(raw_value, dict):
		return float(raw_value.get("probability", 0.0))
	if isinstance(raw_value, (int, float)):
		return float(raw_value)
	return 0.0


def weighted_choice_from_mapping(mapping: Dict[str, object]) -> Tuple[str, object]:
	if not mapping:
		raise ValueError("Empty distribution provided")
	total = sum(max(_resolve_probability(value), 0.0) for value in mapping.values())
	if total <= 0.0:
		return random.choice(list(mapping.items()))
	target = random.random() * total
	cumulative = 0.0
	for key, raw_value in mapping.items():
		cumulative += max(_resolve_probability(raw_value), 0.0)
		if cumulative >= target:
			return key, raw_value
	return next(reversed(mapping.items()))


def weighted_choice_from_list(options: List[Dict[str, float]]) -> Dict[str, float]:
	if not options:
		raise ValueError("Empty distribution list provided")
	total = sum(max(float(option.get("probability", 0.0)), 0.0) for option in options)
	if total <= 0.0:
		return random.choice(options)
	target = random.random() * total
	cumulative = 0.0
	for option in options:
		cumulative += max(float(option.get("probability", 0.0)), 0.0)
		if cumulative >= target:
			return option
	return options[-1]


def sample_numeric(profile_map: Dict[str, Dict[str, float]], key: str) -> float:
	profile = profile_map.get(key)
	if profile is None and profile_map:
		profile = next(iter(profile_map.values()))
	if profile is None:
		profile = DEFAULT_NUMERIC_PROFILE
	mu = float(profile.get("log_mean", DEFAULT_NUMERIC_PROFILE["log_mean"]))
	sigma = max(float(profile.get("log_std", DEFAULT_NUMERIC_PROFILE["log_std"])), 1e-3)
	raw = math.exp(random.gauss(mu, sigma)) - 1.0
	lower = float(profile.get("p05", DEFAULT_NUMERIC_PROFILE["p05"]))
	upper = float(profile.get("p95", DEFAULT_NUMERIC_PROFILE["p95"]))
	if lower > upper:
		lower, upper = upper, lower
	return min(max(raw, lower), upper)


def sample_share(
	profile_map: Dict[str, Dict[str, Dict[str, float]]],
	group_key: str,
	category: str,
	default_source: Dict[str, Dict[str, Dict[str, float]]],
	default_mean: float = 0.05,
	default_std: float = 0.02,
) -> float:
	stats: Optional[Dict[str, float]] = None
	group = profile_map.get(group_key)
	if isinstance(group, dict):
		stats = group.get(category)
	if stats is None:
		stats = profile_map.get("__global__", {}).get(category)
	if stats is None:
		stats = default_source.get(group_key, {}).get(category)
	if stats is None:
		stats = default_source.get("__global__", {}).get(category)
	mean = float((stats or {}).get("mean", default_mean))
	std = float((stats or {}).get("stddev", default_std))
	value = random.gauss(mean, max(std, 1e-3))
	return min(max(value, 0.0), 0.95)


class JsonlWriter:
	def __init__(self, path: str) -> None:
		self._path = Path(path)
		self._file: Optional[object] = None

	def __enter__(self) -> "JsonlWriter":
		self._path.parent.mkdir(parents=True, exist_ok=True)
		self._file = self._path.open("a", encoding="utf-8")
		LOG.info("Writing synthetic stream to %s", self._path)
		return self

	def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
		if self._file:
			self._file.flush()
			self._file.close()
			self._file = None

	def write(self, payload: Dict[str, object]) -> None:
		if self._file is None:
			raise RuntimeError("Writer is not opened")
		self._file.write(json.dumps(payload, separators=(",", ":")) + "\n")
		self._file.flush()


class KafkaWriter:
	def __init__(self, bootstrap: str, topic: str) -> None:
		if KafkaProducer is None:
			raise RuntimeError("kafka-python is required for Kafka sink but is not installed")
		servers = [server.strip() for server in bootstrap.split(",") if server.strip()]
		if not servers:
			raise ValueError("At least one Kafka bootstrap server must be provided")
		self._producer = KafkaProducer(  # type: ignore[call-arg]
			bootstrap_servers=servers,
			value_serializer=lambda value: json.dumps(value).encode("utf-8"),
		)
		self._topic = topic
		LOG.info("Kafka writer initialised for topic %s", topic)

	def __enter__(self) -> "KafkaWriter":
		return self

	def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
		self._producer.flush()
		self._producer.close()

	def write(self, payload: Dict[str, object]) -> None:
		self._producer.send(self._topic, payload)


class WebHDFSWriter:
	def __init__(
		self,
		client: WebHDFSClient,
		base_path: str,
		partition_template: str,
		file_template: str,
		overwrite: bool = True,
	) -> None:
		self.client = client
		self.base_path = base_path.rstrip("/") or "/"
		self.partition_template = partition_template
		self.file_template = file_template
		self.overwrite = overwrite
		self._temp_file: Optional[tempfile.NamedTemporaryFile] = None
		self._local_path: Optional[str] = None
		self.hdfs_path: Optional[str] = None

	def __enter__(self) -> "WebHDFSWriter":
		now = datetime.now(timezone.utc)
		partition = now.strftime(self.partition_template)
		directory = f"{self.base_path}/{partition}".replace("//", "/")
		filename = now.strftime(self.file_template).format(uuid=uuid.uuid4().hex)
		self.hdfs_path = f"{directory}/{filename}".replace("//", "/")
		self.client.mkdirs(directory)
		self._temp_file = tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False)
		self._local_path = self._temp_file.name
		LOG.info("Buffering synthetic batch before upload to HDFS path %s", self.hdfs_path)
		return self

	def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
		if not self._temp_file or not self._local_path or not self.hdfs_path:
			return
		try:
			self._temp_file.flush()
			self._temp_file.close()
			self.client.upload_file(self._local_path, self.hdfs_path, overwrite=self.overwrite)
			LOG.info("Uploaded batch to HDFS: %s", self.hdfs_path)
		except WebHDFSException as web_exc:
			LOG.error("Failed to upload synthetic batch to HDFS (%s)", web_exc)
			raise
		finally:
			try:
				os.unlink(self._local_path)
			except OSError:
				LOG.debug("Temporary file %s already removed", self._local_path)
			self._temp_file = None
			self._local_path = None

	def write(self, payload: Dict[str, object]) -> None:
		if self._temp_file is None:
			raise RuntimeError("Writer is not opened")
		self._temp_file.write(json.dumps(payload, separators=(",", ":")) + "\n")
		self._temp_file.flush()


class MultiWriter:
	def __init__(self, writers: Sequence[object]) -> None:
		self._writers = list(writers)
		self._active: List[object] = []

	def __enter__(self) -> "MultiWriter":
		self._active = []
		for writer in self._writers:
			entered = writer.__enter__() if hasattr(writer, "__enter__") else writer
			self._active.append(entered)
		return self

	def __exit__(self, exc_type, exc, tb) -> None:  # type: ignore[override]
		for writer in reversed(self._writers):
			if hasattr(writer, "__exit__"):
				writer.__exit__(exc_type, exc, tb)
		self._active = []

	def write(self, payload: Dict[str, object]) -> None:
		for writer in self._active:
			writer.write(payload)

	def rotate(self) -> None:
		"""Force all writers to close and reopen to flush intermediate buffers."""
		if not self._writers:
			return
		self.__exit__(None, None, None)
		self.__enter__()


class SyntheticRecordGenerator:
	def __init__(self, profiles: Profiles) -> None:
		self.profiles = profiles

	def _pick_region(self) -> Tuple[str, Dict[str, float]]:
		name, payload = weighted_choice_from_mapping(self.profiles.region_distribution)
		if not isinstance(payload, dict):
			payload = {"probability": payload}
		return name, payload

	def _pick_authority(self, region: str) -> Dict[str, float]:
		options = self.profiles.local_authorities.get(region) or []
		if not options:
			options = [
				{
					"identifier": f"{region}-authority",
					"name": f"{region} Authority",
					"probability": 1.0,
					"latitude_mean": 53.0,
					"latitude_std": 0.2,
					"longitude_mean": -1.5,
					"longitude_std": 0.2,
					"easting_mean": 225000.0,
					"easting_std": 2000.0,
					"northing_mean": 150000.0,
					"northing_std": 2000.0,
				}
			]
		return weighted_choice_from_list(options)

	def _pick_direction(self) -> str:
		direction, _ = weighted_choice_from_mapping(
			self.profiles.travel_direction_distribution or {"E": 0.5, "W": 0.5}
		)
		return direction

	def _pick_road_type(self) -> str:
		road_type, _ = weighted_choice_from_mapping(self.profiles.road_type_distribution)
		return road_type

	def _pick_hour(self) -> int:
		hour_key, _ = weighted_choice_from_mapping(
			self.profiles.hour_distribution or {str(i): 1 / 24 for i in range(24)}
		)
		return int(hour_key)

	def _pick_day_of_week(self) -> int:
		day_key, _ = weighted_choice_from_mapping(
			self.profiles.day_of_week_distribution or {str(i): 1 / 7 for i in range(1, 8)}
		)
		return int(day_key)

	def _build_timestamp(self, base_time: datetime, hour: int, target_day: int) -> datetime:
		iso_today = base_time.isoweekday()
		spark_today = (iso_today % 7) + 1
		delta_days = (target_day - spark_today) % 7
		date_target = (base_time + timedelta(days=delta_days)).date()
		minute = random.randint(0, 59)
		return datetime(
			date_target.year,
			date_target.month,
			date_target.day,
			hour,
			minute,
			tzinfo=timezone.utc,
		)

	@staticmethod
	def _sample_coordinate(mean: float, std: float, minimum_std: float = 0.01) -> float:
		std = max(std, minimum_std)
		return random.gauss(mean, std)

	def _build_vehicle_breakdown(
		self,
		road_type: str,
		total: int,
		heavy_share: float,
	) -> Tuple[Dict[str, int], Dict[str, int], int]:
		vehicle_shares: Dict[str, float] = {}
		for column in VEHICLE_COLUMNS:
			vehicle_shares[column] = sample_share(
				self.profiles.vehicle_share_profiles,
				road_type,
				column,
				DEFAULT_PROFILES["vehicle_share_profiles"],
				default_mean=0.05,
				default_std=0.03,
			)
		remainder = max(1.0 - heavy_share, 0.0)
		share_total = sum(vehicle_shares.values())
		scale = (remainder / share_total) if share_total > 0 else 0.0
		counts: Dict[str, int] = {
			column: max(int(round(total * share * scale)), 0)
			for column, share in vehicle_shares.items()
		}

		heavy_total = max(int(round(total * heavy_share)), 0)
		heavy_shares: Dict[str, float] = {}
		for column in HEAVY_COLUMNS:
			heavy_shares[column] = sample_share(
				self.profiles.heavy_breakdown_profiles,
				road_type,
				column,
				DEFAULT_PROFILES["heavy_breakdown_profiles"],
				default_mean=1.0 / max(len(HEAVY_COLUMNS), 1),
				default_std=0.05,
			)
		heavy_share_total = sum(heavy_shares.values())
		heavy_scale = (1.0 / heavy_share_total) if heavy_share_total > 0 else 0.0
		heavy_counts: Dict[str, int] = {
			column: max(int(round(heavy_total * share * heavy_scale)), 0)
			for column, share in heavy_shares.items()
		}

		assigned = sum(counts.values()) + heavy_total
		if assigned != total:
			diff = total - assigned
			counts.setdefault("car_and_taxi_count", 0)
			counts["car_and_taxi_count"] = max(counts["car_and_taxi_count"] + diff, 0)

		heavy_assigned = sum(heavy_counts.values())
		if heavy_assigned != heavy_total and heavy_counts:
			key = max(heavy_counts, key=heavy_counts.get)
			heavy_counts[key] = max(heavy_counts[key] + (heavy_total - heavy_assigned), 0)

		return counts, heavy_counts, heavy_total

	def _build_road_names(self, road_type: str) -> Tuple[str, str, str]:
		road_prefix = {
			"Motorway": "M",
			"Trunk Road": "A",
			"Primary A Road": "A",
			"Major": "A",
			"Secondary B Road": "B",
			"Minor Road": "C",
			"Minor": "C",
		}.get(road_type, "R")
		road_number = random.randint(1, 9999)
		road_name = f"{road_prefix}{road_number}"
		start_junction = f"{road_name}-J{random.randint(1, 50)}"
		end_junction = f"{road_name}-J{random.randint(51, 99)}"
		return road_name, start_junction, end_junction

	def sample(self, base_time: Optional[datetime] = None) -> Dict[str, object]:
		base_time = base_time or datetime.now(timezone.utc)
		region_name, _ = self._pick_region()
		authority = self._pick_authority(region_name)
		direction = self._pick_direction()
		road_type = self._pick_road_type()
		hour = self._pick_hour()
		day = self._pick_day_of_week()
		event_ts = self._build_timestamp(base_time, hour, day)

		link_length = sample_numeric(self.profiles.link_length_profiles, road_type)
		density = sample_numeric(self.profiles.density_profiles, road_type)
		regional_volume = sample_numeric(self.profiles.vehicle_profiles, region_name)
		heavy_share = sample_numeric(self.profiles.heavy_share_profiles, road_type)
		heavy_share = max(min(heavy_share, 0.95), 0.0)

		vehicles = max((density * link_length + regional_volume) / 2.0, 1.0)
		total_count = max(int(round(vehicles)), 1)

		counts, heavy_counts, heavy_total = self._build_vehicle_breakdown(
			road_type, total_count, heavy_share
		)

		latitude = self._sample_coordinate(
			float(authority.get("latitude_mean", 53.0)),
			float(authority.get("latitude_std", 0.1)),
		)
		longitude = self._sample_coordinate(
			float(authority.get("longitude_mean", -1.5)),
			float(authority.get("longitude_std", 0.1)),
		)
		easting = self._sample_coordinate(
			float(authority.get("easting_mean", 225000.0)),
			float(authority.get("easting_std", 2000.0)),
			minimum_std=100.0,
		)
		northing = self._sample_coordinate(
			float(authority.get("northing_mean", 150000.0)),
			float(authority.get("northing_std", 2000.0)),
			minimum_std=100.0,
		)
		road_name, start_junction, end_junction = self._build_road_names(road_type)

		return {
			"count_point_identifier": f"CP-{uuid.uuid4().hex[:10].upper()}",
			"travel_direction": direction,
			"observation_year": event_ts.year,
			"observation_date": event_ts.date().isoformat(),
			"hour_of_day": f"{hour:02d}",
			"region_name": region_name,
			"local_authority_name": authority.get("name", f"{region_name} Authority"),
			"road_name": road_name,
			"road_type": road_type,
			"start_junction_road_name": start_junction,
			"end_junction_road_name": end_junction,
			"british_national_grid_easting": int(round(easting)),
			"british_national_grid_northing": int(round(northing)),
			"latitude": round(latitude, 6),
			"longitude": round(longitude, 6),
			"link_length_kilometers": round(link_length, 3),
			"link_length_miles": round(link_length * KM_TO_MILES, 3),
			"pedal_cycle_count": counts.get("pedal_cycle_count", 0),
			"two_wheeled_motor_vehicle_count": counts.get("two_wheeled_motor_vehicle_count", 0),
			"car_and_taxi_count": counts.get("car_and_taxi_count", 0),
			"bus_and_coach_count": counts.get("bus_and_coach_count", 0),
			"light_goods_vehicle_count": counts.get("light_goods_vehicle_count", 0),
			"heavy_goods_vehicle_2_rigid_axles_count": heavy_counts.get(
				"heavy_goods_vehicle_2_rigid_axles_count", 0
			),
			"heavy_goods_vehicle_3_rigid_axles_count": heavy_counts.get(
				"heavy_goods_vehicle_3_rigid_axles_count", 0
			),
			"heavy_goods_vehicle_4_plus_rigid_axles_count": heavy_counts.get(
				"heavy_goods_vehicle_4_plus_rigid_axles_count", 0
			),
			"heavy_goods_vehicle_3_or_4_articulated_axles_count": heavy_counts.get(
				"heavy_goods_vehicle_3_or_4_articulated_axles_count", 0
			),
			"heavy_goods_vehicle_5_articulated_axles_count": heavy_counts.get(
				"heavy_goods_vehicle_5_articulated_axles_count", 0
			),
			"heavy_goods_vehicle_6_articulated_axles_count": heavy_counts.get(
				"heavy_goods_vehicle_6_articulated_axles_count", 0
			),
			"all_heavy_goods_vehicle_count": heavy_total,
			"all_motor_vehicle_count": total_count,
		}


class ProducerRunner:
	def __init__(
		self,
		generator: SyntheticRecordGenerator,
		writer: MultiWriter,
		rate_per_minute: float,
		rotation_size: Optional[int],
	) -> None:
		self.generator = generator
		self.writer = writer
		self.interval = 60.0 / max(rate_per_minute, 1.0)
		self.rotation_size = rotation_size if rotation_size and rotation_size > 0 else None

	def run(self, max_records: Optional[int], preview_records: int) -> None:
		produced = 0
		next_emit = time.perf_counter()
		preview_remaining = max(preview_records, 0)
		with self.writer as out:
			while not STOP_REQUESTED:
				if max_records is not None and produced >= max_records:
					break
				now = time.perf_counter()
				if now < next_emit:
					time.sleep(min(self.interval, next_emit - now))
				record = self.generator.sample()
				out.write(record)
				produced += 1
				if preview_remaining > 0:
					LOG.info("Preview record: %s", record)
					preview_remaining -= 1
				next_emit += self.interval
				if self.rotation_size and produced % self.rotation_size == 0:
					out.rotate()
				if produced % 500 == 0:
					LOG.info("Generated %s records", produced)
		LOG.info("Producer stopped after emitting %s records", produced)


def configure_logging(level: str) -> None:
	logging.basicConfig(
		level=getattr(logging, level.upper(), logging.INFO),
		format="%(asctime)s %(levelname)s %(name)s - %(message)s",
	)


def _handle_stop(signum: int, frame: object) -> None:  # pragma: no cover - signal handler
	del signum, frame
	global STOP_REQUESTED
	STOP_REQUESTED = True
	LOG.info("Termination signal received; shutting down producer loop")


def main(argv: Optional[Iterable[str]] = None) -> int:
	args = parse_args(argv)
	configure_logging(args.log_level)
	if args.seed is not None:
		random.seed(int(args.seed))
		LOG.info("Random seed set to %s", args.seed)

	signal.signal(signal.SIGTERM, _handle_stop)
	signal.signal(signal.SIGINT, _handle_stop)

	profiles = load_profiles(args.profile_path)
	generator = SyntheticRecordGenerator(profiles)

	try:
		sink_targets = resolve_sink_targets(args.sink, os.environ.get("PRODUCER_SINK"))
	except ValueError as exc:
		LOG.error("%s", exc)
		return 1

	sinks: List[object] = []
	if "file" in sink_targets:
		sinks.append(JsonlWriter(args.output_path))
	if "kafka" in sink_targets:
		try:
			sinks.append(KafkaWriter(args.kafka_bootstrap, args.kafka_topic))
		except Exception as exc:  # pragma: no cover - defer failure reporting to logs
			LOG.error("Failed to initialise Kafka writer: %s", exc)
			return 1
	if "hdfs" in sink_targets:
		client = WebHDFSClient(base_url=args.webhdfs_url, user=args.hdfs_user)
		try:
			sinks.append(
				WebHDFSWriter(
					client=client,
					base_path=args.hdfs_base_path,
					partition_template=args.hdfs_partition_template,
					file_template=args.hdfs_file_template,
				)
			)
		except WebHDFSException as exc:
			LOG.error("Failed to configure WebHDFS writer: %s", exc)
			return 1

	if not sinks:
		LOG.error("No output sink configured; use --sink to select at least one destination")
		return 1

	writer = MultiWriter(sinks)
	runner = ProducerRunner(
		generator,
		writer,
		rate_per_minute=args.rate_per_minute,
		rotation_size=args.rotate_records,
	)

	try:
		runner.run(
			max_records=args.max_records,
			preview_records=args.preview_records,
		)
	except WebHDFSException as exc:
		LOG.error("Producer halted due to HDFS error: %s", exc)
		return 1
	except ValueError as exc:
		LOG.error("Producer halted due to invalid configuration: %s", exc)
		return 1
	return 0


if __name__ == "__main__":
	sys.exit(main())
