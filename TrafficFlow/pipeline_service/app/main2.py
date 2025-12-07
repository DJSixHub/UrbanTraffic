"""Core Kafka pipeline implementation for the traffic flow service."""
from __future__ import annotations

import json
import logging
import os
import re
import signal
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from kafka import KafkaConsumer
from kafka.coordinator.assignors.roundrobin import RoundRobinPartitionAssignor
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata, TopicPartition


LOG = logging.getLogger("pipeline")
STOP_EVENT = threading.Event()
DEFAULT_CONSUMER_TIMEOUT_MS = 1000
MIN_FLUSH_INTERVAL_SECONDS = 5

VEHICLE_FIELDS: Sequence[Tuple[str, str]] = (
    ("pedal_cycle_count", "pedal_cycles"),
    ("two_wheeled_motor_vehicle_count", "two_wheeled_motor_vehicles"),
    ("car_and_taxi_count", "cars_and_taxis"),
    ("bus_and_coach_count", "buses_and_coaches"),
    ("light_goods_vehicle_count", "light_goods_vehicles"),
    ("heavy_goods_vehicle_2_rigid_axles_count", "hgv_2_rigid_axles"),
    ("heavy_goods_vehicle_3_rigid_axles_count", "hgv_3_rigid_axles"),
    ("heavy_goods_vehicle_4_plus_rigid_axles_count", "hgv_4_plus_rigid_axles"),
    ("heavy_goods_vehicle_3_or_4_articulated_axles_count", "hgv_3_or_4_articulated_axles"),
    ("heavy_goods_vehicle_5_articulated_axles_count", "hgv_5_articulated_axles"),
    ("heavy_goods_vehicle_6_articulated_axles_count", "hgv_6_articulated_axles"),
)


class WebHDFSException(RuntimeError):
    """Raised when the WebHDFS API reports an error."""


class WebHDFSClient:
    """Minimal WebHDFS helper supporting directory creation and file writes."""

    def __init__(
        self,
        base_url: str,
        user: str = "hdfs",
        timeout: float = 30.0,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.timeout = timeout
        self.session = session or requests.Session()

    def _build_url(self, path: str, op: str, **params: object) -> str:
        from urllib.parse import quote, urlencode

        normalized = path.lstrip("/")
        encoded_path = quote(normalized, safe="/=")
        query: Dict[str, object] = {"op": op, "user.name": self.user}
        query.update({key: value for key, value in params.items() if value is not None})
        return f"{self.base_url}/webhdfs/v1/{encoded_path}?{urlencode(query)}"

    def _request(self, method: str, url: str, **kwargs: object) -> requests.Response:
        return self.session.request(method, url, timeout=self.timeout, **kwargs)

    def _handle(self, response: requests.Response) -> Dict[str, object]:
        if response.status_code < 400:
            try:
                return response.json()
            except json.JSONDecodeError:
                return {}
        try:
            payload = response.json()
            message = payload.get("RemoteException", {}).get("message")
        except json.JSONDecodeError:
            payload = {}
            message = response.text or response.reason
        raise WebHDFSException(message or f"WebHDFS error {response.status_code}")

    def mkdirs(self, path: str) -> None:
        url = self._build_url(path, "MKDIRS")
        response = self._request("PUT", url)
        payload = self._handle(response)
        if not payload.get("boolean", False):
            raise WebHDFSException(f"Failed to ensure directory exists at {path}")

    def write_file(self, path: str, data: str, overwrite: bool = True) -> None:
        payload = data.encode("utf-8")
        create_url = self._build_url(path, "CREATE", overwrite=str(overwrite).lower())
        response = self._request("PUT", create_url, allow_redirects=False)
        if response.status_code == 307:
            upload_url = response.headers.get("Location")
            if not upload_url:
                raise WebHDFSException("Missing upload redirect for WebHDFS CREATE")
            upload_response = self._request("PUT", upload_url, data=payload)
            self._handle(upload_response)
            return
        if response.status_code in (200, 201):
            upload_response = self._request("PUT", create_url, data=payload)
            self._handle(upload_response)
            return
        self._handle(response)


@dataclass
class Config:
    kafka_bootstrap_servers: Sequence[str]
    kafka_topics: Sequence[str]
    kafka_group_id: Optional[str]
    silver_base_path: str
    gold_output_path: str
    status_path: Path
    webhdfs_url: str
    hdfs_user: str
    flush_interval_seconds: int
    batch_size: int
    log_level: str
    consumer_timeout_ms: int = DEFAULT_CONSUMER_TIMEOUT_MS


@dataclass
class PendingRecord:
    event: Dict[str, object]
    topic: str
    partition: int
    offset: int


def slugify(value: str) -> str:
    lowered = value.lower()
    tokens = re.findall(r"[a-z0-9]+", lowered)
    return "-".join(tokens) or "unknown"


def _safe_int(value: object) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def _safe_float(value: object) -> float:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0.0


def parse_timestamp(value: Optional[str]) -> datetime:
    if isinstance(value, str) and value:
        candidate = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(candidate).astimezone(timezone.utc)
        except ValueError:
            for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z"):
                try:
                    return datetime.strptime(candidate, fmt).astimezone(timezone.utc)
                except ValueError:
                    continue
    return datetime.now(timezone.utc)


def _serialize_records(records: Iterable[Dict[str, object]]) -> str:
    return "\n".join(json.dumps(record, separators=(",", ":")) for record in records) + "\n"


def aggregate_events(events: Sequence[Dict[str, object]], batch_id: int) -> List[Dict[str, object]]:
    # Aggregate per-region and per-authority metrics for downstream gold tables.
    region_rollups: Dict[str, Dict[str, object]] = {}
    authority_rollups: Dict[Tuple[str, str], Dict[str, object]] = {}

    for event in events:
        region_name = str(event.get("region_name") or "Unknown")
        authority_name = str(event.get("local_authority_name") or "Unknown")

        region_bucket = region_rollups.setdefault(
            region_name,
            {
                "region_name": region_name,
                "total_vehicles": 0,
                "event_count": 0,
                "heavy_vehicles": 0,
                **{target: 0 for _, target in VEHICLE_FIELDS},
            },
        )

        authority_bucket = authority_rollups.setdefault(
            (region_name, authority_name),
            {
                "region_name": region_name,
                "local_authority_name": authority_name,
                "total_vehicles": 0,
                "event_count": 0,
                "heavy_vehicles": 0,
                **{target: 0 for _, target in VEHICLE_FIELDS},
            },
        )

        vehicles = _safe_int(event.get("all_motor_vehicle_count"))
        heavy_value = event.get("all_heavy_goods_vehicle_count")
        if heavy_value is None:
            heavy_sum = sum(
                _safe_int(event.get(source))
                for source, _ in VEHICLE_FIELDS
                if "hgv" in source
            )
        else:
            heavy_sum = _safe_int(heavy_value)

        for bucket in (region_bucket, authority_bucket):
            bucket["total_vehicles"] += vehicles
            bucket["event_count"] += 1
            bucket["heavy_vehicles"] += heavy_sum
            for source, target in VEHICLE_FIELDS:
                bucket[target] += _safe_int(event.get(source))

    batch_timestamp = int(time.time())
    results: List[Dict[str, object]] = []

    for entry in region_rollups.values():
        event_count = max(1, int(entry["event_count"]))
        entry["avg_vehicles"] = entry["total_vehicles"] / float(event_count)
        entry.update(
            {
                "batch_id": batch_id,
                "batch_timestamp": batch_timestamp,
                "role": "primary",
                "writer_failover_active": False,
            }
        )
        results.append(entry)

    for entry in authority_rollups.values():
        event_count = max(1, int(entry["event_count"]))
        entry["avg_vehicles"] = entry["total_vehicles"] / float(event_count)
        entry.update(
            {
                "batch_id": batch_id,
                "batch_timestamp": batch_timestamp,
                "role": "authority",
                "writer_failover_active": False,
            }
        )
        results.append(entry)

    return results


def write_status(config: Config, batch_id: int, total_events: int, gold_rows: int) -> None:
    payload = {
        "batch_id": batch_id,
        "last_flush_time": time.time(),
        "total_events": total_events,
        "gold_records": gold_rows,
        "kafka_topics": list(config.kafka_topics),
    }
    try:
        config.status_path.parent.mkdir(parents=True, exist_ok=True)
        config.status_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    except OSError as exc:
        LOG.warning("Failed to update status file %s: %s", config.status_path, exc)


def flush_batches(
    client: WebHDFSClient,
    config: Config,
    events: Sequence[Dict[str, object]],
    batch_id: int,
) -> int:
    if not events:
        return 0

    silver_groups: Dict[Tuple[str, str, str, str], List[Dict[str, object]]] = defaultdict(list)
    for event in events:
        region_name = str(event.get("region_name") or "Unknown")
        region_slug = slugify(region_name)
        timestamp = parse_timestamp(event.get("event_timestamp"))
        dt_value = timestamp.strftime("%Y%m%d")
        hour_value = timestamp.strftime("%H")
        minute_value = timestamp.strftime("%M")
        key = (region_slug, dt_value, hour_value, minute_value)
        silver_groups[key].append(event)

    for (region_slug, dt_value, hour_value, minute_value), records in silver_groups.items():
        directory = f"{config.silver_base_path}/{region_slug}/dt={dt_value}/hour={hour_value}"
        filename = f"{region_slug}_{dt_value}{hour_value}{minute_value}_{uuid.uuid4().hex}.jsonl"
        target = f"{directory}/{filename}".replace("//", "/")
        client.mkdirs(directory)
        client.write_file(target, _serialize_records(records))
        LOG.debug("Wrote %s silver records to %s", len(records), target)

    client.mkdirs(config.gold_output_path)
    gold_records = aggregate_events(events, batch_id)
    gold_target = f"{config.gold_output_path}/batch_{int(time.time())}_{batch_id}.jsonl"
    client.write_file(gold_target, _serialize_records(gold_records))
    LOG.info("Wrote %s gold records to %s", len(gold_records), gold_target)

    write_status(config, batch_id, len(events), len(gold_records))
    return len(gold_records)


def _flush_and_commit(
    consumer: KafkaConsumer,
    client: WebHDFSClient,
    config: Config,
    records: Sequence[PendingRecord],
    batch_id: int,
) -> bool:
    if not records:
        return True

    events = [record.event for record in records]
    try:
        flush_batches(client, config, events, batch_id)
    except (WebHDFSException, requests.RequestException) as exc:
        LOG.error("Failed to persist batch %s: %s", batch_id, exc)
        return False

    commit_map: Dict[TopicPartition, OffsetAndMetadata] = {}
    for record in records:
        partition = TopicPartition(record.topic, record.partition)
        next_offset = record.offset + 1
        current = commit_map.get(partition)
        if current is None or next_offset > current.offset:
            commit_map[partition] = OffsetAndMetadata(next_offset, None)

    if not commit_map:
        return True

    try:
        consumer.commit(offsets=commit_map)
    except KafkaError as exc:
        LOG.error("Failed to commit offsets for batch %s: %s", batch_id, exc)
        return False
    return True


def run_pipeline(config: Config) -> int:
    configure_logging(config.log_level)
    STOP_EVENT.clear()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, _handle_signal)

    client = WebHDFSClient(config.webhdfs_url, config.hdfs_user)

    try:
        consumer = KafkaConsumer(
            *config.kafka_topics,
            bootstrap_servers=list(config.kafka_bootstrap_servers),
            group_id=config.kafka_group_id,
            value_deserializer=lambda value: value.decode("utf-8"),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=config.consumer_timeout_ms,
            partition_assignment_strategy=[RoundRobinPartitionAssignor],
        )
    except KafkaError as exc:
        LOG.error("Failed to connect to Kafka: %s", exc)
        return 1

    pending_records: List[PendingRecord] = []
    last_flush = time.time()
    batch_id = 0

    try:
        while not STOP_EVENT.is_set():
            now = time.time()
            should_flush = pending_records and (
                len(pending_records) >= config.batch_size
                or (now - last_flush) >= config.flush_interval_seconds
            )
            if should_flush:
                if _flush_and_commit(consumer, client, config, pending_records, batch_id):
                    pending_records.clear()
                    batch_id += 1
                    last_flush = now
                else:
                    time.sleep(5)
                    continue

            try:
                polled = consumer.poll(timeout_ms=config.consumer_timeout_ms, max_records=config.batch_size)
            except KafkaError as exc:
                LOG.error("Kafka poll failed: %s", exc)
                time.sleep(5)
                continue

            if not polled:
                continue

            for messages in polled.values():
                for message in messages:
                    try:
                        event = json.loads(message.value)
                    except (TypeError, json.JSONDecodeError):
                        LOG.debug("Discarded malformed payload: %r", message.value)
                        continue
                    pending_records.append(
                        PendingRecord(
                            event=event,
                            topic=message.topic,
                            partition=message.partition,
                            offset=message.offset,
                        )
                    )

        if pending_records:
            if not _flush_and_commit(consumer, client, config, pending_records, batch_id):
                return 1
    finally:
        consumer.close()

    return 0


def parse_topics(raw: str) -> List[str]:
    topics = [topic.strip() for topic in raw.split(",") if topic.strip()]
    if not topics:
        raise SystemExit("KAFKA_TOPICS must include at least one topic")
    return topics


def parse_bootstrap_servers(raw: str) -> List[str]:
    servers = [server.strip() for server in raw.split(",") if server.strip()]
    if not servers:
        raise SystemExit("KAFKA_BOOTSTRAP_SERVERS must include at least one endpoint")
    return servers


def _parse_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise SystemExit(f"{name} must be an integer") from exc


def load_config() -> Config:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topics_raw = os.getenv("KAFKA_TOPICS", "")
    kafka_group_id = os.getenv("KAFKA_GROUP_ID") or None
    silver_base = os.getenv("SILVER_BASE_PATH", "/data/silver/regions").rstrip("/") or "/data/silver/regions"
    gold_path = os.getenv("GOLD_OUTPUT_PATH", "/data/gold/management/primary").rstrip("/") or "/data/gold/management/primary"
    status_path = Path(os.getenv("STATUS_PATH", "/opt/pipeline/status/pipeline.json"))
    webhdfs_url = os.getenv("WEBHDFS_URL", "http://namenode:9870")
    hdfs_user = os.getenv("HDFS_USER", "hdfs")

    flush_interval = max(MIN_FLUSH_INTERVAL_SECONDS, _parse_int_env("FLUSH_INTERVAL_SECONDS", 20))
    batch_size = max(1, _parse_int_env("BATCH_SIZE", 500))
    consumer_timeout = max(100, _parse_int_env("KAFKA_CONSUMER_TIMEOUT_MS", DEFAULT_CONSUMER_TIMEOUT_MS))
    log_level = os.getenv("LOG_LEVEL", "INFO")

    return Config(
        kafka_bootstrap_servers=parse_bootstrap_servers(kafka_bootstrap),
        kafka_topics=parse_topics(kafka_topics_raw),
        kafka_group_id=kafka_group_id,
        silver_base_path=silver_base,
        gold_output_path=gold_path,
        status_path=status_path,
        webhdfs_url=webhdfs_url,
        hdfs_user=hdfs_user,
        flush_interval_seconds=flush_interval,
        batch_size=batch_size,
        log_level=log_level,
        consumer_timeout_ms=consumer_timeout,
    )


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _handle_signal(signum: int, frame: object) -> None:
    del signum, frame
    STOP_EVENT.set()
    LOG.info("Shutdown signal received; stopping pipeline loop")
