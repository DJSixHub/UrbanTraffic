"""Kafka-driven processing pipeline writing silver and gold datasets to HDFS."""
from __future__ import annotations

import fcntl
import json
import logging
import os
import signal
import sys
import threading
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote, urlencode

import requests
from kafka import KafkaConsumer
from kafka.errors import KafkaError

LOG = logging.getLogger("pipeline")
STOP_EVENT = threading.Event()

VEHICLE_FIELDS: Tuple[Tuple[str, str], ...] = (
    ("pedal_cycle_count", "pedal_cycle_total"),
    ("two_wheeled_motor_vehicle_count", "two_wheeled_total"),
    ("car_and_taxi_count", "car_and_taxi_total"),
    ("bus_and_coach_count", "bus_and_coach_total"),
    ("light_goods_vehicle_count", "light_goods_total"),
    ("heavy_goods_vehicle_2_rigid_axles_count", "hgv2_total"),
    ("heavy_goods_vehicle_3_rigid_axles_count", "hgv3_total"),
    ("heavy_goods_vehicle_4_plus_rigid_axles_count", "hgv4_total"),
    ("heavy_goods_vehicle_3_or_4_articulated_axles_count", "hgv34_total"),
    ("heavy_goods_vehicle_5_articulated_axles_count", "hgv5_total"),
    ("heavy_goods_vehicle_6_articulated_axles_count", "hgv6_total"),
)


class WebHDFSException(RuntimeError):
    """Raised when WebHDFS operations fail."""


class WebHDFSClient:
    """Minimal WebHDFS client for directory creation and file uploads."""

    def __init__(self, base_url: str, user: str, timeout: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.timeout = timeout
        self.session = requests.Session()

    def _build_url(self, path: str, op: str, **params: str) -> str:
        normalized = path.lstrip("/")
        encoded_path = quote(normalized, safe="/=")
        query = {"op": op, "user.name": self.user}
        query.update({key: value for key, value in params.items() if value is not None})
        return f"{self.base_url}/webhdfs/v1/{encoded_path}?{urlencode(query)}"

    def _handle_response(self, response: requests.Response) -> None:
        if response.status_code < 400:
            return
        try:
            payload = response.json()
            message = payload.get("RemoteException", {}).get("message")
        except (ValueError, json.JSONDecodeError):
            message = response.text or response.reason
        raise WebHDFSException(message or f"WebHDFS error {response.status_code}")

    def mkdirs(self, path: str) -> None:
        url = self._build_url(path, "MKDIRS")
        response = self.session.put(url, timeout=self.timeout)
        self._handle_response(response)

    def write_file(self, path: str, data: str, overwrite: bool = True) -> None:
        url = self._build_url(path, "CREATE", overwrite=str(overwrite).lower())
        response = self.session.put(url, allow_redirects=False, timeout=self.timeout)
        if response.status_code == 307:
            upload_url = response.headers.get("Location")
            if not upload_url:
                raise WebHDFSException("Missing redirect target for WebHDFS upload")
            upload_resp = self.session.put(upload_url, data=data.encode("utf-8"), timeout=self.timeout)
            self._handle_response(upload_resp)
            return
        self._handle_response(response)


class DiskBacklog:
    """File-backed queue used to persist events when HDFS is unavailable."""

    def __init__(self, path: Path) -> None:
        self.root = Path(path)
        self.root.mkdir(parents=True, exist_ok=True)
        self.file = self.root / "backlog.jsonl"

    def has_events(self) -> bool:
        if not self.file.exists():
            return False
        try:
            return self.file.stat().st_size > 0
        except OSError:
            return False

    def enqueue(self, events: Sequence[Dict[str, object]]) -> None:
        if not events:
            return
        self.root.mkdir(parents=True, exist_ok=True)
        with self.file.open("a", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            for event in events:
                handle.write(json.dumps(event, separators=(",", ":")) + "\n")
            handle.flush()
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def prepend(self, events: Sequence[Dict[str, object]]) -> None:
        if not events:
            return
        self.root.mkdir(parents=True, exist_ok=True)
        if self.file.exists():
            try:
                with self.file.open("r+", encoding="utf-8") as handle:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                    existing = handle.read()
                    handle.seek(0)
                    handle.truncate(0)
                    for event in events:
                        handle.write(json.dumps(event, separators=(",", ":")) + "\n")
                    if existing:
                        handle.write(existing)
                    handle.flush()
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                return
            except OSError:
                pass
        try:
            with self.file.open("w", encoding="utf-8") as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                for event in events:
                    handle.write(json.dumps(event, separators=(",", ":")) + "\n")
                handle.flush()
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass

    def pop_batch(self, limit: int) -> List[Dict[str, object]]:
        if not self.file.exists():
            return []
        lines: List[str] = []
        try:
            with self.file.open("r+", encoding="utf-8") as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                lines = handle.readlines()
                if not lines:
                    handle.seek(0)
                    handle.truncate(0)
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    return []
                take_count = len(lines) if limit <= 0 else min(limit, len(lines))
                remainder = lines[take_count:]
                handle.seek(0)
                handle.truncate(0)
                if remainder:
                    handle.writelines(remainder)
                handle.flush()
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                selected = lines[:take_count]
        except OSError:
            return []

        events: List[Dict[str, object]] = []
        for line in selected:
            if not line.strip():
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue

        return events


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
    spool_path: Path


def slugify(value: str) -> str:
    value = value.strip().lower()
    cleaned: List[str] = []
    for char in value:
        if char.isalnum():
            cleaned.append(char)
        elif cleaned and cleaned[-1] != "-":
            cleaned.append("-")
    slug = "".join(cleaned).strip("-")
    return slug or "unknown"


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


def load_config() -> Config:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_topics_raw = os.getenv("KAFKA_TOPICS", "")
    kafka_group_id = os.getenv("KAFKA_GROUP_ID") or None
    silver_base = os.getenv("SILVER_BASE_PATH", "/data/silver/regions").rstrip("/") or "/data/silver/regions"
    gold_path = os.getenv("GOLD_OUTPUT_PATH", "/data/gold/management/primary").rstrip("/") or "/data/gold/management/primary"
    status_path = Path(os.getenv("STATUS_PATH", "/opt/pipeline/status/pipeline.json"))
    webhdfs_url = os.getenv("WEBHDFS_URL", "http://namenode:9870")
    hdfs_user = os.getenv("HDFS_USER", "hdfs")
    flush_interval = int(os.getenv("FLUSH_INTERVAL_SECONDS", "20"))
    batch_size = int(os.getenv("BATCH_SIZE", "500"))
    log_level = os.getenv("LOG_LEVEL", "INFO")
    spool_path = Path(os.getenv("PIPELINE_SPOOL_PATH", "/opt/pipeline/spool"))

    return Config(
        kafka_bootstrap_servers=parse_bootstrap_servers(kafka_bootstrap),
        kafka_topics=parse_topics(kafka_topics_raw),
        kafka_group_id=kafka_group_id,
        silver_base_path=silver_base,
        gold_output_path=gold_path,
        status_path=status_path,
        webhdfs_url=webhdfs_url,
        hdfs_user=hdfs_user,
        flush_interval_seconds=max(5, flush_interval),
        batch_size=max(1, batch_size),
        log_level=log_level,
        spool_path=spool_path,
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


def parse_timestamp(value: Optional[str]) -> datetime:
    if isinstance(value, str) and value:
        candidate = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(candidate).astimezone(timezone.utc)
        except ValueError:
            try:
                return datetime.strptime(candidate, "%Y-%m-%dT%H:%M:%S%z").astimezone(timezone.utc)
            except ValueError:
                pass
    return datetime.now(timezone.utc)


def _safe_int(value: object) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: object) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _serialize_records(records: Iterable[Dict[str, object]]) -> str:
    return "\n".join(json.dumps(record, separators=(",", ":")) for record in records) + "\n"


def aggregate_events(events: Sequence[Dict[str, object]], batch_id: int) -> List[Dict[str, object]]:
    region_aggregates: Dict[str, Dict[str, object]] = {}
    authority_aggregates: Dict[Tuple[str, str], Dict[str, object]] = {}

    for event in events:
        region_name = str(event.get("region_name") or "Unknown")
        authority_name = str(event.get("local_authority_name") or "Unknown")

        region_bucket = region_aggregates.setdefault(
            region_name,
            {
                "region_name": region_name,
                "total_vehicles": 0,
                "event_count": 0,
                "heavy_vehicles": 0,
                **{target: 0 for _, target in VEHICLE_FIELDS},
            },
        )

        authority_bucket = authority_aggregates.setdefault(
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
            heavy_sum = sum(_safe_int(event.get(source)) for source, _ in VEHICLE_FIELDS if "hgv" in source)
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

    for entry in region_aggregates.values():
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

    for entry in authority_aggregates.values():
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
    config.status_path.parent.mkdir(parents=True, exist_ok=True)
    config.status_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


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


def run_pipeline(config: Config) -> int:
    configure_logging(config.log_level)

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
            enable_auto_commit=True,
            consumer_timeout_ms=1000,
        )
    except KafkaError as exc:
        LOG.error("Failed to connect to Kafka: %s", exc)
        return 1

    backlog = DiskBacklog(config.spool_path)

    def flush_backlog(current_batch_id: int) -> int:
        while backlog.has_events():
            buffered_events = backlog.pop_batch(config.batch_size)
            if not buffered_events:
                break
            try:
                flush_batches(client, config, buffered_events, current_batch_id)
            except (WebHDFSException, requests.RequestException) as exc:
                LOG.error("Failed to persist backlog batch %s: %s", current_batch_id, exc)
                backlog.prepend(buffered_events)
                raise
            current_batch_id += 1
        return current_batch_id

    pending_events: List[Dict[str, object]] = []
    last_flush = time.time()
    batch_id = 0

    try:
        while not STOP_EVENT.is_set():
            try:
                batch_id = flush_backlog(batch_id)
            except (WebHDFSException, requests.RequestException):
                time.sleep(5)
                continue
            try:
                records = consumer.poll(timeout_ms=1000, max_records=config.batch_size)
            except KafkaError as exc:
                LOG.error("Kafka poll failed: %s", exc)
                time.sleep(5)
                continue

            for messages in records.values():
                for message in messages:
                    try:
                        event = json.loads(message.value)
                    except (TypeError, json.JSONDecodeError):
                        LOG.debug("Discarded malformed payload: %r", message.value)
                        continue
                    pending_events.append(event)

            now = time.time()
            should_flush = (
                pending_events
                and (
                    len(pending_events) >= config.batch_size
                    or (now - last_flush) >= config.flush_interval_seconds
                )
            )
            if should_flush:
                try:
                    batch_id = flush_backlog(batch_id)
                    flush_batches(client, config, pending_events, batch_id)
                except (WebHDFSException, requests.RequestException) as exc:
                    LOG.error("Failed to persist batch %s: %s", batch_id, exc)
                    backlog.enqueue(list(pending_events))
                    pending_events = []
                    time.sleep(5)
                    continue
                pending_events = []
                batch_id += 1
                last_flush = now

        if pending_events:
            try:
                batch_id = flush_backlog(batch_id)
                flush_batches(client, config, pending_events, batch_id)
            except (WebHDFSException, requests.RequestException) as exc:
                LOG.error("Failed to persist final batch %s: %s", batch_id, exc)
                backlog.enqueue(list(pending_events))
                return 1
    finally:
        consumer.close()

    return 0


def main() -> int:
    try:
        config = load_config()
    except SystemExit as exc:
        LOG.error(str(exc))
        return exc.code if isinstance(exc.code, int) else 1
    return run_pipeline(config)


if __name__ == "__main__":
    sys.exit(main())
