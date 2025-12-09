from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional, Sequence, Tuple


VEHICLE_FIELDS: Sequence[str] = (
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


@dataclass
class _Stat:
    count: int = 0
    mean: float = 0.0
    m2: float = 0.0

    def update(self, value: float) -> None:
        self.count += 1
        delta = value - self.mean
        self.mean += delta / float(self.count)
        delta2 = value - self.mean
        self.m2 += delta * delta2

    def as_dict(self) -> Dict[str, float]:
        return {
            "count": self.count,
            "mean": self.mean,
            "m2": self.m2,
        }


class RuntimeProfileUpdater:
    """Acumula estadísticas por carretera y hora y genera perfiles dinámicos."""

    def __init__(
        self,
        output_path: Path,
        state_path: Optional[Path] = None,
        *,
        min_observations: int = 1,
    ) -> None:
        self.output_path = output_path
        self.state_path = state_path or output_path.with_suffix(".state.json")
        self.min_observations = max(1, int(min_observations))
        self._state: Dict[str, Dict[str, object]] = self._load_state()

    def update(self, events: Sequence[Dict[str, object]]) -> None:
        if not events:
            return

        minute_totals: Dict[Tuple[str, datetime], float] = defaultdict(float)
        metadata_by_road: Dict[str, Dict[str, str]] = {}
        direction_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))

        for event in events:
            road_id = self._extract_str(event.get("road_id"))
            if not road_id:
                continue
            timestamp = self._parse_timestamp(event)
            minute_key = timestamp.replace(second=0, microsecond=0)
            vehicle_count = self._extract_vehicle_count(event)
            minute_totals[(road_id, minute_key)] += vehicle_count
            metadata_by_road.setdefault(road_id, self._extract_metadata(event))
            direction = self._extract_direction(event)
            if direction:
                direction_counts[road_id][direction] += max(int(round(vehicle_count)), 1)

        if not minute_totals:
            return

        for (road_id, minute_key), total in minute_totals.items():
            entry = self._state.setdefault(road_id, self._build_state_entry(metadata_by_road.get(road_id)))
            self._merge_metadata(entry["meta"], metadata_by_road.get(road_id))
            self._update_stat(entry["overall"], total)
            hour_key = str(minute_key.hour)
            hour_stat = entry["hours"].setdefault(hour_key, _Stat())
            hour_stat.update(total)

        for road_id, counts in direction_counts.items():
            entry = self._state.setdefault(road_id, self._build_state_entry(metadata_by_road.get(road_id)))
            for direction, value in counts.items():
                entry["direction_counts"][direction] = entry["direction_counts"].get(direction, 0) + int(value)

        self._persist()

    # region: state management
    def _load_state(self) -> Dict[str, Dict[str, object]]:
        if not self.state_path.exists():
            return {}
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        roads = payload.get("roads") if isinstance(payload, dict) else None
        if not isinstance(roads, dict):
            return {}
        restored: Dict[str, Dict[str, object]] = {}
        for road_id, descriptor in roads.items():
            if not isinstance(descriptor, dict):
                continue
            meta = descriptor.get("meta") if isinstance(descriptor.get("meta"), dict) else {}
            hours_raw = descriptor.get("hours") if isinstance(descriptor.get("hours"), dict) else {}
            hours: Dict[str, _Stat] = {}
            for hour, stats in hours_raw.items():
                if not isinstance(stats, dict):
                    continue
                hours[str(hour)] = self._stat_from_dict(stats)
            overall = self._stat_from_dict(descriptor.get("overall"))
            direction_counts = {}
            raw_counts = descriptor.get("direction_counts")
            if isinstance(raw_counts, dict):
                for key, value in raw_counts.items():
                    try:
                        direction_counts[str(key)] = int(value)
                    except (TypeError, ValueError):
                        continue
            restored[road_id] = {
                "meta": self._normalize_meta(road_id, meta),
                "hours": hours,
                "overall": overall,
                "direction_counts": direction_counts,
            }
        return restored

    def _persist(self) -> None:
        state_payload = {
            "meta": {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "schema_version": 1,
                "road_count": len(self._state),
            },
            "roads": {
                road_id: {
                    "meta": data["meta"],
                    "hours": {hour: stats.as_dict() for hour, stats in data["hours"].items()},
                    "overall": data["overall"].as_dict(),
                    "direction_counts": data["direction_counts"],
                }
                for road_id, data in self._state.items()
            },
        }
        self._atomic_write(self.state_path, json.dumps(state_payload, indent=2))
        profile_payload = self._build_profile_payload()
        self._atomic_write(self.output_path, json.dumps(profile_payload, indent=2))

    # endregion

    # region: helpers
    def _build_state_entry(self, metadata: Optional[Dict[str, str]]) -> Dict[str, object]:
        return {
            "meta": self._normalize_meta(None, metadata or {}),
            "hours": {},
            "overall": _Stat(),
            "direction_counts": {},
        }

    def _merge_metadata(self, target: Dict[str, str], source: Optional[Dict[str, str]]) -> None:
        if not source:
            return
        for key, value in source.items():
            if value and not target.get(key):
                target[key] = value

    def _update_stat(self, stat: _Stat, value: float) -> None:
        stat.update(max(value, 0.0))

    def _extract_vehicle_count(self, event: Dict[str, object]) -> float:
        raw_value = event.get("all_motor_vehicle_count")
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0.0:
            return value
        total = 0.0
        for field in VEHICLE_FIELDS:
            try:
                field_value = float(event.get(field) or 0.0)
            except (TypeError, ValueError):
                field_value = 0.0
            total += max(field_value, 0.0)
        return total

    def _extract_metadata(self, event: Dict[str, object]) -> Dict[str, str]:
        road_id = self._extract_str(event.get("road_id"))
        region_name = self._extract_str(event.get("region_name"), default="Unknown")
        local_authority = self._extract_str(event.get("local_authority_name"), default="Unknown")
        return {
            "road_id": road_id or "",
            "road_name": self._extract_str(event.get("road_name"), default=road_id or ""),
            "region_name": region_name,
            "region_id": self._extract_str(event.get("region_id")),
            "local_authority_name": local_authority,
            "road_type": self._extract_str(event.get("road_type")),
        }

    def _extract_direction(self, event: Dict[str, object]) -> Optional[str]:
        for key in ("direction", "direction_of_travel", "travel_direction"):
            candidate = self._extract_str(event.get(key))
            if candidate:
                return candidate
        return None

    def _parse_timestamp(self, event: Dict[str, object]) -> datetime:
        value = event.get("event_timestamp") or event.get("timestamp") or event.get("observation_time")
        if isinstance(value, str) and value:
            candidate = value.replace("Z", "+00:00")
            for fmt in (None, "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S%z"):
                try:
                    if fmt is None:
                        parsed = datetime.fromisoformat(candidate)
                    else:
                        parsed = datetime.strptime(candidate, fmt)
                    return parsed.astimezone(timezone.utc)
                except ValueError:
                    continue
        return datetime.now(timezone.utc)

    def _extract_str(self, value: Optional[object], *, default: str = "") -> str:
        if value is None:
            return default
        result = str(value).strip()
        return result if result else default

    def _stat_from_dict(self, payload: Optional[Dict[str, object]]) -> _Stat:
        stat = _Stat()
        if not isinstance(payload, dict):
            return stat
        count = payload.get("count")
        mean = payload.get("mean")
        m2 = payload.get("m2")
        try:
            stat.count = int(count)
        except (TypeError, ValueError):
            stat.count = 0
        try:
            stat.mean = float(mean)
        except (TypeError, ValueError):
            stat.mean = 0.0
        try:
            stat.m2 = float(m2)
        except (TypeError, ValueError):
            stat.m2 = 0.0
        return stat

    def _normalize_meta(self, road_id: Optional[str], meta: Dict[str, object]) -> Dict[str, str]:
        base_id = road_id or self._extract_str(meta.get("road_id"))
        return {
            "road_id": base_id or "",
            "road_name": self._extract_str(meta.get("road_name"), default=base_id or ""),
            "region_name": self._extract_str(meta.get("region_name"), default="Unknown"),
            "region_id": self._extract_str(meta.get("region_id")),
            "local_authority_name": self._extract_str(meta.get("local_authority_name"), default="Unknown"),
            "road_type": self._extract_str(meta.get("road_type")),
        }

    def _build_profile_payload(self) -> Dict[str, object]:
        generated_at = datetime.now(timezone.utc).isoformat()
        road_index = []
        roads_payload = {}
        regions_accumulator: Dict[str, Dict[str, object]] = {}

        for road_id, data in self._state.items():
            metadata = data["meta"]
            overall = data["overall"]
            baseline = overall.mean if overall.count > 0 else 0.0
            hourly_profile = {}
            for hour, stats in data["hours"].items():
                if stats.count < self.min_observations:
                    continue
                variance = stats.m2 / float(stats.count) if stats.count > 0 else 0.0
                hourly_profile[str(hour)] = {
                    "mean_per_minute": stats.mean,
                    "std_per_minute": math.sqrt(max(variance, 0.0)),
                    "observations": stats.count,
                }
            direction_distribution = self._normalize_distribution(data["direction_counts"])
            descriptor = {
                "road_id": metadata["road_id"] or road_id,
                "road_name": metadata["road_name"] or road_id,
                "region_name": metadata["region_name"],
                "local_authority_name": metadata["local_authority_name"],
                "road_type": metadata.get("road_type") or "",
                "baseline_rate_per_minute": baseline,
                "hourly_profile": hourly_profile,
                "direction_distribution": direction_distribution,
                "observation_count": overall.count,
            }
            roads_payload[road_id] = descriptor
            road_index.append(
                {
                    "id": metadata["road_id"] or road_id,
                    "road_name": metadata["road_name"] or road_id,
                    "region_name": metadata["region_name"],
                    "local_authority_name": metadata["local_authority_name"],
                    "baseline_rate_per_minute": baseline,
                }
            )

            region_name = metadata["region_name"]
            region_id = metadata.get("region_id") or f"region:{self._slugify(region_name)}"
            region_entry = regions_accumulator.setdefault(
                region_id,
                {
                    "region_id": region_id,
                    "region_name": region_name,
                    "baseline_sum": 0.0,
                    "road_count": 0,
                    "roads": {},
                },
            )
            region_entry["baseline_sum"] += baseline
            region_entry["road_count"] += 1
            region_entry["roads"][road_id] = {
                "road_id": metadata["road_id"] or road_id,
                "road_name": metadata["road_name"] or road_id,
                "baseline_rate_per_minute": baseline,
            }

        region_index = []
        regions_payload = {}
        for region_id, entry in regions_accumulator.items():
            baseline_rate = (
                entry["baseline_sum"] / float(entry["road_count"])
                if entry["road_count"] > 0
                else 0.0
            )
            region_index.append(
                {
                    "id": region_id,
                    "region_name": entry["region_name"],
                    "road_count": entry["road_count"],
                    "baseline_rate_per_minute": baseline_rate,
                }
            )
            regions_payload[region_id] = {
                "region_id": region_id,
                "region_name": entry["region_name"],
                "road_count": entry["road_count"],
                "baseline_rate_per_minute": baseline_rate,
                "roads": entry["roads"],
            }

        profile_payload = {
            "meta": {
                "generated_at": generated_at,
                "source": "pipeline-runtime",
                "road_count": len(roads_payload),
                "region_count": len(regions_payload),
                "min_observations": self.min_observations,
            },
            "road_index": road_index,
            "roads": roads_payload,
            "region_index": region_index,
            "regions": regions_payload,
        }
        return profile_payload

    def _normalize_distribution(self, counts: Dict[str, int]) -> Dict[str, float]:
        total = sum(value for value in counts.values() if value > 0)
        if total <= 0:
            return {}
        return {key: value / float(total) for key, value in counts.items() if value > 0}

    def _slugify(self, value: str) -> str:
        tokens = []
        for char in value.lower():
            if char.isalnum():
                tokens.append(char)
            elif tokens and tokens[-1] != "-":
                tokens.append("-")
        slug = "".join(tokens).strip("-")
        return slug or "unknown"

    def _atomic_write(self, path: Path, payload: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(payload + "\n", encoding="utf-8")
        tmp_path.replace(path)

    # endregion


__all__ = ["RuntimeProfileUpdater"]
