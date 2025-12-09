# Servicio FastAPI que expone predicciones de flujo vehicular basadas en perfiles precalculados.
from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

def _build_fallback_paths(primary: Path) -> List[Path]:
    candidates = [
        os.getenv("FALLBACK_PROFILES_PATH"),
        os.getenv("STATIC_PROFILES_PATH"),
        "/opt/ml/generated/distributions.json",
        "/opt/ml/generated/fallback_distributions.json",
    ]
    fallbacks: List[Path] = []
    for raw in candidates:
        if not raw:
            continue
        path = Path(raw)
        if path == primary or path in fallbacks:
            continue
        fallbacks.append(path)
    return fallbacks


DEFAULT_PROFILES_PATH = Path(os.getenv("PROFILES_PATH", "/opt/ml/generated/runtime_distributions.json"))
DEFAULT_FALLBACK_PATHS = _build_fallback_paths(DEFAULT_PROFILES_PATH)
DEFAULT_CLEAN_DATA_PATH = Path(os.getenv("CLEAN_DATA_PATH", "/opt/ml/raw/clean_data.csv"))
DEFAULT_MODEL_NAME = os.getenv("MODEL_NAME", "baseline-hourly-profile")


# Define los campos que acepta la petición de predicción.
class PredictionRequest(BaseModel):
    road_id: Optional[str] = Field(default=None, description="Identificador único de carretera (road:{slug}).")
    road_name: Optional[str] = Field(default=None, description="Nombre legible de la carretera si no se dispone de road_id.")
    hour_of_day: Optional[int] = Field(default=None, ge=0, le=23, description="Hora del día (0-23). Si se omite se usa la hora actual en UTC.")
    direction: Optional[str] = Field(default=None, description="Dirección de viaje para ajustar la predicción si hay distribución disponible.")
    scaling_factor: float = Field(default=1.0, ge=0.0, description="Factor multiplicativo adicional para escenarios hipotéticos.")


# Representa los valores calculados que devuelve la predicción solicitada.
class PredictionResponse(BaseModel):
    model: str
    generated_at: datetime
    road_id: str
    road_name: str
    hour_of_day: int
    expected_flow_per_minute: float
    lower_bound_per_minute: float
    upper_bound_per_minute: float
    expected_flow_per_hour: float
    distribution_direction: Optional[Dict[str, float]]
    direction_applied: Optional[str]
    confidence_level: str
    notes: str


# Gestiona la lectura y consulta de perfiles de carreteras en disco.
class ProfileStore:
    # Inicializa el almacén con la ruta del archivo de perfiles generado.
    def __init__(self, profiles_path: Path, fallback_paths: Optional[Sequence[Path]] = None) -> None:
        self._profiles_path = profiles_path
        self._fallback_paths = [path for path in (fallback_paths or []) if path]
        self._roads: Dict[str, Dict[str, object]] = {}
        self._by_name: Dict[str, str] = {}
        self._loaded_at: Optional[datetime] = None
        self._last_mtime: Optional[float] = None
        self._last_path: Optional[Path] = None
        self._active_path: Optional[Path] = None

    # Carga el archivo de perfiles únicamente cuando cambia su marca de tiempo.
    def load(self) -> None:
        selected_path: Optional[Path] = None
        candidate_paths = [self._profiles_path]
        for fallback in self._fallback_paths:
            if fallback not in candidate_paths:
                candidate_paths.append(fallback)
        for candidate in candidate_paths:
            if candidate.exists():
                selected_path = candidate
                break
        if selected_path is None:
            raise FileNotFoundError(
                f"Perfiles no encontrados en ningun candidato: {', '.join(str(path) for path in candidate_paths)}"
            )

        mtime = selected_path.stat().st_mtime
        if self._last_path == selected_path and self._last_mtime and mtime == self._last_mtime:
            return
        payload = json.loads(selected_path.read_text(encoding="utf-8"))
        roads = payload.get("roads") if isinstance(payload, dict) else None
        if not isinstance(roads, dict):
            raise ValueError("El archivo de perfiles no contiene diccionario 'roads'.")
        indexed: Dict[str, Dict[str, object]] = {}
        by_name: Dict[str, str] = {}
        for road_id, descriptor in roads.items():
            if not isinstance(descriptor, dict):
                continue
            road_id_str = str(road_id)
            indexed[road_id_str] = descriptor
            name = descriptor.get("road_name")
            if isinstance(name, str) and name.strip():
                by_name[name.strip().lower()] = road_id_str
        self._roads = indexed
        self._by_name = by_name
        self._loaded_at = datetime.now(timezone.utc)
        self._last_mtime = mtime
        self._last_path = selected_path
        self._active_path = selected_path

    # Garantiza que el archivo esté cargado, propagando fallos con un mensaje legible.
    def ensure_loaded(self) -> None:
        try:
            self.load()
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"No se pudo cargar el perfil de carreteras: {exc}") from exc

    # Devuelve el descriptor de una carretera por ID o nombre, levantando error si no existe.
    def get(self, road_id: Optional[str], road_name: Optional[str]) -> Dict[str, object]:
        self.ensure_loaded()
        if road_id:
            descriptor = self._roads.get(road_id)
            if descriptor:
                return descriptor
        if road_name:
            lookup_id = self._by_name.get(road_name.strip().lower())
            if lookup_id and lookup_id in self._roads:
                return self._roads[lookup_id]
        raise KeyError("Carretera no encontrada en el perfil generado.")

    @property
    # Informa cuántas carreteras están indexadas en memoria.
    def count(self) -> int:
        return len(self._roads)

    @property
    # Expone la fecha de carga más reciente de los perfiles.
    def loaded_at(self) -> Optional[datetime]:
        return self._loaded_at

    @property
    # Indica la ruta actualmente cargada de perfiles.
    def source_path(self) -> Optional[Path]:
        return self._active_path

    # Lista los IDs de carretera disponibles para otros componentes.
    def road_ids(self) -> List[str]:
        return list(self._roads.keys())


# Ajusta el caudal base aplicando multiplicadores por dirección cuando existen.
def _apply_direction_adjustment(
    base_value: float, direction: Optional[str], profile: Dict[str, object]
) -> tuple[float, Optional[str]]:
    distribution = profile.get("direction_distribution")
    if not direction or not isinstance(distribution, dict):
        return base_value, None
    direction_key = None
    for candidate in distribution.keys():
        if candidate.lower() == direction.lower():
            direction_key = candidate
            break
    if not direction_key:
        return base_value, None
    multiplier = float(distribution.get(direction_key, 1.0))
    adjusted = base_value * max(multiplier * len(distribution), 0.0)
    return adjusted, direction_key


# Obtiene la distribución estadística para una hora concreta del día.
def _extract_hour_profile(profile: Dict[str, object], hour: int) -> Optional[Dict[str, float]]:
    hourly_profile = profile.get("hourly_profile")
    if not isinstance(hourly_profile, dict):
        return None
    return hourly_profile.get(str(hour))


# Calcula límites superior e inferior usando dos desviaciones estándar.
def _compute_bounds(rate_per_minute: float, std_per_minute: float) -> tuple[float, float]:
    std = max(std_per_minute, 0.0)
    lower = max(rate_per_minute - (2 * std), 0.0)
    upper = max(rate_per_minute + (2 * std), 0.0)
    return lower, upper


profile_store = ProfileStore(DEFAULT_PROFILES_PATH, fallback_paths=DEFAULT_FALLBACK_PATHS)
app = FastAPI(title="TrafficFlow ML Service", version="0.1.0")


@app.on_event("startup")
# Precarga los perfiles de carreteras al iniciar el servicio.
async def startup_event() -> None:
    profile_store.ensure_loaded()


@app.get("/health")
# Ofrece un resumen del estado del modelo y la última carga de perfiles.
def healthcheck() -> Dict[str, object]:
    loaded_at = profile_store.loaded_at
    timestamp = loaded_at.isoformat() if loaded_at else None
    return {
        "status": "ok",
        "model": DEFAULT_MODEL_NAME,
        "roads_indexed": profile_store.count,
        "profiles_loaded_at": timestamp,
        "profile_source": str(profile_store.source_path) if profile_store.source_path else None,
    }


@app.post("/predict", response_model=PredictionResponse)
# Calcula la predicción de flujo vehicular para la carretera solicitada.
def predict(payload: PredictionRequest) -> PredictionResponse:
    try:
        profile = profile_store.get(payload.road_id, payload.road_name)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    road_id = payload.road_id or profile.get("road_id") or "desconocido"
    road_name = str(profile.get("road_name") or payload.road_name or road_id)

    hour = payload.hour_of_day
    if hour is None:
        hour = datetime.now(timezone.utc).hour
    if hour < 0 or hour > 23:
        raise HTTPException(status_code=400, detail="hour_of_day debe estar entre 0 y 23")

    hour_profile = _extract_hour_profile(profile, hour)
    baseline_rate = float(profile.get("baseline_rate_per_minute") or 0.0)
    rate_per_minute = baseline_rate
    std_per_minute = 0.0

    if hour_profile:
        rate_per_minute = float(hour_profile.get("mean_per_minute") or baseline_rate)
        std_per_minute = float(hour_profile.get("std_per_minute") or 0.0)

    rate_per_minute = max(rate_per_minute, 0.0)
    lower_bound, upper_bound = _compute_bounds(rate_per_minute, std_per_minute)

    adjusted_rate, applied_direction = _apply_direction_adjustment(
        rate_per_minute, payload.direction, profile
    )
    if adjusted_rate > 0:
        direction_multiplier = adjusted_rate / rate_per_minute if rate_per_minute else 1.0
        rate_per_minute = adjusted_rate
        lower_bound *= direction_multiplier
        upper_bound *= direction_multiplier

    if payload.scaling_factor not in (0, 1.0):
        factor = max(float(payload.scaling_factor), 0.0)
        rate_per_minute *= factor
        lower_bound *= factor
        upper_bound *= factor

    response = PredictionResponse(
        model=DEFAULT_MODEL_NAME,
        generated_at=datetime.now(timezone.utc),
        road_id=str(road_id),
        road_name=road_name,
        hour_of_day=hour,
        expected_flow_per_minute=rate_per_minute,
        lower_bound_per_minute=lower_bound,
        upper_bound_per_minute=upper_bound,
        expected_flow_per_hour=rate_per_minute * 60.0,
        distribution_direction=profile.get("direction_distribution")
        if isinstance(profile.get("direction_distribution"), dict)
        else None,
        direction_applied=applied_direction,
        confidence_level="medium" if std_per_minute > 0 else "low",
        notes="Predicción basada en perfiles horarios agregados y tasa base por minuto.",
    )
    return response


@app.get("/roads")
# Devuelve el listado completo de carreteras indexadas.
def list_roads() -> Dict[str, object]:
    profile_store.ensure_loaded()
    return {
        "count": profile_store.count,
        "roads": sorted(profile_store.road_ids()),
    }
