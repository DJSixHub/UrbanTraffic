from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT / "Dataset Propuesto"
OUT_DIR = ROOT / "data" / "raw"
OUT_PATH = OUT_DIR / "clean_data.csv"


COLUMN_MAPPING: Dict[str, str] = {
    "count_point_id": "count_point_identifier",
    "count_date": "observation_date",
    "hour": "hour_of_day",
    "direction_of_travel": "travel_direction",
    "region_name": "region_name",
    "local_authority_name": "local_authority_name",
    "road_name": "road_name",
    "road_type": "road_type",
    "start_junction_road_name": "start_junction_road_name",
    "end_junction_road_name": "end_junction_road_name",
    "easting": "british_national_grid_easting",
    "northing": "british_national_grid_northing",
    "latitude": "latitude",
    "longitude": "longitude",
    "link_length_km": "link_length_kilometers",
    "link_length_miles": "link_length_miles",
    "pedal_cycles": "pedal_cycle_count",
    "two_wheeled_motor_vehicles": "two_wheeled_motor_vehicle_count",
    "cars_and_taxis": "car_and_taxi_count",
    "buses_and_coaches": "bus_and_coach_count",
    "lgvs": "light_goods_vehicle_count",
    "hgvs_2_rigid_axle": "heavy_goods_vehicle_2_rigid_axles_count",
    "hgvs_3_rigid_axle": "heavy_goods_vehicle_3_rigid_axles_count",
    "hgvs_4_or_more_rigid_axle": "heavy_goods_vehicle_4_plus_rigid_axles_count",
    "hgvs_3_or_4_articulated_axle": "heavy_goods_vehicle_3_or_4_articulated_axles_count",
    "hgvs_5_articulated_axle": "heavy_goods_vehicle_5_articulated_axles_count",
    "hgvs_6_articulated_axle": "heavy_goods_vehicle_6_articulated_axles_count",
    "all_hgvs": "all_heavy_goods_vehicle_count",
    "all_motor_vehicles": "all_motor_vehicle_count",
}


DROP_COLUMNS = {
    "region_id",
    "local_authority_id",
}


TRAVEL_DIRECTION_MAP = {
    "1": "Northbound",
    "2": "Southbound",
    "3": "Eastbound",
    "4": "Westbound",
    "5": "Inner",
    "6": "Outer",
    "7": "Clockwise",
    "8": "Anticlockwise",
}


# Localiza el archivo CSV principal dentro del dataset propuesto.
def find_primary_file() -> Path:
    candidates = [
        SRC_DIR / "dft_traffic_counts_raw_counts.csv",
        SRC_DIR / "dft_traffic_counts.csv",
    ]
    for p in candidates:
        if p.exists():
            return p
    if SRC_DIR.exists():
        for p in SRC_DIR.iterdir():
            if p.suffix.lower() == ".csv":
                return p
    raise FileNotFoundError(f"No source CSV found in {SRC_DIR}")


# Aplica normalización de columnas y tipos al DataFrame leído.
def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {}
    for col in df.columns:
        col_stripped = col.strip()
        if col_stripped in COLUMN_MAPPING:
            rename_map[col] = COLUMN_MAPPING[col_stripped]
            continue
        low = col_stripped.lower()
        if low in COLUMN_MAPPING:
            rename_map[col] = COLUMN_MAPPING[low]
            continue
        if low == "count_date" or low == "date":
            rename_map[col] = "observation_date"
        if low == "count_point_identifier":
            rename_map[col] = "count_point_identifier"

    df = df.rename(columns=rename_map)

    df = df.drop(columns=[col for col in DROP_COLUMNS if col in df.columns])

    essential = [
        "count_point_identifier",
        "observation_date",
        "hour_of_day",
        "region_name",
        "local_authority_name",
        "road_name",
        "road_type",
        "link_length_kilometers",
        "all_motor_vehicle_count",
    ]
    for col in essential:
        if col not in df.columns:
            df[col] = None

    numeric_cols = [
        "link_length_kilometers",
        "link_length_miles",
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
        "all_heavy_goods_vehicle_count",
        "all_motor_vehicle_count",
        "british_national_grid_easting",
        "british_national_grid_northing",
        "latitude",
        "longitude",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "all_motor_vehicles" in df.columns and "all_motor_vehicle_count" not in df.columns:
        df["all_motor_vehicle_count"] = pd.to_numeric(df["all_motor_vehicles"], errors="coerce")

    if "hour_of_day" in df.columns:
        try:
            df["hour_of_day"] = df["hour_of_day"].astype(int)
        except Exception:
            df["hour_of_day"] = pd.to_numeric(df["hour_of_day"], errors="coerce")

    string_columns = [
        "count_point_identifier",
        "travel_direction",
        "region_name",
        "local_authority_name",
        "road_name",
        "road_type",
        "start_junction_road_name",
        "end_junction_road_name",
    ]
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    if "travel_direction" in df.columns:
        df["travel_direction"] = df["travel_direction"].map(
            lambda value: TRAVEL_DIRECTION_MAP.get(str(value).strip(), str(value).strip())
            if value is not None
            else value
        )

    if "observation_date" in df.columns:
        df["observation_date"] = pd.to_datetime(df["observation_date"], errors="coerce").dt.date

    return df


# Ejecuta el flujo completo de transformación y escritura del CSV limpio.
def main() -> None:
    src = find_primary_file()
    print(f"Source CSV: {src}")

    df = pd.read_csv(src, dtype=str, quoting=csv.QUOTE_MINIMAL)

    df = transform_df(df)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df.to_csv(OUT_PATH, index=False, date_format="%Y-%m-%d")
    print(f"Wrote transformed CSV to {OUT_PATH}")


if __name__ == "__main__":
    main()
