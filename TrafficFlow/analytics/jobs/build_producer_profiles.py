"""Spark job that extracts statistical profiles for the synthetic traffic producer."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

VEHICLE_COLUMNS: List[str] = [
    "pedal_cycle_count",
    "two_wheeled_motor_vehicle_count",
    "car_and_taxi_count",
    "bus_and_coach_count",
    "light_goods_vehicle_count",
]

HEAVY_COLUMNS: List[str] = [
    "heavy_goods_vehicle_2_rigid_axles_count",
    "heavy_goods_vehicle_3_rigid_axles_count",
    "heavy_goods_vehicle_4_plus_rigid_axles_count",
    "heavy_goods_vehicle_3_or_4_articulated_axles_count",
    "heavy_goods_vehicle_5_articulated_axles_count",
    "heavy_goods_vehicle_6_articulated_axles_count",
]

NUMERIC_COLUMNS: List[str] = [
    "link_length_kilometers",
    "link_length_miles",
    "british_national_grid_easting",
    "british_national_grid_northing",
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
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build probability distributions for the synthetic producer")
    parser.add_argument(
        "--input-path",
        default="hdfs://namenode:8020/data/raw/data.csv",
        help="Input dataset location (CSV with descriptive headers)",
    )
    parser.add_argument(
        "--output-path",
        default="/opt/spark-apps/producer/distributions.json",
        help="Destination for the generated JSON profiles",
    )
    parser.add_argument(
        "--min-observations",
        type=int,
        default=50,
        help="Minimum number of rows required for a conditional distribution to be included",
    )
    return parser.parse_args()


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("ProducerProfileBuilder")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def collect_region_distribution(df: DataFrame) -> Dict[str, Dict[str, float]]:
    region_counts = df.groupBy("region_name").agg(F.count(F.lit(1)).alias("count")).collect()
    total = sum(row["count"] for row in region_counts)
    return {row["region_name"]: {"probability": row["count"] / total} for row in region_counts if total}


def _slugify(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "-" for ch in value).strip("-") or "unknown"


def collect_local_authority_distribution(df: DataFrame, min_obs: int) -> Dict[str, List[Dict[str, float]]]:
    grouped = (
        df.groupBy("region_name", "local_authority_name")
        .agg(
            F.count(F.lit(1)).alias("count"),
            F.avg(F.col("latitude").cast("double")).alias("latitude_mean"),
            F.stddev_pop(F.col("latitude").cast("double")).alias("latitude_std"),
            F.avg(F.col("longitude").cast("double")).alias("longitude_mean"),
            F.stddev_pop(F.col("longitude").cast("double")).alias("longitude_std"),
            F.avg(F.col("british_national_grid_easting")).alias("easting_mean"),
            F.stddev_pop(F.col("british_national_grid_easting")).alias("easting_std"),
            F.avg(F.col("british_national_grid_northing")).alias("northing_mean"),
            F.stddev_pop(F.col("british_national_grid_northing")).alias("northing_std"),
        )
        .collect()
    )

    by_region: Dict[str, List[Dict[str, float]]] = {}
    region_totals: Dict[str, int] = {}
    for row in grouped:
        if row["count"] < min_obs:
            continue
        region = row["region_name"]
        region_totals.setdefault(region, 0)
        region_totals[region] += row["count"]
        by_region.setdefault(region, []).append(
            {
                "identifier": _slugify(row["local_authority_name"]),
                "name": row["local_authority_name"],
                "count": row["count"],
                "latitude_mean": row["latitude_mean"],
                "latitude_std": row["latitude_std"] or 0.01,
                "longitude_mean": row["longitude_mean"],
                "longitude_std": row["longitude_std"] or 0.01,
                "easting_mean": row["easting_mean"],
                "easting_std": row["easting_std"] or 1.0,
                "northing_mean": row["northing_mean"],
                "northing_std": row["northing_std"] or 1.0,
            }
        )

    for region, entries in by_region.items():
        total = region_totals.get(region, 0) or 1
        for entry in entries:
            entry["probability"] = entry["count"] / total
            del entry["count"]

    return by_region


def collect_categorical_distribution(df: DataFrame, column: str) -> Dict[str, float]:
    rows = df.groupBy(column).agg(F.count(F.lit(1)).alias("count")).collect()
    total = sum(row["count"] for row in rows)
    return {row[column]: row["count"] / total for row in rows if total}


def collect_numeric_profiles(df: DataFrame, column: str, group_column: str) -> Dict[str, Dict[str, float]]:
    base = df.select(group_column, F.col(column).cast("double").alias("value")).where(F.col("value").isNotNull())
    stats = (
        base.groupBy(group_column)
        .agg(
            F.count(F.lit(1)).alias("count"),
            F.avg(F.log1p(F.col("value"))).alias("log_mean"),
            F.stddev_pop(F.log1p(F.col("value"))).alias("log_std"),
            F.expr("percentile_approx(value, 0.05)").alias("p05"),
            F.expr("percentile_approx(value, 0.95)").alias("p95"),
        )
        .collect()
    )

    profiles: Dict[str, Dict[str, float]] = {}
    for row in stats:
        if row["count"] == 0 or row["log_mean"] is None:
            continue
        profiles[str(row[group_column])] = {
            "log_mean": row["log_mean"],
            "log_std": max(row["log_std"] or 0.01, 0.01),
            "p05": row["p05"],
            "p95": row["p95"],
        }
    return profiles


def collect_hour_distribution(df: DataFrame) -> Dict[str, float]:
    hours = (
        df.withColumn("hour_int", F.col("hour_of_day").cast("int"))
        .groupBy("hour_int")
        .agg(F.count(F.lit(1)).alias("count"))
        .collect()
    )
    total = sum(row["count"] for row in hours)
    return {str(row["hour_int"]): row["count"] / total for row in hours if total}


def collect_day_of_week_distribution(df: DataFrame) -> Dict[str, float]:
    enriched = df.withColumn("day_of_week", F.dayofweek("event_timestamp"))
    rows = enriched.groupBy("day_of_week").agg(F.count(F.lit(1)).alias("count")).collect()
    total = sum(row["count"] for row in rows)
    return {str(row["day_of_week"]): row["count"] / total for row in rows if total}


def collect_vehicle_shares(df: DataFrame, columns: List[str], total_col: str, group_col: str) -> Dict[str, Dict[str, Dict[str, float]]]:
    result: Dict[str, Dict[str, Dict[str, float]]] = {}
    for vehicle_col in columns:
        base = (
            df.select(
                group_col,
                (F.col(vehicle_col).cast("double") / F.col(total_col).cast("double")).alias("ratio"),
            )
            .where((F.col(total_col) > 0) & F.col(vehicle_col).isNotNull())
        )
        stats = (
            base.groupBy(group_col)
            .agg(
                F.count(F.lit(1)).alias("count"),
                F.avg("ratio").alias("mean"),
                F.stddev_pop("ratio").alias("stddev"),
            )
            .collect()
        )
        for row in stats:
            if row["count"] == 0 or row["mean"] is None:
                continue
            bucket = result.setdefault(row[group_col], {})
            bucket[vehicle_col] = {
                "mean": float(row["mean"]),
                "stddev": max(float(row["stddev"] or 0.01), 0.005),
            }

        global_stats = base.agg(
            F.count(F.lit(1)).alias("count"),
            F.avg("ratio").alias("mean"),
            F.stddev_pop("ratio").alias("stddev"),
        ).collect()
        if global_stats:
            row = global_stats[0]
            if row["count"] and row["mean"] is not None:
                bucket = result.setdefault("__global__", {})
                bucket[vehicle_col] = {
                    "mean": float(row["mean"]),
                    "stddev": max(float(row["stddev"] or 0.01), 0.005),
                }

    return result


def collect_direction_distribution(df: DataFrame) -> Dict[str, float]:
    rows = df.groupBy("travel_direction").agg(F.count(F.lit(1)).alias("count")).collect()
    total = sum(row["count"] for row in rows)
    return {row["travel_direction"]: row["count"] / total for row in rows if total}


def cast_numeric_columns(df: DataFrame) -> DataFrame:
    for column in NUMERIC_COLUMNS:
        if column in df.columns:
            df = df.withColumn(column, F.col(column).cast("double"))
    return df


def build_profiles(args: argparse.Namespace) -> Dict[str, object]:
    spark = get_spark()
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("timestampFormat", "yyyy-MM-dd")
        .csv(args.input_path)
        .withColumn("event_timestamp", F.to_timestamp("observation_date"))
    )

    df = cast_numeric_columns(df)
    df = df.withColumn(
        "vehicles_per_kilometer",
        F.when(
            F.col("link_length_kilometers") > 0,
            F.col("all_motor_vehicle_count") / F.col("link_length_kilometers"),
        ).otherwise(None),
    )
    df = df.withColumn(
        "heavy_vehicle_share",
        F.when(
            F.col("all_motor_vehicle_count") > 0,
            F.col("all_heavy_goods_vehicle_count") / F.col("all_motor_vehicle_count"),
        ).otherwise(None),
    )

    profiles: Dict[str, object] = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "input_path": args.input_path,
            "min_observations": args.min_observations,
        }
    }

    profiles["region_distribution"] = collect_region_distribution(df)
    profiles["local_authority_distribution"] = collect_local_authority_distribution(df, args.min_observations)
    profiles["road_type_distribution"] = collect_categorical_distribution(df, "road_type")
    profiles["travel_direction_distribution"] = collect_direction_distribution(df)
    profiles["hour_distribution"] = collect_hour_distribution(df)
    profiles["day_of_week_distribution"] = collect_day_of_week_distribution(df)
    profiles["link_length_profiles"] = collect_numeric_profiles(df, "link_length_kilometers", "road_type")
    profiles["vehicle_profiles"] = collect_numeric_profiles(df, "all_motor_vehicle_count", "region_name")
    profiles["density_profiles"] = collect_numeric_profiles(df, "vehicles_per_kilometer", "road_type")
    profiles["heavy_vehicle_share_profile"] = collect_numeric_profiles(df, "heavy_vehicle_share", "road_type")
    profiles["vehicle_share_profiles"] = collect_vehicle_shares(df, VEHICLE_COLUMNS, "all_motor_vehicle_count", "road_type")
    profiles["heavy_breakdown_profiles"] = collect_vehicle_shares(
        df,
        HEAVY_COLUMNS,
        "all_heavy_goods_vehicle_count",
        "road_type",
    )

    return profiles


def main() -> None:
    args = parse_args()
    profiles = build_profiles(args)

    output_path = Path(args.output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as fh:
        json.dump(profiles, fh, indent=2)


if __name__ == "__main__":
    main()
