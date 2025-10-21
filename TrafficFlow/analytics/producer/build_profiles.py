"""Generate statistical profiles from the cleaned traffic dataset for synthetic data production."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build probability distributions for the synthetic producer")
    parser.add_argument(
        "--input-path",
        default="hdfs://namenode:8020/data/silver/dft_traffic_clean",
        help="Input dataset location (Parquet)",
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
    region_counts = (
        df.groupBy("region_name", "region_id")
        .agg(F.count(F.lit(1)).alias("count"))
        .collect()
    )
    total = sum(row["count"] for row in region_counts)
    distribution: Dict[str, Dict[str, float]] = {}
    for row in region_counts:
        prob = row["count"] / total if total else 0.0
        distribution[row["region_name"]] = {
            "probability": prob,
            "region_id": row["region_id"],
        }
    return distribution


def collect_local_authority_distribution(df: DataFrame, min_obs: int) -> Dict[str, List[Dict[str, float]]]:
    grouped = (
        df.groupBy("region_name", "local_authority_id", "local_authority_name")
        .agg(
            F.count(F.lit(1)).alias("count"),
            F.avg(F.col("latitude").cast("double")).alias("latitude_mean"),
            F.stddev_pop(F.col("latitude").cast("double")).alias("latitude_std"),
            F.avg(F.col("longitude").cast("double")).alias("longitude_mean"),
            F.stddev_pop(F.col("longitude").cast("double")).alias("longitude_std"),
        )
        .collect()
    )

    by_region: Dict[str, List[Dict[str, float]]] = {}
    region_totals: Dict[str, int] = {}
    for row in grouped:
        if row["count"] < min_obs:
            continue
        region_totals.setdefault(row["region_name"], 0)
        region_totals[row["region_name"]] += row["count"]
        by_region.setdefault(row["region_name"], []).append(
            {
                "id": row["local_authority_id"],
                "name": row["local_authority_name"],
                "count": row["count"],
                "latitude_mean": row["latitude_mean"],
                "latitude_std": row["latitude_std"] or 0.01,
                "longitude_mean": row["longitude_mean"],
                "longitude_std": row["longitude_std"] or 0.01,
            }
        )

    for region, entries in by_region.items():
        total = region_totals.get(region, 0) or 1
        for entry in entries:
            entry["probability"] = entry["count"] / total
            del entry["count"]

    return by_region


def collect_categorical_distribution(df: DataFrame, column: str) -> Dict[str, float]:
    rows = (
        df.groupBy(column)
        .agg(F.count(F.lit(1)).alias("count"))
        .collect()
    )
    total = sum(row["count"] for row in rows)
    return {row[column]: row["count"] / total for row in rows if total}


def collect_numeric_profiles(df: DataFrame, column: str, group_column: str) -> Dict[str, Dict[str, float]]:
    base = df.select(group_column, F.col(column).cast("double").alias(column)).where(F.col(column).isNotNull())
    stats = (
        base.groupBy(group_column)
        .agg(
            F.count(F.lit(1)).alias("count"),
            F.avg(F.log1p(F.col(column))).alias("log_mean"),
            F.stddev_pop(F.log1p(F.col(column))).alias("log_std"),
            F.expr(f"percentile_approx({column}, 0.05)").alias("p05"),
            F.expr(f"percentile_approx({column}, 0.95)").alias("p95"),
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


def collect_hour_distribution_safe(df: DataFrame) -> Dict[str, float]:
    hours = (
        df.withColumn("hour_int", F.col("hour").cast("int"))
        .groupBy("hour_int")
        .agg(F.count(F.lit(1)).alias("count"))
        .collect()
    )
    total = sum(row["count"] for row in hours)
    return {str(row["hour_int"]): row["count"] / total for row in hours if total}


def build_profiles(args: argparse.Namespace) -> Dict[str, object]:
    spark = get_spark()
    df = spark.read.parquet(args.input_path)

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
    profiles["hour_distribution"] = collect_hour_distribution_safe(df)
    profiles["day_of_week_distribution"] = collect_categorical_distribution(
        df.withColumn("day_of_week", F.dayofweek("event_timestamp")),
        "day_of_week",
    )
    profiles["link_length_profiles"] = collect_numeric_profiles(df, "link_length_km", "road_type")
    profiles["vehicle_profiles"] = collect_numeric_profiles(df, "all_motor_vehicles", "region_name")
    profiles["density_profiles"] = collect_numeric_profiles(df, "vehicles_per_km", "road_type")
    profiles["heavy_vehicle_share_profile"] = collect_numeric_profiles(df, "heavy_vehicle_share", "road_type")

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
