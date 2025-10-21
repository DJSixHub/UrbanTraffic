"""Initial analytical queries over the cleaned silver dataset."""
from __future__ import annotations

import argparse
from typing import Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run basic analytical queries over the cleaned traffic dataset")
    parser.add_argument("--input-path", required=True, help="HDFS path to the cleaned (silver) dataset")
    parser.add_argument(
        "--output-path",
        help="Optional HDFS path to store the aggregated KPIs (Parquet)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top regions/authorities to display",
    )
    return parser.parse_args()


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("TfExploratoryQueries").getOrCreate()


def aggregate_by_region(df: DataFrame, top_n: int) -> Tuple[DataFrame, DataFrame]:
    by_region = (
        df.groupBy("region_name")
        .agg(
            F.sum("all_motor_vehicles").alias("total_vehicles"),
            F.avg("vehicles_per_km").alias("avg_density"),
        )
        .orderBy(F.desc("total_vehicles"))
    )
    year_region = (
        df.groupBy("region_name", "year")
        .agg(F.sum("all_motor_vehicles").alias("total_vehicles"))
        .orderBy("region_name", "year")
    )
    return by_region.limit(top_n), year_region


def aggregate_by_authority(df: DataFrame, top_n: int) -> DataFrame:
    return (
        df.groupBy("local_authority_id", "local_authority_name")
        .agg(
            F.sum("all_motor_vehicles").alias("total_vehicles"),
            F.avg("vehicles_per_km").alias("avg_density"),
        )
        .orderBy(F.desc("total_vehicles"))
        .limit(top_n)
    )


def aggregate_by_road_type(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("road_type")
        .agg(
            F.sum("all_motor_vehicles").alias("total_vehicles"),
            F.avg("vehicles_per_km").alias("avg_density"),
            F.avg("heavy_vehicle_share").alias("avg_heavy_share"),
        )
        .orderBy(F.desc("total_vehicles"))
    )


def main() -> None:
    args = parse_args()
    spark = get_spark()

    df = spark.read.parquet(args.input_path)

    region_top, region_by_year = aggregate_by_region(df, args.top_n)
    authority_top = aggregate_by_authority(df, args.top_n)
    road_type_stats = aggregate_by_road_type(df)

    print("=== Top regiones por vehículos ===")
    region_top.show(truncate=False)

    print("=== Distribución anual por región ===")
    region_by_year.show(100, truncate=False)

    print("=== Top autoridades locales ===")
    authority_top.show(truncate=False)

    print("=== Estadísticas por tipo de carretera ===")
    road_type_stats.show(truncate=False)

    if args.output_path:
        (region_top
         .coalesce(1)
         .write.mode("overwrite")
         .parquet(args.output_path.rstrip("/") + "/region_top"))
        (authority_top
         .coalesce(1)
         .write.mode("overwrite")
         .parquet(args.output_path.rstrip("/") + "/authority_top"))
        (road_type_stats
         .coalesce(1)
         .write.mode("overwrite")
         .parquet(args.output_path.rstrip("/") + "/road_type_stats"))


if __name__ == "__main__":
    main()
