"""Batch pipeline: rain_sensors_delta -> hourly_weather_summary_delta.

Reads the raw rain sensor readings, aggregates to one row per
(place_name, hour), and writes the result as a new Delta table.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

_EXAMPLES_DIR = Path(__file__).resolve().parent
SOURCE_PATH = str(_EXAMPLES_DIR / "datasets" / "rain_sensors_delta")
SINK_PATH = str(_EXAMPLES_DIR / "datasets" / "hourly_weather_summary_delta")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName("hourly-weather-summary")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def run(spark: SparkSession) -> None:
    raw = spark.read.format("delta").load(SOURCE_PATH)

    with_ts = raw.withColumn(
        "reading_ts",
        (F.col("timestamp_ms") / 1000).cast("timestamp"),
    ).withColumn(
        "hour_bucket",
        F.date_trunc("hour", F.col("reading_ts")),
    )

    rain_only = with_ts.filter(F.col("is_raining") == True)  # noqa: E712

    mode_intensity = (
        rain_only.groupBy("location.place_name", "hour_bucket", "rain_intensity")
        .agg(F.count("*").alias("cnt"))
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("place_name", "hour_bucket").orderBy(F.desc("cnt"))
            ),
        )
        .filter(F.col("rn") == 1)
        .select("place_name", "hour_bucket", F.col("rain_intensity").alias("dominant_rain_intensity"))
    )

    summary = (
        with_ts.groupBy(
            F.col("location.place_name").alias("place_name"),
            "hour_bucket",
        )
        .agg(
            F.count("*").alias("reading_count"),
            F.round(F.avg("precipitation_mm"), 2).alias("avg_precipitation_mm"),
            F.round(F.max("precipitation_mm"), 2).alias("max_precipitation_mm"),
            F.round(F.sum("precipitation_mm"), 2).alias("total_precipitation_mm"),
            F.round(F.avg("temperature_celsius"), 1).alias("avg_temperature_c"),
            F.round(F.min("temperature_celsius"), 1).alias("min_temperature_c"),
            F.round(F.max("temperature_celsius"), 1).alias("max_temperature_c"),
            F.round(F.avg("humidity_percent"), 1).alias("avg_humidity_pct"),
            F.round(F.avg("pressure_hpa"), 1).alias("avg_pressure_hpa"),
            F.round(F.avg("wind_speed_kmh"), 1).alias("avg_wind_speed_kmh"),
            F.round(F.max("wind_speed_kmh"), 1).alias("max_wind_speed_kmh"),
            F.round(
                F.sum(F.when(F.col("is_raining") == True, 1).otherwise(0)) / F.count("*") * 100,  # noqa: E712
                1,
            ).alias("rain_pct"),
        )
    )

    result = summary.join(mode_intensity, on=["place_name", "hour_bucket"], how="left")

    result = result.orderBy("place_name", "hour_bucket")

    print("=== OUTPUT SCHEMA ===")
    result.printSchema()
    print(f"=== ROWS: {result.count()} ===")
    result.show(20, truncate=False)

    Path(SINK_PATH).mkdir(parents=True, exist_ok=True)
    result.write.format("delta").mode("overwrite").save(SINK_PATH)
    print(f"\nWrote Delta table to {SINK_PATH}")


if __name__ == "__main__":
    spark = build_spark()
    try:
        run(spark)
    finally:
        spark.stop()
