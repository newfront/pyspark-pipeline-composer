# Examples

End-to-end examples that demonstrate the pyspark-pipeline-composer framework
using real sensor data and Delta Lake.

## Directory layout

```
examples/
├── README.md                              # this file
├── hourly_weather_summary.py              # PySpark aggregation pipeline
├── run_hourly_weather_summary.sh          # shell wrapper for spark-submit
├── dashboard/
│   └── rain-sensor-dashboard.canvas.tsx   # Cursor Canvas interactive dashboard
└── datasets/
    ├── rain_sensors_delta/                # source — raw rain sensor readings
    └── hourly_weather_summary_delta/      # sink — hourly aggregated output
```

## Prerequisites

- **Apache Spark 4.x** installed locally with `SPARK_HOME` set.
- **Delta Lake** JARs available (the run script auto-detects cached JARs in
  `~/.ivy2*`, or downloads them via `--packages` if not found).

## Running the pipeline

```bash
# From the project root
./examples/run_hourly_weather_summary.sh
```

The script reads `examples/datasets/rain_sensors_delta`, computes hourly
weather summaries per location, and writes the result to
`examples/datasets/hourly_weather_summary_delta`.

You can also supply your own cached JAR paths:

```bash
DELTA_JAR=/path/to/delta-spark.jar \
STORAGE_JAR=/path/to/delta-storage.jar \
./examples/run_hourly_weather_summary.sh
```

### What the pipeline produces

One row per (place_name, hour) with:

| Column | Description |
|--------|-------------|
| `place_name` | Sensor location |
| `hour_bucket` | Truncated hour timestamp |
| `reading_count` | Number of readings in the hour |
| `avg_precipitation_mm` | Mean precipitation |
| `max_precipitation_mm` | Peak precipitation |
| `total_precipitation_mm` | Cumulative precipitation |
| `avg_temperature_c` | Mean temperature |
| `min_temperature_c` / `max_temperature_c` | Temperature range |
| `avg_humidity_pct` | Mean humidity |
| `avg_pressure_hpa` | Mean barometric pressure |
| `avg_wind_speed_kmh` / `max_wind_speed_kmh` | Wind statistics |
| `rain_pct` | Percentage of readings with active rain |
| `dominant_rain_intensity` | Most frequent intensity (LIGHT / MODERATE / HEAVY) |

## Viewing the dashboard

The `dashboard/rain-sensor-dashboard.canvas.tsx` file is a
[Cursor Canvas](https://docs.cursor.com) that renders charts and tables from
the aggregated data. To use it:

1. Open the project in Cursor.
2. Copy the canvas file into your workspace's `canvases/` directory (Cursor
   auto-detects it there).
3. Open the canvas from the sidebar to explore the interactive dashboard.

The dashboard has four tabs: **Overview**, **Precipitation**, **Climate**, and
**All Data**.

## Source dataset

The `rain_sensors_delta` table contains ~1M raw readings from 20 global sensor
locations spanning Jan 2024 – Dec 2025. Each reading includes temperature,
humidity, pressure, wind, precipitation, and rain intensity fields, plus a
nested `location` struct with latitude, longitude, altitude, and place name.
