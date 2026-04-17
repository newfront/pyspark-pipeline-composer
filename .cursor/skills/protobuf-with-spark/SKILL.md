---
name: protobuf-with-spark
description: >-
  Use Protobuf messages with Apache Spark and Delta Lake — SparkSession setup,
  building and loading FileDescriptorSet files, deserializing binary protobuf
  into DataFrames via from_protobuf, and writing to Delta. Use when working with
  protobuf in PySpark, converting proto bytes to DataFrames, reading descriptor
  files, or ingesting protobuf data into Delta Lake.
---

# Protobuf with Apache Spark

How to deserialize Protobuf messages into Spark DataFrames using `from_protobuf`,
FileDescriptorSet binary descriptors, and Delta Lake for storage.

## 1. SparkSession setup

The `spark-protobuf` JAR must be on the classpath. Match the Scala and Spark
versions to your cluster (example below uses Spark 4.x / Scala 2.13).

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("my-app")
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.13:4.1.0,"
        "org.apache.spark:spark-protobuf_2.13:4.1.1",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
```

Key points:
- `spark-protobuf_2.13` provides the `from_protobuf` / `to_protobuf` SQL functions.
- Version must match your Spark version (e.g. `4.1.1` for Spark 4.1).
- Delta Lake jars are optional; include only if writing Delta tables.

## 2. Building the FileDescriptorSet (descriptor.bin)

Spark's `from_protobuf` needs a **serialized `FileDescriptorSet`** so it knows
the schema of your messages. Build one that contains **all** your `.proto` files.

### Using the Buf CLI

```bash
# From the directory containing buf.yaml (project root)
mkdir -p gen/descriptors
buf build -o gen/descriptors/descriptor.bin
```

This produces a single binary file covering every proto in the module. The
`--exclude-source-info` flag can shrink the file if desired.

### Using protoc directly

```bash
protoc \
  --include_imports \
  --descriptor_set_out=gen/descriptors/descriptor.bin \
  -I protos \
  protos/**/*.proto
```

`--include_imports` is **required** — without it, dependencies (e.g.
`google.protobuf.Timestamp`, `buf.validate`) are missing and Spark will fail
to resolve the message.

### Makefile target (recommended pattern)

```makefile
descriptor:
	@mkdir -p gen/descriptors
	buf build -o gen/descriptors/descriptor.bin
```

Run `make descriptor` after any proto change and before Spark ingestion.

## 3. Loading the descriptor in Python

Read the binary file into a `bytes` object and pass it to `from_protobuf` via
the `binaryDescriptorSet` parameter.

```python
from pathlib import Path

def read_descriptor_bytes(path: Path | str) -> bytes:
    return Path(path).read_bytes()
```

## 4. Deserializing protobuf bytes into a DataFrame

### Core pattern

```python
from pyspark.sql.functions import col
from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.types import BinaryType, StructField, StructType

def protobuf_to_df(
    data: list[bytes],
    spark: SparkSession,
    descriptor_path: Path | str,
    message_name: str,
    column_name: str = "wrapper",
) -> "DataFrame":
    """Convert serialized protobuf bytes to a Spark DataFrame.

    Args:
        data: [msg.SerializeToString() for msg in messages]
        descriptor_path: path to the FileDescriptorSet binary
        message_name: fully qualified name, e.g. "user.v1.User"
    """
    if not data:
        schema = StructType([StructField(column_name, BinaryType(), True)])
        return spark.createDataFrame([], schema)

    descriptor_bytes = Path(descriptor_path).read_bytes()
    schema = StructType([StructField(column_name, BinaryType(), True)])
    df = spark.createDataFrame([(bytes(b),) for b in data], schema)

    return df.withColumn(
        column_name,
        from_protobuf(
            col(column_name),
            messageName=message_name,
            options={"mode": "FAILFAST"},
            binaryDescriptorSet=descriptor_bytes,
        ),
    ).selectExpr(f"{column_name}.*")
```

### Step-by-step explanation

1. **Wrap each serialized message as a row** — one `bytes` value per row in a
   single-column `BinaryType` DataFrame.
2. **Call `from_protobuf`** with:
   - `col(column_name)` — the binary column.
   - `messageName` — the **fully qualified** proto message name (e.g.
     `"order.v1.Order"`, not just `"Order"`).
   - `binaryDescriptorSet` — the descriptor bytes loaded in step 3.
   - `options={"mode": "FAILFAST"}` — fail loudly on corrupt data.
3. **Expand the struct** — `selectExpr("wrapper.*")` flattens the decoded
   struct into top-level columns matching the proto field names.

### from_protobuf parameters

| Parameter | Type | Required | Notes |
|-----------|------|----------|-------|
| `data` | Column | yes | Binary column of serialized proto bytes |
| `messageName` | str | yes | Fully qualified (e.g. `"user.v1.User"`) |
| `binaryDescriptorSet` | bytes | yes* | The FileDescriptorSet bytes |
| `descFilePath` | str | alt* | HDFS / local path to the descriptor file |
| `options` | dict | no | `{"mode": "FAILFAST"}` recommended |

*Provide **either** `binaryDescriptorSet` (preferred — no file I/O on
executors) **or** `descFilePath` (when the descriptor lives on HDFS / shared FS).

## 5. Writing to Delta Lake

```python
def write_df_to_delta(df, delta_path: str | Path, mode: str = "overwrite"):
    Path(delta_path).parent.mkdir(parents=True, exist_ok=True)
    df.write.format("delta").mode(mode).save(str(delta_path))
```

## 6. Large-dataset batched writes

For millions of messages, avoid passing the entire list to Spark at once.
Batch serialize and append:

```python
batch_size = 100_000
for start in range(0, total, batch_size):
    end = min(start + batch_size, total)
    batch = gen.generate_range(start, end)
    data = [r.SerializeToString() for r in batch]
    df = protobuf_to_df(data, spark, descriptor_path, message_name)
    mode = "overwrite" if start == 0 else "append"
    write_df_to_delta(df, output_path, mode=mode)
```

## 7. End-to-end example

```python
from pathlib import Path
from pyspark.sql import SparkSession

# 1. Build / locate descriptor
descriptor_path = Path("gen/descriptors/descriptor.bin")

# 2. Serialize proto messages
data = [msg.SerializeToString() for msg in messages]

# 3. Convert to DataFrame
df = protobuf_to_df(
    data=data,
    spark=spark,
    descriptor_path=descriptor_path,
    message_name="rain_sensor.v1.RainSensorReading",
)
df.show(5, truncate=False)

# 4. Write Delta table
write_df_to_delta(df, "output/rain_sensors")
```

## Common mistakes

| Mistake | Symptom | Fix |
|---------|---------|-----|
| Short message name (`"User"`) | `DescriptorNotFoundException` | Use fully qualified: `"user.v1.User"` |
| Missing `--include_imports` in protoc | `Cannot find dependency` errors | Add `--include_imports` or use `buf build` |
| Wrong spark-protobuf version | `ClassNotFoundException` | Match `spark-protobuf_2.13:X.Y.Z` to your Spark version |
| Passing `str` instead of `bytes` to DataFrame | Schema mismatch | Wrap with `bytes(b)` or ensure `SerializeToString()` output |
| Descriptor out of date after proto change | Schema drift / decode errors | Re-run `make descriptor` or `buf build -o ...` |

## Additional resources

- [Spark Protobuf Data Source](https://spark.apache.org/docs/latest/sql-data-sources-protobuf.html)
- [Buf CLI descriptors](https://buf.build/docs/reference/descriptors/)
- [from_protobuf PySpark API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.protobuf.functions.from_protobuf.html)
