# pyspark-pipeline-composer

A minimal framework for composing PySpark structured streaming pipelines.
Define sources, transforms, and sinks using a Python builder API or declarative
YAML configuration.

## Install

```bash
uv sync --all-extras
```

## Quick start

### Python API

```python
from pyspark.sql import SparkSession
from pyspark_pipeline_composer.pipeline import Pipeline
from pyspark_pipeline_composer.sources.delta import DeltaSource
from pyspark_pipeline_composer.transforms.sql import SQLTransform
from pyspark_pipeline_composer.sinks.delta import DeltaSink

spark = SparkSession.builder.master("local[*]").appName("demo").getOrCreate()

query = (
    Pipeline()
    .source(DeltaSource(path="/data/input"))
    .transform(SQLTransform(expression="SELECT *, current_timestamp() AS ts FROM input"))
    .sink(DeltaSink(path="/data/output", checkpoint_location="/tmp/ckpt"))
    .start(spark)
)

query.awaitTermination()
```

### YAML config

```yaml
# pipeline.yaml
source:
  type: delta
  path: /data/input
  options:
    maxFilesPerTrigger: "100"

transforms:
  - type: sql
    expression: "SELECT *, current_timestamp() AS ts FROM input"

sink:
  type: delta
  path: /data/output
  checkpoint_location: /tmp/ckpt
  output_mode: append
```

```python
pipeline = Pipeline.from_yaml("pipeline.yaml")
query = pipeline.start(spark)
```

## Development

```bash
make install   # install deps
make test      # run tests
make lint      # check style
make format    # auto-fix style
```

## License

Apache 2.0
