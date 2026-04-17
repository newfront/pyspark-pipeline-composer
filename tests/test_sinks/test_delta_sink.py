from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from pyspark_pipeline_composer.sinks.base import Sink
from pyspark_pipeline_composer.sinks.delta import DeltaSink

SOURCE_PATH = "tests/_tmp/delta_sink_source"
SINK_PATH = "tests/_tmp/delta_sink_output"
CKPT_PATH = "tests/_tmp/delta_sink_ckpt"


@pytest.fixture(autouse=True)
def _clean(spark):
    for p in (SOURCE_PATH, SINK_PATH, CKPT_PATH):
        path = Path(p)
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)

    data = [(1, "x"), (2, "y")]
    df = spark.createDataFrame(data, ["id", "val"])
    df.write.format("delta").mode("overwrite").save(SOURCE_PATH)
    yield
    for p in (SOURCE_PATH, SINK_PATH, CKPT_PATH):
        shutil.rmtree(p, ignore_errors=True)


def test_delta_sink_satisfies_protocol():
    snk = DeltaSink(path=SINK_PATH, checkpoint_location=CKPT_PATH)
    assert isinstance(snk, Sink)


def test_write_stream_produces_output(spark):
    stream_df = spark.readStream.format("delta").load(SOURCE_PATH)
    snk = DeltaSink(path=SINK_PATH, checkpoint_location=CKPT_PATH)
    query = snk.write_stream(stream_df)
    try:
        query.processAllAvailable()
        result = spark.read.format("delta").load(SINK_PATH)
        assert result.count() == 2
    finally:
        query.stop()
