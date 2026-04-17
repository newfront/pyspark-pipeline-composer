from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from pyspark_pipeline_composer.sources.base import Source
from pyspark_pipeline_composer.sources.delta import DeltaSource

DELTA_PATH = "tests/_tmp/delta_source_test"


@pytest.fixture(autouse=True)
def _clean(spark):
    path = Path(DELTA_PATH)
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    data = [(1, "alice"), (2, "bob")]
    df = spark.createDataFrame(data, ["id", "name"])
    df.write.format("delta").mode("overwrite").save(DELTA_PATH)
    yield
    shutil.rmtree(path, ignore_errors=True)


def test_delta_source_satisfies_protocol():
    src = DeltaSource(path=DELTA_PATH)
    assert isinstance(src, Source)


def test_read_stream_returns_streaming_df(spark):
    src = DeltaSource(path=DELTA_PATH)
    df = src.read_stream(spark)
    assert df.isStreaming


def test_read_stream_with_options(spark):
    src = DeltaSource(path=DELTA_PATH, options={"maxFilesPerTrigger": "1"})
    df = src.read_stream(spark)
    assert df.isStreaming
