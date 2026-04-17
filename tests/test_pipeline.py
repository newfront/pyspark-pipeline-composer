from __future__ import annotations

import textwrap

import pytest

from pyspark_pipeline_composer.pipeline import Pipeline
from pyspark_pipeline_composer.sinks.base import Sink
from pyspark_pipeline_composer.sinks.delta import DeltaSink
from pyspark_pipeline_composer.sources.base import Source
from pyspark_pipeline_composer.sources.delta import DeltaSource
from pyspark_pipeline_composer.transforms.base import Transform
from pyspark_pipeline_composer.transforms.fn import FnTransform
from pyspark_pipeline_composer.transforms.sql import SQLTransform


def test_builder_fluent_api():
    src = DeltaSource(path="/tmp/in")
    t = SQLTransform(expression="SELECT * FROM input")
    snk = DeltaSink(path="/tmp/out", checkpoint_location="/tmp/ckpt")

    p = Pipeline().source(src).transform(t).sink(snk)

    assert isinstance(p._source, Source)
    assert len(p._transforms) == 1
    assert isinstance(p._transforms[0], Transform)
    assert isinstance(p._sink, Sink)


def test_builder_multiple_transforms():
    p = (
        Pipeline()
        .source(DeltaSource(path="/tmp/in"))
        .transform(SQLTransform(expression="SELECT * FROM input"))
        .transform(FnTransform(func=lambda df: df.limit(10)))
        .sink(DeltaSink(path="/tmp/out", checkpoint_location="/tmp/ckpt"))
    )
    assert len(p._transforms) == 2


def test_start_raises_without_source():
    p = Pipeline().sink(DeltaSink(path="/tmp/out", checkpoint_location="/tmp/ckpt"))
    with pytest.raises(ValueError, match="no source"):
        p.start(None)  # type: ignore[arg-type]


def test_start_raises_without_sink():
    p = Pipeline().source(DeltaSource(path="/tmp/in"))
    with pytest.raises(ValueError, match="no sink"):
        p.start(None)  # type: ignore[arg-type]


def test_from_yaml_string_builds_pipeline():
    yaml_text = textwrap.dedent("""\
        source:
          type: delta
          path: /data/input
          options:
            maxFilesPerTrigger: "100"
        transforms:
          - type: sql
            expression: "SELECT *, 1 AS flag FROM input"
        sink:
          type: delta
          path: /data/output
          checkpoint_location: /tmp/ckpt
          output_mode: append
    """)

    p = Pipeline.from_yaml_string(yaml_text)

    assert isinstance(p._source, DeltaSource)
    assert p._source.path == "/data/input"
    assert p._source.options == {"maxFilesPerTrigger": "100"}
    assert len(p._transforms) == 1
    assert isinstance(p._transforms[0], SQLTransform)
    assert isinstance(p._sink, DeltaSink)
    assert p._sink.output_mode == "append"


def test_from_yaml_string_unknown_source_raises():
    yaml_text = textwrap.dedent("""\
        source:
          type: kafka
          topic: test
        sink:
          type: delta
          path: /out
          checkpoint_location: /ckpt
    """)
    with pytest.raises(ValueError, match="Unknown source type"):
        Pipeline.from_yaml_string(yaml_text)


def test_from_yaml_string_unknown_transform_raises():
    yaml_text = textwrap.dedent("""\
        source:
          type: delta
          path: /in
        transforms:
          - type: magic
            spell: abracadabra
        sink:
          type: delta
          path: /out
          checkpoint_location: /ckpt
    """)
    with pytest.raises(ValueError, match="Unknown transform type"):
        Pipeline.from_yaml_string(yaml_text)
