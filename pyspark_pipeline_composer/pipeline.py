from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from pyspark_pipeline_composer.sinks.base import Sink
from pyspark_pipeline_composer.sources.base import Source
from pyspark_pipeline_composer.transforms.base import Transform

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
    from pyspark.sql.streaming import StreamingQuery


_SOURCE_REGISTRY: dict[str, type] = {}
_TRANSFORM_REGISTRY: dict[str, type] = {}
_SINK_REGISTRY: dict[str, type] = {}


def _populate_registries() -> None:
    """Lazily populate type registries on first YAML load."""
    if _SOURCE_REGISTRY:
        return

    from pyspark_pipeline_composer.sinks.delta import DeltaSink
    from pyspark_pipeline_composer.sources.delta import DeltaSource
    from pyspark_pipeline_composer.transforms.protobuf import ProtobufTransform
    from pyspark_pipeline_composer.transforms.sql import SQLTransform

    _SOURCE_REGISTRY.update({"delta": DeltaSource})
    _TRANSFORM_REGISTRY.update({"sql": SQLTransform, "protobuf": ProtobufTransform})
    _SINK_REGISTRY.update({"delta": DeltaSink})


@dataclass
class Pipeline:
    """Builder for composing a streaming pipeline from a source, transforms, and a sink."""

    _source: Source | None = field(default=None, repr=False)
    _transforms: list[Transform] = field(default_factory=list, repr=False)
    _sink: Sink | None = field(default=None, repr=False)

    def source(self, src: Source) -> Pipeline:
        self._source = src
        return self

    def transform(self, t: Transform) -> Pipeline:
        self._transforms.append(t)
        return self

    def sink(self, snk: Sink) -> Pipeline:
        self._sink = snk
        return self

    def start(self, spark: SparkSession) -> StreamingQuery:
        if self._source is None:
            raise ValueError("Pipeline has no source configured")
        if self._sink is None:
            raise ValueError("Pipeline has no sink configured")

        df = self._source.read_stream(spark)
        for t in self._transforms:
            df = t.apply(df)
        return self._sink.write_stream(df)

    # ------------------------------------------------------------------
    # YAML loading
    # ------------------------------------------------------------------

    @classmethod
    def from_yaml(cls, path: str | Path) -> Pipeline:
        """Load a pipeline definition from a YAML file."""
        raw = yaml.safe_load(Path(path).read_text())
        return cls._from_dict(raw)

    @classmethod
    def from_yaml_string(cls, text: str) -> Pipeline:
        """Load a pipeline definition from a YAML string."""
        raw = yaml.safe_load(text)
        return cls._from_dict(raw)

    @classmethod
    def _from_dict(cls, raw: dict) -> Pipeline:
        _populate_registries()
        pipeline = cls()

        src_cfg = raw["source"]
        src_type = src_cfg.pop("type")
        if src_type not in _SOURCE_REGISTRY:
            raise ValueError(f"Unknown source type: {src_type!r}")
        pipeline.source(_SOURCE_REGISTRY[src_type](**src_cfg))

        for t_cfg in raw.get("transforms", []):
            t_type = t_cfg.pop("type")
            if t_type not in _TRANSFORM_REGISTRY:
                raise ValueError(f"Unknown transform type: {t_type!r}")
            pipeline.transform(_TRANSFORM_REGISTRY[t_type](**t_cfg))

        sink_cfg = raw["sink"]
        sink_type = sink_cfg.pop("type")
        if sink_type not in _SINK_REGISTRY:
            raise ValueError(f"Unknown sink type: {sink_type!r}")
        pipeline.sink(_SINK_REGISTRY[sink_type](**sink_cfg))

        return pipeline
