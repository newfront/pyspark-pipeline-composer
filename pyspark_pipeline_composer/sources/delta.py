from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class DeltaSource:
    """Read a Delta Lake table as a streaming DataFrame."""

    path: str
    options: dict[str, str] = field(default_factory=dict)

    def read_stream(self, spark: SparkSession) -> DataFrame:
        reader = spark.readStream.format("delta")
        for k, v in self.options.items():
            reader = reader.option(k, v)
        return reader.load(self.path)
