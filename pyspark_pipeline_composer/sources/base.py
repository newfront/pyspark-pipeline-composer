from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@runtime_checkable
class Source(Protocol):
    """Readable streaming source that produces a DataFrame."""

    def read_stream(self, spark: SparkSession) -> DataFrame: ...
