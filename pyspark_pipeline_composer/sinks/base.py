from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.streaming import StreamingQuery


@runtime_checkable
class Sink(Protocol):
    """Writable streaming sink that consumes a DataFrame."""

    def write_stream(self, df: DataFrame) -> StreamingQuery: ...
