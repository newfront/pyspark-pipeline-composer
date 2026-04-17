from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@runtime_checkable
class Transform(Protocol):
    """A transformation applied to a DataFrame."""

    def apply(self, df: DataFrame) -> DataFrame: ...
