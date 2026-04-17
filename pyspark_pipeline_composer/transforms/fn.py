from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass(frozen=True)
class FnTransform:
    """Wrap an arbitrary ``DataFrame -> DataFrame`` callable as a Transform."""

    func: Callable[[DataFrame], DataFrame]

    def apply(self, df: DataFrame) -> DataFrame:
        return self.func(df)
