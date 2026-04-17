from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass(frozen=True)
class SQLTransform:
    """Run a SQL expression against the incoming DataFrame.

    The DataFrame is registered as a temp view named ``view_name``
    (default ``"input"``), then the expression is executed via
    ``spark.sql()``.
    """

    expression: str
    view_name: str = "input"

    def apply(self, df: DataFrame) -> DataFrame:
        df.createOrReplaceTempView(self.view_name)
        return df.sparkSession.sql(self.expression)
