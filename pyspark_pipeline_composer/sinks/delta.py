from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.streaming import StreamingQuery


@dataclass(frozen=True)
class DeltaSink:
    """Write a streaming DataFrame to a Delta Lake table."""

    path: str
    checkpoint_location: str
    output_mode: str = "append"
    options: dict[str, str] = field(default_factory=dict)

    def write_stream(self, df: DataFrame) -> StreamingQuery:
        writer = (
            df.writeStream.format("delta")
            .outputMode(self.output_mode)
            .option("checkpointLocation", self.checkpoint_location)
        )
        for k, v in self.options.items():
            writer = writer.option(k, v)
        return writer.start(self.path)
