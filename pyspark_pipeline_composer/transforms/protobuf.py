from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from pyspark.sql.functions import col
from pyspark.sql.protobuf.functions import from_protobuf

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


@dataclass(frozen=True)
class ProtobufTransform:
    """Deserialize a binary protobuf column into struct fields.

    Reads a ``FileDescriptorSet`` from *descriptor_path*, applies
    ``from_protobuf`` to *column_name*, and flattens the resulting
    struct into top-level columns.
    """

    message_name: str
    descriptor_path: str | Path
    column_name: str = "value"
    options: dict[str, str] = field(default_factory=lambda: {"mode": "FAILFAST"})

    def apply(self, df: DataFrame) -> DataFrame:
        descriptor_bytes = Path(self.descriptor_path).read_bytes()
        return df.withColumn(
            self.column_name,
            from_protobuf(
                col(self.column_name),
                messageName=self.message_name,
                binaryDescriptorSet=descriptor_bytes,
                options=self.options,
            ),
        ).selectExpr(f"{self.column_name}.*")
