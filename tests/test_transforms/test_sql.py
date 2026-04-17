from __future__ import annotations

from pyspark_pipeline_composer.transforms.base import Transform
from pyspark_pipeline_composer.transforms.sql import SQLTransform


def test_sql_transform_satisfies_protocol():
    t = SQLTransform(expression="SELECT 1")
    assert isinstance(t, Transform)


def test_sql_transform_apply(spark):
    data = [(1, "a"), (2, "b"), (3, "c")]
    df = spark.createDataFrame(data, ["id", "letter"])

    t = SQLTransform(expression="SELECT id, upper(letter) AS letter FROM input WHERE id > 1")
    result = t.apply(df)

    rows = result.collect()
    assert len(rows) == 2
    assert {r["letter"] for r in rows} == {"B", "C"}


def test_sql_transform_custom_view_name(spark):
    df = spark.createDataFrame([(10,)], ["val"])
    t = SQLTransform(expression="SELECT val * 2 AS doubled FROM src", view_name="src")
    result = t.apply(df)
    assert result.first()["doubled"] == 20
