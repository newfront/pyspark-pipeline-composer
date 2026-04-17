from __future__ import annotations

from pyspark_pipeline_composer.transforms.base import Transform
from pyspark_pipeline_composer.transforms.fn import FnTransform


def test_fn_transform_satisfies_protocol():
    t = FnTransform(func=lambda df: df)
    assert isinstance(t, Transform)


def test_fn_transform_apply(spark):
    df = spark.createDataFrame([(1,), (2,), (3,)], ["n"])
    t = FnTransform(func=lambda d: d.filter("n > 1"))
    result = t.apply(df)
    assert result.count() == 2
