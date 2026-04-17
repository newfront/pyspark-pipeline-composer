from __future__ import annotations

from pyspark_pipeline_composer.transforms.base import Transform
from pyspark_pipeline_composer.transforms.protobuf import ProtobufTransform


def test_protobuf_transform_satisfies_protocol():
    t = ProtobufTransform(
        message_name="test.v1.Msg",
        descriptor_path="fake.bin",
    )
    assert isinstance(t, Transform)


def test_protobuf_transform_fields():
    t = ProtobufTransform(
        message_name="events.v1.Event",
        descriptor_path="/tmp/descriptor.bin",
        column_name="payload",
        options={"mode": "PERMISSIVE"},
    )
    assert t.message_name == "events.v1.Event"
    assert t.column_name == "payload"
    assert t.options == {"mode": "PERMISSIVE"}
