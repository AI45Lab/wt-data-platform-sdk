import json

import pandas as pd
import pyarrow as pa

import wt_sdk._time as sdk_time
from wt_sdk import LandingRecord, ServingRecord
from wt_sdk.core.schemas import (
    JSON_STRING_COLUMNS,
    JSON_STRING_TYPE,
    LANDING_PARTITION_COLUMN,
    LANDING_PARTITION_TYPE,
    LANDING_PARTITIONS,
    LANDING_SCALAR_INDEXES,
    LANDING_SCHEMA,
    SERVING_PARTITION_COLUMN,
    SERVING_PARTITION_TYPE,
    SERVING_PARTITIONS,
    SERVING_SCALAR_INDEXES,
    SERVING_SCHEMA,
)
from wt_sdk.models import LandingRecordBatch
from wt_sdk.utils.converters import (
    dataframe_to_dict_records,
    dataframe_to_landing_records,
    deserialize_json_columns,
    landing_batch_to_arrow,
)


def test_dataframe_query_dicts_recursively_exclude_none_fields():
    dataframe = pd.DataFrame(
        [
            {
                "id": "record-1",
                "reward": 0.0,
                "is_terminal": False,
                "optional": None,
                "nested": {"value": "hello", "optional": None},
                "positions": ["first", None, "third"],
                "empty_list": [],
            }
        ]
    )

    compact = dataframe_to_dict_records(dataframe)
    assert compact == [
        {
            "id": "record-1",
            "reward": 0.0,
            "is_terminal": False,
            "nested": {"value": "hello"},
            "positions": ["first", None, "third"],
            "empty_list": [],
        }
    ]

    with_nulls = dataframe_to_dict_records(dataframe, exclude_none=False)
    assert with_nulls[0]["optional"] is None
    assert with_nulls[0]["nested"]["optional"] is None


def test_trajectory_payloads_round_trip_as_opaque_json_strings():
    messages = json.dumps(
        [
            {"role": "user", "content": "question"},
            {
                "role": "assistant",
                "content": "answer",
                "future_openai_field": {"any": ["shape", None]},
            },
        ]
    )
    response = json.dumps(
        {
            "role": "assistant",
            "content": "answer",
            "provider_extension": {"unvalidated": True},
        }
    )
    chosen_trace = json.dumps([{"role": "assistant", "content": "chosen"}])
    rejected_trace = json.dumps([{"role": "assistant", "content": "rejected"}])

    record = LandingRecord(
        dataset_type="test",
        id="record-json",
        created_at=1_700_000_000,
        job_id="job-json",
        messages=messages,
        response=response,
        chosen_trace=chosen_trace,
        rejected_trace=rejected_trace,
        meta_json=json.dumps({"provider": "anthropic", "raw_response": {"thinking": "..."}}),
        gateway_metrics=json.dumps({"latency_ms": 12, "prompt_tokens": 7}),
        job_name="job-json",
        tokens_info=json.dumps({"prompt_tokens": 7, "completion_tokens": 11}),
        weight_version=3,
    )

    table = landing_batch_to_arrow(LandingRecordBatch(records=[record]), LANDING_SCHEMA)
    table.validate(full=True)

    for field_name in JSON_STRING_COLUMNS:
        assert table.schema.field(field_name).type == JSON_STRING_TYPE
    for field_name in (
        "messages",
        "response",
        "chosen_trace",
        "rejected_trace",
        "meta_json",
    ):
        assert table.column(field_name)[0].as_py() == getattr(record, field_name)

    dataframe = table.to_pandas(types_mapper=pd.ArrowDtype)
    restored = dataframe_to_landing_records(dataframe)[0]
    decoded = deserialize_json_columns(dataframe).iloc[0]
    assert restored.messages == messages
    assert restored.response == response
    assert restored.chosen_trace == chosen_trace
    assert restored.rejected_trace == rejected_trace
    assert restored.gateway_metrics == record.gateway_metrics
    assert restored.job_name == "job-json"
    assert restored.weight_version == 3
    assert decoded["messages"] == json.loads(messages)
    assert decoded["response"] == json.loads(response)
    assert decoded["tokens_info"] == json.loads(record.tokens_info)


def test_sdk_initializes_source_time_and_preserves_explicit_time(monkeypatch):
    monkeypatch.setattr(sdk_time, "now_ms", lambda: 1_800_000_000_123)

    generated = LandingRecord(
        dataset_type="test",
        id="generated-time",
        created_at=1_700_000_000,
    )
    explicit = ServingRecord(
        dataset_type="test",
        id="explicit-time",
        created_at=1_700_000_000,
        source_updated_at=1_600_000_000_456,
        serving_updated_at=1_650_000_000_789,
    )

    assert generated.source_updated_at == 1_800_000_000_123
    assert generated.serving_updated_at is None
    assert explicit.source_updated_at == 1_600_000_000_456
    assert explicit.serving_updated_at == 1_650_000_000_789

    table = landing_batch_to_arrow(
        LandingRecordBatch(records=[generated, explicit]),
        LANDING_SCHEMA,
    )
    assert table.column("source_updated_at").to_pylist() == [
        1_800_000_000_123,
        1_600_000_000_456,
    ]
    assert table.column("serving_updated_at").to_pylist() == [
        None,
        1_650_000_000_789,
    ]


def test_json_payload_shape_is_not_validated_by_landing_model():
    payload = json.dumps(
        {
            "not": "an OpenAI message",
            "arbitrary_nested_provider_data": [{"type": "thinking", "signature": "opaque"}],
        }
    )
    record = LandingRecord(
        dataset_type="test",
        id="record-unvalidated",
        created_at=1_700_000_000,
        messages=payload,
        response=payload,
    )

    assert record.messages == payload
    assert record.response == payload


def test_deserialize_json_preserves_nested_nulls_and_original_strings():
    messages = '[{"role":"assistant","content":null,"tool_calls":null}]'
    dataframe = pd.DataFrame(
        [{"id": "record-1", "messages": messages, "response": "not-json", "optional": None}]
    )

    records = dataframe_to_dict_records(
        dataframe,
        exclude_none=True,
        deserialize_json=True,
    )
    decoded_frame = deserialize_json_columns(dataframe)

    assert records == [
        {
            "id": "record-1",
            "messages": [{"role": "assistant", "content": None, "tool_calls": None}],
            "response": "not-json",
        }
    ]
    assert decoded_frame.iloc[0]["messages"] == records[0]["messages"]
    assert decoded_frame.iloc[0]["response"] == "not-json"
    assert dataframe.iloc[0]["messages"] == messages


def test_blob_manifest_is_derived_best_effort_from_json_payloads():
    record = LandingRecord(
        dataset_type="test",
        id="record-blobs",
        created_at=1_700_000_000,
        messages=json.dumps(
            [
                {
                    "role": "user",
                    "content": [
                        {"type": "image_url", "image_url": {"url": "s3://bucket/image.png"}},
                        {"type": "input_audio", "input_audio": {"url": "s3://bucket/audio.wav"}},
                    ],
                }
            ]
        ),
        chosen_trace="not parsed by the model",
    )

    assert record.blob_manifest == ["s3://bucket/image.png", "s3://bucket/audio.wav"]


def test_blob_manifest_derivation_failure_never_blocks_record_creation(monkeypatch):
    def fail_derivation(_self):
        raise RuntimeError("unexpected blob parser failure")

    monkeypatch.setattr(LandingRecord, "_extract_multimodal_blobs", fail_derivation)

    record = LandingRecord(
        dataset_type="test",
        id="record-blob-failure",
        created_at=1_700_000_000,
        messages='[{"role":"user","content":"still storable"}]',
    )
    table = landing_batch_to_arrow(LandingRecordBatch(records=[record]), LANDING_SCHEMA)

    assert record.blob_manifest == []
    assert table.column("blob_manifest")[0].as_py() == []


def test_landing_and_serving_use_the_same_schema_and_hash_partition():
    assert LANDING_SCHEMA is SERVING_SCHEMA
    # Job splitting: landing shards writes by sub_job_id; serving keeps the
    # user-visible job_id as its partition value.
    assert LANDING_PARTITION_COLUMN == "sub_job_id"
    assert SERVING_PARTITION_COLUMN == "job_id"
    assert LANDING_PARTITION_TYPE == SERVING_PARTITION_TYPE == "HASH"
    assert LANDING_PARTITIONS == SERVING_PARTITIONS == 128
    assert LandingRecord.model_fields.keys() == ServingRecord.model_fields.keys()

    # All JSON payload columns are opaque large_string columns (64-bit offsets).
    for field_name in JSON_STRING_COLUMNS:
        assert LANDING_SCHEMA.field(field_name).type == pa.large_string()

    # 2026.10 schema evolution v2 fields.
    assert LANDING_SCHEMA.field("job_name").type == pa.string()
    assert LANDING_SCHEMA.field("role").type == pa.string()
    assert LANDING_SCHEMA.field("harness").type == pa.string()
    assert LANDING_SCHEMA.field("weight_version").type == pa.int64()
    assert LANDING_SCHEMA.field("group_id").type == pa.string()
    for field_name in ("job_name", "role", "harness", "weight_version", "group_id"):
        assert LANDING_SCHEMA.field(field_name).nullable is True

    assert LANDING_SCHEMA.field("tags").type == pa.list_(pa.string())
    assert LANDING_SCHEMA.field("source_updated_at").type == pa.int64()
    assert LANDING_SCHEMA.field("source_updated_at").nullable is False
    assert LANDING_SCHEMA.field("serving_updated_at").type == pa.int64()
    assert LANDING_SCHEMA.field("serving_updated_at").nullable is True
    assert LANDING_SCHEMA.field("search_text").type == pa.string()
    assert "chosen_response" not in LANDING_SCHEMA.names
    assert "rejected_response" not in LANDING_SCHEMA.names
    assert "instruction_vector" not in LANDING_SCHEMA.names
    assert "vector_file_path" not in LANDING_SCHEMA.names


def test_schema_index_definitions_match_landing_and_serving_access_patterns():
    assert LANDING_SCALAR_INDEXES == [
        ("id", "BTREE"),
        ("job_id", "BTREE"),
        ("sub_job_id", "BTREE"),
        ("session_id", "BTREE"),
        ("created_at", "BTREE"),
        ("source_updated_at", "BTREE"),
        ("is_terminal", "BITMAP"),
        ("is_trainable", "BITMAP"),
        ("is_session_completed", "BITMAP"),
        ("group_id", "BTREE"),
        ("role", "BITMAP"),
        ("harness", "BITMAP"),
        ("weight_version", "BTREE"),
    ]
    assert SERVING_SCALAR_INDEXES == [
        ("id", "BTREE"),
        ("job_id", "BTREE"),
        ("session_id", "BTREE"),
        ("created_at", "BTREE"),
        ("source_updated_at", "BTREE"),
        ("serving_updated_at", "BTREE"),
        ("dataset_type", "BITMAP"),
        ("is_terminal", "BITMAP"),
        ("is_trainable", "BITMAP"),
        ("is_session_completed", "BITMAP"),
        ("step_reward", "BTREE"),
        ("reward", "BTREE"),
        ("agent_model", "BTREE"),
        ("env_name", "BTREE"),
        ("tags", "LABEL_LIST"),
        ("group_id", "BTREE"),
        ("role", "BITMAP"),
        ("harness", "BITMAP"),
        ("weight_version", "BTREE"),
    ]
