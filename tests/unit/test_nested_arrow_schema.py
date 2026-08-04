import json

import pandas as pd
import pyarrow as pa

from wt_sdk import LandingRecord, ServingRecord
from wt_sdk.core.schemas import (
    JSON_TYPE,
    LANDING_PARTITION_COLUMN,
    LANDING_PARTITION_TYPE,
    LANDING_SCALAR_INDEXES,
    LANDING_SCHEMA,
    SERVING_PARTITION_COLUMN,
    SERVING_PARTITION_TYPE,
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
    )

    table = landing_batch_to_arrow(LandingRecordBatch(records=[record]), LANDING_SCHEMA)
    table.validate(full=True)

    for field_name in (
        "messages",
        "response",
        "chosen_trace",
        "rejected_trace",
        "meta_json",
    ):
        assert table.schema.field(field_name).type == JSON_TYPE
        assert table.column(field_name)[0].as_py() == getattr(record, field_name)

    dataframe = table.to_pandas(types_mapper=pd.ArrowDtype)
    restored = dataframe_to_landing_records(dataframe)[0]
    decoded = deserialize_json_columns(dataframe).iloc[0]
    assert restored.messages == messages
    assert restored.response == response
    assert restored.chosen_trace == chosen_trace
    assert restored.rejected_trace == rejected_trace
    assert decoded["messages"] == json.loads(messages)
    assert decoded["response"] == json.loads(response)


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
    assert LANDING_PARTITION_COLUMN == SERVING_PARTITION_COLUMN == "job_id"
    assert LANDING_PARTITION_TYPE == SERVING_PARTITION_TYPE == "HASH"
    assert LandingRecord.model_fields.keys() == ServingRecord.model_fields.keys()

    for field_name in (
        "messages",
        "response",
        "chosen_trace",
        "rejected_trace",
        "meta_json",
    ):
        assert LANDING_SCHEMA.field(field_name).type == pa.json_(pa.string())

    assert LANDING_SCHEMA.field("tags").type == pa.list_(pa.string())
    assert LANDING_SCHEMA.field("search_text").type == pa.string()
    assert "chosen_response" not in LANDING_SCHEMA.names
    assert "rejected_response" not in LANDING_SCHEMA.names
    assert "instruction_vector" not in LANDING_SCHEMA.names
    assert "vector_file_path" not in LANDING_SCHEMA.names


def test_schema_index_definitions_match_landing_and_serving_access_patterns():
    assert LANDING_SCALAR_INDEXES == [
        ("id", "BTREE"),
        ("job_id", "BTREE"),
        ("session_id", "BTREE"),
        ("created_at", "BTREE"),
        ("is_terminal", "BITMAP"),
        ("is_trainable", "BITMAP"),
    ]
    assert SERVING_SCALAR_INDEXES == [
        ("id", "BTREE"),
        ("job_id", "BTREE"),
        ("session_id", "BTREE"),
        ("created_at", "BTREE"),
        ("dataset_type", "BITMAP"),
        ("is_terminal", "BITMAP"),
        ("step_reward", "BTREE"),
        ("reward", "BTREE"),
        ("agent_model", "BTREE"),
        ("env_name", "BTREE"),
        ("tags", "LABEL_LIST"),
    ]
