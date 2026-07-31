import pyarrow as pa
import pandas as pd

from wt_sdk import ChatMessage, ContentItem, LandingRecord, ServingRecord
from wt_sdk.core.schemas import (
    LANDING_PARTITION_COLUMN,
    LANDING_PARTITION_TYPE,
    LANDING_SCALAR_INDEXES,
    LANDING_SCHEMA,
    SERVING_PARTITION_COLUMN,
    SERVING_PARTITION_TYPE,
    SERVING_SCALAR_INDEXES,
    SERVING_SCHEMA,
    message_type,
)
from wt_sdk.models import LandingRecordBatch
from wt_sdk.utils.converters import (
    dataframe_to_dict_records,
    dataframe_to_landing_records,
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
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": "hello",
                                "image_url": None,
                            }
                        ],
                        "tool_calls": None,
                    }
                ],
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
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "hello"}],
                }
            ],
            "positions": ["first", None, "third"],
            "empty_list": [],
        }
    ]

    with_nulls = dataframe_to_dict_records(dataframe, exclude_none=False)
    assert with_nulls[0]["optional"] is None
    assert with_nulls[0]["messages"][0]["tool_calls"] is None
    assert with_nulls[0]["messages"][0]["content"][0]["image_url"] is None


def test_landing_arrow_preserves_null_nested_structs():
    records = [
        LandingRecord(
            dataset_type="test",
            id=f"record-{index}",
            session_id="session-1",
            created_at=1_700_000_000 + index,
            job_id="job-1",
            messages=[
                ChatMessage(role="user", content=[ContentItem(type="text", text="question")]),
                ChatMessage(role="assistant", content=[ContentItem(type="text", text="answer")]),
            ],
        )
        for index in range(3)
    ]

    table = landing_batch_to_arrow(LandingRecordBatch(records=records), LANDING_SCHEMA)
    table.validate(full=True)

    messages = table.column("messages").combine_chunks()
    content_items = messages.values.field("content").values
    image_url = content_items.field("image_url")
    input_audio = content_items.field("input_audio")
    tool_calls = messages.values.field("tool_calls")

    assert len(content_items) == 6
    assert image_url.null_count == 6
    assert input_audio.null_count == 6
    assert tool_calls.null_count == 6
    assert table.column("response").null_count == 3
    assert table.column("chosen_trace").null_count == 3
    assert table.column("rejected_trace").null_count == 3
    assert table.column("search_text").null_count == 3
    assert table.column("tags").null_count == 3

    dataframe = table.to_pandas(types_mapper=pd.ArrowDtype)
    round_tripped = pa.Table.from_pandas(dataframe, schema=LANDING_SCHEMA, preserve_index=False)
    round_tripped.validate(full=True)
    round_trip_items = round_tripped.column("messages").combine_chunks().values.field("content").values
    assert round_trip_items.field("image_url").null_count == 6
    assert round_trip_items.field("input_audio").null_count == 6
    round_trip_messages = round_tripped.column("messages").combine_chunks().values
    assert round_trip_messages.field("tool_calls").null_count == 6

    restored_records = dataframe_to_landing_records(dataframe)
    assert len(restored_records) == 3
    for record in restored_records:
        assert len(record.messages) == 2
        for message in record.messages:
            assert message.tool_calls is None
            assert len(message.content) == 1
            assert message.content[0].image_url is None
            assert message.content[0].input_audio is None


def test_landing_and_serving_use_the_same_schema_and_hash_partition():
    assert LANDING_SCHEMA is SERVING_SCHEMA
    assert LANDING_PARTITION_COLUMN == SERVING_PARTITION_COLUMN == "job_id"
    assert LANDING_PARTITION_TYPE == SERVING_PARTITION_TYPE == "HASH"
    assert LandingRecord.model_fields.keys() == ServingRecord.model_fields.keys()

    assert LANDING_SCHEMA.field("chosen_trace").type == pa.list_(message_type)
    assert LANDING_SCHEMA.field("rejected_trace").type == pa.list_(message_type)
    assert LANDING_SCHEMA.field("meta_json").type == pa.json_(pa.string())
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


def test_meta_json_stays_a_string_and_trace_is_a_message_list():
    record = LandingRecord(
        dataset_type="test",
        id="record-trace",
        created_at=1_700_000_000,
        job_id="job-trace",
        meta_json='{"group_id":"group-a"}',
        search_text="question answer",
        chosen_trace=[
            ChatMessage(role="user", content=[ContentItem(type="text", text="question")]),
            ChatMessage(role="assistant", content=[ContentItem(type="text", text="answer")]),
        ],
    )

    table = landing_batch_to_arrow(LandingRecordBatch(records=[record]), LANDING_SCHEMA)
    table.validate(full=True)

    assert isinstance(record.meta_json, str)
    assert table.column("meta_json")[0].as_py() == record.meta_json
    assert table.column("search_text")[0].as_py() == record.search_text
    assert len(table.column("chosen_trace")[0].as_py()) == 2
    assert table.column("rejected_trace").null_count == 1
    assert table.column("tags").null_count == 1
