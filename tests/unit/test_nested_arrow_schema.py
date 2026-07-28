import pyarrow as pa
import pandas as pd

from wt_sdk import ChatMessage, ContentItem, LandingRecord
from wt_sdk.core.schemas import LANDING_SCHEMA
from wt_sdk.models import LandingRecordBatch
from wt_sdk.utils.converters import (
    dataframe_to_landing_records,
    landing_batch_to_arrow,
)


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
