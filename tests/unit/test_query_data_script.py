import importlib.util
import json
from pathlib import Path

import pandas as pd


SCRIPT_PATH = Path(__file__).parents[2] / "scripts" / "inspect" / "query_data.py"
SPEC = importlib.util.spec_from_file_location("query_data_script", SCRIPT_PATH)
query_data = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(query_data)


def test_dataframe_to_json_records_preserves_nested_values():
    frame = pd.DataFrame(
        [
            {
                "id": "event-1",
                "messages": json.dumps(
                    [{"role": "user", "content": [{"type": "text", "text": "hello"}]}]
                ),
                "meta_json": json.dumps(
                    {"task_id": "task-1", "env_state": json.dumps({"step": 3})}
                ),
                "reward": 1.0,
                "optional": float("nan"),
            }
        ]
    )

    records = query_data._dataframe_to_json_records(frame)

    assert records == [
        {
            "id": "event-1",
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": "hello"}],
                }
            ],
            "meta_json": {
                "task_id": "task-1",
                "env_state": {"step": 3},
            },
            "reward": 1.0,
            "optional": None,
        }
    ]


def test_write_json_output_creates_parent_directories(tmp_path):
    output_path = tmp_path / "nested" / "sample.json"

    resolved_path = query_data._write_json_output(
        str(output_path),
        {"returned_rows": 1, "rows": [{"id": "event-1"}]},
    )

    assert resolved_path == output_path.resolve()
    assert json.loads(output_path.read_text(encoding="utf-8")) == {
        "returned_rows": 1,
        "rows": [{"id": "event-1"}],
    }
