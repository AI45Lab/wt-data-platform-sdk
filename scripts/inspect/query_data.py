"""
Query script for wind tunnel data tables.

Supports count_rows and flexible filtering with configurable conditions.
Uses DLDB SDK to properly handle logical tables and their physical partitions.

Usage:
  # Query from default database (s3://wind-tunnel-dldb)
  python scripts/inspect/query_data.py --table landing_test

  # Show only row count
  python scripts/inspect/query_data.py --table landing_test --count

  # Query with filter
  python scripts/inspect/query_data.py --table landing_test --query "dataset_type = 'TEST'"

  # Query from custom database
  python scripts/inspect/query_data.py --db-uri s3://my-bucket --table my_table --query "reward > 0.5"

  # Query the separate environment-config database by table name
  python scripts/inspect/query_data.py --table evaluation_env_config --limit 5

  # Query with filter and limit
  python scripts/inspect/query_data.py --table landing_test --query "reward > 0.9" --limit 2

  # Select specific columns
  python scripts/inspect/query_data.py --table landing_test --columns "id,dataset_type,agent_model,reward"

  # Show full content (no truncation)
  python scripts/inspect/query_data.py --table landing_test --query "dataset_type = 'TEST'" --limit 1 --no-truncate

  # Show full nested structures (DataFrame format)
  python scripts/inspect/query_data.py --table landing_test --query "dataset_type = 'TEST'" --limit 1 --show-nested

  # Write nested results as pretty JSON instead of printing them
  python scripts/inspect/query_data.py --table landing_test --limit 1 --output ./artifacts/sample.json

Examples:
  # Get chat training data
  python scripts/inspect/query_data.py --table wind_tunnel_landing --query "dataset_type = 'SFT'"

  # Get high reward sessions
  python scripts/inspect/query_data.py --table wind_tunnel_landing --query "reward >= 0.9" --columns "id,session_id,reward,agent_model"

  # Get completed sessions
  python scripts/inspect/query_data.py --table wind_tunnel_landing --query "is_session_completed = true"

  # Count test data
  python scripts/inspect/query_data.py --table landing_test --count

  # Filter by meta_json JSON string (use LIKE for string matching)
  python scripts/inspect/query_data.py --table wind_tunnel_landing --query "meta_json LIKE '%safety_image_ch%'" --count
"""
import argparse
import base64
from datetime import date, datetime
import json
from pathlib import Path
import sys

import numpy as np

import dldb
import pandas as pd
from wt_sdk.config import default_config, resolve_env_config_db_uri


JSON_COLUMNS = {
    "messages",
    "response",
    "chosen_trace",
    "rejected_trace",
    "meta_json",
}

ENV_CONFIG_TABLE_NAMES = {"evaluation_env_config"}


def _resolve_db_uri(table_name: str, explicit_db_uri: str | None) -> str:
    """Resolve the database URI for known logical table families."""
    if explicit_db_uri:
        return explicit_db_uri
    if table_name in ENV_CONFIG_TABLE_NAMES:
        return resolve_env_config_db_uri()
    return default_config.tables.db_uri


def _uses_latest_snapshot_by_default(table_name: str) -> bool:
    """Use latest reads for cross-process control-plane tables."""
    return table_name in ENV_CONFIG_TABLE_NAMES


def _is_partitioned_schema_record(record) -> bool:
    """Return whether a dldb schema record describes a partitioned table."""
    partition_type = str(getattr(record, "partition_type", "") or "").upper()
    partition_column = str(getattr(record, "partition_column", "") or "").strip()
    return partition_type in {"VALUE", "HASH"} and bool(partition_column)


def _pin_exact_dldb_table(session, table_name: str) -> None:
    """Open the exact logical table by dldb metadata, avoiding prefix collisions."""
    try:
        record = session.schema_table.get(table_name)
        if record is None:
            return
        if not _is_partitioned_schema_record(record):
            return
        from dldb.table import open_table_by_partition_type
        session.tables[table_name] = open_table_by_partition_type(
            session.db_conn,
            session.schema_table,
            table_name,
            record.partition_type,
        )
    except Exception as exc:
        print(f"Warning: failed to pin exact table '{table_name}': {exc}")


def _format_content_item(item, max_len=100, truncate=True):
    """Format a ContentItem for display."""
    if not isinstance(item, dict):
        return str(item)[:max_len] if truncate else str(item)

    item_type = item.get('type', 'unknown')
    text = item.get('text')

    if item_type == 'text' and text:
        if truncate and len(text) > max_len:
            return f'"{text[:max_len]}..."'
        return f'"{text}"'
    elif item_type == 'image_url':
        url = item.get('image_url', {}).get('url', 'no-url')
        if truncate and url and len(url) > 50:
            return f'[image: {url[:50]}...]'
        return f'[image: {url}]'
    elif item_type == 'input_audio':
        url = item.get('input_audio', {}).get('url', 'no-url')
        if truncate and url and len(url) > 50:
            return f'[audio: {url[:50]}...]'
        return f'[audio: {url}]'
    else:
        return f'[{item_type}]'

def _format_chat_message(msg, indent=2, truncate=True):
    """Format a ChatMessage (dict) for readable display."""
    if not isinstance(msg, dict) or not msg:
        return "None"

    role = msg.get('role', 'None')
    content = msg.get('content')

    parts = []
    parts.append(f"role: {role}")

    if content is not None:
        content_list = content.tolist() if isinstance(content, np.ndarray) else content
        if isinstance(content_list, list) and len(content_list) > 0:
            # Format content items - show all, not just first 3
            content_summary = ", ".join(_format_content_item(item, max_len=1000, truncate=truncate) for item in content_list)
            parts.append(f"content: [{content_summary}]")
        else:
            parts.append("content: []")
    else:
        parts.append("content: None")

    # Show other non-None fields
    for key in ['name', 'refusal', 'tool_call_id']:
        val = msg.get(key)
        if val is not None and val != '':
            parts.append(f"{key}: {val}")

    tool_calls = msg.get('tool_calls')
    if tool_calls is not None and isinstance(tool_calls, (list, np.ndarray)) and len(tool_calls) > 0:
        parts.append(f"tool_calls: {len(tool_calls)} calls")

    # Join with separators
    result = ", ".join(parts)
    return result


def _format_messages_list(messages, max_messages=2, truncate=True):
    """Format a list of ChatMessage objects."""
    if messages is None:
        return "[]"
    if isinstance(messages, np.ndarray):
        if len(messages) == 0:
            return "[]"
        messages = messages.tolist()
    if not isinstance(messages, list) or len(messages) == 0:
        return "[]"

    count = len(messages)
    formatted = []

    # Show all messages when not truncating
    limit = count if not truncate else min(max_messages, count)

    for i, msg in enumerate(messages[:limit]):
        formatted.append(f"  {i+1}. {_format_chat_message(msg, indent=4, truncate=truncate)}")

    if count > limit:
        formatted.append(f"  ... (+{count - limit} more messages)")

    return "\n".join(formatted)


def _format_field_value(val, col_name, no_truncate=False):
    """Format a field value for display."""
    truncate = not no_truncate
    # Handle None/NaN
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "None"

    if col_name in JSON_COLUMNS and isinstance(val, str):
        parsed = _parse_embedded_json(val)
        if parsed is not val:
            val = parsed

    # Handle ChatMessage-like structs (dict with 'role' and 'content' keys)
    if isinstance(val, dict) and 'role' in val:
        return f"ChatMessage({_format_chat_message(val, indent=2, truncate=truncate)})"

    # Handle list of ChatMessages (messages field)
    if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict) and 'role' in val[0]:
        return f"[\n{_format_messages_list(val, max_messages=2, truncate=truncate)}\n]"

    # Handle numpy arrays
    if isinstance(val, np.ndarray):
        if len(val) == 0:
            return "[]"
        if isinstance(val[0], dict) and 'role' in val[0]:
            # Array of ChatMessages
            return f"[\n{_format_messages_list(val.tolist(), max_messages=2, truncate=truncate)}\n]"
        return f"<array {val.dtype} length={len(val)}>"

    # Handle other lists
    if isinstance(val, list):
        if len(val) == 0:
            return "[]"
        if len(val) == 1:
            return f"[{val[0]}]"
        return f"[{len(val)} items]"

    # Handle other dicts
    if isinstance(val, dict):
        return f"<dict with {len(val)} keys>"

    # Handle strings - truncate if needed
    if isinstance(val, str):
        if not no_truncate and len(val) > 100:
            return f'"{val[:100]}..."'
        return f'"{val}"'

    return str(val)


def _to_json_compatible(value):
    """Recursively convert DataFrame and Arrow-derived values to JSON types."""
    if value is None:
        return None
    if isinstance(value, np.ndarray):
        return [_to_json_compatible(item) for item in value.tolist()]
    if isinstance(value, np.generic):
        return _to_json_compatible(value.item())
    if isinstance(value, dict):
        return {str(key): _to_json_compatible(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_json_compatible(item) for item in value]
    if isinstance(value, (bytes, bytearray, memoryview)):
        encoded = base64.b64encode(bytes(value)).decode("ascii")
        return {"encoding": "base64", "data": encoded}
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _parse_embedded_json(value):
    """Expand embedded JSON strings for easier inspection."""
    if isinstance(value, dict):
        return {key: _parse_embedded_json(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_parse_embedded_json(item) for item in value]
    if isinstance(value, str) and value.lstrip().startswith(("{", "[")):
        try:
            return _parse_embedded_json(json.loads(value))
        except (TypeError, ValueError, json.JSONDecodeError):
            return value
    return value


def _dataframe_to_json_records(frame: pd.DataFrame):
    records = []
    for raw_record in frame.to_dict(orient="records"):
        record = {
            str(key): _to_json_compatible(value)
            for key, value in raw_record.items()
        }
        for column in JSON_COLUMNS:
            if isinstance(record.get(column), str):
                record[column] = _parse_embedded_json(record[column])
        records.append(record)
    return records


def _write_json_output(output_path: str, payload: dict) -> Path:
    path = Path(output_path).expanduser()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return path.resolve()


def main():
    parser = argparse.ArgumentParser(
        description="Query wind tunnel data tables",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Required: table name
    parser.add_argument("--table", type=str, required=True,
                       help="Table name to query")

    # Optional: database URI
    parser.add_argument("--db-uri", type=str, default=None,
                       help="Database URI (default: s3://wind-tunnel-dldb)")

    # Query options
    parser.add_argument("--count", action="store_true",
                       help="Only show row count")
    parser.add_argument("--query", type=str, default=None,
                       help="Filter query (e.g., \"dataset_type = 'TEST'\")")
    parser.add_argument("--limit", type=int, default=None,
                       help="Limit number of results")
    parser.add_argument("--columns", type=str, default=None,
                       help="Comma-separated list of columns to select (e.g., 'id,session_id,agent_model')")
    parser.add_argument("--show-nested", action="store_true",
                       help="Show full nested structures (default: simplified view)")
    parser.add_argument("--no-truncate", action="store_true",
                       help="Don't truncate long string values (default: truncate at 100 chars)")
    parser.add_argument("--output", type=str, default=None,
                       help="Write results as pretty JSON to this path instead of printing rows")

    args = parser.parse_args()

    # Determine database and table names
    table_name = args.table
    db_name = _resolve_db_uri(table_name, args.db_uri)
    checkout_latest = _uses_latest_snapshot_by_default(table_name)

    print(f"Database: {db_name}")
    print(f"Table: {table_name}")
    if checkout_latest:
        print("Checkout latest: true")
    print("=" * 80)

    # Initialize DLDB session
    print(f"Connecting to {db_name}...")
    try:
        session = dldb.connect(
            db_name,
            storage_options=default_config.s3.to_storage_options()
        )
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return 1

    # Check if table exists
    if not session.table_exists(table_name):
        print(f"Error: Table '{table_name}' does not exist in database '{db_name}'")
        print(f"Available tables: {session.list_tables()}")
        print("\nTip: Use 'python scripts/ops/table_manager.py list' to see all tables")
        session.shutdown()
        return 1
    _pin_exact_dldb_table(session, table_name)

    # dldb 1.0 rejects an empty WHERE expression, so use a universal
    # predicate internally while keeping the CLI output as "(all rows)".
    requested_query = args.query.strip() if args.query else ""
    query = requested_query or "1 = 1"
    print(f"Filter query: {requested_query if requested_query else '(all rows)'}")
    if args.limit:
        print(f"Limit: {args.limit}")
    print("=" * 80)

    # Parse columns selection
    columns = None
    if args.columns:
        columns = [col.strip() for col in args.columns.split(',')]

    # If only count requested, show count with/without filter
    if args.count:
        try:
            total_count = session.count_rows(table_name)
            print(f"Total rows in table: {total_count}")
            filtered_count = None
            if requested_query:
                # Count rows matching the filter
                result = session.filter(
                    table_name,
                    query=query,
                    limit=None,
                    columns=columns,
                    checkout_latest=checkout_latest,
                )
                filtered_count = len(result)
                print("=" * 80)
                print(f"Rows matching filter: {filtered_count}")
            else:
                print("No filter specified, showing total count")
            if args.output:
                output_path = _write_json_output(
                    args.output,
                    {
                        "database": db_name,
                        "table": table_name,
                        "filter": requested_query or None,
                        "checkout_latest": checkout_latest,
                        "total_rows": total_count,
                        "filtered_rows": filtered_count,
                    },
                )
                print(f"JSON output: {output_path}")
        except Exception as e:
            print(f"Error counting rows: {e}")
            session.shutdown()
            return 1
        print("=" * 80)
        session.shutdown()
        return 0

    # Show total count for regular queries
    total_count = None
    try:
        total_count = session.count_rows(table_name)
        print(f"Total rows in table: {total_count}")
        print("=" * 80)
    except Exception as e:
        print(f"Error getting row count: {e}")
        # Continue with query anyway

    try:
        result = session.filter(
            table_name,
            query=query,
            limit=args.limit,
            columns=columns,
            checkout_latest=checkout_latest,
        )
    except Exception as e:
        print(f"Error executing query: {e}")
        session.shutdown()
        return 1

    if args.output:
        try:
            output_path = _write_json_output(
                args.output,
                {
                    "database": db_name,
                    "table": table_name,
                    "filter": requested_query or None,
                    "checkout_latest": checkout_latest,
                    "total_rows": total_count,
                    "returned_rows": len(result),
                    "rows": _dataframe_to_json_records(result),
                },
            )
        except Exception as e:
            print(f"Error writing JSON output: {e}")
            session.shutdown()
            return 1
        print(f"JSON output: {output_path}")
    elif len(result) == 0:
        print("No results found.")
    else:
        print(f"\nFound {len(result)} rows:\n")

        if args.show_nested:
            # Decode JSON extension columns for readable nested display.
            display_result = result.copy()
            for column in JSON_COLUMNS.intersection(display_result.columns):
                display_result[column] = display_result[column].map(_parse_embedded_json)
            with pd.option_context('display.max_colwidth', None, 'display.max_columns', None):
                print(display_result.to_string())
        else:
            # Simplified view - format nested structs for readability
            for idx, row in result.iterrows():
                print(f"\n--- Row {idx} ---")
                for col in result.columns:
                    val = row[col]
                    # Use custom formatting for better readability
                    formatted_val = _format_field_value(val, col, args.no_truncate)

                    # Multi-line formatting for ChatMessage lists
                    if formatted_val.startswith("ChatMessage("):
                        # Extract the inner content for indented display
                        inner = formatted_val[12:-1]  # Remove "ChatMessage(" prefix and ")" suffix
                        print(f"  {col}:")
                        # Split by ", " but handle the parts correctly
                        parts = inner.split(", ")
                        for part in parts:
                            print(f"    {part}")
                    elif formatted_val.startswith("[\n"):
                        # Multi-line message list
                        print(f"  {col}: {formatted_val}")
                    else:
                        print(f"  {col}: {formatted_val}")

    session.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
