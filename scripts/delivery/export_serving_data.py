"""Export serving rows into sharded JSONL files.

This command is intentionally stateless. Each invocation captures the IDs that
match the caller's filter through ``WTGatewayClient.export_data_batches()`` and
publishes one self-contained export directory. Callers own incremental filters,
deduplication, and cursor persistence between invocations.
"""

from __future__ import annotations

import argparse
import base64
from datetime import date, datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any, Iterable, Optional, Sequence
import uuid

import pandas as pd

from wt_sdk.client import WTGatewayClient
from wt_sdk.config import (
    DEFAULT_LANDING_TABLE,
    DEFAULT_SERVING_TABLE,
    GatewayConfig,
    TEST_LANDING_TABLE,
    TEST_SERVING_TABLE,
    TableConfig,
)
from wt_sdk.core.schemas import SERVING_SCHEMA


DEFAULT_ROWS_PER_FILE = 1000
ALLOWED_SERVING_TABLES = (DEFAULT_SERVING_TABLE, TEST_SERVING_TABLE)

# This is an explicit external-delivery contract. Keep it explicit so a future
# internal serving column is not exported automatically without review.
DEFAULT_DELIVERY_COLUMNS = (
    "dataset_type",
    "dt",
    "id",
    "session_id",
    "created_at",
    "source_updated_at",
    "serving_updated_at",
    "step_id",
    "is_terminal",
    "env_id",
    "job_id",
    "is_truncated",
    "step_reward",
    "reward",
    "messages",
    "response",
    "chosen_trace",
    "rejected_trace",
    "ground_truth_answer",
    "reference_answer",
    "agent_model",
    "env_name",
    "is_session_completed",
    "is_trainable",
    "meta_json",
    "tags",
    "blob_manifest",
)


class ExportFailed(RuntimeError):
    """Report a failed export while preserving its partial directory."""

    def __init__(self, partial_dir: Path, cause: BaseException):
        self.partial_dir = partial_dir
        self.cause = cause
        super().__init__(f"Export failed; partial data kept at {partial_dir}: {cause}")


def build_config(serving_table: str = DEFAULT_SERVING_TABLE) -> GatewayConfig:
    """Build a config pinned to one of the two active serving tables."""
    if serving_table not in ALLOWED_SERVING_TABLES:
        raise ValueError(
            "delivery export table must be one of: "
            f"{', '.join(ALLOWED_SERVING_TABLES)}"
        )
    is_test = serving_table == TEST_SERVING_TABLE
    return GatewayConfig(
        tables=TableConfig(
            profile="test" if is_test else "production",
            landing_table=TEST_LANDING_TABLE if is_test else DEFAULT_LANDING_TABLE,
            serving_table=serving_table,
        )
    )


def parse_columns(raw_columns: Optional[str]) -> list[str]:
    """Resolve and validate the external delivery column selection."""
    if raw_columns is None:
        return list(DEFAULT_DELIVERY_COLUMNS)

    columns = [column.strip() for column in raw_columns.split(",") if column.strip()]
    if not columns:
        raise ValueError("--columns must contain at least one column")
    if len(columns) != len(set(columns)):
        raise ValueError("--columns must not contain duplicate column names")

    unknown = sorted(set(columns) - set(SERVING_SCHEMA.names))
    if unknown:
        raise ValueError(f"unknown serving columns: {', '.join(unknown)}")
    return columns


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _format_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _new_export_id(started_at: datetime) -> str:
    timestamp = started_at.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"export-{timestamp}-{uuid.uuid4().hex[:8]}"


def _json_compatible(value: Any) -> Any:
    """Recursively convert Pandas/Arrow-derived values into strict JSON values."""
    if value is None:
        return None
    if isinstance(value, dict):
        return {str(key): _json_compatible(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_compatible(item) for item in value]
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {
            "encoding": "base64",
            "data": base64.b64encode(bytes(value)).decode("ascii"),
        }
    if isinstance(value, (pd.Timestamp, datetime, date)):
        return value.isoformat()

    # NumPy arrays/scalars and Arrow-backed scalar wrappers expose one of these
    # conversion methods. Handle them without making NumPy a direct SDK import.
    if not isinstance(value, (str, int, float, bool)):
        tolist = getattr(value, "tolist", None)
        if callable(tolist):
            converted = tolist()
            if converted is not value:
                return _json_compatible(converted)
        item = getattr(value, "item", None)
        if callable(item):
            converted = item()
            if converted is not value:
                return _json_compatible(converted)

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


class _JsonlShardWriter:
    """Write records continuously and rotate files at a fixed row count."""

    def __init__(self, directory: Path, rows_per_file: int):
        self.directory = directory
        self.rows_per_file = rows_per_file
        self.files: list[dict[str, Any]] = []
        self.total_rows = 0
        self._handle = None
        self._path: Optional[Path] = None
        self._rows_in_file = 0
        self._next_part = 0

    def _open_part(self) -> None:
        name = f"part-{self._next_part:05d}.jsonl"
        self._next_part += 1
        self._path = self.directory / name
        self._handle = self._path.open("x", encoding="utf-8", newline="\n")
        self._rows_in_file = 0

    def _close_part(self) -> None:
        if self._handle is None or self._path is None:
            return
        self._handle.close()
        self.files.append(
            {
                "name": self._path.name,
                "rows": self._rows_in_file,
                "bytes": self._path.stat().st_size,
            }
        )
        self._handle = None
        self._path = None
        self._rows_in_file = 0

    def write(self, record: dict[str, Any]) -> None:
        if self._handle is None:
            self._open_part()
        elif self._rows_in_file >= self.rows_per_file:
            self._close_part()
            self._open_part()

        payload = _json_compatible(record)
        line = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )
        self._handle.write(line + "\n")
        self._rows_in_file += 1
        self.total_rows += 1

    def finish(self) -> None:
        self._close_part()

    def abort(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None


def export_serving_data(
    client: WTGatewayClient,
    *,
    filter_query: str,
    columns: Sequence[str],
    output_dir: Path,
    rows_per_file: int = DEFAULT_ROWS_PER_FILE,
    export_id: Optional[str] = None,
    started_at: Optional[datetime] = None,
) -> Path:
    """Export one stateless serving snapshot and return its directory."""
    normalized_filter = filter_query.strip()
    if not normalized_filter:
        raise ValueError("filter_query must be non-empty")
    if isinstance(rows_per_file, bool) or not isinstance(rows_per_file, int) or rows_per_file <= 0:
        raise ValueError("rows_per_file must be a positive integer")

    selected_columns = list(columns)
    if not selected_columns:
        raise ValueError("columns must contain at least one column")
    if len(selected_columns) != len(set(selected_columns)):
        raise ValueError("columns must not contain duplicate column names")
    unknown = sorted(set(selected_columns) - set(SERVING_SCHEMA.names))
    if unknown:
        raise ValueError(f"unknown serving columns: {', '.join(unknown)}")

    table_name = client.config.tables.serving_table
    if table_name not in ALLOWED_SERVING_TABLES:
        raise ValueError(
            "delivery export table must be one of "
            f"{ALLOWED_SERVING_TABLES}, got '{table_name}'"
        )

    root = Path(output_dir).expanduser()
    if root.exists() and not root.is_dir():
        raise ValueError(f"output path is not a directory: {root}")
    root.mkdir(parents=True, exist_ok=True)

    started = started_at or _utc_now()
    resolved_export_id = export_id or _new_export_id(started)
    partial_dir = root / f".{resolved_export_id}.partial"
    final_dir = root / resolved_export_id
    if partial_dir.exists() or final_dir.exists():
        raise FileExistsError(f"export path already exists for export ID '{resolved_export_id}'")
    partial_dir.mkdir()

    writer = _JsonlShardWriter(partial_dir, rows_per_file)
    try:
        batches: Iterable[pd.DataFrame] = client.export_data_batches(
            filter_query=normalized_filter,
            batch_size=rows_per_file,
            columns=selected_columns,
            deserialize_json=True,
        )
        for batch in batches:
            for record in batch.to_dict(orient="records"):
                writer.write(record)
        writer.finish()

        finished = _utc_now()
        manifest = {
            "export_id": resolved_export_id,
            "table": table_name,
            "filter": normalized_filter,
            "columns": selected_columns,
            "format": "jsonl",
            "rows_per_file": rows_per_file,
            "started_at": _format_utc(started),
            "finished_at": _format_utc(finished),
            "row_count": writer.total_rows,
            "files": writer.files,
        }
        (partial_dir / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        (partial_dir / "_SUCCESS").touch(exist_ok=False)
        partial_dir.rename(final_dir)
        return final_dir.resolve()
    except BaseException as exc:
        writer.abort()
        raise ExportFailed(partial_dir.resolve(), exc) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Export matching rows from a serving table into sharded JSONL files. "
            "The command is stateless; callers own "
            "incremental filters and deduplication."
        )
    )
    parser.add_argument(
        "--table",
        choices=ALLOWED_SERVING_TABLES,
        default=DEFAULT_SERVING_TABLE,
        help=(
            "Serving table to export. Defaults to wind_tunnel_serving; "
            "use serving_test for integration validation."
        ),
    )
    parser.add_argument(
        "--filter",
        required=True,
        dest="filter_query",
        help="DLDB WHERE predicate without the WHERE keyword.",
    )
    parser.add_argument(
        "--columns",
        default=None,
        help=(
            "Comma-separated serving columns. Defaults to the external delivery "
            "column set, which excludes search_text."
        ),
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        type=Path,
        help="Root directory under which a new export-<id> directory is published.",
    )
    parser.add_argument(
        "--rows-per-file",
        type=int,
        default=DEFAULT_ROWS_PER_FILE,
        help=f"Maximum rows per JSONL part file (default: {DEFAULT_ROWS_PER_FILE}).",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        columns = parse_columns(args.columns)
        config = build_config(args.table)
        with WTGatewayClient(config=config) as client:
            export_dir = export_serving_data(
                client,
                filter_query=args.filter_query,
                columns=columns,
                output_dir=args.output_dir,
                rows_per_file=args.rows_per_file,
            )
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(f"Export completed: {export_dir}")
    print(f"Manifest: {export_dir / 'manifest.json'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
