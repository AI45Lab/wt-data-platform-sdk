#!/usr/bin/env python3
"""Show fragment and scalar-index health for dldb HASH partitions.

This command is read-only. It reports aggregate Lance fragment statistics,
actual index coverage returned by dldb, and the indexes expected by the SDK.

Examples:
  python scripts/inspect/show_partition_status.py \
    --table landing_test --partition 34 --partition 94

  python scripts/inspect/show_partition_status.py \
    --table wind_tunnel_landing --all-partitions

  python scripts/inspect/show_partition_status.py \
    --table wind_tunnel_serving --partition 15 --show-all-indexes
"""

import argparse
from dataclasses import dataclass
import sys
from typing import Any, Mapping, Optional, Sequence

import dldb

from wt_sdk.config import (
    DEFAULT_LANDING_TABLE,
    DEFAULT_SERVING_TABLE,
    TEST_LANDING_TABLE,
    TEST_SERVING_TABLE,
    default_config,
)
from wt_sdk.core.schemas import LANDING_SCALAR_INDEXES, SERVING_SCALAR_INDEXES


TABLE_INDEXES = {
    DEFAULT_LANDING_TABLE: LANDING_SCALAR_INDEXES,
    TEST_LANDING_TABLE: LANDING_SCALAR_INDEXES,
    DEFAULT_SERVING_TABLE: SERVING_SCALAR_INDEXES,
    TEST_SERVING_TABLE: SERVING_SCALAR_INDEXES,
}


@dataclass(frozen=True)
class IndexReport:
    name: str
    indexed_rows: Optional[int]
    unindexed_rows: Optional[int]
    fully_indexed: bool


@dataclass(frozen=True)
class PartitionReport:
    partition: int
    state: str
    materialized: bool
    version: Optional[int] = None
    rows: Optional[int] = None
    total_bytes: Optional[int] = None
    fragments: Optional[int] = None
    small_fragments: Optional[int] = None
    fragment_min: Optional[int] = None
    fragment_p50: Optional[int] = None
    fragment_p99: Optional[int] = None
    fragment_max: Optional[int] = None
    indexes: tuple[IndexReport, ...] = ()
    missing_indexes: tuple[str, ...] = ()
    unexpected_indexes: tuple[str, ...] = ()
    error: Optional[str] = None

    @property
    def index_tails(self) -> tuple[IndexReport, ...]:
        return tuple(index for index in self.indexes if not index.fully_indexed)


def _read_attr(value: Any, name: str, default=None):
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def expected_indexes_for_table(table_name: str) -> dict[str, str]:
    """Return configured index name -> type for one supported table."""
    try:
        definitions = TABLE_INDEXES[table_name]
    except KeyError as exc:
        raise ValueError(
            f"Unsupported table {table_name!r}; expected one of: "
            f"{', '.join(sorted(TABLE_INDEXES))}"
        ) from exc
    return {f"{column}_idx": index_type for column, index_type in definitions}


def build_partition_report(
    partition: int,
    status: Any,
    expected_index_names: set[str],
) -> PartitionReport:
    """Normalize one dldb PartitionStatus and apply SDK business expectations."""
    if not bool(_read_attr(status, "materialized", False)):
        return PartitionReport(
            partition=partition,
            state="unmaterialized",
            materialized=False,
        )

    coverage = []
    for item in _read_attr(status, "coverage", []) or []:
        coverage.append(
            IndexReport(
                name=str(_read_attr(item, "index_name")),
                indexed_rows=_read_attr(item, "num_indexed_rows"),
                unindexed_rows=_read_attr(item, "num_unindexed_rows"),
                fully_indexed=bool(_read_attr(item, "fully_indexed", False)),
            )
        )
    coverage.sort(key=lambda item: item.name)

    actual_index_names = {item.name for item in coverage}
    missing_indexes = tuple(sorted(expected_index_names - actual_index_names))
    unexpected_indexes = tuple(sorted(actual_index_names - expected_index_names))

    stats = _read_attr(status, "stats")
    if stats is None:
        return PartitionReport(
            partition=partition,
            state="stats_unavailable",
            materialized=True,
            version=_read_attr(status, "version"),
            indexes=tuple(coverage),
            missing_indexes=missing_indexes,
            unexpected_indexes=unexpected_indexes,
        )

    fragment_stats = _read_attr(stats, "fragment_stats")
    lengths = _read_attr(fragment_stats, "lengths")
    rows = _read_attr(stats, "num_rows")
    fragments = _read_attr(fragment_stats, "num_fragments")
    small_fragments = _read_attr(fragment_stats, "num_small_fragments")

    if rows == 0:
        state = "empty_shell"
    else:
        issues = []
        if missing_indexes:
            issues.append("missing_idx")
        if any(not item.fully_indexed for item in coverage):
            issues.append("index_tail")
        # One small fragment cannot be compacted with another fragment. Treat
        # multiple fragments plus at least one small fragment as actionable.
        if (fragments or 0) > 1 and (small_fragments or 0) > 0:
            issues.append("fragmented")
        state = "+".join(issues) if issues else "ok"

    return PartitionReport(
        partition=partition,
        state=state,
        materialized=True,
        version=_read_attr(status, "version"),
        rows=rows,
        total_bytes=_read_attr(stats, "total_bytes"),
        fragments=fragments,
        small_fragments=small_fragments,
        fragment_min=_read_attr(lengths, "min"),
        fragment_p50=_read_attr(lengths, "p50"),
        fragment_p99=_read_attr(lengths, "p99"),
        fragment_max=_read_attr(lengths, "max"),
        indexes=tuple(coverage),
        missing_indexes=missing_indexes,
        unexpected_indexes=unexpected_indexes,
    )


def _error_report(partition: int, exc: BaseException) -> PartitionReport:
    return PartitionReport(
        partition=partition,
        state="error",
        materialized=False,
        error=f"{type(exc).__name__}: {exc}",
    )


def _get_hash_table_record(session, table_name: str):
    """Validate and return one exact HASH-table information-schema record."""
    record = session.schema_table.get(table_name)
    if record is None:
        raise ValueError(f"Logical table not found: {table_name}")
    if str(record.partition_type).upper() != "HASH":
        raise ValueError(
            f"{table_name!r} uses partition type {record.partition_type!r}; "
            "partition_status requires HASH"
        )
    if not isinstance(record.partitions, int) or record.partitions <= 0:
        raise ValueError(f"{table_name!r} has invalid HASH partition count: {record.partitions!r}")

    return record


def _resolve_partitions(requested: Sequence[int], all_partitions: bool, count: int) -> list[int]:
    partitions = list(range(count)) if all_partitions else sorted(set(requested))
    invalid = [partition for partition in partitions if partition < 0 or partition >= count]
    if invalid:
        raise ValueError(
            f"Partition(s) outside valid range [0, {count}): "
            f"{', '.join(map(str, invalid))}"
        )
    return partitions


def _format_number(value: Optional[int]) -> str:
    return "-" if value is None else f"{value:,}"


def _format_bytes(value: Optional[int]) -> str:
    if value is None:
        return "-"
    amount = float(value)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(amount) < 1024 or unit == "TiB":
            return f"{amount:.1f}{unit}" if unit != "B" else f"{int(amount)}B"
        amount /= 1024
    return f"{value}B"


def _format_index_rows(value: Optional[int]) -> str:
    return "unknown" if value is None else f"{value:,}"


def _print_report_header(
    table_name: str,
    partition_column: str,
    logical_partitions: int,
    expected_indexes: Mapping[str, str],
) -> None:
    print(f"Table: {table_name}")
    print(f"Partitioning: HASH({partition_column}), logical_buckets={logical_partitions}")
    print(
        "Expected indexes: "
        + ", ".join(
            f"{name}({index_type})" for name, index_type in sorted(expected_indexes.items())
        )
    )
    print()
    print(
        f"{'BUCKET':>6}  {'STATE':<36} {'ROWS':>10} {'VER':>6} {'BYTES':>10} "
        f"{'FRAGS':>6} {'SMALL':>6} {'MIN':>8} {'P50':>8} {'P99':>8} {'MAX':>8} "
        f"{'IDX':>7} {'TAIL':>5}"
    )
    print("-" * 152)


def _print_partition_report(
    report: PartitionReport,
    expected_index_count: int,
    *,
    show_all_indexes: bool,
) -> None:
    index_count = len(report.indexes)
    tail_count = len(report.index_tails)
    print(
        f"{report.partition:>6}  {report.state:<36} "
        f"{_format_number(report.rows):>10} {_format_number(report.version):>6} "
        f"{_format_bytes(report.total_bytes):>10} "
        f"{_format_number(report.fragments):>6} "
        f"{_format_number(report.small_fragments):>6} "
        f"{_format_number(report.fragment_min):>8} "
        f"{_format_number(report.fragment_p50):>8} "
        f"{_format_number(report.fragment_p99):>8} "
        f"{_format_number(report.fragment_max):>8} "
        f"{f'{index_count}/{expected_index_count}':>7} {tail_count:>5}",
        flush=True,
    )

    if report.error:
        print(f"        error: {report.error}", flush=True)
    if report.missing_indexes and report.rows != 0:
        print(f"        missing: {', '.join(report.missing_indexes)}", flush=True)
    if report.unexpected_indexes:
        print(f"        unexpected: {', '.join(report.unexpected_indexes)}", flush=True)

    indexes_to_show = report.indexes if show_all_indexes else report.index_tails
    for index in indexes_to_show:
        print(
            f"        {index.name}: indexed={_format_index_rows(index.indexed_rows)} "
            f"unindexed={_format_index_rows(index.unindexed_rows)} "
            f"fully_indexed={str(index.fully_indexed).lower()}",
            flush=True,
        )


def _print_report_summary(reports: Sequence[PartitionReport]) -> None:
    materialized = [report for report in reports if report.materialized]
    nonempty = [report for report in materialized if (report.rows or 0) > 0]
    unmaterialized = sum(
        not report.materialized and not report.error for report in reports
    )
    print("-" * 152)
    print(
        "Summary: "
        f"inspected={len(reports)}, "
        f"materialized={len(materialized)}, "
        f"unmaterialized={unmaterialized}, "
        f"empty={sum(report.rows == 0 for report in materialized)}, "
        f"missing_idx={sum(bool(report.missing_indexes) for report in nonempty)}, "
        f"index_tail={sum(bool(report.index_tails) for report in nonempty)}, "
        f"fragmented={sum('fragmented' in report.state for report in nonempty)}, "
        f"errors={sum(report.error is not None for report in reports)}, "
        f"rows={sum(report.rows or 0 for report in materialized):,}, "
        f"fragments={sum(report.fragments or 0 for report in materialized):,}, "
        f"small_fragments={sum(report.small_fragments or 0 for report in materialized):,}"
    )


def print_report(
    table_name: str,
    partition_column: str,
    logical_partitions: int,
    reports: Sequence[PartitionReport],
    expected_indexes: Mapping[str, str],
    *,
    show_all_indexes: bool,
) -> None:
    """Print a complete report; inspect_table streams the same sections incrementally."""
    _print_report_header(
        table_name,
        partition_column,
        logical_partitions,
        expected_indexes,
    )
    expected_count = len(expected_indexes)
    for report in reports:
        _print_partition_report(
            report,
            expected_count,
            show_all_indexes=show_all_indexes,
        )
    _print_report_summary(reports)


def inspect_table(
    table_name: str,
    db_uri: str,
    requested_partitions: Sequence[int],
    *,
    all_partitions: bool,
    show_all_indexes: bool,
) -> int:
    expected_indexes = expected_indexes_for_table(table_name)
    session = dldb.connect(
        db_uri,
        storage_options=default_config.s3.to_storage_options(),
    )
    try:
        if not callable(getattr(session, "partition_status", None)):
            raise RuntimeError(
                "Installed dldb does not expose partition_status(); upgrade dldb first"
            )

        record = _get_hash_table_record(session, table_name)
        partitions = _resolve_partitions(
            requested_partitions,
            all_partitions,
            record.partitions,
        )

        reports = []
        expected_names = set(expected_indexes)
        _print_report_header(
            table_name,
            record.partition_column,
            record.partitions,
            expected_indexes,
        )
        for partition in partitions:
            try:
                status = session.partition_status(table_name, partition=partition)
                report = build_partition_report(partition, status, expected_names)
            except Exception as exc:
                report = _error_report(partition, exc)
            reports.append(report)
            _print_partition_report(
                report,
                len(expected_indexes),
                show_all_indexes=show_all_indexes,
            )

        _print_report_summary(reports)
        return 1 if any(report.error for report in reports) else 0
    finally:
        session.shutdown()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Show read-only fragment and index status for dldb HASH buckets"
    )
    parser.add_argument(
        "--table",
        required=True,
        choices=sorted(TABLE_INDEXES),
        help="Supported landing or serving table.",
    )
    parser.add_argument(
        "--db-uri",
        default=None,
        help="Database URI (default: configured WT_SDK_DB_URI).",
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument(
        "--partition",
        action="append",
        type=int,
        default=[],
        help="HASH bucket integer. Can be repeated.",
    )
    selection.add_argument(
        "--all-partitions",
        action="store_true",
        help="Inspect every logical HASH bucket, including unmaterialized buckets.",
    )
    parser.add_argument(
        "--show-all-indexes",
        action="store_true",
        help="Print every actual index; by default only missing indexes and tails are detailed.",
    )
    args = parser.parse_args()

    try:
        return inspect_table(
            args.table,
            args.db_uri or default_config.tables.db_uri,
            args.partition,
            all_partitions=args.all_partitions,
            show_all_indexes=args.show_all_indexes,
        )
    except Exception as exc:
        print(f"Error: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
