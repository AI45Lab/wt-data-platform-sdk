"""
Cleanup script for wind tunnel data tables.

Allows deletion of data from any table with configurable filters.
Uses DLDB SDK to properly handle logical tables and their physical partitions.

Usage:
  # Cleanup from default database (s3://wind-tunnel-dldb)
  python scripts/ops/cleanup_data.py --table landing_test --query "dataset_type = 'TEST'"

  # Preview what would be deleted (dry run)
  python scripts/ops/cleanup_data.py --table landing_test --query "dataset_type = 'TEST'" --dry-run

  # Delete from custom database
  python scripts/ops/cleanup_data.py --db-uri s3://my-bucket --table my_table --query "dataset_type = 'SFT'"

  # Delete from the separate environment-config database by table name
  python scripts/ops/cleanup_data.py --table evaluation_env_config --query "job_id = 'gateway'" --dry-run

  # Delete all data from a table (requires --force flag)
  python scripts/ops/cleanup_data.py --table landing_test --force

  # Use legacy --uri format (backward compatible)
  python scripts/ops/cleanup_data.py --uri s3://wind-tunnel-dldb/wind_tunnel_landing.lance --query "dataset_type = 'SFT'"

Examples:
  # 1. Check current data
  python scripts/ops/table_manager.py list

  # 2. Preview what to delete
  python scripts/ops/cleanup_data.py --table landing_test --query "session_id = 'test_session_0'" --dry-run

  # 3. Confirm and delete
  python scripts/ops/cleanup_data.py --table landing_test --query "session_id = 'test_session_0'"

  # 4. Verify deletion
  python scripts/inspect/query_data.py --table landing_test --count
"""
import argparse
import sys

import dldb
import pandas as pd
from wt_sdk.client import WTGatewayClient
from wt_sdk.config import (
    DEFAULT_LANDING_TABLE,
    DEFAULT_SERVING_TABLE,
    TEST_LANDING_TABLE,
    TEST_SERVING_TABLE,
    GatewayConfig,
    TableConfig,
    default_config,
    resolve_env_config_db_uri,
)


ENV_CONFIG_TABLE_NAMES = {"evaluation_env_config"}
LANDING_TABLE_NAMES = {DEFAULT_LANDING_TABLE, TEST_LANDING_TABLE}
SERVING_TABLE_NAMES = {DEFAULT_SERVING_TABLE, TEST_SERVING_TABLE}
TRAJECTORY_TABLE_NAMES = LANDING_TABLE_NAMES | SERVING_TABLE_NAMES
PREVIEW_COLUMNS = ["id", "job_id", "dataset_type", "session_id", "agent_model", "created_at"]


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


def _use_gateway_client(table_name: str) -> bool:
    """Return whether this table should use SDK partition-pruned cleanup."""
    return table_name in TRAJECTORY_TABLE_NAMES


def _table_role(table_name: str) -> str:
    if table_name in LANDING_TABLE_NAMES:
        return "landing"
    if table_name in SERVING_TABLE_NAMES:
        return "serving"
    raise ValueError(f"unsupported trajectory table: {table_name}")


def _build_gateway_client(table_name: str, db_uri: str) -> WTGatewayClient:
    """Create a table-pinned client without relying on WT_SDK_PROFILE."""
    landing_table = (
        table_name
        if table_name in LANDING_TABLE_NAMES
        else default_config.tables.landing_table
    )
    serving_table = (
        table_name
        if table_name in SERVING_TABLE_NAMES
        else default_config.tables.serving_table
    )
    return WTGatewayClient(
        GatewayConfig(
            s3=default_config.s3,
            tables=TableConfig(
                db_uri=db_uri,
                landing_table=landing_table,
                serving_table=serving_table,
            ),
            dldb_model=default_config.dldb_model,
            enable_dldb_timing_logs=default_config.enable_dldb_timing_logs,
            log_dldb_metrics_summary_on_close=default_config.log_dldb_metrics_summary_on_close,
            dldb_metrics_log_path=default_config.dldb_metrics_log_path,
        )
    )


def _query_trajectory_rows(
    client: WTGatewayClient,
    table_name: str,
    query: str,
    *,
    columns: list[str],
    limit: int | None = None,
) -> list[dict]:
    """Query active trajectory tables, treating missing HASH buckets as empty."""
    try:
        return client.query_data(
            filter_query=query,
            limit=limit,
            columns=columns,
            table=table_name,
            exclude_none=False,
            deserialize_json=False,
            checkout_latest=True,
        )
    except ValueError as exc:
        if "partition" in str(exc) and "does not exist" in str(exc):
            return []
        raise


def _print_preview(records: list[dict]) -> None:
    if not records:
        return
    preview = pd.DataFrame(records)
    preview_cols = [column for column in PREVIEW_COLUMNS if column in preview.columns]
    print("\nPreview of rows to be deleted:")
    print(preview[preview_cols].to_string())


def _cleanup_trajectory_table(
    *,
    db_name: str,
    table_name: str,
    query: str,
    dry_run: bool,
) -> int:
    """Fast filtered cleanup path for active landing/serving tables."""
    print(f"Filter query: {query}")
    print("=" * 80)
    print(
        "Using WTGatewayClient fast path: skipping full table count and "
        "counting matches with id-only partition-pruned reads."
    )

    client = _build_gateway_client(table_name, db_name)
    try:
        preview = _query_trajectory_rows(
            client,
            table_name,
            query,
            columns=PREVIEW_COLUMNS,
            limit=5,
        )
        matching_ids = _query_trajectory_rows(
            client,
            table_name,
            query,
            columns=["id"],
            limit=None,
        )
        delete_count = len(matching_ids)

        print(f"Rows matching filter: {delete_count}")
        _print_preview(preview)

        if delete_count == 0:
            print("\nNo rows to delete.")
            return 0

        if dry_run:
            print(f"\n[DRY RUN] Would delete {delete_count} rows")
            return 0

        try:
            confirm = input(f"\nDelete {delete_count} rows? (yes/no): ")
            if confirm.lower() != "yes":
                print("Aborted.")
                return 0
        except (EOFError, KeyboardInterrupt):
            print("\nAborted.")
            return 0

        role = _table_role(table_name)
        if role == "landing":
            deleted = client.delete_landing(query)
        else:
            deleted = client.delete_serving(query)
        print(f"\n✓ Delete submitted for {deleted} rows")
        print("Tip: verify with scripts/inspect/query_data.py --count if needed.")
        return 0
    except Exception as exc:
        print(f"Error executing query/delete: {exc}")
        return 1
    finally:
        client.close()


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


def main():
    parser = argparse.ArgumentParser(
        description="Cleanup wind tunnel data tables",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    # Mutually exclusive: either --uri OR --table (with optional --db-uri)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--uri", type=str,
                       help="Full table URI (legacy format, e.g., s3://wind-tunnel-dldb/table.lance)")
    group.add_argument("--table", type=str,
                       help="Table name (use with default or custom --db-uri)")

    # Optional: database URI (used with --table)
    parser.add_argument("--db-uri", type=str, default=None,
                       help="Database URI (default: s3://wind-tunnel-dldb)")

    # Filter options
    parser.add_argument("--query", type=str, default=None,
                       help="Delete only rows matching this filter (e.g., \"dataset_type = 'TEST'\"). "
                            "If not provided, ALL data will be deleted!")
    parser.add_argument("--force", action="store_true",
                       help="Required for deleting all data (without --query)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be deleted without actually deleting")

    args = parser.parse_args()

    # Determine db_name and table_name
    if args.uri:
        # Legacy format: parse from URI
        if not args.uri.endswith(".lance"):
            print(f"Error: URI must end with '.lance', got '{args.uri}'")
            return 1

        path_parts = args.uri.rstrip("/").split("/")
        if len(path_parts) < 2:
            print(f"Error: URI must contain at least a directory and filename, got '{args.uri}'")
            return 1

        db_name = "/".join(path_parts[:-1])
        table_name = path_parts[-1][:-6]  # Remove .lance suffix
        print(f"Using legacy URI format: {args.uri}")
    else:
        # New format: use --db-uri and --table
        table_name = args.table
        db_name = _resolve_db_uri(table_name, args.db_uri)
        print(f"Using database: {db_name}")
        print(f"Using table: {table_name}")
    checkout_latest = _uses_latest_snapshot_by_default(table_name)
    if checkout_latest:
        print("Checkout latest: true")

    if args.query and _use_gateway_client(table_name):
        return _cleanup_trajectory_table(
            db_name=db_name,
            table_name=table_name,
            query=args.query,
            dry_run=args.dry_run,
        )

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

    # Check if table exists (using DLDB's logical table view)
    if not session.table_exists(table_name):
        print(f"Error: Table '{table_name}' does not exist in database '{db_name}'")
        print(f"Available tables: {session.list_tables()}")
        print("\nTip: Use 'python scripts/ops/table_manager.py list' to see all tables")
        session.shutdown()
        return 1
    _pin_exact_dldb_table(session, table_name)

    # Get current count
    try:
        current_count = session.count_rows(table_name)
    except Exception as e:
        print(f"Error getting row count: {e}")
        session.shutdown()
        return 1

    print(f"Current row count: {current_count}")
    print("=" * 80)

    # Determine what to delete
    if args.query:
        # Delete with filter
        print(f"Filter query: {args.query}")
        print("=" * 80)

        # Preview what will be deleted
        try:
            preview = session.filter(
                table_name,
                args.query,
                limit=5,
                checkout_latest=checkout_latest,
            )
            # count_rows doesn't accept filter query, so we count the preview results
            # For accurate count, we need to fetch all matching rows
            all_matching = session.filter(
                table_name,
                args.query,
                checkout_latest=checkout_latest,
            )
            delete_count = len(all_matching)
        except Exception as e:
            print(f"Error executing query: {e}")
            session.shutdown()
            return 1

        print(f"Rows matching filter: {delete_count}")

        if len(preview) > 0:
            # Show relevant columns for preview
            preview_cols = ['id']
            for col in ['dataset_type', 'session_id', 'agent_model', 'created_at']:
                if col in preview.columns:
                    preview_cols.append(col)
            # Limit to 5 columns
            preview_cols = preview_cols[:5]

            print("\nPreview of rows to be deleted:")
            print(preview[preview_cols].to_string())

        if delete_count == 0:
            print("\nNo rows to delete.")
            session.shutdown()
            return 0

        if args.dry_run:
            print(f"\n[DRY RUN] Would delete {delete_count} rows")
            session.shutdown()
            return 0

        # Confirm
        try:
            confirm = input(f"\nDelete {delete_count} rows? (yes/no): ")
            if confirm.lower() != 'yes':
                print("Aborted.")
                session.shutdown()
                return 0
        except (EOFError, KeyboardInterrupt):
            print("\nAborted.")
            session.shutdown()
            return 0

        # Delete using DLDB SDK (handles all physical partitions automatically)
        try:
            session.delete(table_name, args.query)
            print(f"\n✓ Deleted {delete_count} rows")
        except Exception as e:
            print(f"Error deleting rows: {e}")
            session.shutdown()
            return 1

    else:
        # Delete all data
        print("WARNING: No filter specified. This will delete ALL data!")
        print("=" * 80)

        if not args.force:
            print("Error: --force flag is required to delete all data")
            print("Use --force to confirm, or use --query to delete specific rows")
            session.shutdown()
            return 1

        if args.dry_run:
            print(f"[DRY RUN] Would delete all {current_count} rows")
            session.shutdown()
            return 0

        # Confirm
        try:
            confirm = input(f"Delete ALL {current_count} rows? (type 'DELETE ALL' to confirm): ")
            if confirm != 'DELETE ALL':
                print("Aborted.")
                session.shutdown()
                return 0
        except (EOFError, KeyboardInterrupt):
            print("\nAborted.")
            session.shutdown()
            return 0

        # Delete all rows using DLDB SDK
        try:
            session.delete(table_name, "true")  # "true" matches all rows
            print(f"\n✓ Deleted all {current_count} rows")
        except Exception as e:
            print(f"Error deleting rows: {e}")
            session.shutdown()
            return 1

    # Show remaining count
    remaining_count = session.count_rows(table_name)
    print("=" * 80)
    print(f"Remaining rows: {remaining_count}")

    session.shutdown()
    return 0


if __name__ == "__main__":
    sys.exit(main())
