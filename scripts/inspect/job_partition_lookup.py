#!/usr/bin/env python3
"""Map job_id values to their dldb HASH bucket for a HASH(job_id) table.

Pure offline computation using dldb.utils.stable_hash, the same function the
SDK uses for partition pruning (wt_sdk/client.py). No S3 connection is made.
Landing/serving tables use HASH(job_id) with 128 buckets; use --partitions to
override for other tables.

Examples:
  python scripts/inspect/job_partition_lookup.py job_abc123 job_def456
  cat job_ids.txt | python scripts/inspect/job_partition_lookup.py -
  python scripts/inspect/job_partition_lookup.py job_abc123 --partitions 64
"""

import argparse
import sys

from dldb.utils import stable_hash


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Map job_id values to dldb HASH buckets (offline, read-only)."
    )
    parser.add_argument(
        "job_ids",
        nargs="+",
        help="job_id values, or '-' to read one per line from stdin.",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        default=128,
        help=(
            "Total HASH bucket count of the target table. Landing and serving "
            "tables use 128 (the default)."
        ),
    )
    args = parser.parse_args()

    if args.job_ids == ["-"]:
        job_ids = [line.strip() for line in sys.stdin if line.strip()]
    else:
        job_ids = args.job_ids

    if not job_ids:
        parser.error("no job_id values provided")

    if args.partitions <= 0:
        parser.error("--partitions must be a positive integer")

    bucket_of = {}
    for job_id in job_ids:
        bucket = stable_hash(job_id) % args.partitions
        bucket_of.setdefault(bucket, []).append(job_id)
        print(f"{job_id}\t{bucket}")

    if len(bucket_of) > 1 or len(job_ids) > 1:
        print()
        print(f"# {len(job_ids)} job_id(s) -> {len(bucket_of)} distinct bucket(s)")
        for bucket in sorted(bucket_of):
            print(f"# bucket {bucket}: {len(bucket_of[bucket])} job_id(s)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
