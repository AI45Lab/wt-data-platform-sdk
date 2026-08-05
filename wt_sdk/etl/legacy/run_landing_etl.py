#!/usr/bin/env python3
"""
Main ETL script for ingesting LLM benchmark data from S3 into landing table.

Usage:
    python -m wt_sdk.etl.legacy.run_landing_etl --s3-prefix s3://wind-tunnel-landing/liuqihua/safety_image_ch/

This script processes LLM benchmark data from S3 and ingests it into the landing table.
"""
import argparse
from loguru import logger

from wt_sdk.etl.legacy.ingest_landing_data import LandingDataETL


def main():
    parser = argparse.ArgumentParser(
        description='ETL LLM benchmark data from S3 to landing table',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run (validate only)
  python -m wt_sdk.etl.legacy.run_landing_etl --dry-run

  # Skip blob verification
  python -m wt_sdk.etl.legacy.run_landing_etl --skip-blob-check

  # Custom S3 prefix with batch size
  python -m wt_sdk.etl.legacy.run_landing_etl \\
      --s3-prefix "s3://wind-tunnel-landing/owner/dataset/type/2025-01-04_v1/" \\
      --batch-size 200

  # Ingest from specific path (make sure to set --s3-prefix)
  python -m wt_sdk.etl.legacy.run_landing_etl \\
      --s3-prefix "s3://wind-tunnel-landing/owner/dataset/type/dt_v1/"
        """
    )
    parser.add_argument(
        '--s3-prefix',
        default='s3://wind-tunnel-landing/liuqihua/safety_image_ch/',
        help='S3 prefix path to process (default: s3://wind-tunnel-landing/liuqihua/safety_image_ch/)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=100,
        help='Number of records to ingest per batch (default: 100)'
    )
    parser.add_argument(
        '--skip-schema-validation',
        action='store_true',
        help='Skip schema compatibility checks (use for trusted data)'
    )
    parser.add_argument(
        '--skip-blob-check',
        action='store_true',
        help='Skip blob existence verification (use if blobs are elsewhere)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Validate and convert data but don\'t insert into landing table'
    )
    parser.add_argument(
        '--s3-endpoint',
        default=None,
        help='Custom S3 endpoint URL (optional)'
    )
    parser.add_argument(
        '--allow-duplicates',
        action='store_true',
        help='Disable idempotence and allow duplicate records (not recommended)'
    )

    args = parser.parse_args()

    logger.info("=" * 80)
    logger.info("Landing Table ETL")
    logger.info("=" * 80)
    logger.info(f"S3 Prefix: {args.s3_prefix}")
    logger.info(f"Batch Size: {args.batch_size}")
    logger.info(f"Skip Schema Validation: {args.skip_schema_validation}")
    logger.info(f"Skip Blob Check: {args.skip_blob_check}")
    logger.info(f"Skip Duplicates: {not args.allow_duplicates}")
    logger.info(f"Dry Run: {args.dry_run}")
    logger.info("=" * 80)

    # Initialize ETL processor
    etl = LandingDataETL(s3_endpoint=args.s3_endpoint)

    # Run ingestion
    try:
        summary = etl.ingest_dataset(
            s3_prefix=args.s3_prefix,
            batch_size=args.batch_size,
            skip_schema_validation=args.skip_schema_validation,
            skip_blob_check=args.skip_blob_check,
            skip_duplicates=not args.allow_duplicates,
            dry_run=args.dry_run
        )

        # Print summary
        logger.info("=" * 80)
        logger.info("ETL Summary:")
        logger.info(f"  Files Processed: {summary['files_processed']}")
        logger.info(f"  Records Read: {summary['records_read']}")
        logger.info(f"  Records Valid: {summary['records_valid']}")
        logger.info(f"  Records Invalid: {summary['records_invalid']}")
        if 'records_skipped' in summary:
            logger.info(f"  Records Skipped (duplicates): {summary['records_skipped']}")
        logger.info(f"  Records Ingested: {summary['records_ingested']}")

        # Show error breakdown if there are errors
        if summary.get('error_breakdown'):
            breakdown = summary['error_breakdown']
            if sum(breakdown.values()) > 0:
                logger.info(f"  Errors Breakdown:")
                if breakdown.get('schema_validation', 0) > 0:
                    logger.info(f"    - Schema validation failed: {breakdown['schema_validation']}")
                if breakdown.get('missing_blobs', 0) > 0:
                    logger.info(f"    - Missing blobs: {breakdown['missing_blobs']}")
                if breakdown.get('conversion_failed', 0) > 0:
                    logger.info(f"    - Conversion failed: {breakdown['conversion_failed']}")

        # Note: Error details are written to report files by the ETL, not logged here
        if summary.get('errors'):
            logger.info(f"  (See errors report file for detailed line numbers and error messages)")
        logger.info("=" * 80)

        # Exit with appropriate code
        if summary['records_invalid'] > 0 and summary['records_ingested'] == 0:
            logger.error("✗ ETL failed: No records were ingested")
            sys.exit(1)
        elif summary['records_ingested'] > 0:
            logger.info("✓ ETL completed successfully")
            sys.exit(0)
        else:
            logger.warning("⚠ ETL completed but no records were processed")
            sys.exit(2)

    except Exception as e:
        logger.error(f"✗ ETL failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
