"""
ETL module for ingesting raw LLM benchmark data from S3 into landing table.

This is an operational tool for data ingestion, not part of the SDK library.

Data path format:
  s3://wind-tunnel-landing/{data_owner_name}/{dataset_name}/{dataset_type}/{dt}_{version}/data.jsonl

Blobs path format:
  s3://wind-tunnel-landing/{data_owner_name}/{dataset_name}/{dataset_type}/{dt}_{version}/blobs/...
"""
import json
import time
import os
from pathlib import Path
from typing import List, Optional, Dict, Any, Iterator
from loguru import logger
import s3fs
import sys
from datetime import datetime
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from wt_sdk.client import WTGatewayClient
from wt_sdk.models import LandingRecord, ChatMessage, ContentItem
from wt_sdk.utils import generate_deterministic_id, generate_event_id, retry_on_s3_error, resolve_hostname_to_ips


class LandingDataETL:
    """
    ETL processor for LLM benchmark data from S3 to landing table.

    Handles:
    1. Discovery of data files in S3
    2. Validation against landing_table schema
    3. Verification of blob references
    4. Conversion to LandingRecord
    5. Ingestion into landing_table
    """

    def __init__(
        self,
        s3_endpoint: Optional[str] = None,
        client: Optional[WTGatewayClient] = None
    ):
        """
        Initialize the ETL processor.

        Args:
            s3_endpoint: S3 endpoint URL (if None, uses config from WTGatewayClient)
            client: WTGatewayClient instance (if None, creates new one)
        """
        self.client = client or WTGatewayClient()

        # Use S3 credentials from the client's config
        s3_config = self.client.config.s3
        endpoint = s3_endpoint or s3_config.aws_endpoint

        # Configure boto3 retry settings for transient errors (502, 503, etc.)
        # This provides an additional layer of retry at the boto3 level
        # Note: s3fs uses 'config_kwargs' (not client_kwargs['config']) to pass botocore.Config
        self.s3 = s3fs.S3FileSystem(
            key=s3_config.aws_access_key_id,
            secret=s3_config.aws_secret_access_key,
            endpoint_url=endpoint,
            client_kwargs={'endpoint_url': endpoint},
            config_kwargs={
                'retries': {
                    'max_attempts': 5,  # Boto3 will retry up to 5 times
                    'mode': 'adaptive'  # Adaptive retry mode adjusts based on error rates
                },
                'connect_timeout': 10,  # 10 seconds connection timeout
                'read_timeout': 60,     # 60 seconds read timeout
            }
        )

        # Store endpoint for troubleshooting
        self.s3_endpoint = endpoint

        # Resolve and log S3 endpoint IP addresses for troubleshooting
        # This helps S3 DevOps team identify which server is receiving requests
        try:
            # Extract hostname from endpoint URL
            if endpoint:
                from urllib.parse import urlparse
                parsed_endpoint = urlparse(endpoint)
                hostname = parsed_endpoint.hostname

                if hostname:
                    resolved_ips = resolve_hostname_to_ips(hostname)
                    if resolved_ips:
                        logger.info(
                            f"LandingDataETL initialized with retry configuration (max_attempts=5, mode=adaptive)\n"
                            f"  S3 Endpoint: {endpoint}\n"
                            f"  Hostname: {hostname}\n"
                            f"  Resolved IPs: {', '.join(resolved_ips)}"
                        )
                    else:
                        logger.info(
                            f"LandingDataETL initialized with retry configuration (max_attempts=5, mode=adaptive)\n"
                            f"  S3 Endpoint: {endpoint}\n"
                            f"  Hostname: {hostname}\n"
                            f"  Resolved IPs: [failed to resolve]"
                        )
                else:
                    logger.info(f"LandingDataETL initialized with retry configuration (max_attempts=5, mode=adaptive)")
            else:
                logger.info(f"LandingDataETL initialized with retry configuration (max_attempts=5, mode=adaptive)")
        except Exception as e:
            logger.warning(f"Failed to resolve S3 endpoint IPs: {e}")
            logger.info(f"LandingDataETL initialized with retry configuration (max_attempts=5, mode=adaptive)")

    def _log_s3_endpoint_ips(self, context: str = ""):
        """
        Log current S3 endpoint IP resolution for troubleshooting.

        This helps S3 DevOps team identify which server requests are being directed to,
        especially useful when 502 errors occur to see if DNS resolution has changed.

        Args:
            context: Additional context for the log message (e.g., "on 502 error")
        """
        try:
            if self.s3_endpoint:
                from urllib.parse import urlparse
                parsed_endpoint = urlparse(self.s3_endpoint)
                hostname = parsed_endpoint.hostname

                if hostname:
                    resolved_ips = resolve_hostname_to_ips(hostname)
                    if resolved_ips:
                        logger.info(
                            f"S3 Endpoint DNS Resolution {context}:\n"
                            f"  Endpoint: {self.s3_endpoint}\n"
                            f"  Hostname: {hostname}\n"
                            f"  Current Resolved IPs: {', '.join(resolved_ips)}"
                        )
                    else:
                        logger.warning(
                            f"S3 Endpoint DNS Resolution {context}:\n"
                            f"  Endpoint: {self.s3_endpoint}\n"
                            f"  Hostname: {hostname}\n"
                            f"  Current Resolved IPs: [failed to resolve - DNS issue?]"
                        )
        except Exception as e:
            logger.warning(f"Failed to log S3 endpoint IPs {context}: {e}")

    def _on_s3_error(self, exc: Exception, attempt: int):
        """
        Callback function called on each S3 error for DNS resolution logging.

        This helps S3 DevOps team troubleshoot by seeing which IP requests are directed to.

        Args:
            exc: The exception that occurred
            attempt: The attempt number (1-indexed)
        """
        from wt_sdk.utils import is_retryable_s3_error
        if is_retryable_s3_error(exc):
            # Log DNS resolution for troubleshooting 502/503/504 errors
            error_code = "unknown"
            if "502" in str(exc):
                error_code = "502"
            elif "503" in str(exc):
                error_code = "503"
            elif "504" in str(exc):
                error_code = "504"

            self._log_s3_endpoint_ips(f"on {error_code} error (attempt {attempt})")

    def _is_lancedb_s3_error(self, exc: Exception) -> bool:
        """
        Check if an exception is a LanceDB S3 error (retryable).

        LanceDB errors include "lance error" or "LanceError" in the message.

        Args:
            exc: The exception to check

        Returns:
            True if this is a retryable LanceDB S3 error
        """
        error_str = str(exc).lower()

        # Check for LanceDB errors
        if "lance" not in error_str and "lancedb" not in error_str:
            return False

        # Check for S3-related errors in LanceDB
        s3_indicators = ["s3 error", "502", "503", "504", "bad gateway", "timeout"]
        return any(indicator in error_str for indicator in s3_indicators)

    def _ingest_batch_with_retry(self, batch_records: list, dry_run: bool = False):
        """
        Ingest a batch of records with infinite retry on LanceDB S3 errors.

        This wraps client.ingest_landing_batch() to handle 502 errors that occur
        when LanceDB tries to write to S3.

        Args:
            batch_records: List of LandingRecord objects to ingest
            dry_run: If True, don't actually insert data

        Raises:
            Exception: If a non-retryable error occurs
        """
        if dry_run or not batch_records:
            return

        import time

        delay = 60.0
        max_delay = 120.0
        attempt = 0

        while True:
            try:
                self.client.ingest_landing_batch(batch_records)
                return  # Success!

            except Exception as e:
                # Check if this is a retryable LanceDB S3 error
                if not self._is_lancedb_s3_error(e):
                    logger.error(f"Non-retryable error during ingestion: {str(e)}")
                    raise

                # This is a retryable LanceDB S3 error
                attempt += 1

                # Log DNS resolution on error
                self._on_s3_error(e, attempt)

                # Log retry with context
                logger.warning(
                    f"LanceDB S3 write error during batch ingestion "
                    f"(attempt {attempt}): {str(e)}. "
                    f"Retrying in {delay:.1f}s... (infinite retry mode)"
                )

                time.sleep(delay)
                delay = min(delay * 2.0, max_delay)

    def _s3_ls(self, path: str, detail: bool = False):
        """
        List S3 directory with infinite retry logic and DNS logging.

        Retries indefinitely on transient S3 errors (502, 503, 504).
        Logs DNS resolution on each error for troubleshooting.
        Delay between retries: 60s → 120s (capped at max_delay).

        Args:
            path: S3 path to list
            detail: Whether to return detailed info

        Returns:
            List of paths or detailed info
        """
        from wt_sdk.utils import is_retryable_s3_error
        import time

        delay = 60.0
        max_delay = 120.0
        attempt = 0

        while True:
            try:
                return self.s3.ls(path, detail=detail)
            except Exception as e:
                attempt += 1
                if not is_retryable_s3_error(e):
                    logger.debug(f"Non-retryable S3 error in _s3_ls: {str(e)}")
                    raise

                # Log DNS on error
                self._on_s3_error(e, attempt)

                # Log retry
                logger.warning(
                    f"S3 operation _s3_ls encountered transient error "
                    f"(attempt {attempt}): {str(e)}. "
                    f"Retrying in {delay:.1f}s... (infinite retry mode)"
                )

                time.sleep(delay)
                delay = min(delay * 2.0, max_delay)

    def _s3_isdir(self, path: str) -> bool:
        """
        Check if S3 path is a directory with infinite retry logic and DNS logging.

        Retries indefinitely on transient S3 errors (502, 503, 504).
        Logs DNS resolution on each error for troubleshooting.

        Args:
            path: S3 path to check

        Returns:
            True if directory, False otherwise
        """
        from wt_sdk.utils import is_retryable_s3_error
        import time

        delay = 60.0
        max_delay = 120.0
        attempt = 0

        while True:
            try:
                return self.s3.isdir(path)
            except Exception as e:
                attempt += 1
                if not is_retryable_s3_error(e):
                    logger.debug(f"Non-retryable S3 error in _s3_isdir: {str(e)}")
                    raise

                # Log DNS on error
                self._on_s3_error(e, attempt)

                # Log retry
                logger.warning(
                    f"S3 operation _s3_isdir encountered transient error "
                    f"(attempt {attempt}): {str(e)}. "
                    f"Retrying in {delay:.1f}s... (infinite retry mode)"
                )

                time.sleep(delay)
                delay = min(delay * 2.0, max_delay)

    def _s3_open(self, path: str, mode: str = 'r'):
        """
        Open S3 file with infinite retry logic and DNS logging.

        Retries indefinitely on transient S3 errors (502, 503, 504).
        Logs DNS resolution on each error for troubleshooting.

        Args:
            path: S3 path to open
            mode: File mode (default: 'r')

        Returns:
            File object
        """
        from wt_sdk.utils import is_retryable_s3_error
        import time

        delay = 60.0
        max_delay = 120.0
        attempt = 0

        while True:
            try:
                return self.s3.open(path, mode)
            except Exception as e:
                attempt += 1
                if not is_retryable_s3_error(e):
                    logger.debug(f"Non-retryable S3 error in _s3_open: {str(e)}")
                    raise

                # Log DNS on error
                self._on_s3_error(e, attempt)

                # Log retry
                logger.warning(
                    f"S3 operation _s3_open encountered transient error "
                    f"(attempt {attempt}): {str(e)} [path: {path}]. "
                    f"Retrying in {delay:.1f}s... (infinite retry mode)"
                )

                time.sleep(delay)
                delay = min(delay * 2.0, max_delay)

    def discover_data_files(
        self,
        s3_prefix: str
    ) -> List[str]:
        """
        Discover all data.jsonl files under the given S3 prefix.

        Expected S3 path structure:
        s3://wind-tunnel-landing/{data_owner_name}/{dataset_name}/{dataset_type}/{dt}_{version}/data.jsonl

        This method:
        1. Lists all items under the given prefix
        2. If data.jsonl is directly in the list, adds it
        3. If a directory is found, checks if data.jsonl exists inside it
        4. Does NOT recurse into subdirectories (like /blobs/) which only contain multimodal data

        Args:
            s3_prefix: S3 prefix path (e.g., s3://wind-tunnel-landing/liuqihua/safety_image_ch/)
                      Can be at any level: owner/, owner/dataset/, owner/dataset/type/, or owner/dataset/type/dt_version/

        Returns:
            List of S3 URIs to data.jsonl files
        """
        # Remove trailing slash
        s3_prefix = s3_prefix.rstrip('/')

        data_files = []

        try:
            # List all paths (files and directories) under the prefix
            # Uses retry-wrapped method to handle transient S3 errors
            paths = self._s3_ls(s3_prefix, detail=False)

            for path in paths:
                # Remove 's3://' prefix if present (ls() returns paths without it)
                clean_path = path.replace('s3://', '')
                full_path = f"s3://{clean_path}"

                # Check if it's a data.jsonl file directly in the listing
                if clean_path.endswith('data.jsonl'):
                    data_files.append(full_path)
                    logger.debug(f"Found data.jsonl: {full_path}")
                # Check if it's a directory (uses retry-wrapped method)
                elif self._s3_isdir(full_path):
                    # Check if data.jsonl exists in this directory (uses retry-wrapped method)
                    potential_jsonl = f"{full_path.rstrip('/')}/data.jsonl"
                    if self._s3_path_exists(potential_jsonl):
                        data_files.append(potential_jsonl)
                        logger.debug(f"Found data.jsonl in directory: {full_path}")

            # If no files found at this level, the prefix might be too high (e.g., at owner/ level)
            # Try to go one level deeper and search {dt}_{version} directories
            if not data_files:
                logger.debug(f"No data.jsonl found at {s3_prefix}, checking subdirectories...")
                paths = self._s3_ls(s3_prefix, detail=False)

                for path in paths:
                    clean_path = path.replace('s3://', '')
                    full_path = f"s3://{clean_path}"

                    if self._s3_isdir(full_path):
                        # Recurse one level to find {dt}_{version} directories
                        sub_files = self.discover_data_files(full_path)
                        data_files.extend(sub_files)

        except Exception as e:
            logger.error(f"Error discovering files in {s3_prefix}: {e}")

        logger.info(f"Found {len(data_files)} data.jsonl files in {s3_prefix}")
        return data_files

    def read_jsonl(self, s3_uri: str) -> Iterator[tuple[int, Dict[str, Any]]]:
        """
        Read and parse JSONL file from S3 with infinite retry logic.

        Retries entire file reading operation on transient S3 errors.
        This is necessary because 502 errors can occur during file reading,
        not just during file opening.

        Args:
            s3_uri: S3 URI to data.jsonl file

        Yields:
            Tuples of (line_number, data_dict) from the JSONL file
        """
        from wt_sdk.utils import is_retryable_s3_error
        import time

        delay = 60.0
        max_delay = 120.0
        attempt = 0
        last_successful_line = 0

        while True:
            try:
                # Open file and read from where we left off
                with self.s3.open(s3_uri, 'r') as f:
                    for line_num, line in enumerate(f, 1):
                        # Skip lines we've already successfully yielded
                        if line_num <= last_successful_line:
                            continue

                        line = line.strip()
                        if not line:
                            continue

                        try:
                            data = json.loads(line)
                            # Log progress every 1000 records
                            if line_num % 1000 == 0:
                                logger.info(f"Read {line_num} lines from {s3_uri}")

                            # Yield the data
                            yield (line_num, data)
                            last_successful_line = line_num

                        except json.JSONDecodeError as e:
                            # JSON errors are not S3 errors - log and skip the line
                            logger.error(f"Invalid JSON at line {line_num} in {s3_uri}: {e}")
                            last_successful_line = line_num

                # If we get here, file reading completed successfully
                break

            except Exception as e:
                # Check if this is a retryable S3 error
                if not is_retryable_s3_error(e):
                    logger.error(f"Non-retryable error reading {s3_uri}: {str(e)}")
                    raise

                # This is a retryable error (502, 503, 504, etc.)
                attempt += 1

                # Log DNS resolution on error
                self._on_s3_error(e, attempt)

                # Log retry with context
                logger.warning(
                    f"S3 file reading encountered transient error at line {last_successful_line + 1} "
                    f"(attempt {attempt}): {str(e)} [file: {s3_uri}]. "
                    f"Will resume from line {last_successful_line + 1} after {delay:.1f}s... (infinite retry mode)"
                )

                time.sleep(delay)
                delay = min(delay * 2.0, max_delay)

    def validate_schema_compatibility(
        self,
        data: Dict[str, Any]
    ) -> tuple[bool, List[str]]:
        """
        Check if data aligns with landing_table schema.

        Args:
            data: Raw data dict from JSONL

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []

        # Check required fields
        # Note: dataset_type is optional here as it can be auto-filled from S3 path
        required_fields = ['messages']  # Only messages is truly required
        for field in required_fields:
            if field not in data:
                errors.append(f"Missing required field: {field}")

        # Check if messages is a non-empty list
        if 'messages' in data:
            if not isinstance(data['messages'], list):
                errors.append(f"'messages' must be a list, got {type(data['messages'])}")
            elif len(data['messages']) == 0:
                errors.append("'messages' cannot be empty")
            else:
                # Validate each message
                for i, msg in enumerate(data['messages']):
                    if not isinstance(msg, dict):
                        errors.append(f"messages[{i}] must be a dict")
                        continue
                    if 'role' not in msg:
                        errors.append(f"messages[{i}] missing 'role' field")
                    if 'content' not in msg:
                        errors.append(f"messages[{i}] missing 'content' field")

        # Check dataset_type format if present (will be auto-filled from path if missing)
        if 'dataset_type' in data:
            valid_types = ['SFT', 'DPO', 'RL', 'Benchmark', 'PreTrain']
            # Case-insensitive comparison
            if data['dataset_type'].capitalize() not in valid_types and data['dataset_type'] not in valid_types:
                errors.append(f"Invalid dataset_type: {data['dataset_type']}. Must be one of {valid_types}")

        is_valid = len(errors) == 0
        return is_valid, errors

    def verify_blob_references(
        self,
        data: Dict[str, Any],
        base_s3_path: str
    ) -> tuple[bool, List[str]]:
        """
        Check if referenced multimodal data exists in /blobs folder.

        Args:
            data: Raw data dict from JSONL (URLs already normalized to full S3 paths)
            base_s3_path: Base S3 path for the dataset (used to derive blobs path)

        Returns:
            Tuple of (all_exist, list of missing blob paths - returned as relative paths)
        """
        missing_blobs = []

        # Extract blob URIs from content (S3 URIs or relative paths)
        blob_uris = self._extract_blob_s3_uris(data)

        # For each blob URI, check if it exists
        for blob_uri in blob_uris:
            try:
                # Convert relative path to full S3 URL for checking
                if blob_uri.startswith('blobs/') or blob_uri.startswith('./blobs/'):
                    # This should already be normalized by _normalize_s3_urls
                    # But if we get here, keep the original relative path for reporting
                    full_url = f"{base_s3_path.rstrip('/')}/{blob_uri.lstrip('./')}"
                    if not self._s3_path_exists(full_url):
                        missing_blobs.append(blob_uri)
                elif blob_uri.startswith('s3://'):
                    # Full S3 URL
                    if not self._s3_path_exists(blob_uri):
                        # Convert to relative path for summary
                        if '/blobs/' in blob_uri:
                            relative = "blobs/" + blob_uri.split('/blobs/')[-1]
                            missing_blobs.append(relative)
                        else:
                            missing_blobs.append(blob_uri)
            except Exception as e:
                # On error, record as missing
                if '/blobs/' in blob_uri:
                    relative = "blobs/" + blob_uri.split('/blobs/')[-1]
                    missing_blobs.append(relative)
                else:
                    missing_blobs.append(blob_uri)

        all_exist = len(missing_blobs) == 0
        return all_exist, missing_blobs

    def _extract_blob_s3_uris(self, data: Dict[str, Any]) -> List[str]:
        """
        Extract all blob URIs (S3 URIs or relative paths) from the data.

        Args:
            data: Raw data dict

        Returns:
            List of blob URIs found in the data
        """
        uris = []

        def extract_from_value(value: Any):
            if isinstance(value, str):
                # Check if it's a blob reference (S3 URI or relative path)
                if value.startswith('s3://') or value.startswith('blobs/') or value.startswith('./blobs/'):
                    uris.append(value)
            elif isinstance(value, dict):
                for v in value.values():
                    extract_from_value(v)
            elif isinstance(value, list):
                for item in value:
                    extract_from_value(item)

        extract_from_value(data)
        return uris

    def _s3_path_exists(self, s3_uri: str) -> bool:
        """
        Check if S3 path exists with infinite retry and DNS logging.

        Retries indefinitely on transient S3 errors (502, 503, 504).
        Logs DNS resolution on each error for troubleshooting.

        Args:
            s3_uri: S3 URI to check

        Returns:
            True if exists, False otherwise
        """
        from wt_sdk.utils import is_retryable_s3_error
        import time

        delay = 60.0
        max_delay = 120.0
        attempt = 0

        while True:
            try:
                return self.s3.exists(s3_uri)
            except Exception as e:
                attempt += 1
                if not is_retryable_s3_error(e):
                    logger.debug(f"Non-retryable S3 error in _s3_path_exists: {str(e)}")
                    raise

                # Log DNS on error
                self._on_s3_error(e, attempt)

                # Log retry
                logger.warning(
                    f"S3 operation _s3_path_exists encountered transient error "
                    f"(attempt {attempt}): {str(e)} [uri: {s3_uri}]. "
                    f"Retrying in {delay:.1f}s... (infinite retry mode)"
                )

                time.sleep(delay)
                delay = min(delay * 2.0, max_delay)

    def _write_missing_blobs_report(
        self,
        missing_blobs: List[str],
        s3_source_path: str,
        summary: Dict[str, Any]
    ) -> Optional[str]:
        """
        Write missing blobs report to a file.

        Args:
            missing_blobs: List of missing blob URLs (may contain duplicates)
            s3_source_path: Relative S3 path (e.g., "wind-tunnel-landing/owner/dataset/type/dt_version")
            summary: ETL summary dict with statistics

        Returns:
            Path to the report file, or None if no missing blobs
        """
        if not missing_blobs:
            return None

        # Extract data owner from path (e.g., "wind-tunnel-landing/liuqihua/..." -> "liuqihua")
        path_parts = s3_source_path.split('/')
        if len(path_parts) >= 2:
            data_owner = path_parts[1]  # Second part is the owner name
        else:
            data_owner = "unknown"

        # Create output directory structure: scripts/existing_data_etl/output/{data_owner}/
        output_dir = Path(__file__).parent / "output" / data_owner
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename based on dataset path and timestamp
        # Format: missing_blobs_{dataset}_{timestamp}.txt
        # Remove owner prefix from filename since it's now in directory structure
        path_parts_without_owner = path_parts[2:] if len(path_parts) > 2 else path_parts
        dataset_identifier = "_".join(path_parts_without_owner).replace("\\", "_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"missing_blobs_{dataset_identifier}_{timestamp}.txt"
        output_path = output_dir / filename

        # Deduplicate and sort the missing blobs
        unique_missing = sorted(set(missing_blobs))

        # Write to file
        try:
            with open(output_path, 'w') as f:
                f.write(f"Missing Blobs Report\n")
                f.write(f"===================\n")
                f.write(f"\n")
                f.write(f"Source: {s3_source_path}\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"\n")
                f.write(f"ETL Summary:\n")
                f.write(f"------------\n")
                f.write(f"  Files Processed: {summary.get('files_processed', 0)}\n")
                f.write(f"  Records Read: {summary.get('records_read', 0)}\n")
                f.write(f"  Records Valid: {summary.get('records_valid', 0)}\n")
                f.write(f"  Records Invalid: {summary.get('records_invalid', 0)}\n")
                f.write(f"  Records Skipped (duplicates): {summary.get('records_skipped', 0)}\n")
                f.write(f"  Records Ingested: {summary.get('records_ingested', 0)}\n")
                f.write(f"\n")
                f.write(f"Missing Blobs Details:\n")
                f.write(f"----------------------\n")
                f.write(f"Total Unique Missing: {len(unique_missing)}\n")
                f.write(f"Total References: {len(missing_blobs)}\n")
                f.write(f"\n")
                f.write(f"Missing Blob Paths (relative):\n")
                f.write(f"{'-' * 80}\n")
                for blob_url in unique_missing:
                    # Try to show relative path if it's a full S3 URL
                    if '/blobs/' in blob_url:
                        relative_path = "blobs/" + blob_url.split('/blobs/')[-1]
                        f.write(f"{relative_path}\n")
                    else:
                        f.write(f"{blob_url}\n")

            logger.info(f"Missing blobs report written to: {output_path}")
            return str(output_path)
        except Exception as e:
            logger.error(f"Failed to write missing blobs report: {e}")
            return None

    def _write_errors_report(
        self,
        errors: List[Dict[str, Any]],
        s3_source_path: str,
        summary: Dict[str, Any]
    ) -> Optional[str]:
        """
        Write validation/conversion errors report to a file.

        Args:
            errors: List of error dicts with file, line, record_id, error_type, error_message
            s3_source_path: Relative S3 path (e.g., "wind-tunnel-landing/owner/dataset/type/dt_version")
            summary: ETL summary dict with statistics

        Returns:
            Path to the report file, or None if no errors
        """
        if not errors:
            return None

        # Extract data owner from path
        path_parts = s3_source_path.split('/')
        if len(path_parts) >= 2:
            data_owner = path_parts[1]
        else:
            data_owner = "unknown"

        # Create output directory structure: scripts/existing_data_etl/output/{data_owner}/
        output_dir = Path(__file__).parent / "output" / data_owner
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename
        path_parts_without_owner = path_parts[2:] if len(path_parts) > 2 else path_parts
        dataset_identifier = "_".join(path_parts_without_owner).replace("\\", "_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"errors_{dataset_identifier}_{timestamp}.txt"
        output_path = output_dir / filename

        # Group errors by type
        errors_by_type: Dict[str, List[Dict]] = {}
        for error in errors:
            error_type = error.get('error_type', 'unknown')
            errors_by_type.setdefault(error_type, []).append(error)

        # Write to file
        try:
            with open(output_path, 'w') as f:
                f.write(f"ETL Errors Report\n")
                f.write(f"=================\n")
                f.write(f"\n")
                f.write(f"Source: {s3_source_path}\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"\n")
                f.write(f"ETL Summary:\n")
                f.write(f"------------\n")
                f.write(f"  Files Processed: {summary.get('files_processed', 0)}\n")
                f.write(f"  Records Read: {summary.get('records_read', 0)}\n")
                f.write(f"  Records Valid: {summary.get('records_valid', 0)}\n")
                f.write(f"  Records Invalid: {summary.get('records_invalid', 0)}\n")
                f.write(f"  Records Skipped (duplicates): {summary.get('records_skipped', 0)}\n")
                f.write(f"  Records Ingested: {summary.get('records_ingested', 0)}\n")
                f.write(f"  Total Errors: {len(errors)}\n")
                f.write(f"\n")
                f.write(f"Errors by Type:\n")
                f.write(f"{'-' * 80}\n")
                for error_type, type_errors in errors_by_type.items():
                    f.write(f"\n{error_type.upper()}: {len(type_errors)} errors\n")
                    f.write(f"{'=' * 80}\n")
                    for error in type_errors[:100]:  # Limit to first 100 per type
                        file_name = error.get('file', '').split('/')[-1]
                        f.write(f"  Line {error.get('line')}: {error.get('record_id')} in {file_name}\n")
                        f.write(f"    Error: {error.get('error_message')}\n")
                    if len(type_errors) > 100:
                        f.write(f"  ... and {len(type_errors) - 100} more {error_type} errors\n")

            logger.info(f"Errors report written to: {output_path}")
            return str(output_path)
        except Exception as e:
            logger.error(f"Failed to write errors report: {e}")
            return None

    def convert_to_landing_record(
        self,
        data: Dict[str, Any],
        dataset_owner: str,
        dataset_name: str,
        dt: str,
        version: str,
        s3_source_path: Optional[str] = None,
        dataset_type_from_path: Optional[str] = None
    ) -> Optional[LandingRecord]:
        """
        Convert raw data to LandingRecord.

        Args:
            data: Raw data dict from JSONL
            dataset_owner: Dataset owner name
            dataset_name: Dataset name
            dt: Date string
            version: Version string
            s3_source_path: Relative S3 path without s3:// prefix (e.g., "wind-tunnel-landing/owner/dataset/type/dt_version")
                           Used for backtrace and batch deletion filtering
            dataset_type_from_path: Dataset type extracted from S3 path (used if missing in data)

        Returns:
            LandingRecord or None if conversion fails
        """
        try:
            # Auto-generate ID if missing
            # Use deterministic ID based on content to ensure idempotence
            record_id = data.get('id')
            if not record_id or not isinstance(record_id, str) or len(record_id) == 0:
                record_id = generate_deterministic_id(data)
                logger.debug(f"Auto-generated deterministic ID: {record_id}")

            # Get dataset_type: use value from data, or fall back to path extraction
            dataset_type = data.get('dataset_type')
            if not dataset_type and dataset_type_from_path:
                dataset_type = dataset_type_from_path
                logger.debug(f"Using dataset_type from S3 path: {dataset_type}")

            if not dataset_type:
                dataset_type = 'chat_training'  # Default fallback
                logger.warning(f"No dataset_type found, using default: {dataset_type}")

            # Normalize dataset_type to capitalized form
            valid_types = ['SFT', 'DPO', 'RL', 'Benchmark', 'PreTrain']
            if dataset_type not in valid_types:
                # Try to capitalize (e.g., "sft" -> "SFT")
                dataset_type = dataset_type.capitalize()

            # Convert messages to ChatMessage objects
            # NOTE: Content normalization is already handled here:
            # - String content → [{"type": "text", "text": "..."}]
            # - This ensures uniform structure in the database
            messages = []
            for msg_data in data.get('messages', []):
                # Convert content to ContentItem if it's a string
                content = msg_data.get('content')
                if isinstance(content, str):
                    # Normalize: String → [{"type": "text", "text": "..."}]
                    content = [ContentItem(type="text", text=content)]
                elif isinstance(content, list):
                    # Convert each content item
                    content_items = []
                    for item in content:
                        if isinstance(item, str):
                            # Normalize: String item → ContentItem
                            content_items.append(ContentItem(type="text", text=item))
                        elif isinstance(item, dict):
                            content_items.append(ContentItem(**item))
                        else:
                            content_items.append(item)
                    content = content_items
                else:
                    content = []

                msg = ChatMessage(
                    role=msg_data.get('role', 'user'),
                    content=content,
                    name=msg_data.get('name'),
                    refusal=msg_data.get('refusal')
                )
                messages.append(msg)

            # session_id: Use provided value only, leave as None if not provided
            # For some dataset types (like PreTrain), session_id may not be applicable
            session_id = data.get('session_id')  # Can be None

            # Handle meta_json: serialize dict to JSON string
            # Add s3_source_path for backtrace and batch deletion filtering
            meta_json = data.get('meta_json')
            if isinstance(meta_json, str):
                # Parse existing JSON string to dict
                try:
                    import json as json_mod
                    meta_json = json_mod.loads(meta_json)
                except:
                    meta_json = {}

            if not isinstance(meta_json, dict):
                meta_json = {}

            # Add s3_source_path if provided (for future backtrace and batch deletion)
            if s3_source_path:
                meta_json['s3_source_path'] = s3_source_path

            # Serialize to JSON string
            import json as json_mod
            meta_json = json_mod.dumps(meta_json)

            # Helper function to convert list to string if needed
            def list_to_string(val):
                if isinstance(val, list):
                    return str(val)
                return val

            # Helper function to validate and normalize ChatMessage fields
            # Returns (ChatMessage, error_message) - if error_message is not None, conversion failed
            def validate_chatmessage(val, field_name, allow_null_content=False):
                if val is None:
                    return None, None  # None is valid (optional field)
                if isinstance(val, dict):
                    # Normalize content field: convert string to list of ContentItem
                    content = val.get('content')

                    # Special handling: response.content can be null (LLM refused to answer)
                    if content is None:
                        if allow_null_content:
                            # Set to empty list for null content in response field
                            val['content'] = []
                        else:
                            # For other fields (messages, chosen_response, rejected_response), null is invalid
                            return None, f"Invalid {field_name}.content: cannot be null (only response.content can be null)"

                    if isinstance(content, str):
                        # Normalize: String → [{"type": "text", "text": "..."}]
                        val['content'] = [ContentItem(type="text", text=content)]
                    elif isinstance(content, list):
                        # Normalize each content item
                        content_items = []
                        for item in content:
                            if isinstance(item, str):
                                # Normalize: String item → ContentItem
                                content_items.append(ContentItem(type="text", text=item))
                            elif isinstance(item, dict):
                                content_items.append(ContentItem(**item))
                            else:
                                content_items.append(item)
                        val['content'] = content_items
                    elif content is not None:
                        # Invalid content type (and not None)
                        return None, f"Invalid {field_name}.content type: expected string, list, or null, got {type(content).__name__}"

                    # Try to construct ChatMessage from normalized dict
                    try:
                        return ChatMessage(**val), None
                    except Exception as e:
                        return None, f"Invalid {field_name} format: {str(e)}"
                # If it's a string or other type, that's an error
                return None, f"Invalid {field_name} type: expected dict with role/content, got {type(val).__name__}"

            # Validate ChatMessage fields before creating LandingRecord
            # NOTE: response.content can be null (LLM refused to answer), but chosen_response/rejected_response cannot have null content
            response, response_error = validate_chatmessage(data.get('response'), 'response', allow_null_content=True)
            chosen_response, chosen_error = validate_chatmessage(data.get('chosen_response'), 'chosen_response', allow_null_content=False)
            rejected_response, rejected_error = validate_chatmessage(data.get('rejected_response'), 'rejected_response', allow_null_content=False)

            # If any ChatMessage field is invalid, fail the conversion
            if response_error:
                raise ValueError(response_error)
            if chosen_error:
                raise ValueError(chosen_error)
            if rejected_error:
                raise ValueError(rejected_error)

            # Create LandingRecord
            # Set created_at to current timestamp if not provided
            created_at = data.get('created_at') or int(time.time())

            record = LandingRecord(
                dataset_type=dataset_type,
                # dt will be auto-calculated from created_at in SDK
                id=record_id,
                session_id=session_id,
                created_at=created_at,
                step_id=data.get('step_id'),
                is_terminal=data.get('is_terminal'),
                step_reward=data.get('step_reward'),
                reward=data.get('reward'),
                messages=messages,
                response=response,
                chosen_response=chosen_response,
                rejected_response=rejected_response,
                ground_truth_answer=list_to_string(data.get('ground_truth_answer')),
                reference_answer=list_to_string(data.get('reference_answer')),
                agent_model=data.get('agent_model', 'unknown'),
                env_name=data.get('env_name', 'production'),
                is_session_completed=data.get('is_session_completed', False),
                meta_json=meta_json,
                # blob_manifest will be auto-extracted from content in SDK
            )

            return record

        except Exception as e:
            logger.error(f"Error converting data to LandingRecord: {e}")
            return None

    def _normalize_s3_urls(
        self,
        data: Dict[str, Any],
        base_s3_path: str
    ) -> Dict[str, Any]:
        """
        Convert relative S3 URLs to full S3 URLs in the data.

        Args:
            data: Raw data dict from JSONL
            base_s3_path: Base S3 path for the dataset (e.g., s3://wind-tunnel-landing/owner/dataset/type/dt_version)

        Returns:
            Data with normalized URLs (modified in place)
        """
        def normalize_value(value: Any):
            if isinstance(value, str):
                # Convert relative URL to full S3 URL
                if value.startswith('blobs/') or value.startswith('./blobs/'):
                    # Remove leading './' if present
                    clean_path = value.lstrip('./')
                    # Construct full S3 URL
                    full_url = f"{base_s3_path.rstrip('/')}/{clean_path}"
                    return full_url
                return value
            elif isinstance(value, dict):
                return {k: normalize_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [normalize_value(item) for item in value]
            return value

        return normalize_value(data)

    def ingest_dataset(
        self,
        s3_prefix: str,
        batch_size: int = 100,
        skip_schema_validation: bool = False,
        skip_blob_check: bool = False,
        skip_duplicates: bool = True,
        dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Ingest a dataset from S3 prefix into landing table.

        Args:
            s3_prefix: S3 prefix path to dataset
            batch_size: Number of records to ingest per batch (controls memory usage)
            skip_schema_validation: Skip schema compatibility checks
            skip_blob_check: Skip blob existence verification
            skip_duplicates: Skip records that already exist (idempotence)
            dry_run: If True, don't actually insert data

        Returns:
            Summary dict with statistics
        """
        summary = {
            'files_processed': 0,
            'records_read': 0,
            'records_valid': 0,
            'records_invalid': 0,
            'records_skipped': 0,
            'records_ingested': 0,
            'errors': [],
            'error_breakdown': {
                'schema_validation': 0,
                'missing_blobs': 0,
                'conversion_failed': 0
            }
        }

        # Discover all data files
        data_files = self.discover_data_files(s3_prefix)
        if not data_files:
            logger.warning(f"No data files found in {s3_prefix}")
            return summary

        logger.info(f"Processing {len(data_files)} data files...")
        if skip_duplicates:
            logger.info("Idempotence enabled: will skip existing records")
        else:
            logger.warning("Idempotence disabled: may create duplicate records")

        # Process each file
        for s3_uri in data_files:
            logger.info(f"Processing {s3_uri}")

            # Parse path components
            path_parts = s3_uri.replace('s3://', '').split('/')
            # Expected: wind-tunnel-landing/owner/dataset/type/dt_version/data.jsonl
            if len(path_parts) < 5:
                logger.error(f"Invalid path format: {s3_uri}")
                continue

            dataset_owner = path_parts[1]
            dataset_name = path_parts[2]
            dataset_type = path_parts[3]
            dt_version = path_parts[4]
            dt_parts = dt_version.split('_')
            dt = dt_parts[0] if len(dt_parts) > 0 else None
            version = dt_parts[1] if len(dt_parts) > 1 else None

            # Construct base S3 path for this dataset (for URL normalization)
            # Format: s3://wind-tunnel-landing/owner/dataset/type/dt_version
            base_s3_path = f"s3://{'/'.join(path_parts[:5])}"

            # Extract relative S3 path without s3:// prefix (for s3_source_path in meta_json)
            # Format: wind-tunnel-landing/owner/dataset/type/dt_version
            s3_source_path = '/'.join(path_parts[:5])

            batch_records = []

            # Read and process each record
            # Note: S3 operations use infinite retry, so transient errors (502, 503, 504)
            # will be automatically retried until success. Only non-retryable errors
            # (404, 403) will cause immediate failure.
            for line_num, data in self.read_jsonl(s3_uri):
                summary['records_read'] += 1

                # Normalize relative S3 URLs to full S3 URLs
                data = self._normalize_s3_urls(data, base_s3_path)

                # Track record ID for error reporting
                record_id = data.get('id', f'<no_id_line_{line_num}>')

                # Validate schema
                if not skip_schema_validation:
                    is_valid, errors = self.validate_schema_compatibility(data)
                    if not is_valid:
                        summary['records_invalid'] += 1
                        summary['error_breakdown']['schema_validation'] += 1
                        error_msg = f"Line {line_num} (id={record_id}): Schema validation failed - {'; '.join(errors)}"
                        logger.warning(error_msg)
                        summary['errors'].append({
                            'file': s3_uri,
                            'line': line_num,
                            'record_id': record_id,
                            'error_type': 'schema_validation',
                            'error_message': '; '.join(errors)
                        })
                        continue

                # Verify blob references
                if not skip_blob_check:
                    blobs_exist, missing = self.verify_blob_references(data, s3_uri)
                    if not blobs_exist:
                        summary['records_invalid'] += 1
                        summary['error_breakdown']['missing_blobs'] += 1
                        error_msg = f"Line {line_num} (id={record_id}): Missing {len(missing)} blob(s)"
                        logger.warning(error_msg)
                        summary['errors'].append({
                            'file': s3_uri,
                            'line': line_num,
                            'record_id': record_id,
                            'error_type': 'missing_blobs',
                            'error_message': f'Missing {len(missing)} blob(s): {", ".join(missing[:5])}' + ('...' if len(missing) > 5 else '')
                        })
                        # Store missing blob URLs for summary (use original relative path if possible)
                        summary.setdefault('missing_blobs', []).extend(missing)
                        continue

                # Convert to LandingRecord
                record = self.convert_to_landing_record(
                    data, dataset_owner, dataset_name, dt or 'unknown', version or 'v1', s3_source_path, dataset_type
                )

                if record is None:
                    summary['records_invalid'] += 1
                    summary['error_breakdown']['conversion_failed'] += 1
                    error_msg = f"Line {line_num} (id={record_id}): Conversion to LandingRecord failed"
                    logger.error(error_msg)
                    summary['errors'].append({
                        'file': s3_uri,
                        'line': line_num,
                        'record_id': record_id,
                        'error_type': 'conversion_failed',
                        'error_message': 'Failed to convert to LandingRecord (likely validation error)'
                    })
                    continue

                summary['records_valid'] += 1
                batch_records.append(record)

                # Ingest batch when full
                if len(batch_records) >= batch_size:
                    # Batch duplicate check before insertion
                    if skip_duplicates and not dry_run:
                        record_ids = [r.id for r in batch_records]
                        existing_ids = self.client._check_existing_records("landing", record_ids)
                        # Filter out existing records
                        new_records = [r for r in batch_records if r.id not in existing_ids]
                        skipped = len(batch_records) - len(new_records)
                        summary['records_skipped'] += skipped
                        if skipped > 0:
                            logger.info(f"Skipping {skipped} duplicate records in batch")
                        batch_records = new_records

                    if batch_records:
                        self._ingest_batch_with_retry(batch_records, dry_run)
                        summary['records_ingested'] += len(batch_records)
                        logger.info(f"Ingested batch of {len(batch_records)} records")
                    batch_records = []

            # Ingest remaining records
            if batch_records:
                # Batch duplicate check before insertion
                if skip_duplicates and not dry_run:
                    record_ids = [r.id for r in batch_records]
                    existing_ids = self.client._check_existing_records("landing", record_ids)
                    # Filter out existing records
                    new_records = [r for r in batch_records if r.id not in existing_ids]
                    skipped = len(batch_records) - len(new_records)
                    summary['records_skipped'] += skipped
                    if skipped > 0:
                        logger.info(f"Skipping {skipped} duplicate records in final batch")
                    batch_records = new_records

                if batch_records:
                    self._ingest_batch_with_retry(batch_records, dry_run)
                    summary['records_ingested'] += len(batch_records)
                    logger.info(f"Ingested final batch of {len(batch_records)} records")

            summary['files_processed'] += 1

        logger.info(f"ETL complete. Summary: {summary}")

        # Write missing blobs report to file if any
        if summary.get('missing_blobs'):
            logger.warning("=" * 80)
            unique_count = len(set(summary['missing_blobs']))
            logger.warning(f"Missing Blobs Summary: {unique_count} unique files not found (total references: {len(summary['missing_blobs'])})")

            # Write report to file
            # Use the s3_prefix to extract a meaningful identifier
            s3_prefix_path = s3_prefix.replace('s3://', '').rstrip('/')
            report_path = self._write_missing_blobs_report(summary['missing_blobs'], s3_prefix_path, summary)

            if report_path:
                logger.warning(f"Full list written to: {report_path}")
            else:
                logger.warning("Failed to write missing blobs report file")
            logger.warning("=" * 80)

        # Filter out missing_blobs errors from errors list (they're already in the missing_blobs report)
        if summary.get('errors'):
            filtered_errors = [e for e in summary['errors'] if e.get('error_type') != 'missing_blobs']
            summary['errors'] = filtered_errors

        # Write errors report to file if any (excluding missing_blobs which are in a separate report)
        if summary.get('errors'):
            logger.warning("=" * 80)
            logger.warning(f"Errors Summary: {len(summary['errors'])} errors encountered")

            # Write report to file
            s3_prefix_path = s3_prefix.replace('s3://', '').rstrip('/')
            error_report_path = self._write_errors_report(summary['errors'], s3_prefix_path, summary)

            if error_report_path:
                logger.warning(f"Full list written to: {error_report_path}")
            else:
                logger.warning("Failed to write errors report file")
            logger.warning("=" * 80)

        return summary