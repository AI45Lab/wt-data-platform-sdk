"""
ETL module for ingesting data from S3 into the serving table.

This is a one-time job to ingest data with tags directly into wind_tunnel_serving table.
Data can come from:
1. S3 files with tags (similar to landing ETL)
2. ETL from landing table (future enhancement)

For this one-time job:
- search_text: extracted from messages content for keyword search
- instruction_vector: empty (None)
- vector_file_path: empty (None)
- blob_manifest: check if valid, else extract from content
- meta_json: include s3_source_path

Data path format:
  s3://wind-tunnel-landing/{data_owner_name}/{dataset_name}/{dataset_type}/{dt}_{version}/data.jsonl
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
from wt_sdk.models import ServingRecord, ChatMessage, ContentItem
from wt_sdk.utils import generate_deterministic_id, retry_on_s3_error, resolve_hostname_to_ips


def extract_search_text_from_messages(messages: List) -> str:
    """
    Extract text content from messages for full-text search.

    This aggregates all text content from the messages field into a single
    searchable string, enabling efficient keyword search without needing
    to query the complex nested structure.

    Args:
        messages: List of ChatMessage objects or dicts

    Returns:
        Aggregated text content as a string
    """
    if not messages:
        return ""

    text_parts = []

    for msg in messages:
        if isinstance(msg, dict):
            role = msg.get('role', '')
            content = msg.get('content', [])
            # Add role as context
            if role:
                text_parts.append(f"[{role}]")
            # Extract text from content items
            if isinstance(content, list):
                for item in content:
                    if isinstance(item, dict):
                        # Extract text from ContentItem
                        item_type = item.get('type', '')
                        if item_type == 'text':
                            text_val = item.get('text', '')
                            if text_val:
                                text_parts.append(str(text_val))
                        elif item_type == 'image_url':
                            # For images, note their presence
                            url = item.get('image_url', {}).get('url', '')
                            if url:
                                text_parts.append(f"[image: {url}]")
                        elif item_type == 'input_audio':
                            url = item.get('input_audio', {}).get('url', '')
                            if url:
                                text_parts.append(f"[audio: {url}]")
                    elif isinstance(item, str):
                        text_parts.append(item)
        elif hasattr(msg, 'role') and hasattr(msg, 'content'):
            # Pydantic model
            role = msg.role
            content = msg.content
            if role:
                text_parts.append(f"[{role}]")
            if isinstance(content, list):
                for item in content:
                    if hasattr(item, 'type') and item.type == 'text' and hasattr(item, 'text'):
                        text_parts.append(str(item.text))
                    elif hasattr(item, 'type') and item.type == 'image_url':
                        if hasattr(item, 'image_url') and hasattr(item.image_url, 'url'):
                            text_parts.append(f"[image: {item.image_url.url}]")

    return " ".join(text_parts).strip()


class ServingDataETL:
    """
    ETL processor for ingesting data from S3 into the serving table.

    This is a one-time job for:
    - Ingesting tagged data directly into serving table
    - search_text/instruction_vector/vector_file_path left empty
    - blob_manifest preserved from source or extracted from content
    - s3_source_path added to meta_json for tracking

    Handles:
    1. Discovery of data files in S3
    2. Validation against serving_table schema
    3. Verification of blob references (optional)
    4. Conversion to ServingRecord
    5. Ingestion into serving_table
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
        self.s3 = s3fs.S3FileSystem(
            key=s3_config.aws_access_key_id,
            secret=s3_config.aws_secret_access_key,
            endpoint_url=endpoint,
            client_kwargs={'endpoint_url': endpoint},
            config_kwargs={
                'retries': {
                    'max_attempts': 5,
                    'mode': 'adaptive'
                },
                'connect_timeout': 10,
                'read_timeout': 60,
            }
        )

        # Store endpoint for troubleshooting
        self.s3_endpoint = endpoint

        # Resolve and log S3 endpoint IP addresses for troubleshooting
        try:
            if endpoint:
                from urllib.parse import urlparse
                parsed_endpoint = urlparse(endpoint)
                hostname = parsed_endpoint.hostname

                if hostname:
                    resolved_ips = resolve_hostname_to_ips(hostname)
                    if resolved_ips:
                        logger.info(
                            f"ServingDataETL initialized with retry configuration (max_attempts=5, mode=adaptive)\n"
                            f"  S3 Endpoint: {endpoint}\n"
                            f"  Hostname: {hostname}\n"
                            f"  Resolved IPs: {', '.join(resolved_ips)}"
                        )
                    else:
                        logger.info(
                            f"ServingDataETL initialized with retry configuration (max_attempts=5, mode=adaptive)\n"
                            f"  S3 Endpoint: {endpoint}\n"
                            f"  Hostname: {hostname}"
                        )
                else:
                    logger.info(f"ServingDataETL initialized with retry configuration (max_attempts=5, mode=adaptive)")
            else:
                logger.info(f"ServingDataETL initialized with retry configuration (max_attempts=5, mode=adaptive)")
        except Exception as e:
            logger.warning(f"Failed to resolve S3 endpoint IPs: {e}")
            logger.info(f"ServingDataETL initialized with retry configuration (max_attempts=5, mode=adaptive)")

    def _on_s3_error(self, exc: Exception, attempt: int):
        """Callback function called on each S3 error for DNS resolution logging."""
        from wt_sdk.utils import is_retryable_s3_error
        if is_retryable_s3_error(exc):
            error_code = "unknown"
            if "502" in str(exc):
                error_code = "502"
            elif "503" in str(exc):
                error_code = "503"
            elif "504" in str(exc):
                error_code = "504"

            try:
                if self.s3_endpoint:
                    from urllib.parse import urlparse
                    parsed_endpoint = urlparse(self.s3_endpoint)
                    hostname = parsed_endpoint.hostname

                    if hostname:
                        resolved_ips = resolve_hostname_to_ips(hostname)
                        if resolved_ips:
                            logger.info(
                                f"S3 Endpoint DNS Resolution on {error_code} error (attempt {attempt}):\n"
                                f"  Endpoint: {self.s3_endpoint}\n"
                                f"  Hostname: {hostname}\n"
                                f"  Current Resolved IPs: {', '.join(resolved_ips)}"
                            )
            except Exception:
                pass

    def _is_lancedb_s3_error(self, exc: Exception) -> bool:
        """Check if an exception is a retryable LanceDB S3 error."""
        error_str = str(exc).lower()
        if "lance" not in error_str and "lancedb" not in error_str:
            return False
        s3_indicators = ["s3 error", "502", "503", "504", "bad gateway", "timeout"]
        return any(indicator in error_str for indicator in s3_indicators)

    def _ingest_batch_with_retry(self, batch_records: list, dry_run: bool = False):
        """Ingest a batch of records with infinite retry on LanceDB S3 errors."""
        if dry_run or not batch_records:
            return

        delay = 60.0
        max_delay = 120.0
        attempt = 0

        while True:
            try:
                self.client.ingest_serving_batch(batch_records)
                return

            except Exception as e:
                if not self._is_lancedb_s3_error(e):
                    logger.error(f"Non-retryable error during ingestion: {str(e)}")
                    raise

                attempt += 1
                self._on_s3_error(e, attempt)

                logger.warning(
                    f"LanceDB S3 write error during batch ingestion "
                    f"(attempt {attempt}): {str(e)}. "
                    f"Retrying in {delay:.1f}s... (infinite retry mode)"
                )

                time.sleep(delay)
                delay = min(delay * 2.0, max_delay)

    def _s3_ls(self, path: str, detail: bool = False):
        """List S3 directory with infinite retry logic and DNS logging."""
        from wt_sdk.utils import is_retryable_s3_error

        delay = 60.0
        max_delay = 120.0
        attempt = 0

        while True:
            try:
                return self.s3.ls(path, detail=detail)
            except Exception as e:
                attempt += 1
                if not is_retryable_s3_error(e):
                    raise
                self._on_s3_error(e, attempt)
                logger.warning(f"S3 ls error (attempt {attempt}): {str(e)}. Retrying in {delay:.1f}s...")
                time.sleep(delay)
                delay = min(delay * 2.0, max_delay)

    def _s3_isdir(self, path: str) -> bool:
        """Check if S3 path is a directory with infinite retry logic."""
        from wt_sdk.utils import is_retryable_s3_error

        delay = 60.0
        max_delay = 120.0
        attempt = 0

        while True:
            try:
                return self.s3.isdir(path)
            except Exception as e:
                attempt += 1
                if not is_retryable_s3_error(e):
                    raise
                self._on_s3_error(e, attempt)
                logger.warning(f"S3 isdir error (attempt {attempt}): {str(e)}. Retrying in {delay:.1f}s...")
                time.sleep(delay)
                delay = min(delay * 2.0, max_delay)

    def _s3_path_exists(self, s3_uri: str) -> bool:
        """Check if S3 path exists with infinite retry logic."""
        from wt_sdk.utils import is_retryable_s3_error

        delay = 60.0
        max_delay = 120.0
        attempt = 0

        while True:
            try:
                return self.s3.exists(s3_uri)
            except Exception as e:
                attempt += 1
                if not is_retryable_s3_error(e):
                    raise
                self._on_s3_error(e, attempt)
                logger.warning(f"S3 exists error (attempt {attempt}): {str(e)}. Retrying in {delay:.1f}s...")
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

        Args:
            s3_prefix: S3 prefix path

        Returns:
            List of S3 URIs to data.jsonl files
        """
        s3_prefix = s3_prefix.rstrip('/')
        data_files = []

        try:
            paths = self._s3_ls(s3_prefix, detail=False)

            for path in paths:
                clean_path = path.replace('s3://', '')
                full_path = f"s3://{clean_path}"

                if clean_path.endswith('data.jsonl'):
                    data_files.append(full_path)
                    logger.debug(f"Found data.jsonl: {full_path}")
                elif self._s3_isdir(full_path):
                    potential_jsonl = f"{full_path.rstrip('/')}/data.jsonl"
                    if self._s3_path_exists(potential_jsonl):
                        data_files.append(potential_jsonl)
                        logger.debug(f"Found data.jsonl in directory: {full_path}")

            # If no files found at this level, recurse into subdirectories
            if not data_files:
                logger.debug(f"No data.jsonl found at {s3_prefix}, checking subdirectories...")
                for path in paths:
                    clean_path = path.replace('s3://', '')
                    full_path = f"s3://{clean_path}"

                    if self._s3_isdir(full_path):
                        sub_files = self.discover_data_files(full_path)
                        data_files.extend(sub_files)

        except Exception as e:
            logger.error(f"Error discovering files in {s3_prefix}: {e}")

        logger.info(f"Found {len(data_files)} data.jsonl files in {s3_prefix}")
        return data_files

    def read_jsonl(self, s3_uri: str) -> Iterator[tuple[int, Dict[str, Any]]]:
        """Read and parse JSONL file from S3 with infinite retry logic."""
        from wt_sdk.utils import is_retryable_s3_error

        delay = 60.0
        max_delay = 120.0
        attempt = 0
        last_successful_line = 0

        while True:
            try:
                with self.s3.open(s3_uri, 'r') as f:
                    for line_num, line in enumerate(f, 1):
                        if line_num <= last_successful_line:
                            continue

                        line = line.strip()
                        if not line:
                            continue

                        try:
                            data = json.loads(line)
                            if line_num % 1000 == 0:
                                logger.info(f"Read {line_num} lines from {s3_uri}")
                            yield (line_num, data)
                            last_successful_line = line_num

                        except json.JSONDecodeError as e:
                            logger.error(f"Invalid JSON at line {line_num} in {s3_uri}: {e}")
                            last_successful_line = line_num

                break

            except Exception as e:
                if not is_retryable_s3_error(e):
                    logger.error(f"Non-retryable error reading {s3_uri}: {str(e)}")
                    raise

                attempt += 1
                self._on_s3_error(e, attempt)
                logger.warning(
                    f"S3 file reading error at line {last_successful_line + 1} "
                    f"(attempt {attempt}): {str(e)}. Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)
                delay = min(delay * 2.0, max_delay)

    def validate_schema_compatibility(
        self,
        data: Dict[str, Any]
    ) -> tuple[bool, List[str]]:
        """
        Check if data aligns with serving_table schema.

        Args:
            data: Raw data dict from JSONL

        Returns:
            Tuple of (is_valid, list of error messages)
        """
        errors = []

        # Check required fields
        required_fields = ['messages', 'tags']
        for field in required_fields:
            if field not in data:
                errors.append(f"Missing required field: {field}")

        # Check messages
        if 'messages' in data:
            if not isinstance(data['messages'], list):
                errors.append(f"'messages' must be a list, got {type(data['messages'])}")
            elif len(data['messages']) == 0:
                errors.append("'messages' cannot be empty")

        # Check tags
        if 'tags' in data:
            if not isinstance(data['tags'], list):
                errors.append(f"'tags' must be a list, got {type(data['tags'])}")

        # Check dataset_type format if present
        if 'dataset_type' in data:
            valid_types = ['SFT', 'DPO', 'RL', 'Benchmark', 'PreTrain']
            if data['dataset_type'].capitalize() not in valid_types and data['dataset_type'] not in valid_types:
                errors.append(f"Invalid dataset_type: {data['dataset_type']}")

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
            data: Raw data dict from JSONL
            base_s3_path: Base S3 path for the dataset

        Returns:
            Tuple of (all_exist, list of missing blob paths)
        """
        missing_blobs = []
        blob_uris = self._extract_blob_s3_uris(data)

        for blob_uri in blob_uris:
            try:
                if blob_uri.startswith('blobs/') or blob_uri.startswith('./blobs/'):
                    full_url = f"{base_s3_path.rstrip('/')}/{blob_uri.lstrip('./')}"
                    if not self._s3_path_exists(full_url):
                        missing_blobs.append(blob_uri)
                elif blob_uri.startswith('s3://'):
                    if not self._s3_path_exists(blob_uri):
                        if '/blobs/' in blob_uri:
                            relative = "blobs/" + blob_uri.split('/blobs/')[-1]
                            missing_blobs.append(relative)
                        else:
                            missing_blobs.append(blob_uri)
            except Exception:
                if '/blobs/' in blob_uri:
                    relative = "blobs/" + blob_uri.split('/blobs/')[-1]
                    missing_blobs.append(relative)
                else:
                    missing_blobs.append(blob_uri)

        all_exist = len(missing_blobs) == 0
        return all_exist, missing_blobs

    def _extract_blob_s3_uris(self, data: Dict[str, Any]) -> List[str]:
        """Extract all blob URIs from the data."""
        uris = []

        def extract_from_value(value: Any):
            if isinstance(value, str):
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

    def convert_to_serving_record(
        self,
        data: Dict[str, Any],
        dataset_owner: str,
        dataset_name: str,
        dt: str,
        version: str,
        s3_source_path: Optional[str] = None,
        dataset_type_from_path: Optional[str] = None
    ) -> Optional[ServingRecord]:
        """
        Convert raw data to ServingRecord.

        Args:
            data: Raw data dict from JSONL
            dataset_owner: Dataset owner name
            dataset_name: Dataset name
            dt: Date string
            version: Version string
            s3_source_path: Relative S3 path without s3:// prefix
            dataset_type_from_path: Dataset type extracted from S3 path

        Returns:
            ServingRecord or None if conversion fails
        """
        try:
            # Auto-generate ID if missing
            record_id = data.get('id')
            if not record_id or not isinstance(record_id, str) or len(record_id) == 0:
                record_id = generate_deterministic_id(data)
                logger.debug(f"Auto-generated deterministic ID: {record_id}")

            # Get dataset_type
            dataset_type = data.get('dataset_type')
            if not dataset_type and dataset_type_from_path:
                dataset_type = dataset_type_from_path

            if not dataset_type:
                dataset_type = 'chat_training'
                logger.warning(f"No dataset_type found, using default: {dataset_type}")

            # Normalize dataset_type
            valid_types = ['SFT', 'DPO', 'RL', 'Benchmark', 'PreTrain']
            if dataset_type not in valid_types:
                dataset_type = dataset_type.capitalize()

            # Convert messages to ChatMessage objects
            messages = []
            for msg_data in data.get('messages', []):
                content = msg_data.get('content')
                if isinstance(content, str):
                    content = [ContentItem(type="text", text=content)]
                elif isinstance(content, list):
                    content_items = []
                    for item in content:
                        if isinstance(item, str):
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

            # session_id
            session_id = data.get('session_id')

            # Handle meta_json: add s3_source_path
            meta_json = data.get('meta_json')
            if isinstance(meta_json, str):
                try:
                    meta_json = json.loads(meta_json)
                except:
                    meta_json = {}

            if not isinstance(meta_json, dict):
                meta_json = {}

            # Add s3_source_path for tracking
            if s3_source_path:
                meta_json['s3_source_path'] = s3_source_path

            meta_json = json.dumps(meta_json)

            # Helper function to convert list to string
            def list_to_string(val):
                if isinstance(val, list):
                    return str(val)
                return val

            # Validate ChatMessage fields
            def validate_chatmessage(val, field_name, allow_null_content=False):
                if val is None:
                    return None, None
                if isinstance(val, dict):
                    content = val.get('content')
                    if content is None:
                        if allow_null_content:
                            val['content'] = []
                        else:
                            return None, f"Invalid {field_name}.content: cannot be null"

                    if isinstance(content, str):
                        val['content'] = [ContentItem(type="text", text=content)]
                    elif isinstance(content, list):
                        content_items = []
                        for item in content:
                            if isinstance(item, str):
                                content_items.append(ContentItem(type="text", text=item))
                            elif isinstance(item, dict):
                                content_items.append(ContentItem(**item))
                            else:
                                content_items.append(item)
                        val['content'] = content_items
                    elif content is not None:
                        return None, f"Invalid {field_name}.content type"

                    try:
                        return ChatMessage(**val), None
                    except Exception as e:
                        return None, f"Invalid {field_name} format: {str(e)}"
                return None, f"Invalid {field_name} type"

            response, response_error = validate_chatmessage(data.get('response'), 'response', allow_null_content=True)
            chosen_response, chosen_error = validate_chatmessage(data.get('chosen_response'), 'chosen_response', allow_null_content=False)
            rejected_response, rejected_error = validate_chatmessage(data.get('rejected_response'), 'rejected_response', allow_null_content=False)

            if response_error:
                raise ValueError(response_error)
            if chosen_error:
                raise ValueError(chosen_error)
            if rejected_error:
                raise ValueError(rejected_error)

            # created_at
            created_at = data.get('created_at') or int(time.time())

            # Get tags from data (required for serving table)
            tags = data.get('tags', [])
            if not isinstance(tags, list):
                tags = []
                logger.warning(f"tags is not a list, using empty list")

            # Handle blob_manifest: check if valid (length > 0), otherwise will be extracted
            # If data has blob_manifest field and it's valid, use it
            # Otherwise, LandingRecord's auto_derive_fields will extract it from content
            blob_manifest = data.get('blob_manifest', [])
            if not isinstance(blob_manifest, list):
                blob_manifest = []

            # Extract search_text from messages for keyword search
            # This aggregates text content from all messages into a searchable string
            search_text = extract_search_text_from_messages(messages)

            # Create ServingRecord
            record = ServingRecord(
                dataset_type=dataset_type,
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
                blob_manifest=blob_manifest,  # Will be extracted if empty by auto_derive_fields
                # Serving-specific fields
                search_text=search_text or None,  # Extracted from messages for keyword search
                tags=tags,  # From data
                instruction_vector=None,  # Empty for now
                vector_file_path=None,  # Empty for now
            )

            return record

        except Exception as e:
            logger.error(f"Error converting data to ServingRecord: {e}")
            return None

    def _normalize_s3_urls(
        self,
        data: Dict[str, Any],
        base_s3_path: str
    ) -> Dict[str, Any]:
        """Convert relative S3 URLs to full S3 URLs in the data."""
        def normalize_value(value: Any):
            if isinstance(value, str):
                if value.startswith('blobs/') or value.startswith('./blobs/'):
                    clean_path = value.lstrip('./')
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
        Ingest a dataset from S3 prefix into serving table.

        Args:
            s3_prefix: S3 prefix path to dataset
            batch_size: Number of records to ingest per batch
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

        logger.info(f"Processing {len(data_files)} data files for serving table...")
        if skip_duplicates:
            logger.info("Idempotence enabled: will skip existing records")
        else:
            logger.warning("Idempotence disabled: may create duplicate records")

        # Process each file
        for s3_uri in data_files:
            logger.info(f"Processing {s3_uri}")

            # Parse path components
            path_parts = s3_uri.replace('s3://', '').split('/')
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

            base_s3_path = f"s3://{'/'.join(path_parts[:5])}"
            s3_source_path = '/'.join(path_parts[:5])

            batch_records = []

            # Read and process each record
            for line_num, data in self.read_jsonl(s3_uri):
                summary['records_read'] += 1

                # Normalize relative S3 URLs to full S3 URLs
                data = self._normalize_s3_urls(data, base_s3_path)

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
                        summary.setdefault('missing_blobs', []).extend(missing)
                        continue

                # Convert to ServingRecord
                record = self.convert_to_serving_record(
                    data, dataset_owner, dataset_name, dt or 'unknown', version or 'v1', s3_source_path, dataset_type
                )

                if record is None:
                    summary['records_invalid'] += 1
                    summary['error_breakdown']['conversion_failed'] += 1
                    error_msg = f"Line {line_num} (id={record_id}): Conversion to ServingRecord failed"
                    logger.error(error_msg)
                    summary['errors'].append({
                        'file': s3_uri,
                        'line': line_num,
                        'record_id': record_id,
                        'error_type': 'conversion_failed',
                        'error_message': 'Failed to convert to ServingRecord'
                    })
                    continue

                summary['records_valid'] += 1
                batch_records.append(record)

                # Ingest batch when full
                if len(batch_records) >= batch_size:
                    if skip_duplicates and not dry_run:
                        record_ids = [r.id for r in batch_records]
                        existing_ids = self.client._check_existing_records("serving", record_ids)
                        new_records = [r for r in batch_records if r.id not in existing_ids]
                        skipped = len(batch_records) - len(new_records)
                        summary['records_skipped'] += skipped
                        if skipped > 0:
                            logger.info(f"Skipping {skipped} duplicate records in batch")
                        batch_records = new_records

                    if batch_records:
                        self._ingest_batch_with_retry(batch_records, dry_run)
                        summary['records_ingested'] += len(batch_records)
                        logger.info(f"Ingested batch of {len(batch_records)} records to serving table")
                    batch_records = []

            # Ingest remaining records
            if batch_records:
                if skip_duplicates and not dry_run:
                    record_ids = [r.id for r in batch_records]
                    existing_ids = self.client._check_existing_records("serving", record_ids)
                    new_records = [r for r in batch_records if r.id not in existing_ids]
                    skipped = len(batch_records) - len(new_records)
                    summary['records_skipped'] += skipped
                    if skipped > 0:
                        logger.info(f"Skipping {skipped} duplicate records in final batch")
                    batch_records = new_records

                if batch_records:
                    self._ingest_batch_with_retry(batch_records, dry_run)
                    summary['records_ingested'] += len(batch_records)
                    logger.info(f"Ingested final batch of {len(batch_records)} records to serving table")

            summary['files_processed'] += 1

        logger.info(f"ETL complete. Summary: {summary}")

        # Write missing blobs report
        if summary.get('missing_blobs'):
            logger.warning("=" * 80)
            unique_count = len(set(summary['missing_blobs']))
            logger.warning(f"Missing Blobs Summary: {unique_count} unique files not found")

            s3_prefix_path = s3_prefix.replace('s3://', '').rstrip('/')
            report_path = self._write_missing_blobs_report(summary['missing_blobs'], s3_prefix_path, summary)
            if report_path:
                logger.warning(f"Full list written to: {report_path}")
            logger.warning("=" * 80)

        # Filter out missing_blobs errors
        if summary.get('errors'):
            filtered_errors = [e for e in summary['errors'] if e.get('error_type') != 'missing_blobs']
            summary['errors'] = filtered_errors

        # Write errors report
        if summary.get('errors'):
            logger.warning("=" * 80)
            logger.warning(f"Errors Summary: {len(summary['errors'])} errors encountered")

            s3_prefix_path = s3_prefix.replace('s3://', '').rstrip('/')
            error_report_path = self._write_errors_report(summary['errors'], s3_prefix_path, summary)
            if error_report_path:
                logger.warning(f"Full list written to: {error_report_path}")
            logger.warning("=" * 80)

        return summary

    def _write_missing_blobs_report(
        self,
        missing_blobs: List[str],
        s3_source_path: str,
        summary: Dict[str, Any]
    ) -> Optional[str]:
        """Write missing blobs report to a file."""
        if not missing_blobs:
            return None

        path_parts = s3_source_path.split('/')
        data_owner = path_parts[1] if len(path_parts) >= 2 else "unknown"

        output_dir = Path(__file__).parent / "output" / data_owner
        output_dir.mkdir(parents=True, exist_ok=True)

        path_parts_without_owner = path_parts[2:] if len(path_parts) > 2 else path_parts
        dataset_identifier = "_".join(path_parts_without_owner).replace("\\", "_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"missing_blobs_serving_{dataset_identifier}_{timestamp}.txt"
        output_path = output_dir / filename

        unique_missing = sorted(set(missing_blobs))

        try:
            with open(output_path, 'w') as f:
                f.write(f"Missing Blobs Report (Serving Table ETL)\n")
                f.write(f"{'=' * 80}\n\n")
                f.write(f"Source: {s3_source_path}\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write(f"ETL Summary:\n")
                f.write(f"------------\n")
                f.write(f"  Files Processed: {summary.get('files_processed', 0)}\n")
                f.write(f"  Records Read: {summary.get('records_read', 0)}\n")
                f.write(f"  Records Valid: {summary.get('records_valid', 0)}\n")
                f.write(f"  Records Invalid: {summary.get('records_invalid', 0)}\n")
                f.write(f"  Records Skipped: {summary.get('records_skipped', 0)}\n")
                f.write(f"  Records Ingested: {summary.get('records_ingested', 0)}\n\n")
                f.write(f"Missing Blobs Details:\n")
                f.write(f"{'-' * 80}\n")
                f.write(f"Total Unique Missing: {len(unique_missing)}\n")
                f.write(f"Total References: {len(missing_blobs)}\n\n")
                f.write(f"Missing Blob Paths:\n")
                f.write(f"{'-' * 80}\n")
                for blob_url in unique_missing:
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
        """Write errors report to a file."""
        if not errors:
            return None

        path_parts = s3_source_path.split('/')
        data_owner = path_parts[1] if len(path_parts) >= 2 else "unknown"

        output_dir = Path(__file__).parent / "output" / data_owner
        output_dir.mkdir(parents=True, exist_ok=True)

        path_parts_without_owner = path_parts[2:] if len(path_parts) > 2 else path_parts
        dataset_identifier = "_".join(path_parts_without_owner).replace("\\", "_")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"errors_serving_{dataset_identifier}_{timestamp}.txt"
        output_path = output_dir / filename

        # Group errors by type
        errors_by_type: Dict[str, List[Dict]] = {}
        for error in errors:
            error_type = error.get('error_type', 'unknown')
            errors_by_type.setdefault(error_type, []).append(error)

        try:
            with open(output_path, 'w') as f:
                f.write(f"ETL Errors Report (Serving Table)\n")
                f.write(f"{'=' * 80}\n\n")
                f.write(f"Source: {s3_source_path}\n")
                f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                f.write(f"ETL Summary:\n")
                f.write(f"------------\n")
                f.write(f"  Files Processed: {summary.get('files_processed', 0)}\n")
                f.write(f"  Records Read: {summary.get('records_read', 0)}\n")
                f.write(f"  Records Valid: {summary.get('records_valid', 0)}\n")
                f.write(f"  Records Invalid: {summary.get('records_invalid', 0)}\n")
                f.write(f"  Records Skipped: {summary.get('records_skipped', 0)}\n")
                f.write(f"  Records Ingested: {summary.get('records_ingested', 0)}\n")
                f.write(f"  Total Errors: {len(errors)}\n\n")
                f.write(f"Errors by Type:\n")
                f.write(f"{'-' * 80}\n")
                for error_type, type_errors in errors_by_type.items():
                    f.write(f"\n{error_type.upper()}: {len(type_errors)} errors\n")
                    f.write(f"{'=' * 80}\n")
                    for error in type_errors[:100]:
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
