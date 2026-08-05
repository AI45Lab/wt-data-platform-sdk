# LLM Data ETL (Legacy)

> Historical ETL for previously existing datasets. It is not maintained against
> the current landing/serving schema and must not be used as the new serving ETL.

This module provides ETL (Extract, Transform, Load) functionality for ingesting raw LLM benchmark data from S3 into the landing table.

## Overview

The ETL processor:
1. **Discovers** JSONL files in S3 following the path format
2. **Normalizes** relative S3 URLs to full S3 URLs (for training platform)
3. **Validates** data against landing_table schema (minimal required fields)
4. **Verifies** multimodal blob references exist
5. **Converts** data to LandingRecord format with automatic normalizations:
   - Auto-generates deterministic IDs for records missing them (content-based hash)
   - Normalizes content to uniform array structure
   - Handles optional session_id gracefully
6. **Ingests** into landing_table in batches with idempotence support
7. **Automatic retry** for transient S3 errors (502 Bad Gateway, 503, timeouts, etc.)

## S3 Path Format

### Data Files
```
s3://wind-tunnel-landing/{data_owner_name}/{dataset_name}/{dataset_type}/{dt}_{version}/data.jsonl
```

Example:
```
s3://wind-tunnel-landing/liuqihua/safety_image_ch/eval/2025-01-04_v1/data.jsonl
```

### Blob Files
Multimodal data (images, audio) referenced in JSONL should be in:
```
s3://wind-tunnel-landing/{data_owner_name}/{dataset_name}/{dataset_type}/{dt}_{version}/blobs/{filename}
```

## Usage

### Basic Usage

```python
from wt_sdk import LLMDataETL

# Initialize ETL processor
etl = LLMDataETL()

# Run ingestion
summary = etl.ingest_dataset(
    s3_prefix='s3://wind-tunnel-landing/liuqihua/safety_image_ch/',
    batch_size=100
)

print(f"Ingested {summary['records_ingested']} records")
```

### Using the Command Line Script

```bash
# Basic run
python -m wt_sdk.etl.legacy.run_landing_etl

# Dry run (validate but don't insert)
python -m wt_sdk.etl.legacy.run_landing_etl --dry-run

# Skip blob verification
python -m wt_sdk.etl.legacy.run_landing_etl --skip-blob-check

# Custom S3 prefix with batch size
python -m wt_sdk.etl.legacy.run_landing_etl \
    --s3-prefix "s3://wind-tunnel-landing/owner/dataset/type/2025-01-04_v1/" \
    --batch-size 200

# Disable idempotence (allow duplicates)
python -m wt_sdk.etl.legacy.run_landing_etl --allow-duplicates
```

### Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--s3-prefix` | `s3://wind-tunnel-landing/liuqihua/safety_image_ch/` | S3 prefix to process |
| `--batch-size` | 100 | Records per batch (controls memory usage) |
| `--skip-schema-validation` | False | Skip schema compatibility checks |
| `--skip-blob-check` | False | Skip blob existence verification |
| `--allow-duplicates` | False | Disable idempotence and allow duplicate records |
| `--dry-run` | False | Validate and convert but don't insert |

## Retry Mechanism for Transient S3 Errors

**Built-in infinite retry with exponential backoff**

The ETL includes comprehensive infinite retry logic to handle transient S3 errors that are common in production environments:

**Retryable Errors:**
- 502 Bad Gateway
- 503 Service Unavailable
- 504 Gateway Timeout
- 429 Too Many Requests
- Connection errors
- Timeout errors

**Retry Strategy:**
- **Retry mode**: **INFINITE** - Will retry until success
- **Initial delay**: 60 seconds
- **Maximum delay**: 120 seconds (capped)
- **Exponential backoff**: 60s → 120s (then stays at 120s)
- **Dual-layer protection**:
  1. **Boto3 level**: Configured with adaptive retry mode (max 5 attempts)
  2. **Application level**: Infinite retry decorators on all S3 operations

**Protected Operations (all with infinite retry):**
- `s3.exists()` - Checking blob existence
- `s3.ls()` - Listing directories
- `s3.isdir()` - Checking if path is directory
- `s3.open()` - Opening JSONL files for reading

**Example Scenario:**
```
# S3 returns 502 Bad Gateway on file open
[WARNING] S3 operation _s3_open encountered transient error (attempt 1):
  [Errno 5] An error occurred (502) when calling the GetObject operation: Bad Gateway
  Retrying in 60.0s... (infinite retry mode)

# Still failing after 5 attempts
[WARNING] S3 operation _s3_open encountered transient error (attempt 5):
  [Errno 5] An error occurred (502): Bad Gateway
  Retrying in 120.0s... (infinite retry mode)

# Eventually succeeds when S3 recovers
[INFO] Successfully opened file after 12 retry attempts (24 minutes)
```

**Benefits:**
- **Guaranteed completion**: Will eventually succeed when S3 recovers (no matter how long it takes)
- **No manual intervention**: ETL runs unattended until completion
- **Production-ready**: Handles extended S3 outages gracefully
- **Long delays between retries**: 1-2 minute delays avoid overwhelming S3 during recovery
- **Safe for overnight jobs**: Can run for hours/days until S3 stabilizes

**Non-retryable Errors** (fail immediately):
- 404 Not Found (blob doesn't exist)
- 403 Forbidden (permission denied)
- 400 Bad Request (invalid request)

These errors indicate actual problems that retry won't fix, so they're raised immediately for investigation.

**Important Notes:**
- The ETL will **NOT exit** due to transient S3 errors
- It will keep retrying indefinitely until S3 recovers
- Use Ctrl+C to manually stop the ETL if needed
- Monitor logs to see retry attempts and progress

## Idempotence

**Default behavior: ENABLED**

The ETL guarantees idempotence by default using **deterministic content-based ID generation**. If you re-run the ETL after a failure, it will:
- Generate the same ID for the same record content (MD5 hash of messages field)
- Check each record's `id` field (simple and efficient)
- Skip records that already exist in the landing table
- Only insert new records

This prevents duplicate data when re-running the ETL, even if records don't have explicit IDs.

**Performance:** Uses batched SQL `IN` queries (1000 IDs per query) instead of individual checks. For 10,000 records, this means only ~10 queries instead of 10,000.

**Example:**
```bash
# First run: inserts 500 records
python -m wt_sdk.etl.legacy.run_landing_etl

# Fails after 300 records due to network error (502 Bad Gateway)

# Re-run:
# - Deterministic IDs ensure same content → same ID
# - Skips the 300 existing records (detected via ID check)
# - Inserts the remaining 200 records
# - No duplicates created!
python -m wt_sdk.etl.legacy.run_landing_etl
```

**To disable idempotence** (allow duplicates - not recommended):
```bash
python -m wt_sdk.etl.legacy.run_landing_etl --allow-duplicates
```

## Validation

### Schema Validation

The ETL validates:
- **Required fields**: `dataset_type`, `messages` only
- **Optional fields**: `id`, `session_id` (auto-generated or defaulted if missing)
- **Messages structure**: Non-empty list with `role` and `content` fields
- **Dataset type**: Must be one of: `SFT`, `DPO`, `RL`, `Benchmark`, `PreTrain` (case-insensitive)
- **ID**: Auto-generated if missing or empty using **deterministic content-based hash** (format: `evt_{16_hex_chars}`)

### Blob Verification

The ETL checks:
- All S3 URIs in the data exist
- Supports various URI formats in content
- Checks both relative and absolute S3 paths

## Data Conversion

The ETL converts JSONL data to LandingRecord format with automatic normalizations:

### Content Normalization
**All content is normalized to uniform array structure** for database consistency:
- String `"hello"` → `[{"type": "text", "text": "hello"}]`
- List of strings → List of `ContentItem` objects
- Already structured content → Preserved as-is

**Why?** This ensures:
- Uniform database indexing structure
- Downstream ETL processes don't need to handle multiple formats
- Training platform's DataLoader works with consistent schema
- No conditional logic needed for content access

### URL Normalization (NEW!)
**Relative S3 URLs are converted to full S3 URLs** for training platform compatibility:
- Relative `"blobs/image.png"` → Full `"s3://wind-tunnel-landing/owner/dataset/type/dt_version/blobs/image.png"`
- Relative `"./blobs/image.png"` → Full `"s3://wind-tunnel-landing/owner/dataset/type/dt_version/blobs/image.png"`
- Full S3 URLs → Preserved as-is
- Recursive: Converts URLs nested anywhere in the data structure

**Why?** This ensures:
- Training platform can directly load multimodal data without URL reconstruction
- Self-contained records with all necessary resource locations
- No need for base path configuration in training code
- URLs work immediately for data loading pipelines

**Example:**
```json
// Input (relative paths)
{
  "messages": [{
    "content": [{
      "type": "image",
      "url": "blobs/2220_030922_A_ZR166_022.PNG"
    }]
  }]
}

// Output (full S3 URLs)
{
  "messages": [{
    "content": [{
      "type": "image",
      "url": "s3://wind-tunnel-landing/liuqihua/safety_image_ch/eval/2025-01-04_v1/blobs/2220_030922_A_ZR166_022.PNG"
    }]
  }]
}
```

### ID Auto-Generation
Records without an `id` field (or with empty `id`) get auto-generated using **deterministic content-based hashing**:
- **Format**: `evt_{16_hex_chars}` (MD5 hash of messages field)
- **Example**: `evt_a1b2c3d4e5f6g7h8`
- **Idempotence**: Same content → same ID (prevents duplicates on re-run)
- **Hash source**: MD5 hash of the `messages` field (or entire data dict if messages is missing)
- **Logging**: Auto-generated deterministic IDs are logged at debug level
- **Utility**: Uses `generate_deterministic_id()` from `wt_sdk.utils`

**Why deterministic?**
- Connection errors during ingestion are common
- Re-running the ETL with random IDs would create duplicates
- Content-based hashing ensures the same record always gets the same ID
- Enables true idempotence even without explicit IDs in source data

### session_id Handling
- **Optional field**: Not required in input JSONL
- **Fallback logic**:
  1. Use provided `session_id` if present
  2. Fall back to `id` (or auto-generated ID)
  3. Can be empty string for dataset types like `PreTrain`
- **Use case**: Some dataset types (PreTrain) don't have session concepts

### Field Mapping
- `dataset_type` → Normalized to capitalized form (e.g., `sft` → `SFT`)
- `id` → Auto-generated if missing using `evt_{timestamp}_{uuid8}` format
- `session_id` → Falls back to `id`, or empty string if not applicable
- `meta_json` → Dict automatically serialized to JSON string, with `s3_source_path` field added automatically
- `created_at` → 0 if missing
- `agent_model` → "unknown" if missing
- `env_name` → "production" if missing
- `is_session_completed` → False if missing

### s3_source_path in meta_json
**NEW!** The ETL automatically adds the `s3_source_path` field to `meta_json` for all ingested records.

**Purpose:**
- **Backtrace**: Track which S3 dataset the record came from
- **Batch deletion**: Filter and delete all records from a specific S3 path

**Format:**
```json
{
  "s3_source_path": "wind-tunnel-landing/liuqihua/safety_image_ch/RL/20261218_v1",
  ...other meta fields...
}
```

**Example:**
- Source: `s3://wind-tunnel-landing/liuqihua/safety_image_ch/RL/20261218_v1/data.jsonl`
- Stored: `meta_json.s3_source_path = "wind-tunnel-landing/liuqihua/safety_image_ch/RL/20261218_v1"`

**Use cases:**
```python
# Delete all records from a specific S3 source
# Note: meta_json is stored as JSON string, use LIKE for string matching
client.delete_landing("meta_json LIKE '%\"s3_source_path\": \"wind-tunnel-landing/liuqihua/safety_image_ch/RL/20261218_v1\"%'")

# Query records from a specific dataset
results = client.query_data("meta_json LIKE '%safety_image_ch%'")
```

## Error Handling

The ETL:
- **Logs warnings** for invalid records (doesn't stop processing)
- **Continues processing** after errors
- **Provides summary** with statistics
- **Returns non-zero exit code** if all records failed

### Summary Statistics

The ETL returns a summary dict with:
- `files_processed`: Number of data.jsonl files processed
- `records_read`: Total records read from all files
- `records_valid`: Records that passed validation
- `records_invalid`: Records that failed validation
- `records_skipped`: Records skipped as duplicates (when idempotence enabled)
- `records_ingested`: Records successfully inserted
- `errors`: List of error messages

Example:
```
ETL Summary:
  Files Processed: 5
  Records Read: 500
  Records Valid: 498
  Records Invalid: 2
  Records Skipped (duplicates): 300
  Records Ingested: 198
```

### Missing Blobs Report

When blob verification is enabled and missing blobs are detected, the ETL automatically generates a detailed report file instead of logging all missing blob URLs to the console.

**Report location:** `wt_sdk/etl/legacy/output/{data_owner}/missing_blobs_{dataset}_{timestamp}.txt`

**Directory structure:** Reports are organized by data owner name for easy management
- Example: `wt_sdk/etl/legacy/output/liuqihua/missing_blobs_safety_image_ch_RL_20261218_v1_20260105_154108.txt`
- All reports from `liuqihua` are in `wt_sdk/etl/legacy/output/liuqihua/`
- All reports from `zhangsan` are in `wt_sdk/etl/legacy/output/zhangsan/`

**Filename format:**
- Format: `missing_blobs_{dataset}_{timestamp}.txt`
- Example: `missing_blobs_safety_image_ch_RL_20261218_v1_20260105_154108.txt`
- Dataset path excludes owner prefix (since it's in the directory)
- Includes timestamp for uniqueness

**Report contents:**
```
Missing Blobs Report
===================

Source: wind-tunnel-landing/liuqihua/safety_image_ch
Generated: 2026-01-05 16:26:45

ETL Summary:
------------
  Files Processed: 1
  Records Read: 8997
  Records Valid: 5071
  Records Invalid: 3926
  Records Skipped (duplicates): 0
  Records Ingested: 5071

Missing Blobs Details:
----------------------
Total Unique Missing: 3926
Total References: 5000

Missing Blob Paths (relative):
--------------------------------------------------------------------------------
blobs/_543_d30tl4uo2k181.jpg
blobs/2220_030922_A_ZR166_022.PNG
... (full list of all missing blobs, sorted alphabetically)
```

**Log output:**
```
================================================================================
Missing Blobs Summary: 3926 unique files not found (total references: 5000)
Full list written to: wt_sdk/etl/legacy/output/liuqihua/missing_blobs_...txt
================================================================================
```

**Use case:** Share the report file with data owners so they can fix missing blob files. The ETL summary statistics help understand the overall data quality.

## Example Output

```
================================================================================
LLM Data ETL
================================================================================
S3 Prefix: s3://wind-tunnel-landing/liuqihua/safety_image_ch/
Batch Size: 100
Skip Schema Validation: False
Skip Blob Check: False
Skip Duplicates: True
Dry Run: False
================================================================================
Idempotence enabled: will skip existing records
Found 5 data.jsonl files in s3://wind-tunnel-landing/liuqihua/safety_image_ch/
Processing s3://wind-tunnel-landing/liuqihua/safety_image_ch/eval/2025-01-04_v1/data.jsonl
Ingested batch of 100 records
Ingested final batch of 50 records
ETL complete. Summary: {'files_processed': 5, 'records_read': 500, ...}
================================================================================
ETL Summary:
  Files Processed: 5
  Records Read: 500
  Records Valid: 498
  Records Invalid: 2
  Records Skipped (duplicates): 0
  Records Ingested: 498
================================================================================
✓ ETL completed successfully
```

## Input Data Examples

### Minimal Input (IDs and session_id auto-generated)
```json
{
  "dataset_type": "SFT",
  "messages": [
    {"role": "user", "content": "What is the capital of France?"},
    {"role": "assistant", "content": "Paris"}
  ]
}
```
**After ETL processing:**
- `id`: Auto-generated as `evt_a1b2c3d4e5f6g7h8` (deterministic hash of messages)
- `session_id`: Set to same as `id`
- `dataset_type`: Normalized to `SFT` (capitalized)
- `content`: Both messages normalized to `[{"type": "text", "text": "..."}]`
- `meta_json`: Auto-created as `'{"s3_source_path": "wind-tunnel-landing/liuqihua/safety_image_ch/SFT/2025-01-04_v1"}'`
- `s3_source_path`: **Automatically added** to track the source S3 path for backtrace and batch deletion
- **Note**: Re-running the ETL with the same data will generate the same ID, preventing duplicates

### Complete Input
```json
{
  "dataset_type": "RL",
  "id": "custom_id_123",
  "session_id": "session_abc",
  "created_at": 1736067338,
  "messages": [
    {"role": "user", "content": "Question text"},
    {"role": "assistant", "content": [{"type": "text", "text": "Answer"}]}
  ],
  "meta_json": {
    "data_source": "safety",
    "label": "hard"
  },
  "agent_model": "gpt-4",
  "reward": 0.95
}
```
**After ETL processing:**
- `id`: Preserved as `custom_id_123`
- `session_id`: Preserved as `session_abc`
- `dataset_type`: Normalized to `RL`
- `messages[0].content`: String normalized to `[{"type": "text", "text": "Question text"}]`
- `messages[1].content`: Already structured, preserved as-is
- `meta_json`: Dict serialized to JSON string `'{"data_source": "safety", "label": "hard", "s3_source_path": "wind-tunnel-landing/liuqihua/safety_image_ch/RL/20261218_v1"}'`
- `s3_source_path`: **Automatically added** to track the source S3 path for backtrace and batch deletion

## API Reference

### LLMDataETL

```python
class LLMDataETL:
    """ETL processor for LLM benchmark data from S3."""

    def __init__(self, s3_endpoint: Optional[str] = None, client: Optional[WTGatewayClient] = None)

    def discover_data_files(self, s3_prefix: str) -> List[str]
        """Discover all data.jsonl files under the given S3 prefix."""

    def read_jsonl(self, s3_uri: str) -> Iterator[Dict[str, Any]]
        """Read and parse JSONL file from S3."""

    def validate_schema_compatibility(self, data: Dict) -> tuple[bool, List[str]]
        """Check if data aligns with landing_table schema."""

    def verify_blob_references(self, data: Dict, base_s3_path: str) -> tuple[bool, List[str]]
        """Check if referenced multimodal data exists."""

    def convert_to_landing_record(self, data: Dict, ...) -> Optional[LandingRecord]
        """Convert raw data to LandingRecord."""

    def ingest_dataset(self, s3_prefix: str, ...) -> Dict[str, Any]
        """Ingest a dataset from S3 prefix into landing table."""
```
