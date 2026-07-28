"""
ID generation utilities for Wind Tunnel Data Platform.

Provides functions to generate unique, idempotent IDs for records.
"""
import hashlib
import json
import time
import uuid
from typing import Any, Dict, List, Union


def generate_deterministic_id(data: Dict[str, Any]) -> str:
    """
    Generate a deterministic ID based on record content.

    This function ensures idempotence across multiple ingestion runs:
    - Same content → same ID (prevents duplicates on retry)
    - Different content → different ID

    The ID is generated from a hash of the 'messages' field, which contains
    the core conversational content that uniquely identifies a record.

    Args:
        data: Raw data dict containing at least a 'messages' field

    Returns:
        Deterministic ID string in format: evt_{content_hash}

    Example:
        >>> data = {"messages": [{"role": "user", "content": "Hello"}]}
        >>> id1 = generate_deterministic_id(data)
        >>> id2 = generate_deterministic_id(data)
        >>> assert id1 == id2  # Same content produces same ID
        >>> # Output: evt_a1b2c3d4e5f6g7h8

    Notes:
        - Uses MD5 hash (truncated to 16 chars) for short, readable IDs
        - MD5 is sufficient here as this is for deduplication, not security
        - The hash is based on stable JSON serialization (sorted keys)
        - If 'messages' is missing, uses the entire data dict as fallback
    """
    # Use 'messages' as the primary content for hashing
    # This is the core field that defines uniqueness
    content_to_hash = data.get('messages')

    # Fallback: if messages is missing, use entire data dict
    # (excluding fields that might change on re-ingestion like timestamps)
    if content_to_hash is None:
        # Create a copy and remove metadata that shouldn't affect uniqueness
        content_to_hash = {
            k: v for k, v in data.items()
            if k not in ['created_at', 'meta_json']
        }

    # Create stable JSON string (sorted keys for consistency)
    try:
        content_str = json.dumps(content_to_hash, sort_keys=True, ensure_ascii=False)
    except (TypeError, ValueError) as e:
        # If serialization fails, convert to string representation
        content_str = str(content_to_hash)

    # Generate MD5 hash (16 hex chars = 8 bytes)
    # MD5 is deprecated for security but perfectly fine for deduplication
    content_hash = hashlib.md5(content_str.encode('utf-8')).hexdigest()[:16]

    return f"evt_{content_hash}"


def generate_event_id() -> str:
    """
    Generate a random unique event ID.

    WARNING: This function generates NON-DETERMINISTIC IDs using random UUIDs.
    If you need idempotence (same record → same ID across runs), use
    generate_deterministic_id() instead.

    This function is kept for backward compatibility and for cases where
    truly random IDs are needed (e.g., real-time streaming without deduplication).

    Returns:
        Random unique event ID in format: evt_{timestamp}_{uuid8}

    Example:
        >>> id1 = generate_event_id()
        >>> id2 = generate_event_id()
        >>> assert id1 != id2  # Different UUIDs each time
        >>> # Output: evt_1705334400_a1b2c3d4

    Notes:
        - Combines timestamp (seconds) with 8-char UUID suffix
        - Provides uniqueness but NOT idempotence
        - Same record ingested twice → different IDs → duplicates
    """
    timestamp = int(time.time())
    uuid_suffix = uuid.uuid4().hex[:8]
    return f"evt_{timestamp}_{uuid_suffix}"


def validate_id_format(record_id: str) -> bool:
    """
    Validate that an ID follows the expected format.

    Valid formats:
    - evt_{16_hex_chars} (deterministic)
    - evt_{timestamp}_{8_hex_chars} (random)
    - Any custom string (user-provided IDs)

    Args:
        record_id: ID string to validate

    Returns:
        True if valid, False otherwise

    Example:
        >>> validate_id_format("evt_a1b2c3d4e5f6g7h8")
        True
        >>> validate_id_format("evt_1705334400_a1b2c3d4")
        True
        >>> validate_id_format("custom_id_123")
        True
        >>> validate_id_format("")
        False
    """
    if not record_id or not isinstance(record_id, str):
        return False

    # Must be non-empty string
    if len(record_id) == 0:
        return False

    return True