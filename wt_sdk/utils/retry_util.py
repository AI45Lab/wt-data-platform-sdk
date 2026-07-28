"""
Retry utilities for handling transient failures.

Provides decorators and utilities for retrying operations with exponential backoff.
"""
import time
import functools
import socket
from typing import Callable, Type, Tuple, Optional
from loguru import logger


def resolve_hostname_to_ips(hostname: str) -> list:
    """
    Resolve a hostname to its IP addresses.

    Args:
        hostname: The hostname to resolve (e.g., "s3.amazonaws.com")

    Returns:
        List of IP addresses the hostname resolves to

    Example:
        >>> resolve_hostname_to_ips("s3.amazonaws.com")
        ['52.216.0.0', '52.216.0.1']
    """
    try:
        # Get all IP addresses for the hostname
        addr_info = socket.getaddrinfo(hostname, None)
        # Extract unique IPs (IPv4 and IPv6)
        ips = list(set([addr[4][0] for addr in addr_info]))
        return ips
    except Exception as e:
        logger.warning(f"Failed to resolve hostname {hostname}: {e}")
        return []


def extract_s3_endpoint_from_uri(s3_uri: str) -> str:
    """
    Extract the S3 endpoint hostname from an S3 URI.

    Args:
        s3_uri: S3 URI (e.g., "s3://bucket/key" or URL like "https://s3.region.amazonaws.com/...")

    Returns:
        Hostname (e.g., "s3.amazonaws.com" or "localhost")

    Example:
        >>> extract_s3_endpoint_from_uri("s3://my-bucket/key")
        "s3.amazonaws.com"  # Falls back to default if no explicit endpoint
    """
    # For s3:// URIs, we can't extract the endpoint directly
    # The actual endpoint is configured in the S3 client
    # Return None to indicate endpoint should be extracted from client config
    if s3_uri.startswith('s3://'):
        return None

    # For HTTP/HTTPS URLs, extract the hostname
    if s3_uri.startswith('http://') or s3_uri.startswith('https://'):
        from urllib.parse import urlparse
        parsed = urlparse(s3_uri)
        return parsed.hostname

    return None


def retry_with_backoff(
    max_retries: int = 5,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    on_retry: Optional[Callable[[Exception, int], None]] = None
):
    """
    Decorator to retry a function with exponential backoff.

    This decorator is designed to handle transient failures like:
    - S3 502 Bad Gateway errors
    - Network timeouts
    - Temporary service unavailability

    Args:
        max_retries: Maximum number of retry attempts (default: 5)
        initial_delay: Initial delay between retries in seconds (default: 1.0)
        max_delay: Maximum delay between retries in seconds (default: 60.0)
        exponential_base: Base for exponential backoff (default: 2.0)
        exceptions: Tuple of exception types to catch and retry (default: all exceptions)
        on_retry: Optional callback function called on each retry: on_retry(exception, attempt)

    Returns:
        Decorated function that retries on failure

    Example:
        >>> @retry_with_backoff(max_retries=3, initial_delay=2.0)
        ... def fetch_from_s3(key):
        ...     # May raise 502 errors intermittently
        ...     return s3.get_object(key)

        >>> # Retries with delays: 2s, 4s, 8s
        >>> result = fetch_from_s3("my-key")

    Backoff strategy:
        - Attempt 1: initial_delay seconds
        - Attempt 2: initial_delay * exponential_base seconds
        - Attempt 3: initial_delay * exponential_base^2 seconds
        - ...
        - Capped at max_delay seconds
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    # If this was the last attempt, raise the exception
                    if attempt == max_retries:
                        logger.error(
                            f"Function {func.__name__} failed after {max_retries} retries. "
                            f"Last error: {str(e)}"
                        )
                        raise

                    # Log retry attempt
                    logger.warning(
                        f"Function {func.__name__} failed (attempt {attempt + 1}/{max_retries}): {str(e)}. "
                        f"Retrying in {delay:.1f}s..."
                    )

                    # Call custom retry callback if provided
                    if on_retry:
                        try:
                            on_retry(e, attempt + 1)
                        except Exception as callback_error:
                            logger.warning(f"Retry callback failed: {callback_error}")

                    # Sleep before retry
                    time.sleep(delay)

                    # Calculate next delay with exponential backoff
                    delay = min(delay * exponential_base, max_delay)

            # Should never reach here, but just in case
            if last_exception:
                raise last_exception

        return wrapper
    return decorator


def is_retryable_s3_error(exception: Exception) -> bool:
    """
    Check if an S3 error is retryable (transient).

    Retryable errors include:
    - 502 Bad Gateway
    - 503 Service Unavailable
    - 504 Gateway Timeout
    - Connection errors
    - Timeout errors

    Args:
        exception: The exception to check

    Returns:
        True if the error is retryable, False otherwise

    Example:
        >>> try:
        ...     s3.head_object(bucket, key)
        ... except Exception as e:
        ...     if is_retryable_s3_error(e):
        ...         # Retry the operation
        ...         pass
    """
    error_str = str(exception).lower()

    # Check for specific HTTP error codes
    retryable_codes = ['502', '503', '504', '429']  # Bad Gateway, Service Unavailable, Gateway Timeout, Too Many Requests
    if any(code in error_str for code in retryable_codes):
        return True

    # Check for connection errors
    retryable_keywords = [
        'bad gateway',
        'service unavailable',
        'gateway timeout',
        'connection error',
        'timeout',
        'timed out',
        'connection reset',
        'broken pipe',
        'max retries',
    ]

    return any(keyword in error_str for keyword in retryable_keywords)


def retry_on_s3_error(
    max_retries: int = 5,
    initial_delay: float = 2.0,
    max_delay: float = 60.0,
    infinite: bool = False,
    on_error_callback: Optional[Callable[[Exception, int], None]] = None
):
    """
    Decorator specifically for retrying S3 operations that may fail with transient errors.

    This is a specialized version of retry_with_backoff that:
    - Only retries on S3-related errors (502, 503, 504, connection errors)
    - Uses longer delays suitable for S3 recovery
    - Logs S3-specific error information
    - Supports infinite retry for production ETL jobs
    - Supports error callbacks for custom logging (e.g., DNS resolution)

    Args:
        max_retries: Maximum number of retry attempts (default: 5, ignored if infinite=True)
        initial_delay: Initial delay between retries in seconds (default: 2.0)
        max_delay: Maximum delay between retries in seconds (default: 60.0)
        infinite: If True, retry indefinitely until success (default: False)
        on_error_callback: Optional callback function called on each error: callback(exception, attempt)
                          Useful for logging DNS resolution, endpoint info, etc.

    Returns:
        Decorated function that retries on S3 errors

    Example:
        >>> @retry_on_s3_error(max_retries=3)
        ... def check_s3_exists(s3_uri):
        ...     return s3.exists(s3_uri)

        >>> # Infinite retry with custom error logging
        >>> def log_dns_on_error(exc, attempt):
        ...     logger.info(f"Logging DNS resolution on attempt {attempt}")
        ...
        >>> @retry_on_s3_error(infinite=True, on_error_callback=log_dns_on_error)
        ... def read_critical_file(s3_uri):
        ...     return s3.open(s3_uri)

    Note:
        This decorator catches all exceptions but only retries on retryable S3 errors.
        Non-retryable errors (e.g., 404 Not Found, 403 Forbidden) are raised immediately.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            attempt = 0

            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e

                    # Check if this is a retryable error
                    if not is_retryable_s3_error(e):
                        # Non-retryable error (e.g., 404, 403), raise immediately
                        logger.debug(f"Non-retryable S3 error in {func.__name__}: {str(e)}")
                        raise

                    # If not infinite and we've exhausted retries, raise
                    if not infinite and attempt >= max_retries:
                        logger.error(
                            f"S3 operation {func.__name__} failed after {max_retries} retries. "
                            f"Last error: {str(e)}"
                        )
                        raise

                    # Log retry attempt with S3-specific context
                    # Call custom error callback if provided (for DNS resolution logging, etc.)
                    if on_error_callback:
                        try:
                            on_error_callback(e, attempt + 1)
                        except Exception as callback_err:
                            logger.warning(f"Error callback failed: {callback_err}")

                    # Try to extract S3 endpoint info for troubleshooting
                    endpoint_info = ""
                    try:
                        # Try to get S3 endpoint from function arguments or kwargs
                        # Look for common S3 URI patterns in args
                        for arg in args:
                            if isinstance(arg, str) and ('s3://' in arg or 'amazonaws.com' in arg):
                                endpoint_info = f" [URI: {arg}]"
                                break
                    except:
                        pass

                    if infinite:
                        logger.warning(
                            f"S3 operation {func.__name__} encountered transient error "
                            f"(attempt {attempt + 1}): {str(e)}{endpoint_info}. "
                            f"Retrying in {delay:.1f}s... (infinite retry mode)"
                        )
                    else:
                        logger.warning(
                            f"S3 operation {func.__name__} encountered transient error "
                            f"(attempt {attempt + 1}/{max_retries}): {str(e)}{endpoint_info}. "
                            f"Retrying in {delay:.1f}s..."
                        )

                    # Sleep before retry
                    time.sleep(delay)

                    # Calculate next delay with exponential backoff
                    delay = min(delay * 2.0, max_delay)

                    attempt += 1

        return wrapper
    return decorator