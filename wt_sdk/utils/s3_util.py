"""
S3 Utility for Data Generators.

Provides simple synchronous S3 upload and download functionality for multimodal data.
Data generators are responsible for async/batch upload strategies.

Example (upload):
    from wt_sdk.utils import S3Uploader

    uploader = S3Uploader()
    url = uploader.upload(
        data=image_bytes,
        key="training/images/image_001.jpg",
        content_type="image/jpeg"
    )
    # Use url in LandingRecord
    record = LandingRecord(
        ...,
        messages=[
            ChatMessage(
                role="user",
                content=[
                    ContentItem(
                        type="image_url",
                        image_url={"url": url, "detail": "high"}
                    )
                ]
            )
        ]
    )

Example (download):
    from wt_sdk.utils import S3Downloader

    downloader = S3Downloader()
    data = downloader.download("s3://wind-tunnel-golden/training/images/image_001.jpg")
    # or download to file
    downloader.download_file("s3://wind-tunnel-golden/training/images/image_001.jpg", "/tmp/image.jpg")
"""
import os
from typing import Optional
from loguru import logger
import boto3
from botocore.client import Config as BotoConfig


class S3Uploader:
    """
    S3 upload utility for multimodal data.

    Provides simple synchronous upload to S3 with configurable credentials.
    Default configuration matches S3Config from wt_sdk.config.

    Attributes:
        bucket_name: S3 bucket name (default: wind-tunnel-golden)
        aws_access_key_id: AWS access key
        aws_secret_access_key: AWS secret key
        aws_endpoint: S3 endpoint URL
    """

    def __init__(
        self,
        bucket_name: str = "wind-tunnel-golden",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_endpoint: Optional[str] = None,
    ):
        """
        Initialize S3 uploader.

        Args:
            bucket_name: S3 bucket name (default: wind-tunnel-golden)
            aws_access_key_id: AWS access key (if None, uses S3Config default)
            aws_secret_access_key: AWS secret key (if None, uses S3Config default)
            aws_endpoint: S3 endpoint URL (if None, uses S3Config default)
        """
        # Import here to avoid circular dependency
        from wt_sdk.config import S3Config, default_config

        # Use S3Config defaults if not provided
        s3_config = default_config.s3
        self.bucket_name = bucket_name
        self.aws_access_key_id = aws_access_key_id or s3_config.aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key or s3_config.aws_secret_access_key
        self.aws_endpoint = aws_endpoint or s3_config.aws_endpoint

        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            endpoint_url=self.aws_endpoint,
            config=BotoConfig(
                signature_version='s3v4',
                connect_timeout=10,
                read_timeout=30,
                retries={'max_attempts': 3}
            )
        )

        logger.debug(f"S3Uploader initialized: bucket={self.bucket_name}, endpoint={self.aws_endpoint}")

    def upload(
        self,
        data: bytes,
        key: str,
        content_type: Optional[str] = None,
        metadata: Optional[dict] = None
    ) -> str:
        """
        Upload data to S3 and return the URL.

        Args:
            data: Bytes data to upload
            key: S3 object key (path within bucket)
            content_type: Content-Type header (e.g., "image/jpeg", "audio/wav")
            metadata: Optional metadata dict to attach to the object

        Returns:
            S3 URL of the uploaded object (format: s3://bucket/key)

        Raises:
            Exception: If upload fails

        Example:
            >>> uploader = S3Uploader()
            >>> url = uploader.upload(
            ...     data=b"image bytes",
            ...     key="training/images/image_001.jpg",
            ...     content_type="image/jpeg"
            ... )
            >>> print(url)
            s3://wind-tunnel-golden/training/images/image_001.jpg
        """
        if not data:
            raise ValueError("Data cannot be empty")

        if not key:
            raise ValueError("Key cannot be empty")

        # Build S3 URL format
        s3_url = f"s3://{self.bucket_name}/{key.lstrip('/')}"

        try:
            # Prepare upload parameters
            put_params = {
                'Bucket': self.bucket_name,
                'Key': key.lstrip('/'),
                'Body': data
            }

            # Add content type if provided
            if content_type:
                put_params['ContentType'] = content_type

            # Add metadata if provided
            if metadata:
                put_params['Metadata'] = metadata

            # Upload to S3
            logger.info(f"Uploading to S3: {s3_url} ({len(data)} bytes)")
            self.s3_client.put_object(**put_params)

            logger.info(f"Successfully uploaded: {s3_url}")
            return s3_url

        except Exception as e:
            logger.error(f"Failed to upload to {s3_url}: {e}")
            raise

    def upload_file(
        self,
        file_path: str,
        key: Optional[str] = None,
        content_type: Optional[str] = None,
        metadata: Optional[dict] = None
    ) -> str:
        """
        Upload a local file to S3 and return the URL.

        **Returns:** Full S3 URL path in format: `s3://bucket/key`
        Example: `s3://wind-tunnel-golden/training/images/image_001.jpg`

        **Use the returned URL in LandingRecord:**
            >>> url = uploader.upload_file("/tmp/image.jpg", key="training/images/image_001.jpg")
            >>> # Use in LandingRecord
            >>> record = LandingRecord(
            ...     messages=[
            ...         ChatMessage(
            ...             content=[
            ...                 ContentItem(
            ...                     type="image_url",
            ...                     image_url={"url": url, "detail": "high"}
            ...                 )
            ...             ]
            ...         )
            ...     ]
            ... )

        Args:
            file_path: Path to local file (e.g., "/tmp/image.jpg")
            key: S3 object key (if None, uses filename from file_path)
                 Example: "training/images/image_001.jpg"
            content_type: Content-Type header (if None, auto-detected from file extension)
                         Example: "image/jpeg", "audio/wav"
            metadata: Optional metadata dict to attach to the object

        Returns:
            Full S3 URL path: `s3://bucket/key`

        Example:
            >>> uploader = S3Uploader()
            >>> url = uploader.upload_file(
            ...     file_path="/tmp/image.jpg",
            ...     key="training/images/image_001.jpg"
            ... )
            >>> print(url)
            s3://wind-tunnel-golden/training/images/image_001.jpg
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Use filename as key if not provided
        if key is None:
            key = os.path.basename(file_path)

        # Auto-detect content type from file extension if not provided
        if content_type is None:
            import mimetypes
            content_type, _ = mimetypes.guess_type(file_path)
            if content_type is None:
                content_type = "application/octet-stream"

        # Read file and upload
        with open(file_path, 'rb') as f:
            data = f.read()

        return self.upload(data, key, content_type, metadata)

    def exists(self, key_or_url: str) -> bool:
        """
        Check if an object exists in S3.

        **Accepts both formats:**
        - Relative key: `"training/images/image.jpg"`
        - Full S3 URL: `"s3://wind-tunnel-golden/training/images/image.jpg"`

        **Use case:** Check if a file was already uploaded before re-uploading.
            >>> url = uploader.upload(data, key="training/images/image.jpg")
            >>> # Later, check if it still exists
            >>> if uploader.exists(url):  # Can pass full URL
            ...     print("File still exists")
            >>> if uploader.exists("training/images/image.jpg"):  # Or relative key
            ...     print("File still exists")

        Args:
            key_or_url: S3 object key (relative path) or full S3 URL
                       Examples:
                       - Relative: "training/images/image.jpg"
                       - Full URL: "s3://wind-tunnel-golden/training/images/image.jpg"

        Returns:
            True if object exists, False otherwise

        Example:
            >>> uploader.exists("training/images/image.jpg")
            True
            >>> uploader.exists("s3://wind-tunnel-golden/training/images/image.jpg")
            True
            >>> uploader.exists("nonexistent/file.jpg")
            False
        """
        try:
            # Extract key from full S3 URL if provided
            if key_or_url.startswith("s3://"):
                # Parse "s3://bucket/key" format
                parts = key_or_url[5:].split("/", 1)
                if len(parts) == 2:
                    key = parts[1]  # Get the key part
                else:
                    key = key_or_url
            else:
                key = key_or_url

            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=key.lstrip('/')
            )
            return True
        except Exception:
            return False


class S3Downloader:
    def __init__(
        self,
        bucket_name: str = "wind-tunnel-golden",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_endpoint: Optional[str] = None,
    ):
        
        from wt_sdk.config import S3Config, default_config

        # Use S3Config defaults if not provided
        s3_config = default_config.s3
        self.bucket_name = bucket_name
        self.aws_access_key_id = aws_access_key_id or s3_config.aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key or s3_config.aws_secret_access_key
        self.aws_endpoint = aws_endpoint or s3_config.aws_endpoint

        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            endpoint_url=self.aws_endpoint,
            config=BotoConfig(
                signature_version='s3v4',
                connect_timeout=10,
                read_timeout=30,
                retries={'max_attempts': 3}
            )
        )

        logger.debug(f"S3Downloader initialized: bucket={self.bucket_name}, endpoint={self.aws_endpoint}")

    def download(
        self,
        s3_url: str,
    ) -> bytes:
        """
        Example:
            >>> downloader = S3Downloader()
            >>> data = downloader.download("s3://wind-tunnel-golden/training/images/image.jpg")
        """
        # Extract key from full S3 URL if provided
        bucket_name = self.bucket_name
        if s3_url.startswith("s3://"):
            # Parse "s3://bucket/key" format
            parts = s3_url[5:].split("/", 1)
            if len(parts) == 2:
                bucket_name = parts[0]
                key = parts[1]
            else:
                key = s3_url
        else:
            key = s3_url

        key = key.lstrip('/')

        try:
            logger.info(f"Downloading from S3: s3://{bucket_name}/{key}")
            response = self.s3_client.get_object(
                Bucket=bucket_name,
                Key=key
            )
            data = response['Body'].read()
            logger.info(f"Successfully downloaded: s3://{bucket_name}/{key} ({len(data)} bytes)")
            return data

        except self.s3_client.exceptions.NoSuchKey:
            logger.error(f"Object not found: s3://{bucket_name}/{key}")
            raise FileNotFoundError(f"S3 object not found: s3://{bucket_name}/{key}")
        except Exception as e:
            logger.error(f"Failed to download from s3://{bucket_name}/{key}: {e}")
            raise

    def download_file(
        self,
        s3_url: str,
        local_path: str,
    ) -> str:
        """
        Raises:
            FileNotFoundError: If object does not exist in S3
            Exception: If download fails

        Example:
            >>> downloader = S3Downloader()
            >>> local_file = downloader.download_file(
            ...     "s3://wind-tunnel-golden/training/images/image.jpg",
            ...     "/tmp/image.jpg"
            ... )
        """
        data = self.download(s3_url)

        # Create directory if it doesn't exist
        local_dir = os.path.dirname(local_path)
        if local_dir:
            os.makedirs(local_dir, exist_ok=True)

        # Write to file
        with open(local_path, 'wb') as f:
            f.write(data)

        logger.info(f"Saved to: {local_path}")
        return local_path

    def exists(self, key_or_url: str) -> bool:
        """
        Check if an object exists in S3.

        **Accepts both formats:**
        - Relative key: `"training/images/image.jpg"`
        - Full S3 URL: `"s3://wind-tunnel-golden/training/images/image.jpg"`
        """
        try:
            # Extract key from full S3 URL if provided
            if key_or_url.startswith("s3://"):
                parts = key_or_url[5:].split("/", 1)
                if len(parts) == 2:
                    key = parts[1]
                else:
                    key = key_or_url
            else:
                key = key_or_url

            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=key.lstrip('/')
            )
            return True
        except Exception:
            return False