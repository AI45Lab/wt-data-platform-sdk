"""
Wind Tunnel Data Platform SDK.

A Python SDK for data ingestion and retrieval using DLDB and LanceDB.

Example usage:
    from wt_sdk import WTGatewayClient, GatewayConfig, LandingRecord

    # Initialize client with default config
    client = WTGatewayClient()

    # Or with custom config
    config = GatewayConfig(
        use_memory_queue=True,
        flush_every=500
    )
    client = WTGatewayClient(config)

    # Ingest data
    record = LandingRecord(
        dataset_type="chat_training",
        dt="2025-12-11",
        id="unique_id",
        session_id="session_001",
        created_at=1702348800,
        # ... other fields
    )
    client.ingest_landing(record)

    # Query data
    results = client.query_data("dataset_type = 'chat_training'", limit=10)

    # Use context manager for automatic cleanup
    with WTGatewayClient() as client:
        client.ingest_landing_batch(records)
    # Shutdown is called automatically
"""
from wt_sdk.config import (
    S3Config,
    TableConfig,
    GatewayConfig,
    default_config,
)
from wt_sdk.client import WTGatewayClient
from wt_sdk.env_config_client import EnvConfigManager
from wt_sdk.models import (
    # Common models
    ImageUrl,
    InputAudio,
    ContentItem,
    Function,
    ToolCall,
    ChatMessage,
    BlobManifest,
    # Landing models
    LandingRecord,
    LandingRecordBatch,
    # Serving models
    ServingRecord,
    ServingRecordBatch,
    SearchResult,
    SearchResponse,
)
from wt_sdk.utils import S3Uploader


__version__ = "0.2.0"

__all__ = [
    # Version
    "__version__",
    # Configuration
    "S3Config",
    "TableConfig",
    "GatewayConfig",
    "default_config",
    # Clients
    "WTGatewayClient",
    "EnvConfigManager",
    # Common models
    "ImageUrl",
    "InputAudio",
    "ContentItem",
    "Function",
    "ToolCall",
    "ChatMessage",
    "BlobManifest",
    # Landing models
    "LandingRecord",
    "LandingRecordBatch",
    # Serving models
    "ServingRecord",
    "ServingRecordBatch",
    "SearchResult",
    "SearchResponse",
    # Utils
    "S3Uploader",
]
