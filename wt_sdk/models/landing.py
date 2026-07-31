"""
Pydantic models for Landing Table.
Maps 1:1 to LANDING_SCHEMA in wt_sdk.core.schemas.
"""
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import datetime
from .common import ChatMessage, BlobManifest


class LandingRecord(BaseModel):
    """
    Landing table record model.
    Raw data ingested from upstream clients.
    """
    # Partition Keys
    dataset_type: str = Field(..., description="Dataset type (e.g., chat_training, rlhf)")
    dt: Optional[str] = Field(None, description="Date partition (e.g., 2025-12-11), auto-calculated from created_at")

    # Identity & Time
    id: str = Field(..., description="Unique record ID")
    session_id: Optional[str] = Field(None, description="Session identifier (optional)")
    created_at: int = Field(..., description="Creation timestamp (Unix epoch)")

    # RL Core
    step_id: Optional[int] = None
    is_terminal: Optional[bool] = None

    # Rewards
    step_reward: Optional[float] = None
    reward: Optional[float] = None

    # Content Payload
    messages: List[ChatMessage] = Field(default_factory=list)
    response: Optional[ChatMessage] = None
    chosen_trace: Optional[List[ChatMessage]] = None
    rejected_trace: Optional[List[ChatMessage]] = None

    # Answers
    ground_truth_answer: Optional[str] = None
    reference_answer: Optional[str] = None
    search_text: Optional[str] = None

    # Meta Information
    agent_model: Optional[str] = None
    env_name: Optional[str] = None
    is_session_completed: Optional[bool] = None
    is_trainable: Optional[bool] = None
    meta_json: Optional[str] = None  # Arrow JSON logical type; Python API remains a JSON string
    tags: Optional[List[str]] = None

    # Additional Fields (2025-03)
    env_id: Optional[str] = Field(None, description="Environment ID")
    job_id: Optional[str] = Field(None, description="Job ID")
    is_truncated: Optional[bool] = Field(None, description="Whether the response was truncated")

    # Asset Management
    blob_manifest: List[str] = Field(default_factory=list)

    @field_validator("messages", mode="before")
    @classmethod
    def normalize_messages(cls, v):
        """Ensure messages is always a list."""
        if v is None:
            return []
        return v

    @model_validator(mode='after')
    def auto_derive_fields(self):
        """
        Auto-derive dt and blob_manifest after model initialization.

        This runs after all fields are set, allowing us to:
        1. Calculate dt from created_at if not provided
        2. Extract blob_manifest from multimodal content if empty
        """
        # Auto-calculate dt from created_at if not provided
        if self.dt is None:
            # Convert Unix timestamp to YYYY-MM-DD format
            dt_datetime = datetime.fromtimestamp(self.created_at)
            self.dt = dt_datetime.strftime('%Y-%m-%d')

        # Auto-extract blob_manifest if empty and content exists
        if not self.blob_manifest:
            # Extract all S3 URLs from multimodal content
            blobs = self._extract_multimodal_blobs()
            # Set blob_manifest to the extracted list
            # Note: We need to use object.__setattr__ because this is a frozen validation state
            object.__setattr__(self, 'blob_manifest', blobs)

        return self

    def _extract_multimodal_blobs(self) -> List[str]:
        """
        Extract all multimodal blob URLs from content fields.

        Recursively extracts S3 URLs from:
        - messages
        - response
        - chosen_trace
        - rejected_trace

        Returns:
            List of unique S3 blob URLs
        """
        blobs = []

        def extract_from_content_items(items):
            """Recursively extract URLs from ContentItem objects."""
            if items is None:
                return
            for item in items:
                # Handle both ContentItem objects and raw dicts (for backward compatibility)
                if hasattr(item, 'image_url'):
                    # It's a ContentItem object - access attributes directly
                    if item.image_url and hasattr(item.image_url, 'url'):
                        url = item.image_url.url
                        if url and url.startswith('s3://'):
                            blobs.append(url)
                elif isinstance(item, dict) and 'image_url' in item and item['image_url']:
                    # It's a dict - access as dictionary (backward compatibility)
                    url_data = item['image_url']
                    if isinstance(url_data, dict) and 'url' in url_data:
                        url = url_data['url']
                        if url and url.startswith('s3://'):
                            blobs.append(url)

                # Check for input_audio (both object and dict formats)
                if hasattr(item, 'input_audio'):
                    # It's a ContentItem object - access attributes directly
                    if item.input_audio and hasattr(item.input_audio, 'url'):
                        url = item.input_audio.url
                        if url and url.startswith('s3://'):
                            blobs.append(url)
                elif isinstance(item, dict) and 'input_audio' in item and item['input_audio']:
                    # It's a dict - access as dictionary (backward compatibility)
                    url_data = item['input_audio']
                    if isinstance(url_data, dict) and 'url' in url_data:
                        url = url_data['url']
                        if url and url.startswith('s3://'):
                            blobs.append(url)

        # Extract from messages
        for msg in self.messages:
            if msg and hasattr(msg, 'content') and msg.content:
                extract_from_content_items(msg.content)

        # Extract from response
        if self.response and hasattr(self.response, 'content') and self.response.content:
            extract_from_content_items(self.response.content)

        # Extract from chosen/rejected traces
        for trace in (self.chosen_trace, self.rejected_trace):
            for msg in trace or []:
                if msg and hasattr(msg, 'content') and msg.content:
                    extract_from_content_items(msg.content)

        # Return unique blobs (preserve order)
        seen = set()
        unique_blobs = []
        for blob in blobs:
            if blob not in seen:
                seen.add(blob)
                unique_blobs.append(blob)

        return unique_blobs

    class Config:
        json_encoders = {
            bytes: lambda v: v.hex() if v else None,
        }


class LandingRecordBatch(BaseModel):
    """Batch of landing records for bulk insertion."""
    records: List[LandingRecord]

    def __len__(self):
        return len(self.records)

    def __iter__(self):
        return iter(self.records)
