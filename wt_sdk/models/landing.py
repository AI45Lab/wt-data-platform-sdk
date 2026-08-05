"""
Pydantic models for Landing Table.
Maps 1:1 to LANDING_SCHEMA in wt_sdk.core.schemas.
"""
import json
from datetime import datetime
from typing import Any, List, Optional

from pydantic import BaseModel, Field, model_validator

import wt_sdk._time as sdk_time


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
    source_updated_at: Optional[int] = Field(
        None,
        description="Last source change time in Unix epoch milliseconds",
    )
    serving_updated_at: Optional[int] = Field(
        None,
        description="Last serving publication time in Unix epoch milliseconds",
    )

    # RL Core
    step_id: Optional[int] = None
    is_terminal: Optional[bool] = None

    # Rewards
    step_reward: Optional[float] = None
    reward: Optional[float] = None

    # Content Payload
    # Arrow JSON logical types. The SDK stores the supplied JSON strings as-is
    # and intentionally does not validate their provider/message structure.
    messages: Optional[str] = None
    response: Optional[str] = None
    chosen_trace: Optional[str] = None
    rejected_trace: Optional[str] = None

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

        if self.source_updated_at is None:
            self.source_updated_at = sdk_time.now_ms()

        # Auto-extract blob_manifest if empty and content exists
        if not self.blob_manifest:
            # blob_manifest is an optional optimization. No failure in its
            # derivation may prevent an otherwise valid record from being stored.
            try:
                blobs = self._extract_multimodal_blobs()
            except Exception:
                blobs = []
            if not isinstance(blobs, list):
                blobs = []
            # Set blob_manifest to the extracted list
            # Note: We need to use object.__setattr__ because this is a frozen validation state
            object.__setattr__(self, 'blob_manifest', blobs)

        return self

    def _extract_multimodal_blobs(self) -> List[str]:
        """
        Best-effort extraction of multimodal blob URLs from JSON payloads.

        Recursively extracts S3 URLs from:
        - messages
        - response
        - chosen_trace
        - rejected_trace

        Returns:
            List of unique S3 blob URLs
        """
        blobs = []

        def visit(value: Any) -> None:
            if isinstance(value, dict):
                for key, item in value.items():
                    if key in {"image_url", "input_audio"} and isinstance(item, dict):
                        url = item.get("url")
                        if isinstance(url, str) and url.startswith("s3://"):
                            blobs.append(url)
                    visit(item)
            elif isinstance(value, list):
                for item in value:
                    visit(item)

        for payload in (
            self.messages,
            self.response,
            self.chosen_trace,
            self.rejected_trace,
        ):
            if not payload:
                continue
            try:
                visit(json.loads(payload))
            except (TypeError, ValueError):
                # JSON payload validity and shape are deliberately not enforced here.
                continue

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
