"""
Pydantic models for Serving Table.
Maps 1:1 to SERVING_SCHEMA in wt_sdk.core.schemas.
"""
from typing import List, Optional
from pydantic import BaseModel, Field
from .landing import LandingRecord


class ServingRecord(LandingRecord):
    """
    Serving table record model.
    Enriched data with search capabilities and vector embeddings.
    Inherits all fields from LandingRecord and adds serving-specific fields.
    """
    # Search Enhancement
    search_text: Optional[str] = Field(None, description="Full-text search aggregated content")
    tags: Optional[List[str]] = Field(None, description="Business tags")

    # Vector Capabilities
    instruction_vector: Optional[List[float]] = Field(
        None,
        description="1536-dim float32 vector (OpenAI embedding compatible)"
    )

    # Large Vector/Matrix Storage (ETL output)
    vector_file_path: Optional[str] = Field(None, description="S3 path to large vector file")


class ServingRecordBatch(BaseModel):
    """Batch of serving records for bulk insertion."""
    records: List[ServingRecord]

    def __len__(self):
        return len(self.records)

    def __iter__(self):
        return iter(self.records)


class SearchResult(BaseModel):
    """Search result wrapper with metadata."""
    record: ServingRecord
    score: Optional[float] = Field(None, description="Vector similarity score")
    distance: Optional[float] = Field(None, description="Vector distance")


class SearchResponse(BaseModel):
    """Search response containing results and metadata."""
    results: List[SearchResult]
    total_count: int
    query: str
