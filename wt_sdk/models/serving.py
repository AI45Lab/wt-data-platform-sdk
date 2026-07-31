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
    Uses exactly the same fields as LandingRecord.
    """
    pass


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
