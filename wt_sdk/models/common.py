from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class ImageUrl(BaseModel):
    """
    OpenAI compatible image URL.
    Corresponds to: { "url": "...", "detail": "auto" }
    """
    url: str = Field(..., description="S3 path starting with s3://")
    detail: Optional[str] = Field("auto", description="auto, low, or high")


class InputAudio(BaseModel):
    """
    OpenAI compatible audio reference.
    Wind Tunnel adaptation: store S3 URL instead of base64.
    Structure: { "url": "s3://...", "format": "wav" }
    """
    url: str = Field(..., description="S3 path for audio file")
    format: str = Field(..., description="wav, mp3, or pcm16")


class ContentItem(BaseModel):
    """
    Multimodal content unit supporting text, image, and audio.
    Corresponds to content_item_type in Arrow schema.
    """
    type: str = Field(..., description="text, image_url, or input_audio")
    text: Optional[str] = None
    image_url: Optional[ImageUrl] = None
    input_audio: Optional[InputAudio] = None
    media_type: Optional[str] = None  # e.g., image/png
    image_bytes: Optional[bytes] = None  # for storing small binary data


class Function(BaseModel):
    """OpenAI function calling structure."""
    name: str
    arguments: str  # JSON string


class ToolCall(BaseModel):
    """OpenAI ToolCall object."""
    id: str
    type: str = Field("function", description="Usually 'function'")
    function: Function


class ChatMessage(BaseModel):
    """
    Chat message aligned with OpenAI ChatCompletionMessageParam.
    Corresponds to message_type in Arrow schema.
    """
    role: str
    content: Optional[List[ContentItem]] = None  # Always list after normalization
    name: Optional[str] = None  # Sender name (multi-agent scenarios)
    refusal: Optional[str] = None  # Safety refusal content
    tool_calls: Optional[List[ToolCall]] = None  # Assistant tool calls
    tool_call_id: Optional[str] = None  # Tool message response ID
    function_call: Optional[Dict[str, Any]] = None  # Deprecated field for compatibility


class BlobManifest(BaseModel):
    """Asset manifest for blob storage references."""
    blobs: List[str] = Field(default_factory=list, description="List of S3 blob URLs")