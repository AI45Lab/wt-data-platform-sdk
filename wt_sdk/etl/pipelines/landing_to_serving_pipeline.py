"""OpenCode-ready landing-to-serving publication pipeline."""

from ..pipeline import PipelineDefinition
from ..registry import build_serving_publish_pipeline


def build_pipeline() -> PipelineDefinition:
    """Build the current chosen-trace and tags serving pipeline."""

    return build_serving_publish_pipeline(
        name="landing_to_serving_pipeline",
        version="1",
    )


__all__ = ["build_pipeline"]
