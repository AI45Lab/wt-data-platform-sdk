"""Landing-to-serving publication pipeline."""

from ..models import PipelineMode
from ..pipeline import PipelineDefinition
from ..stages import (
    BuildChosenTraceStage,
    BuildSearchTextStage,
    DeriveJobTagsStage,
)


def build_pipeline() -> PipelineDefinition:
    """Build the explicitly declared landing-to-serving pipeline."""

    return PipelineDefinition(
        name="landing_to_serving_pipeline",
        version="2",
        mode=PipelineMode.SERVING,
        stages=(
            BuildChosenTraceStage(),
            BuildSearchTextStage(),
            DeriveJobTagsStage(),
        ),
    )


__all__ = ["build_pipeline"]
