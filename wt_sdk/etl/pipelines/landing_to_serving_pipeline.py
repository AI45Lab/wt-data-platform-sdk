"""Landing-to-serving publication pipeline."""

from wt_sdk.core.schemas import LANDING_SCHEMA

from ..models import PipelineInputScope, PipelineMode
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
        version="3",
        mode=PipelineMode.SERVING,
        input_scope=PipelineInputScope.MATCHED_ROWS,
        source_columns=tuple(
            name
            for name in LANDING_SCHEMA.names
            if name
            not in {
                "chosen_trace",
                "search_text",
                "tags",
                "serving_updated_at",
            }
        ),
        stages=(
            BuildChosenTraceStage(),
            DeriveJobTagsStage(),
            BuildSearchTextStage(),
        ),
    )


__all__ = ["build_pipeline"]
