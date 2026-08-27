"""Landing in-place enrichment pipeline."""

from ..models import PipelineMode
from ..pipeline import PipelineDefinition
from ..stages import FreeCotStage, UpdateIsTrainableStage


def build_pipeline() -> PipelineDefinition:
    """Build the extensible landing in-place enrichment pipeline."""

    return PipelineDefinition(
        name="landing_enrichment_pipeline",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(UpdateIsTrainableStage(), FreeCotStage()),
    )


__all__ = ["build_pipeline"]
