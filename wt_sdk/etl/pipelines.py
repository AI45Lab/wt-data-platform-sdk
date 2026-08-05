"""No-argument pipeline factories usable directly by ``scripts/etl/run.py``."""

from .models import PipelineMode
from .pipeline import PipelineDefinition
from .registry import build_serving_publish_pipeline
from .stages import UpdateIsTrainableStage


def build_landing_pipeline() -> PipelineDefinition:
    """Build the landing trainability pipeline around its TODO business stage."""

    return PipelineDefinition(
        name="landing_trainability",
        version="1",
        mode=PipelineMode.LANDING,
        stages=(UpdateIsTrainableStage(),),
    )


def build_serving_pipeline() -> PipelineDefinition:
    """Build the currently runnable OpenCode serving publication pipeline."""

    return build_serving_publish_pipeline(version="1")


__all__ = ["build_landing_pipeline", "build_serving_pipeline"]
