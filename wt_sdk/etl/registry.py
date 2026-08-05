"""Explicit pipeline registry and canonical serving-pipeline builder."""

from collections.abc import Callable

from .exceptions import PipelineConfigurationError
from .models import PipelineMode
from .pipeline import PipelineDefinition
from .stage import ETLStage, Record, StageContext
from .stages import BuildChosenTraceStage, DeriveJobTagsStage


PipelineFactory = Callable[[], PipelineDefinition]


class PipelineRegistry:
    """Small explicit registry; importing a module never scans arbitrary code."""

    def __init__(self) -> None:
        self._factories: dict[str, PipelineFactory] = {}

    def register(self, name: str, factory: PipelineFactory) -> None:
        if not name.strip():
            raise ValueError("pipeline registry name is required")
        if name in self._factories:
            raise PipelineConfigurationError(f"pipeline already registered: {name}")
        self._factories[name] = factory

    def create(self, name: str) -> PipelineDefinition:
        try:
            factory = self._factories[name]
        except KeyError as exc:
            raise PipelineConfigurationError(
                f"unknown pipeline '{name}'; available={sorted(self._factories)}"
            ) from exc
        pipeline = factory()
        if not isinstance(pipeline, PipelineDefinition):
            raise PipelineConfigurationError(
                f"pipeline factory '{name}' did not return PipelineDefinition"
            )
        return pipeline

    def names(self) -> tuple[str, ...]:
        return tuple(sorted(self._factories))


def build_serving_publish_pipeline(
    claude_stage: ETLStage,
    *,
    version: str = "1",
) -> PipelineDefinition:
    """Build the canonical v1 serving pipeline around a contributed Claude stage."""

    if claude_stage.name != "normalize_claude_messages":
        raise PipelineConfigurationError(
            "the contributed Claude stage must be named 'normalize_claude_messages'"
        )
    if "messages" not in claude_stage.output_fields:
        raise PipelineConfigurationError(
            "the contributed Claude stage must declare 'messages' as an output field"
        )

    def select_trainable_claude(record: Record, context: StageContext) -> bool:
        return record.get("is_trainable") is True and claude_stage.applies(record, context)

    return PipelineDefinition(
        name="serving_publish",
        version=version,
        mode=PipelineMode.SERVING,
        stages=(
            claude_stage,
            BuildChosenTraceStage(),
            DeriveJobTagsStage(),
        ),
        record_selector=select_trainable_claude,
    )


default_registry = PipelineRegistry()


__all__ = [
    "PipelineFactory",
    "PipelineRegistry",
    "build_serving_publish_pipeline",
    "default_registry",
]
