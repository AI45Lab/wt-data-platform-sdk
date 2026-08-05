"""Named ETL pipeline loader.

Each public pipeline is one module in this package. The CLI name is exactly the
module filename without ``.py`` and every module exposes ``build_pipeline()``.
"""

import importlib
import pkgutil
import re

from ..exceptions import PipelineConfigurationError
from ..pipeline import PipelineDefinition


_PIPELINE_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")


def load_pipeline(name: str) -> PipelineDefinition:
    """Load one pipeline by its short module name."""

    normalized = name.strip()
    if not _PIPELINE_NAME_PATTERN.fullmatch(normalized):
        raise PipelineConfigurationError(
            "pipeline name must contain only lowercase letters, digits, and underscores"
        )
    module_name = f"{__name__}.{normalized}"
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        if exc.name != module_name:
            raise
        raise PipelineConfigurationError(
            f"unknown pipeline '{normalized}'; available={list_pipeline_names()}"
        ) from exc
    factory = getattr(module, "build_pipeline", None)
    if not callable(factory):
        raise PipelineConfigurationError(
            f"pipeline module '{module_name}' must expose build_pipeline()"
        )
    pipeline = factory()
    if not isinstance(pipeline, PipelineDefinition):
        raise PipelineConfigurationError(
            f"pipeline module '{module_name}' did not build PipelineDefinition"
        )
    if pipeline.name != normalized:
        raise PipelineConfigurationError(
            f"pipeline module '{normalized}' built mismatched name {pipeline.name!r}"
        )
    return pipeline


def list_pipeline_names() -> tuple[str, ...]:
    """Return public pipeline module names without importing them."""

    return tuple(
        sorted(
            module.name
            for module in pkgutil.iter_modules(__path__)
            if _PIPELINE_NAME_PATTERN.fullmatch(module.name)
        )
    )


__all__ = ["list_pipeline_names", "load_pipeline"]
