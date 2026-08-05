"""Pipeline definition, dependency ordering, validation, and pure execution."""

from collections import defaultdict, deque
from copy import deepcopy
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Callable, Mapping, Sequence

from wt_sdk.core.schemas import LANDING_SCHEMA
from wt_sdk.models import ServingRecord

from .exceptions import (
    PipelineConfigurationError,
    SessionValidationError,
    StageTransformError,
)
from .models import LandingRowPatch, PipelineMode, SessionResult
from .stage import ETLStage, Record, SessionKey, StageContext


RecordSelector = Callable[[Record, StageContext], bool]

IMMUTABLE_ETL_FIELDS = {
    "id",
    "job_id",
    "session_id",
    "created_at",
    "source_updated_at",
    "serving_updated_at",
}
ETL_SCHEMA_FIELDS = frozenset(LANDING_SCHEMA.names)


def _select_all(record: Record, context: StageContext) -> bool:
    _ = record, context
    return True


@dataclass(frozen=True)
class PipelineDefinition:
    """A validated, ordered set of stages and one persistence mode."""

    name: str
    version: str
    mode: PipelineMode
    stages: tuple[ETLStage, ...]
    record_selector: RecordSelector = field(default=_select_all, repr=False)
    _ordered_stages: tuple[ETLStage, ...] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.name, str)
            or not self.name.strip()
            or not isinstance(self.version, str)
            or not self.version.strip()
        ):
            raise PipelineConfigurationError("pipeline name and version are required")
        if not isinstance(self.mode, PipelineMode):
            raise PipelineConfigurationError("pipeline mode must be a PipelineMode")
        if not callable(self.record_selector):
            raise PipelineConfigurationError("pipeline record_selector must be callable")
        object.__setattr__(self, "_ordered_stages", _order_and_validate_stages(self.stages))

    @property
    def ordered_stages(self) -> tuple[ETLStage, ...]:
        return self._ordered_stages

    def process_session(self, rows: Sequence[Mapping[str, object]]) -> SessionResult:
        ordered_rows, session_key = _validate_and_order_session(rows)
        original_session = tuple(_freeze(dict(row)) for row in ordered_rows)
        context = StageContext(
            pipeline_name=self.name,
            pipeline_version=self.version,
            session_key=session_key,
            session=original_session,
        )

        landing_patches: list[LandingRowPatch] = []
        serving_records: list[ServingRecord] = []
        selected_rows = 0

        for source_row in ordered_rows:
            original = dict(source_row)
            if not _evaluate_predicate(
                self.record_selector,
                original,
                context,
                label=f"pipeline '{self.name}' selector",
            ):
                continue
            selected_rows += 1
            working = dict(original)
            executed_stages: set[str] = set()

            for stage in self.ordered_stages:
                if not _evaluate_predicate(
                    stage.applies,
                    working,
                    context,
                    label=f"stage '{stage.name}' applies",
                ):
                    continue
                skipped_dependencies = set(stage.dependencies) - executed_stages
                if skipped_dependencies:
                    raise StageTransformError(
                        f"stage '{stage.name}' applies but its dependencies did not run: "
                        f"{sorted(skipped_dependencies)} for record {working.get('id')!r}"
                    )
                missing = [field for field in stage.required_fields if field not in working]
                if missing:
                    raise StageTransformError(
                        f"stage '{stage.name}' missing required fields {missing} "
                        f"for record {working.get('id')!r}"
                    )
                try:
                    patch = stage.transform(deepcopy(working), context)
                except StageTransformError:
                    raise
                except Exception as exc:
                    raise StageTransformError(
                        f"stage '{stage.name}' failed for record {working.get('id')!r}: {exc}"
                    ) from exc
                _validate_stage_patch(stage, patch)
                working.update(patch)
                executed_stages.add(stage.name)

            if self.mode is PipelineMode.LANDING:
                changed = {
                    key: value
                    for key, value in working.items()
                    if key not in IMMUTABLE_ETL_FIELDS and original.get(key) != value
                }
                if changed:
                    landing_patches.append(
                        LandingRowPatch(
                            record_id=str(original["id"]),
                            job_id=session_key.job_id,
                            session_id=session_key.session_id,
                            updates=changed,
                        )
                    )
            else:
                working["serving_updated_at"] = None
                serving_records.append(ServingRecord(**working))

        return SessionResult(
            session_key=session_key,
            source_rows=len(ordered_rows),
            selected_rows=selected_rows,
            landing_patches=tuple(landing_patches),
            serving_records=tuple(serving_records),
        )


def _order_and_validate_stages(stages: Sequence[ETLStage]) -> tuple[ETLStage, ...]:
    if not stages:
        raise PipelineConfigurationError("a pipeline must contain at least one stage")

    by_name: dict[str, ETLStage] = {}
    output_owner: dict[str, str] = {}
    for stage in stages:
        if not isinstance(stage, ETLStage):
            raise PipelineConfigurationError("every stage must inherit ETLStage")
        if not isinstance(stage.name, str) or not stage.name.strip():
            raise PipelineConfigurationError("every stage must declare a non-empty name")
        if not isinstance(stage.version, str) or not stage.version.strip():
            raise PipelineConfigurationError(
                f"stage '{stage.name}' must declare a non-empty version"
            )
        metadata_values = (
            *stage.required_fields,
            *stage.output_fields,
            *stage.dependencies,
        )
        if any(not isinstance(value, str) or not value.strip() for value in metadata_values):
            raise PipelineConfigurationError(
                f"stage '{stage.name}' field/dependency declarations must be non-empty strings"
            )
        if len(set(stage.required_fields)) != len(stage.required_fields):
            raise PipelineConfigurationError(
                f"stage '{stage.name}' declares duplicate required fields"
            )
        if len(set(stage.dependencies)) != len(stage.dependencies):
            raise PipelineConfigurationError(
                f"stage '{stage.name}' declares duplicate dependencies"
            )
        if not stage.output_fields:
            raise PipelineConfigurationError(
                f"stage '{stage.name}' must declare at least one output field"
            )
        if len(set(stage.output_fields)) != len(stage.output_fields):
            raise PipelineConfigurationError(
                f"stage '{stage.name}' declares duplicate output fields"
            )
        unknown_required = set(stage.required_fields) - ETL_SCHEMA_FIELDS
        unknown_outputs = set(stage.output_fields) - ETL_SCHEMA_FIELDS
        if unknown_required or unknown_outputs:
            raise PipelineConfigurationError(
                f"stage '{stage.name}' declares fields outside the unified schema: "
                f"required={sorted(unknown_required)}, outputs={sorted(unknown_outputs)}"
            )
        if stage.name in by_name:
            raise PipelineConfigurationError(f"duplicate stage name: {stage.name}")
        by_name[stage.name] = stage
        for output in stage.output_fields:
            if output in IMMUTABLE_ETL_FIELDS:
                raise PipelineConfigurationError(
                    f"stage '{stage.name}' cannot own immutable field '{output}'"
                )
            previous = output_owner.get(output)
            if previous:
                raise PipelineConfigurationError(
                    f"output field '{output}' is owned by both '{previous}' and '{stage.name}'"
                )
            output_owner[output] = stage.name

    indegree = {name: 0 for name in by_name}
    downstream: dict[str, list[str]] = defaultdict(list)
    for stage in stages:
        for dependency in stage.dependencies:
            if dependency not in by_name:
                raise PipelineConfigurationError(
                    f"stage '{stage.name}' depends on missing stage '{dependency}'"
                )
            indegree[stage.name] += 1
            downstream[dependency].append(stage.name)

    queue = deque(stage.name for stage in stages if indegree[stage.name] == 0)
    ordered: list[ETLStage] = []
    while queue:
        name = queue.popleft()
        ordered.append(by_name[name])
        for child in downstream[name]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if len(ordered) != len(stages):
        raise PipelineConfigurationError("stage dependency graph contains a cycle")
    return tuple(ordered)


def _evaluate_predicate(
    predicate: Callable[[Record, StageContext], bool],
    record: Record,
    context: StageContext,
    *,
    label: str,
) -> bool:
    try:
        result = predicate(deepcopy(record), context)
    except StageTransformError:
        raise
    except Exception as exc:
        raise StageTransformError(
            f"{label} failed for record {record.get('id')!r}: {exc}"
        ) from exc
    if not isinstance(result, bool):
        raise StageTransformError(
            f"{label} must return bool for record {record.get('id')!r}"
        )
    return result


def _freeze(value: object) -> object:
    """Expose session context as a recursively read-only snapshot."""

    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze(item) for item in value)
    return value


def _validate_stage_patch(stage: ETLStage, patch: object) -> None:
    if not isinstance(patch, dict):
        raise StageTransformError(f"stage '{stage.name}' must return a dict patch")
    undeclared = set(patch) - set(stage.output_fields)
    if undeclared:
        raise StageTransformError(
            f"stage '{stage.name}' returned undeclared fields: {sorted(undeclared)}"
        )


def _validate_and_order_session(
    rows: Sequence[Mapping[str, object]],
) -> tuple[list[dict[str, object]], SessionKey]:
    if not rows:
        raise SessionValidationError("session contains no rows")

    first = rows[0]
    job_id = str(first.get("job_id") or "").strip()
    session_id = str(first.get("session_id") or "").strip()
    if not job_id or not session_id:
        raise SessionValidationError("trajectory ETL requires non-empty job_id and session_id")
    key = SessionKey(job_id=job_id, session_id=session_id)

    ids: set[str] = set()
    steps: set[int] = set()
    env_ids: set[str] = set()
    normalized: list[dict[str, object]] = []
    for row in rows:
        if str(row.get("job_id") or "").strip() != job_id:
            raise SessionValidationError("session contains multiple job_id values")
        if str(row.get("session_id") or "").strip() != session_id:
            raise SessionValidationError("session contains multiple session_id values")
        record_id = str(row.get("id") or "").strip()
        if not record_id or record_id in ids:
            raise SessionValidationError(f"session contains missing/duplicate id: {record_id!r}")
        ids.add(record_id)
        step_id = row.get("step_id")
        if isinstance(step_id, bool) or not isinstance(step_id, int):
            raise SessionValidationError(
                f"trajectory row {record_id!r} has invalid step_id: {step_id!r}"
            )
        step = step_id
        if step in steps:
            raise SessionValidationError(f"session contains duplicate step_id: {step}")
        steps.add(step)
        source_updated_at = row.get("source_updated_at")
        if (
            isinstance(source_updated_at, bool)
            or not isinstance(source_updated_at, int)
            or source_updated_at < 0
        ):
            raise SessionValidationError(
                f"trajectory row {record_id!r} has invalid source_updated_at: "
                f"{source_updated_at!r}"
            )
        env_id = str(row.get("env_id") or "").strip()
        if env_id:
            env_ids.add(env_id)
        normalized.append(dict(row))

    if len(env_ids) > 1:
        raise SessionValidationError(
            f"session {key} contains multiple env_id values: {sorted(env_ids)}"
        )
    normalized.sort(key=lambda row: row["step_id"])
    return normalized, key
