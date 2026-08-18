"""Pipeline definition, dependency ordering, validation, and pure execution."""

import heapq
from collections import defaultdict
from copy import deepcopy
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Mapping, Sequence

from wt_sdk.core.schemas import LANDING_SCHEMA
from wt_sdk.models import ServingRecord

from .exceptions import (
    PipelineConfigurationError,
    SessionValidationError,
    StageTransformError,
)
from .models import LandingRowPatch, PipelineMode, RecordFailure, SessionResult
from .stage import (
    ETLStage,
    Session,
    SessionKey,
    SessionPatch,
    StageContext,
    StageWarning,
)


IMMUTABLE_ETL_FIELDS = {
    "id",
    "job_id",
    "session_id",
    "created_at",
    "source_updated_at",
    "serving_updated_at",
}
ETL_SCHEMA_FIELDS = frozenset(LANDING_SCHEMA.names)


@dataclass(frozen=True)
class PipelineDefinition:
    """A validated, ordered set of stages and one persistence mode."""

    name: str
    version: str
    mode: PipelineMode
    stages: tuple[ETLStage, ...]
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
        object.__setattr__(self, "_ordered_stages", self.validate_dag(self.stages))

    @staticmethod
    def validate_dag(stages: Sequence[ETLStage]) -> tuple[ETLStage, ...]:
        """Validate a stage DAG without executing ETL or accessing tables.

        The returned tuple is the deterministic topological execution order.
        Invalid metadata, dependencies, field ownership, or cycles raise
        PipelineConfigurationError.
        """

        return _order_and_validate_stages(stages)

    @property
    def ordered_stages(self) -> tuple[ETLStage, ...]:
        return self._ordered_stages

    @property
    def job_discovery_filter(self) -> str | None:
        """Return a conservative row filter for complete-job discovery.

        A filter is safe only when every stage declares one. The filters are
        OR-ed because a session must be discovered when any stage could select
        one of its rows. A missing hint deliberately falls back to scanning all
        rows in the requested job; stage execution remains the source of truth.
        """

        filters = [stage.job_discovery_filter for stage in self.ordered_stages]
        if any(value is None for value in filters):
            return None
        unique = tuple(dict.fromkeys(str(value).strip() for value in filters))
        if len(unique) == 1:
            return unique[0]
        return " OR ".join(f"({value})" for value in unique)

    def describe_dag(self) -> dict[str, object]:
        """Return a JSON-serializable stage inventory and dependency graph."""

        stages = []
        edges = []
        for order, stage in enumerate(self.ordered_stages, start=1):
            stages.append(
                {
                    "order": order,
                    "name": stage.name,
                    "version": stage.version,
                    "required_fields": list(stage.required_fields),
                    "output_fields": list(stage.output_fields),
                    "dependencies": list(stage.dependencies),
                    "job_discovery_filter": stage.job_discovery_filter,
                }
            )
            edges.extend(
                {"from": dependency, "to": stage.name}
                for dependency in stage.dependencies
            )
        return {
            "pipeline_name": self.name,
            "pipeline_version": self.version,
            "mode": self.mode.value,
            "execution_order": [stage.name for stage in self.ordered_stages],
            "stages": stages,
            "edges": edges,
        }

    def process_session(
        self,
        rows: Sequence[Mapping[str, object]],
        *,
        collect_failures: bool = False,
    ) -> SessionResult:
        (
            ordered_rows,
            session_key,
            validation_warnings,
        ) = _validate_and_order_session(rows)
        original_by_id = {str(row["id"]): dict(row) for row in ordered_rows}
        working_by_id = deepcopy(original_by_id)
        ordered_ids = tuple(str(row["id"]) for row in ordered_rows)
        selected_ids: set[str] = set()
        emitted_warnings = list(validation_warnings)
        failure_stage = "__stage_execution__"
        failure_record_id: str | None = None

        try:
            for stage in self.ordered_stages:
                failure_stage = stage.name
                stage_input = _freeze_session(working_by_id, ordered_ids)
                _validate_stage_inputs(stage, stage_input)
                context = StageContext(
                    pipeline_name=self.name,
                    pipeline_version=self.version,
                    session_key=session_key,
                    stage_name=stage.name,
                )
                try:
                    try:
                        proposed = stage.transform_session(stage_input, context)
                    except StageTransformError:
                        raise
                    except Exception as exc:
                        raise StageTransformError(
                            f"stage '{stage.name}' failed for session {session_key}: {exc}"
                        ) from exc
                finally:
                    emitted_warnings.extend(context.emitted_warnings)
                stage_patches = _validate_stage_session_patch(
                    stage,
                    proposed,
                    known_record_ids=set(ordered_ids),
                )
                selected_ids.update(stage_patches)
                for record_id, patch in stage_patches.items():
                    working_by_id[record_id].update(deepcopy(patch))

            landing_patches: list[LandingRowPatch] = []
            serving_records: list[ServingRecord] = []
            failure_stage = "__output_validation__"
            for record_id in ordered_ids:
                if record_id not in selected_ids:
                    continue
                failure_record_id = record_id
                original = original_by_id[record_id]
                working = working_by_id[record_id]
                if self.mode is PipelineMode.LANDING:
                    changed = {
                        key: value
                        for key, value in working.items()
                        if key not in IMMUTABLE_ETL_FIELDS and original.get(key) != value
                    }
                    if changed:
                        landing_patches.append(
                            LandingRowPatch(
                                record_id=record_id,
                                job_id=session_key.job_id,
                                session_id=session_key.session_id,
                                updates=changed,
                            )
                        )
                else:
                    serving_record = dict(working)
                    serving_record["serving_updated_at"] = None
                    serving_records.append(ServingRecord(**serving_record))
        except Exception as exc:
            if not collect_failures:
                raise
            attributed_record_id = getattr(exc, "record_id", None) or failure_record_id
            return SessionResult(
                session_key=session_key,
                source_rows=len(ordered_rows),
                selected_rows=len(selected_ids),
                successful_rows=0,
                warnings=tuple(emitted_warnings),
                failures=(
                    RecordFailure(
                        record_id=attributed_record_id,
                        job_id=session_key.job_id,
                        session_id=session_key.session_id,
                        stage_name=failure_stage,
                        error_type=type(exc).__name__,
                        message=str(exc),
                    ),
                ),
            )

        return SessionResult(
            session_key=session_key,
            source_rows=len(ordered_rows),
            selected_rows=len(selected_ids),
            successful_rows=len(selected_ids),
            landing_patches=tuple(landing_patches),
            serving_records=tuple(serving_records),
            warnings=tuple(emitted_warnings),
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
        if stage.job_discovery_filter is not None and (
            not isinstance(stage.job_discovery_filter, str)
            or not stage.job_discovery_filter.strip()
        ):
            raise PipelineConfigurationError(
                f"stage '{stage.name}' job_discovery_filter must be a non-empty string or None"
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

    declaration_order = {stage.name: index for index, stage in enumerate(stages)}
    queue = [
        (declaration_order[stage.name], stage.name)
        for stage in stages
        if indegree[stage.name] == 0
    ]
    heapq.heapify(queue)
    ordered: list[ETLStage] = []
    while queue:
        _, name = heapq.heappop(queue)
        ordered.append(by_name[name])
        for child in downstream[name]:
            indegree[child] -= 1
            if indegree[child] == 0:
                heapq.heappush(queue, (declaration_order[child], child))

    if len(ordered) != len(stages):
        raise PipelineConfigurationError("stage dependency graph contains a cycle")
    return tuple(ordered)


def _freeze(value: object) -> object:
    """Expose session context as a recursively read-only snapshot."""

    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze(item) for item in value)
    return value


def _freeze_session(
    working_by_id: Mapping[str, Mapping[str, object]],
    ordered_ids: Sequence[str],
) -> Session:
    return tuple(_freeze(dict(working_by_id[record_id])) for record_id in ordered_ids)


def _validate_stage_inputs(stage: ETLStage, session: Session) -> None:
    for record in session:
        missing = [field for field in stage.required_fields if field not in record]
        if missing:
            raise StageTransformError(
                f"stage '{stage.name}' missing required fields {missing} "
                f"for record {record.get('id')!r}",
                record_id=str(record.get("id")) if record.get("id") is not None else None,
            )


def _validate_stage_session_patch(
    stage: ETLStage,
    proposed: object,
    *,
    known_record_ids: set[str],
) -> SessionPatch:
    if not isinstance(proposed, dict):
        raise StageTransformError(
            f"stage '{stage.name}' must return a dict keyed by record ID"
        )

    validated: SessionPatch = {}
    for raw_record_id, raw_patch in proposed.items():
        if not isinstance(raw_record_id, str) or not raw_record_id.strip():
            raise StageTransformError(
                f"stage '{stage.name}' returned an invalid record ID: {raw_record_id!r}"
            )
        record_id = raw_record_id.strip()
        if record_id != raw_record_id:
            raise StageTransformError(
                f"stage '{stage.name}' returned a non-canonical record ID: {raw_record_id!r}"
            )
        if record_id not in known_record_ids:
            raise StageTransformError(
                f"stage '{stage.name}' returned patch for unknown record ID {record_id!r}",
                record_id=record_id,
            )
        if not isinstance(raw_patch, dict) or not raw_patch:
            raise StageTransformError(
                f"stage '{stage.name}' must return a non-empty dict patch "
                f"for record {record_id!r}",
                record_id=record_id,
            )
        undeclared = set(raw_patch) - set(stage.output_fields)
        if undeclared:
            raise StageTransformError(
                f"stage '{stage.name}' returned undeclared fields for record "
                f"{record_id!r}: {sorted(undeclared)}",
                record_id=record_id,
            )
        validated[record_id] = dict(raw_patch)
    return validated


def _validate_and_order_session(
    rows: Sequence[Mapping[str, object]],
) -> tuple[list[dict[str, object]], SessionKey, tuple[StageWarning, ...]]:
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
    duplicate_steps: set[int] = set()
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
            duplicate_steps.add(step)
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
    warnings: tuple[StageWarning, ...] = ()
    if duplicate_steps:
        warnings = (
            StageWarning(
                job_id=key.job_id,
                session_id=key.session_id,
                stage_name="__session_validation__",
                warning_type="DuplicateStepId",
                message=(
                    "session contains duplicate step_id values: "
                    f"{sorted(duplicate_steps)}"
                ),
            ),
        )
    return normalized, key, warnings
