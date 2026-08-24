"""Contributor-facing, session-level ETL stage contract."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Mapping


Record = Mapping[str, Any]
Session = tuple[Record, ...]
RecordPatch = dict[str, Any]
SessionPatch = dict[str, RecordPatch]


@dataclass(frozen=True, order=True)
class SessionKey:
    """The logical trajectory identity used by ETL."""

    job_id: str
    session_id: str


@dataclass(frozen=True)
class StageWarning:
    """One non-blocking data-quality diagnostic emitted by a stage."""

    job_id: str
    session_id: str
    stage_name: str
    warning_type: str
    message: str


@dataclass(frozen=True)
class StageContext:
    """Read-only execution metadata and non-blocking warning emitter."""

    pipeline_name: str
    pipeline_version: str
    session_key: SessionKey
    stage_name: str = "__stage__"
    _warnings: list[StageWarning] = field(
        default_factory=list,
        repr=False,
        compare=False,
    )

    def warn(
        self,
        message: str,
        *,
        warning_type: str = "StageWarning",
    ) -> None:
        """Record a warning without interrupting stage or session execution."""

        if not isinstance(message, str) or not message.strip():
            raise ValueError("stage warning message must be a non-empty string")
        if not isinstance(warning_type, str) or not warning_type.strip():
            raise ValueError("stage warning_type must be a non-empty string")
        self._warnings.append(
            StageWarning(
                job_id=self.session_key.job_id,
                session_id=self.session_key.session_id,
                stage_name=self.stage_name,
                warning_type=warning_type.strip(),
                message=message.strip(),
            )
        )

    @property
    def emitted_warnings(self) -> tuple[StageWarning, ...]:
        """Return an immutable snapshot of warnings emitted so far."""

        return tuple(self._warnings)


class ETLStage(ABC):
    """Pure transformation from one immutable session to record patches.

    Every stage receives the complete working session after all prerequisite
    stages have finished. Subclasses decide which records to process and return
    ``{record_id: {field: desired_value}}``. They must not mutate the input or
    perform I/O; the engine owns validation, patch merging, persistence,
    retries, and checkpointing.
    """

    name: str = ""
    version: str = "1"
    required_fields: tuple[str, ...] = ()
    output_fields: tuple[str, ...] = ()
    dependencies: tuple[str, ...] = ()
    job_discovery_filter: str | None = None

    @abstractmethod
    def transform_session(
        self,
        session: Session,
        context: StageContext,
    ) -> SessionPatch:
        """Return desired field patches keyed by existing session record ID."""


__all__ = [
    "ETLStage",
    "Record",
    "RecordPatch",
    "Session",
    "SessionKey",
    "SessionPatch",
    "StageContext",
    "StageWarning",
]
