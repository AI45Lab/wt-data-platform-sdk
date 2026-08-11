"""Contributor-facing, session-level ETL stage contract."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
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
class StageContext:
    """Read-only execution metadata supplied to one session-stage invocation."""

    pipeline_name: str
    pipeline_version: str
    session_key: SessionKey


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
]
