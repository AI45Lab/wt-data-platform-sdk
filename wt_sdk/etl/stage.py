"""Contributor-facing ETL stage contract."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Mapping, Sequence


Record = Mapping[str, Any]
Patch = dict[str, Any]


@dataclass(frozen=True, order=True)
class SessionKey:
    """The logical trajectory identity used by ETL."""

    job_id: str
    session_id: str


@dataclass(frozen=True)
class StageContext:
    """Read-only context supplied to every stage invocation."""

    pipeline_name: str
    pipeline_version: str
    session_key: SessionKey
    session: Sequence[Record]


class ETLStage(ABC):
    """Pure, deterministic transformation that returns an in-memory patch.

    Subclasses declare metadata as class attributes and must not perform table
    reads or writes. The engine owns persistence, retries, and checkpointing.
    """

    name: str = ""
    version: str = "1"
    required_fields: tuple[str, ...] = ()
    output_fields: tuple[str, ...] = ()
    dependencies: tuple[str, ...] = ()

    def applies(self, record: Record, context: StageContext) -> bool:
        """Return whether this stage should run for one record."""

        _ = record, context
        return True

    @abstractmethod
    def transform(self, record: Record, context: StageContext) -> Patch:
        """Return only the fields this stage proposes to change."""

