"""Minimal persistent ETL task queue and serial worker.

This package intentionally stays separate from the ETL execution engine.  The
engine owns pipeline semantics and checkpoints; this package only owns job-level
orchestration state and the env-table completion trigger.
"""

from .discovery import EnvDiscoveryReport, EnvJobSummary, discover_env_jobs
from .models import (
    ETL_TASK_SCHEMA,
    ETLTask,
    TaskStatus,
    format_task_time,
    resolve_task_table,
)
from .store import DldbTaskStore
from .service import (
    BootstrapPlan,
    EnqueueResult,
    apply_bootstrap,
    build_bootstrap_plan,
    discover_and_enqueue,
    submit_if_ready,
)

__all__ = [
    "DldbTaskStore",
    "ETLTask",
    "ETL_TASK_SCHEMA",
    "EnvDiscoveryReport",
    "EnvJobSummary",
    "EnqueueResult",
    "BootstrapPlan",
    "TaskStatus",
    "apply_bootstrap",
    "build_bootstrap_plan",
    "discover_and_enqueue",
    "discover_env_jobs",
    "format_task_time",
    "resolve_task_table",
    "submit_if_ready",
]
