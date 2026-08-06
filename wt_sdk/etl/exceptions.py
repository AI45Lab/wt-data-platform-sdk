"""ETL-specific exceptions with stable failure categories."""


class ETLError(RuntimeError):
    """Base class for ETL failures."""


class PipelineConfigurationError(ETLError):
    """Raised before execution when a pipeline definition is invalid."""


class SessionValidationError(ETLError):
    """Raised when rows do not form one valid trajectory session."""


class StageTransformError(ETLError):
    """Raised when a stage cannot transform otherwise readable source data."""

    def __init__(self, message: str, *, record_id: str | None = None) -> None:
        self.record_id = record_id
        super().__init__(message)


class CheckpointError(ETLError):
    """Raised when durable checkpoint state is missing or inconsistent."""


class ETLRunFailed(ETLError):
    """Raised after recoverable ETL failures have been collected for a run."""

    def __init__(self, summary) -> None:
        self.summary = summary
        super().__init__(
            f"pipeline {summary.pipeline_name!r} completed with "
            f"{summary.failed_rows} failure(s)"
        )
