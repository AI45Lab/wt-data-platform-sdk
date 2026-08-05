"""ETL-specific exceptions with stable failure categories."""


class ETLError(RuntimeError):
    """Base class for ETL failures."""


class PipelineConfigurationError(ETLError):
    """Raised before execution when a pipeline definition is invalid."""


class SessionValidationError(ETLError):
    """Raised when rows do not form one valid trajectory session."""


class StageTransformError(ETLError):
    """Raised when a stage cannot transform otherwise readable source data."""


class CheckpointError(ETLError):
    """Raised when durable checkpoint state is missing or inconsistent."""
