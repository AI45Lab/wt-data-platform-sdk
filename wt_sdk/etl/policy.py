"""Per-run policy values shared by the ETL runner and stages."""

from enum import Enum


class TrainabilityPolicy(str, Enum):
    """Policy used by the trainability stage for one ETL run."""

    NORMAL = "normal"
    DOWNGRADE = "downgrade"


def normalize_trainability_policy(value: object) -> TrainabilityPolicy:
    """Validate and normalize an explicit trainability policy value."""

    if isinstance(value, TrainabilityPolicy):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        for policy in TrainabilityPolicy:
            if normalized == policy.value:
                return policy
    raise ValueError(
        "trainability_policy must be 'normal' or 'downgrade'"
    )


def trainability_policy_from_env_value(value: str | None) -> TrainabilityPolicy:
    """Resolve the CLI's temporary environment-variable input.

    The environment is intentionally interpreted by the CLI boundary only.
    Stages receive the resulting policy through ``StageContext``.
    """

    if value is not None and value.strip().lower() in {"1", "true", "yes", "on"}:
        return TrainabilityPolicy.DOWNGRADE
    return TrainabilityPolicy.NORMAL


__all__ = [
    "TrainabilityPolicy",
    "normalize_trainability_policy",
    "trainability_policy_from_env_value",
]
