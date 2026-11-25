"""Delivery-category parsing shared by session and aggregation code."""

from dataclasses import dataclass
from typing import Optional


_HARNESS_ALIASES = {
    "claude": "claudecode",
    "claude-code": "claudecode",
    "claude_code": "claudecode",
    "claudecode": "claudecode",
}


@dataclass(frozen=True)
class DeliveryCategory:
    dataset: str
    harness: str
    model: str
    task_type: str
    level: str = ""

    @property
    def key(self) -> tuple[str, str, str, str, str]:
        return (self.dataset, self.harness, self.model, self.task_type, self.level)

    def as_dict(self) -> dict[str, str]:
        return {
            "dataset": self.dataset,
            "harness": self.harness,
            "model": self.model,
            "task_type": self.task_type,
            "level": self.level,
        }


def parse_delivery_category(job_id: object) -> Optional[DeliveryCategory]:
    """Parse the dashboard delivery key without using serving tags or env tables."""
    parts = [part.strip() for part in str(job_id or "").split("#")]
    if len(parts) < 4 or not all(parts[:4]):
        return None

    dataset = parts[0].lower()
    raw_harness = parts[1].lower()
    harness = _HARNESS_ALIASES.get(raw_harness, raw_harness)
    level = ""
    if dataset == "cybergym":
        level = next(
            (
                part
                for part in parts[4:]
                if part.startswith("level") and len(part) > len("level")
            ),
            "",
        )

    return DeliveryCategory(
        dataset=dataset,
        harness=harness,
        model=parts[2],
        task_type=parts[3],
        level=level,
    )
