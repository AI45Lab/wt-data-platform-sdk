"""Built-in chosen-trace construction stage."""

import json

from ..exceptions import StageTransformError
from ..stage import ETLStage, Session, SessionPatch, StageContext


class BuildChosenTraceStage(ETLStage):
    name = "build_chosen_trace"
    version = "1"
    required_fields = ("id", "is_trainable", "messages", "response")
    output_fields = ("chosen_trace",)
    job_discovery_filter = "is_trainable = true"

    def transform_session(
        self,
        session: Session,
        context: StageContext,
    ) -> SessionPatch:
        _ = context
        patches: SessionPatch = {}
        for record in session:
            if record.get("is_trainable") is not True:
                continue
            record_id = str(record["id"])
            messages = _decode_json(record.get("messages"), "messages", record_id)
            response = _decode_json(record.get("response"), "response", record_id)
            if not isinstance(messages, list):
                raise StageTransformError(
                    f"messages must be a JSON array for record {record_id!r}",
                    record_id=record_id,
                )
            trace = list(messages)
            if isinstance(response, list):
                trace.extend(response)
            else:
                trace.append(response)
            patches[record_id] = {
                "chosen_trace": json.dumps(
                    trace,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            }
        return patches


def _decode_json(value: object, field: str, record_id: str) -> object:
    if not isinstance(value, str) or not value.strip():
        raise StageTransformError(
            f"{field} must be a non-empty JSON string for record {record_id!r}",
            record_id=record_id,
        )
    try:
        return json.loads(value)
    except (TypeError, ValueError) as exc:
        raise StageTransformError(
            f"{field} contains malformed JSON for record {record_id!r}",
            record_id=record_id,
        ) from exc
