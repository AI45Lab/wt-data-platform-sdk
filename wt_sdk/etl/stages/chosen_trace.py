"""Built-in chosen-trace construction stage."""

import json

from ..exceptions import StageTransformError
from ..stage import ETLStage, Patch, Record, StageContext


class BuildChosenTraceStage(ETLStage):
    name = "build_chosen_trace"
    version = "1"
    required_fields = ("is_trainable", "messages", "response")
    output_fields = ("chosen_trace",)

    def applies(self, record: Record, context: StageContext) -> bool:
        _ = context
        return record.get("is_trainable") is True

    def transform(self, record: Record, context: StageContext) -> Patch:
        _ = context
        messages = _decode_json(record.get("messages"), "messages")
        response = _decode_json(record.get("response"), "response")
        if not isinstance(messages, list):
            raise StageTransformError("messages must be a JSON array")
        trace = list(messages)
        if isinstance(response, list):
            trace.extend(response)
        else:
            trace.append(response)
        return {
            "chosen_trace": json.dumps(
                trace,
                ensure_ascii=False,
                separators=(",", ":"),
            )
        }


def _decode_json(value: object, field: str) -> object:
    if not isinstance(value, str) or not value.strip():
        raise StageTransformError(f"{field} must be a non-empty JSON string")
    try:
        return json.loads(value)
    except (TypeError, ValueError) as exc:
        raise StageTransformError(f"{field} contains malformed JSON") from exc
