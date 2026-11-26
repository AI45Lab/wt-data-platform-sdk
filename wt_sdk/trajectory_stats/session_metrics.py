"""Pure per-session token, step, and tool-call computation."""

import json
from numbers import Integral, Real
from typing import Iterable, Mapping, Optional

from .categories import parse_delivery_category
from .schemas import (
    CLASSIFICATION_STATUS_INVALID_JOB_ID,
    CLASSIFICATION_STATUS_VALID,
    METRIC_STATUS_INCOMPLETE,
    METRIC_STATUS_MISSING,
    METRIC_STATUS_VALID,
)


_NO_REAL_REQUESTS = "NO_REAL_LANDING_REQUESTS"


def compute_session_metrics(
    job_id: str,
    session_id: str,
    landing_rows: Iterable[Mapping[str, object]],
    *,
    updated_at_ms: int,
) -> dict[str, object]:
    """Compute one parent ``(job_id, session_id)`` row without performing I/O.

    All real landing rows, including side chains and rows from multiple
    ``sub_job_id`` shards, must be supplied by the caller. Message history is
    never scanned for tool calls; only each row's new response is inspected.
    """
    accumulator = SessionMetricsAccumulator(job_id, session_id)
    for row in landing_rows:
        accumulator.add_row(row)
    return accumulator.finalize(updated_at_ms=updated_at_ms)


class SessionMetricsAccumulator:
    """Memory-bounded session state for either batch or streaming computation."""

    def __init__(self, job_id: str, session_id: str) -> None:
        if not isinstance(job_id, str) or not job_id.strip():
            raise ValueError("job_id is required")
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("session_id is required")
        self.job_id = job_id
        self.session_id = session_id
        self._request_count = 0
        self._token_total = 0
        self._token_available = 0
        self._tool_total = 0
        self._tool_available = 0
        self._max_step_id: Optional[int] = None

    def add_row(self, row: Mapping[str, object]) -> None:
        row_job_id = row.get("job_id")
        row_session_id = row.get("session_id")
        if row_job_id is not None and row_job_id != self.job_id:
            raise ValueError("landing row belongs to a different job_id")
        if row_session_id is not None and row_session_id != self.session_id:
            raise ValueError("landing row belongs to a different session_id")
        if _is_placeholder(row.get("messages")):
            return

        self._request_count += 1
        token_value = _extract_token_value(row)
        if token_value is not None:
            self._token_total += token_value
            self._token_available += 1
        tool_value = _extract_response_tool_calls(row)
        if tool_value is not None:
            self._tool_total += tool_value
            self._tool_available += 1
        step_id = _valid_step_id(row.get("step_id"))
        if step_id is not None and (
            self._max_step_id is None or step_id > self._max_step_id
        ):
            self._max_step_id = step_id

    def finalize(self, *, updated_at_ms: int) -> dict[str, object]:
        if self._request_count:
            total_tokens, token_status, token_reason = _complete_sum_state(
                self._token_total,
                self._token_available,
                self._request_count,
                missing_reason="TOKEN_USAGE_MISSING",
                incomplete_reason="TOKEN_USAGE_INCOMPLETE",
            )
            tool_calls, tool_status, tool_reason = _complete_sum_state(
                self._tool_total,
                self._tool_available,
                self._request_count,
                missing_reason="TOOL_CALLS_MISSING",
                incomplete_reason="TOOL_CALLS_INCOMPLETE",
            )
            if self._max_step_id is not None:
                total_steps = self._max_step_id
                step_status = METRIC_STATUS_VALID
                step_reason = None
            else:
                total_steps = None
                step_status = METRIC_STATUS_MISSING
                step_reason = "STEP_ID_MISSING"
        else:
            total_tokens = None
            total_steps = None
            tool_calls = None
            token_status = METRIC_STATUS_MISSING
            step_status = METRIC_STATUS_MISSING
            tool_status = METRIC_STATUS_MISSING
            token_reason = _NO_REAL_REQUESTS
            step_reason = _NO_REAL_REQUESTS
            tool_reason = _NO_REAL_REQUESTS

        category = parse_delivery_category(self.job_id)
        if category is None:
            category_fields = {
                "dataset": None,
                "harness": None,
                "model": None,
                "task_type": None,
                "level": "",
            }
            classification_status = CLASSIFICATION_STATUS_INVALID_JOB_ID
        else:
            category_fields = category.as_dict()
            classification_status = CLASSIFICATION_STATUS_VALID

        return {
            "job_id": self.job_id,
            "session_id": self.session_id,
            "classification_status": classification_status,
            **category_fields,
            "total_tokens": total_tokens,
            "total_tokens_status": token_status,
            "total_tokens_reason": token_reason,
            "total_steps": total_steps,
            "total_steps_status": step_status,
            "total_steps_reason": step_reason,
            "tool_calls": tool_calls,
            "tool_calls_status": tool_status,
            "tool_calls_reason": tool_reason,
            "updated_at_ms": updated_at_ms,
        }


def _decode_json(value: object) -> object:
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


def _is_placeholder(messages: object) -> bool:
    decoded = _decode_json(messages)
    return isinstance(decoded, list) and not decoded


def _nonnegative_integer(value: object) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, Integral):
        normalized = int(value)
    elif isinstance(value, Real) and float(value).is_integer():
        normalized = int(value)
    elif isinstance(value, str):
        try:
            normalized = int(value.strip())
        except ValueError:
            return None
    else:
        return None
    return normalized if normalized >= 0 else None


def _token_pair(container: object, input_name: str, output_name: str) -> Optional[int]:
    if not isinstance(container, dict):
        return None
    input_tokens = _nonnegative_integer(container.get(input_name))
    output_tokens = _nonnegative_integer(container.get(output_name))
    if input_tokens is None or output_tokens is None:
        return None
    return input_tokens + output_tokens


def _extract_token_value(row: Mapping[str, object]) -> Optional[int]:
    meta = _decode_json(row.get("meta_json"))
    response = _decode_json(row.get("response"))

    if isinstance(meta, dict):
        direct = _token_pair(meta, "prompt_tokens", "completion_tokens")
        if direct is not None:
            return direct

    usages = []
    if isinstance(response, dict):
        usages.append(response.get("usage"))
    if isinstance(meta, dict) and isinstance(meta.get("response"), dict):
        usages.append(meta["response"].get("usage"))

    for usage in usages:
        for input_name, output_name in (
            ("prompt_tokens", "completion_tokens"),
            ("input_tokens", "output_tokens"),
        ):
            total = _token_pair(usage, input_name, output_name)
            if total is not None:
                return total
    return None


def _valid_step_id(value: object) -> Optional[int]:
    return _nonnegative_integer(value)


def _complete_sum_state(
    total: int,
    available_count: int,
    request_count: int,
    *,
    missing_reason: str,
    incomplete_reason: str,
) -> tuple[Optional[int], str, Optional[str]]:
    if available_count == request_count:
        return total, METRIC_STATUS_VALID, None
    if available_count:
        return None, METRIC_STATUS_INCOMPLETE, incomplete_reason
    return None, METRIC_STATUS_MISSING, missing_reason


def _extract_response_tool_calls(row: Mapping[str, object]) -> Optional[int]:
    response = _decode_json(row.get("response"))
    if isinstance(response, list):
        if not response:
            return None
        return _count_events(response)
    if isinstance(response, dict):
        return _count_event(response)
    return None


def _count_events(events: list[object]) -> Optional[int]:
    total = 0
    for event in events:
        count = _count_event(event)
        if count is None:
            return None
        total += count
    return total


def _count_event(event: object) -> Optional[int]:
    if not isinstance(event, dict):
        return None

    event_type = event.get("type")
    if isinstance(event_type, str) and event_type.endswith("_call"):
        return 1

    if isinstance(event.get("message"), dict):
        return _count_message(event["message"])

    choices = event.get("choices")
    if isinstance(choices, list):
        total = 0
        for choice in choices:
            if not isinstance(choice, dict) or not isinstance(choice.get("message"), dict):
                return None
            count = _count_message(choice["message"])
            if count is None:
                return None
            total += count
        return total

    output = event.get("output")
    if isinstance(output, list):
        return _count_events(output)

    if any(key in event for key in ("role", "content", "tool_calls")):
        return _count_message(event)

    # Codex reasoning, function_call_output, and other explicit non-call events
    # are valid response events that contribute zero new tool calls.
    if isinstance(event_type, str):
        return 0
    return None


def _count_message(message: Mapping[str, object]) -> Optional[int]:
    tool_calls = message.get("tool_calls")
    if tool_calls is not None and not isinstance(tool_calls, list):
        return None
    if isinstance(tool_calls, list) and tool_calls:
        # A normalized tool_calls array and content tool_use entries can describe
        # the same calls. Count only the normalized array in that case.
        return len(tool_calls)

    content = message.get("content")
    if isinstance(content, list):
        return sum(
            1
            for item in content
            if isinstance(item, dict) and item.get("type") == "tool_use"
        )
    if content is None and tool_calls is None and "role" not in message:
        return None
    return 0
