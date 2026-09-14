"""Mark trajectory rows as trainable using the configured policy."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from ..exceptions import StageTransformError
from ..policy import TrainabilityPolicy
from ..stage import ETLStage, Record, Session, SessionPatch, StageContext

MIN_SIDE_CHAIN_LENGTH = 20

_STREAM_FLAG_KEYS = ("stream", "streaming")
_FINISH_REASON_KEYS = ("finish_reason", "finishReason")


class UpdateIsTrainableStage(ETLStage):
    """Mark trainable rows in a completed session using the configured policy.

    Rows with an explicitly non-200 gateway status or an incomplete streaming
    request are excluded before either policy is applied. When the run context
    uses the ``downgrade`` policy, only the remaining record with the greatest
    ``step_id`` is trainable. Otherwise, the remaining rows are grouped into
    append-only chains with canonical message-prefix matching: equivalent user
    text represented as either a string or one text content block is normalized
    before matching. Each chain tail contains the complete messages of one
    structurally separated trajectory. A later strict prefix of an active chain
    tail is treated as a retry reset, so the abandoned longer row is not a
    trainable tail. Identical message snapshots are separate occurrences rather
    than append operations. In a multi-chain session, chains shorter than
    ``MIN_SIDE_CHAIN_LENGTH`` records are treated as short side branches or
    subagents and are not trainable; a single-chain session is never a side
    chain. Filtering error rows does not prevent the remaining rows from being
    processed. This stage copies the completion record's non-null ``reward`` to
    every selected row and assigns no semantic meaning to message contents.
    """

    name = "update_is_trainable"
    version = "7"
    required_fields = (
        "id",
        "step_id",
        "messages",
        "is_session_completed",
        "meta_json",
        "reward",
    )
    output_fields = ("is_trainable", "reward")
    dependencies = ()
    job_discovery_filter = "is_session_completed = true"

    def transform_session(
        self,
        session: Session,
        context: StageContext,
    ) -> SessionPatch:
        if not _is_completed_session(session, context):
            return {}

        eligible_records = tuple(
            record for record in session if _is_eligible_trainability_record(record)
        )
        if context.trainability_policy is TrainabilityPolicy.DOWNGRADE:
            max_step_record = max(eligible_records, key=_step_id, default=None)
            trainable_ids = (
                {_record_id(max_step_record)} if max_step_record is not None else set()
            )
        else:
            trainable_ids = _detect_trainable_record_ids(eligible_records)
        completed_record = next(
            record for record in session if record.get("is_session_completed") is True
        )
        final_reward = completed_record.get("reward")
        patches: SessionPatch = {}
        for record in session:
            record_id = _record_id(record)
            is_trainable = record_id in trainable_ids
            patch: dict[str, object] = {"is_trainable": is_trainable}
            if is_trainable and final_reward is not None:
                patch["reward"] = final_reward
            patches[record_id] = patch
        return patches


@dataclass
class _TrieNode:
    children: dict[str, "_TrieNode"] = field(default_factory=dict)
    terminal_record_ids: list[str] = field(default_factory=list)

    def latest_terminal(self, active_record_ids: set[str]) -> str | None:
        return next(
            (
                record_id
                for record_id in reversed(self.terminal_record_ids)
                if record_id in active_record_ids
            ),
            None,
        )


class _MessagePrefixTrie:
    """Trie of canonical message fingerprints with record IDs at sequence ends."""

    def __init__(self) -> None:
        self.root = _TrieNode()

    def latest_reset_target(
        self,
        fingerprints: Sequence[str],
        active_record_ids: set[str],
        record_positions: Mapping[str, int],
    ) -> str | None:
        """Find the latest active tail strictly extended by ``fingerprints``.

        The new record is a strict prefix of that tail, so adopting it resets
        the chain to a shorter state.
        """

        node = self.root
        for fingerprint in fingerprints:
            node = node.children.get(fingerprint)
            if node is None:
                return None

        candidates: list[str] = []
        pending = [*node.children.values()]
        while pending:
            descendant = pending.pop()
            candidates.extend(
                record_id
                for record_id in descendant.terminal_record_ids
                if record_id in active_record_ids
            )
            pending.extend(descendant.children.values())
        return max(candidates, key=record_positions.__getitem__, default=None)

    def longest_append_target(
        self,
        fingerprints: Sequence[str],
        active_record_ids: set[str],
    ) -> str | None:
        """Find the longest active tail that ``fingerprints`` strictly extend.

        The new record appends messages after that tail.
        """

        node = self.root
        matched_record_id = (
            node.latest_terminal(active_record_ids) if fingerprints else None
        )
        for index, fingerprint in enumerate(fingerprints):
            child = node.children.get(fingerprint)
            if child is None:
                break
            node = child
            if index == len(fingerprints) - 1:
                break
            candidate = node.latest_terminal(active_record_ids)
            if candidate is not None:
                matched_record_id = candidate
        return matched_record_id

    def insert(self, fingerprints: Sequence[str], record_id: str) -> None:
        node = self.root
        for fingerprint in fingerprints:
            node = node.children.setdefault(fingerprint, _TrieNode())
        node.terminal_record_ids.append(record_id)


def _detect_trainable_record_ids(
    session: Sequence[Record],
    *,
    diagnostics: dict[str, dict[str, Any]] | None = None,
) -> set[str]:
    """Select chain tails; optionally collect structural evidence for offline tools."""

    ordered = sorted(session, key=_step_id)
    trie = _MessagePrefixTrie()
    chains: list[list[str]] = []
    active_tail_to_chain: dict[str, int] = {}
    record_positions: dict[str, int] = {}
    diagnostic_fingerprints: dict[str, list[str]] = {}

    for position, record in enumerate(ordered):
        record_id = _record_id(record)
        record_positions[record_id] = position
        messages = _decode_messages(record.get("messages"), record_id)
        fingerprints = _fingerprint_messages(messages)
        active_record_ids = set(active_tail_to_chain)

        matched_record_id = trie.latest_reset_target(
            fingerprints,
            active_record_ids,
            record_positions,
        )
        relation = "strict_prefix_reset"
        if matched_record_id is None:
            matched_record_id = trie.longest_append_target(
                fingerprints,
                active_record_ids,
            )
            relation = "strict_prefix_extension"

        if diagnostics is not None:
            evidence: dict[str, Any] = {
                "message_count": len(messages),
                "relation": relation if matched_record_id is not None else "new_chain",
                "matched_record_id": matched_record_id,
            }
            if matched_record_id is None:
                evidence["identical_active_record_ids"] = [
                    candidate
                    for candidate in active_tail_to_chain
                    if diagnostic_fingerprints[candidate] == fingerprints
                ]
                if position:
                    evidence["previous_eligible_record_comparison"] = (
                        _compare_with_previous(
                            ordered[position - 1],
                            messages,
                            fingerprints,
                            diagnostic_fingerprints,
                        )
                    )
            diagnostics[record_id] = evidence
            diagnostic_fingerprints[record_id] = fingerprints

        if matched_record_id is not None:
            chain_index = active_tail_to_chain[matched_record_id]
            active_tail_to_chain.pop(chains[chain_index][-1], None)
        else:
            chain_index = len(chains)
            chains.append([])
        chains[chain_index].append(record_id)
        active_tail_to_chain[record_id] = chain_index
        trie.insert(fingerprints, record_id)

    if diagnostics is not None:
        _annotate_chain_evidence(chains, diagnostics)

    # Every append-only chain tail contains the complete messages of one
    # structurally separated trajectory. In a multi-chain session, short side
    # branches are subagent-like and carry no trainable trajectory.
    return {
        chain[-1] for chain in chains if not _is_short_side_chain(chain, len(chains))
    }


def _compare_with_previous(
    previous: Record,
    messages: Sequence[Any],
    fingerprints: Sequence[str],
    diagnostic_fingerprints: Mapping[str, Sequence[str]],
) -> dict[str, Any]:
    """Explain a new chain by diffing against the previous eligible record."""

    previous_id = _record_id(previous)
    previous_fingerprints = diagnostic_fingerprints[previous_id]
    common_length = 0
    for left, right in zip(previous_fingerprints, fingerprints):
        if left != right:
            break
        common_length += 1
    return {
        "record_id": previous_id,
        "common_prefix_message_count": common_length,
        "previous_message_count": len(previous_fingerprints),
        "system_messages_equal": _system_message_fingerprints(
            _decode_messages(previous.get("messages"), previous_id)
        )
        == _system_message_fingerprints(messages),
    }


def _system_message_fingerprints(messages: Sequence[Any]) -> list[str]:
    return [
        _fingerprint_message(message)
        for message in messages
        if isinstance(message, Mapping) and message.get("role") == "system"
    ]


def _annotate_chain_evidence(
    chains: Sequence[Sequence[str]],
    diagnostics: dict[str, dict[str, Any]],
) -> None:
    for chain_index, chain in enumerate(chains):
        excluded = _is_short_side_chain(chain, len(chains))
        for record_id in chain:
            diagnostics[record_id].update(
                {
                    "chain_index": chain_index,
                    "chain_count": len(chains),
                    "chain_length": len(chain),
                    "chain_root_record_id": chain[0],
                    "chain_tail_record_id": chain[-1],
                    "reason_code": (
                        "short_side_chain"
                        if excluded
                        else "selected_chain_tail"
                        if record_id == chain[-1]
                        else "superseded_in_chain"
                    ),
                }
            )


def _is_short_side_chain(chain: Sequence[str], chain_count: int) -> bool:
    """Return whether a multi-chain branch is shorter than the threshold."""

    return chain_count > 1 and len(chain) < MIN_SIDE_CHAIN_LENGTH


def _is_eligible_trainability_record(record: Record) -> bool:
    """Return whether a row may participate in trainability selection."""

    return not _has_non_200_status_code(record) and not _is_incomplete_stream_request(
        record
    )


def _has_non_200_status_code(record: Record) -> bool:
    """Return whether one row records an explicitly non-200 gateway result."""

    metadata = _decode_json_object(record.get("meta_json"))
    if metadata is None:
        return False
    nested_objects = [
        _decode_json_object(metadata.get(key)) for key in ("env_state", "telemetry")
    ]
    objects = [metadata, *(obj for obj in nested_objects if obj is not None)]
    return any(
        "status_code" in item and not _is_status_code_200(item["status_code"])
        for item in objects
    )


def _is_incomplete_stream_request(record: Record) -> bool:
    """Return whether any streaming request or response payload is incomplete.

    Providers place request and response metadata in different shapes. Decode
    nested JSON strings and inspect all nested objects, accepting both boolean
    and string stream markers and both snake_case and camelCase finish-reason
    keys. A stream with no finish reason at all is incomplete, as is a finish
    reason represented by null, ``"null"``, or an empty string.
    """

    payloads: list[object] = [record.get("meta_json"), record.get("response")]
    if any(key in record for key in _STREAM_FLAG_KEYS + _FINISH_REASON_KEYS):
        payloads.append(record)
    objects = [
        nested for payload in payloads for nested in _nested_json_objects(payload)
    ]
    if not any(
        _is_truthy_flag(item.get(key))
        for item in objects
        for key in _STREAM_FLAG_KEYS
    ):
        return False
    finish_reasons = [
        item[key] for item in objects for key in _FINISH_REASON_KEYS if key in item
    ]
    return not finish_reasons or any(
        _is_empty_finish_reason(value) for value in finish_reasons
    )


def _nested_json_objects(value: object) -> list[Mapping[str, object]]:
    """Flatten nested mappings, decoding JSON-object strings on the way."""

    decoded = _decode_json_object(value)
    if decoded is None:
        return []
    objects: list[Mapping[str, object]] = [decoded]
    for child in decoded.values():
        if isinstance(child, list):
            objects.extend(
                nested for item in child for nested in _nested_json_objects(item)
            )
        else:
            objects.extend(_nested_json_objects(child))
    return objects


def _is_truthy_flag(value: object) -> bool:
    if value is True:
        return True
    if isinstance(value, int) and not isinstance(value, bool):
        return value == 1
    return isinstance(value, str) and value.strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _is_empty_finish_reason(value: object) -> bool:
    return value is None or (
        isinstance(value, str) and value.strip().lower() in {"", "null"}
    )


def _is_status_code_200(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value == 200
    if isinstance(value, str):
        return value.strip() == "200"
    return False


def _is_completed_session(
    session: Sequence[Record],
    context: StageContext,
) -> bool:
    if not session:
        raise StageTransformError("session must contain at least one row")

    completed_step_id: int | None = None
    completed_record_id: str | None = None
    max_step_id: int | None = None
    for record in session:
        record_id = _record_id(record)
        step_id = _step_id(record)
        max_step_id = step_id if max_step_id is None else max(max_step_id, step_id)
        value = record.get("is_session_completed")
        if value is not None and not isinstance(value, bool):
            raise StageTransformError(
                "is_session_completed must be bool or null",
                record_id=record_id,
            )
        if value is True:
            if completed_step_id is not None:
                raise StageTransformError(
                    "There is exactly one `is_session_completed`.",
                    record_id=record_id,
                )
            completed_step_id = step_id
            completed_record_id = record_id

    if completed_step_id is None:
        return False
    if completed_step_id != max_step_id:
        context.warn(
            "is_session_completed is not set on the maximum step_id record; "
            f"completed_record_id={completed_record_id!r}, "
            f"completed_step_id={completed_step_id}, max_step_id={max_step_id}; "
            "continuing trainability processing",
            warning_type="CompletionMarkerBeforeMaxStep",
        )
    return True


def _decode_json_object(value: object) -> Mapping[str, object] | None:
    if isinstance(value, Mapping):
        return value
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError):
        return None
    return decoded if isinstance(decoded, dict) else None


def _decode_messages(value: object, record_id: str) -> list[Any]:
    if not isinstance(value, str) or not value.strip():
        raise StageTransformError(
            f"messages must be a non-empty JSON string for record {record_id!r}",
            record_id=record_id,
        )
    try:
        messages = json.loads(value)
    except (TypeError, ValueError) as exc:
        raise StageTransformError(
            f"messages contains malformed JSON for record {record_id!r}",
            record_id=record_id,
        ) from exc
    if not isinstance(messages, list):
        raise StageTransformError(
            f"messages must be a JSON array for record {record_id!r}",
            record_id=record_id,
        )
    return messages


def _fingerprint_message(message: Any) -> str:
    canonical = json.dumps(
        _normalize_message(message),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _fingerprint_messages(messages: Sequence[Any]) -> list[str]:
    return [_fingerprint_message(message) for message in messages]


def _normalize_message(message: Any) -> Any:
    """Canonicalize equivalent user text shapes without mutating the input."""

    if not isinstance(message, Mapping) or message.get("role") != "user":
        return message
    content = message.get("content")
    if not isinstance(content, str):
        return message
    normalized = dict(message)
    normalized["content"] = [{"type": "text", "text": content}]
    return normalized


def _record_id(record: Record) -> str:
    value = record.get("id")
    if not isinstance(value, str) or not value.strip():
        raise StageTransformError(f"record has invalid id: {value!r}")
    return value.strip()


def _step_id(record: Record) -> int:
    value = record.get("step_id")
    if isinstance(value, bool) or not isinstance(value, int):
        record_id = _record_id(record)
        raise StageTransformError(
            f"record {record_id!r} has invalid step_id: {value!r}",
            record_id=record_id,
        )
    return value
