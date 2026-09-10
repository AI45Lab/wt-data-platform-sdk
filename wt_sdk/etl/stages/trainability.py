"""Mark trajectory rows as trainable using the configured policy."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Sequence

from ..exceptions import StageTransformError
from ..policy import TrainabilityPolicy
from ..stage import ETLStage, Record, Session, SessionPatch, StageContext

MIN_SIDE_CHAIN_LENGTH = 20
CLAUDE_FILTER_ENABLED = os.getenv("CLAUDE_TRAINABILITY_FILTER", "true").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}


class UpdateIsTrainableStage(ETLStage):
    """Mark trainable rows in a completed session using the configured policy.

    Rows with an explicitly non-200 gateway status are excluded before either
    policy is applied. When the run context uses the ``downgrade`` policy, only
    the remaining record with the greatest ``step_id`` is trainable. Otherwise,
    the remaining inputs are grouped into append-only chains with canonical
    message-prefix matching. Equivalent user text
    represented as either a string or one text content block is normalized
    before matching. Each chain tail contains the complete messages of one
    structurally separated trajectory. A later strict prefix of an active chain
    tail is treated as a retry reset, so the abandoned longer row is not a
    trainable tail. Identical message snapshots are separate occurrences rather
    than append operations. In a multi-chain session, independent chains shorter
    than ``TRAINABILITY_MIN_SIDE_CHAIN_LENGTH`` records are treated as short side
    branches or subagents and are not trainable. A single-chain session is not
    considered a side chain. Incomplete streaming requests with an empty finish reason and
    rows whose top-level messages disagree with their original request are also
    filtered. Filtering error rows does not prevent the remaining rows from
    being processed. This stage copies the completion record's non-null
    ``reward`` to every selected row and assigns no semantic meaning to message
    contents.
    """

    name = "update_is_trainable"
    version = "6"
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
            record
            for record in session
            if _is_eligible_trainability_record(record, enabled=CLAUDE_FILTER_ENABLED)
        )
        if context.trainability_policy is TrainabilityPolicy.DOWNGRADE:
            max_step_record = max(eligible_records, key=_step_sort_key, default=None)
            trainable_ids = (
                {_record_id(max_step_record)}
                if max_step_record is not None
                else set()
            )
        else:
            trainable_ids = _detect_trainable_record_ids(eligible_records)
        completed_record = next(
            record
            for record in session
            if record.get("is_session_completed") is True
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


class _MessagePrefixTrie:
    """Trie of canonical message hashes with record IDs at input boundaries."""

    def __init__(self) -> None:
        self.root = _TrieNode()

    def longest_eligible_strict_prefix_terminal(
        self,
        fingerprints: Sequence[str],
        eligible_record_ids: set[str],
    ) -> str | None:
        """Find the longest active tail strictly extended by this record."""

        node = self.root
        matched_record_id = (
            _latest_eligible_terminal(node, eligible_record_ids)
            if fingerprints
            else None
        )
        for index, fingerprint in enumerate(fingerprints):
            child = node.children.get(fingerprint)
            if child is None:
                break
            node = child
            if index == len(fingerprints) - 1:
                break
            candidate = _latest_eligible_terminal(node, eligible_record_ids)
            if candidate is not None:
                matched_record_id = candidate
        return matched_record_id

    def latest_eligible_strict_descendant(
        self,
        fingerprints: Sequence[str],
        eligible_record_ids: set[str],
        record_positions: Mapping[str, int],
    ) -> str | None:
        """Find the latest active tail that strictly extends ``fingerprints``."""

        node = self.root
        for fingerprint in fingerprints:
            node = node.children.get(fingerprint)
            if node is None:
                return None

        candidates: list[str] = []
        pending = list(node.children.values())
        while pending:
            descendant = pending.pop()
            candidates.extend(
                record_id
                for record_id in descendant.terminal_record_ids
                if record_id in eligible_record_ids
            )
            pending.extend(descendant.children.values())
        return max(
            candidates,
            key=record_positions.__getitem__,
            default=None,
        )

    def insert(self, fingerprints: Sequence[str], record_id: str) -> None:
        node = self.root
        for fingerprint in fingerprints:
            node = node.children.setdefault(fingerprint, _TrieNode())
        node.terminal_record_ids.append(record_id)


@dataclass
class _Chain:
    record_ids: list[str] = field(default_factory=list)


def _detect_trainable_record_ids(
    session: Sequence[Record],
    *,
    diagnostics: dict[str, dict[str, Any]] | None = None,
) -> set[str]:
    """Select chain tails; optionally collect structural evidence for offline tests."""
    ordered = sorted(session, key=_step_sort_key)
    trie = _MessagePrefixTrie()
    chains: list[_Chain] = []
    latest_record_to_chain: dict[str, int] = {}
    record_positions: dict[str, int] = {}
    diagnostic_fingerprints: dict[str, list[str]] = {}

    for position, record in enumerate(ordered):
        record_id = _record_id(record)
        record_positions[record_id] = position
        messages = _decode_messages(record.get("messages"), record_id)
        fingerprints = [_message_fingerprint(message) for message in messages]
        active_record_ids = set(latest_record_to_chain)
        matched_record_id = trie.latest_eligible_strict_descendant(
            fingerprints,
            active_record_ids,
            record_positions,
        )
        relation = "strict_prefix_reset"
        if matched_record_id is None:
            matched_record_id = trie.longest_eligible_strict_prefix_terminal(
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
                    for candidate in latest_record_to_chain
                    if diagnostic_fingerprints[candidate] == fingerprints
                ]
                if position:
                    previous = ordered[position - 1]
                    previous_id = _record_id(previous)
                    previous_hashes = diagnostic_fingerprints[previous_id]
                    common_length = 0
                    for left, right in zip(previous_hashes, fingerprints):
                        if left != right:
                            break
                        common_length += 1
                    previous_messages = _decode_messages(previous.get("messages"), previous_id)
                    evidence["previous_eligible_record_comparison"] = {
                        "record_id": previous_id,
                        "common_prefix_message_count": common_length,
                        "previous_message_count": len(previous_hashes),
                        "system_messages_equal": [
                            _message_fingerprint(message)
                            for message in previous_messages
                            if isinstance(message, Mapping) and message.get("role") == "system"
                        ] == [
                            _message_fingerprint(message)
                            for message in messages
                            if isinstance(message, Mapping) and message.get("role") == "system"
                        ],
                    }
            diagnostics[record_id] = evidence
            diagnostic_fingerprints[record_id] = fingerprints

        if matched_record_id is not None:
            chain_index = latest_record_to_chain[matched_record_id]
            chain = chains[chain_index]
            latest_record_to_chain.pop(chain.record_ids[-1], None)
        else:
            chain_index = len(chains)
            chain = _Chain()
            chains.append(chain)

        chain.record_ids.append(record_id)
        latest_record_to_chain[record_id] = chain_index
        trie.insert(fingerprints, record_id)

    if diagnostics is not None:
        for chain_index, chain in enumerate(chains):
            excluded = _is_short_side_chain(chain, len(chains))
            for record_id in chain.record_ids:
                is_tail = record_id == chain.record_ids[-1]
                diagnostics[record_id].update({
                    "chain_index": chain_index,
                    "chain_count": len(chains),
                    "chain_length": len(chain.record_ids),
                    "chain_root_record_id": chain.record_ids[0],
                    "chain_tail_record_id": chain.record_ids[-1],
                    "reason_code": (
                        "short_side_chain" if excluded
                        else "selected_chain_tail" if is_tail
                        else "superseded_in_chain"
                    ),
                })

    return _select_trainable_record_ids(chains)


def _select_trainable_record_ids(chains: Sequence[_Chain]) -> set[str]:
    """Select tails from non-short-side chains.

    Every append-only chain tail contains the complete messages of one
    structurally separated trajectory. In a multi-chain session, short side
    branches are subagent-like and carry no trainable trajectory.
    """

    chain_count = len(chains)
    return {
        chain.record_ids[-1]
        for chain in chains
        if not _is_short_side_chain(chain, chain_count)
    }


def _is_short_side_chain(
    chain: _Chain,
    chain_count: int,
) -> bool:
    """Return whether a multi-chain side branch is shorter than the threshold."""

    return (
        chain_count > 1
        and len(chain.record_ids) < MIN_SIDE_CHAIN_LENGTH
    )


def _has_non_200_status_code(record: Record) -> bool:
    """Return whether one row records an explicitly non-200 gateway result."""

    metadata = _decode_json_object(record.get("meta_json"))
    if metadata is None:
        return False

    metadata_objects = [metadata]
    for key in ("env_state", "telemetry"):
        nested = _decode_json_object(metadata.get(key))
        if nested is not None:
            metadata_objects.append(nested)

    for item in metadata_objects:
        if "status_code" in item and not _is_status_code_200(
            item["status_code"]
        ):
            return True
    return False


def _is_incomplete_stream_request(record: Record) -> bool:
    metadata = _decode_json_object(record.get("meta_json"))
    if metadata is None:
        return False
    request = _decode_json_object(metadata.get("request"))
    stream = request.get("stream") if request is not None else metadata.get("stream")
    if stream is not True:
        return False
    return metadata.get("finish_reason") in {None, "null"}


def _is_eligible_trainability_record(record: Record, *, enabled: bool = True) -> bool:
    return (
        not _has_non_200_status_code(record)
        and (
            not enabled
            or not _is_incomplete_stream_request(record)
        )
    )


def _has_consistent_request_messages(record: Record) -> bool:
    metadata = _decode_json_object(record.get("meta_json"))
    request = _decode_json_object(metadata.get("request")) if metadata else None
    if request is None or not isinstance(request.get("messages"), list):
        return True
    record_id = _record_id(record)
    if not all(isinstance(message, Mapping) for message in request["messages"]):
        return False
    messages = _decode_messages(record.get("messages"), record_id)
    return _message_fingerprints(messages) == _message_fingerprints(request["messages"])


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
        step_id = _step_sort_key(record)
        max_step_id = (
            step_id if max_step_id is None else max(max_step_id, step_id)
        )
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


def _message_fingerprint(message: Any) -> str:
    canonical = json.dumps(
        _normalize_message_for_prefix_matching(message),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _message_fingerprints(messages: Sequence[Any]) -> list[str]:
    return [_message_fingerprint(message) for message in messages]


def _normalize_message_for_prefix_matching(message: Any) -> Any:
    """Canonicalize equivalent user text shapes without mutating the input."""

    if not isinstance(message, Mapping) or message.get("role") != "user":
        return message
    content = message.get("content")
    if not isinstance(content, str):
        return message
    normalized = dict(message)
    normalized["content"] = [{"type": "text", "text": content}]
    return normalized


def _latest_eligible_terminal(
    node: _TrieNode,
    eligible_record_ids: set[str],
) -> str | None:
    return next(
        (
            record_id
            for record_id in reversed(node.terminal_record_ids)
            if record_id in eligible_record_ids
        ),
        None,
    )


def _record_id(record: Record) -> str:
    value = record.get("id")
    if not isinstance(value, str) or not value.strip():
        raise StageTransformError(f"record has invalid id: {value!r}")
    return value.strip()


def _step_sort_key(record: Record) -> int:
    value = record.get("step_id")
    if isinstance(value, bool) or not isinstance(value, int):
        record_id = _record_id(record)
        raise StageTransformError(
            f"record {record_id!r} has invalid step_id: {value!r}",
            record_id=record_id,
        )
    return value
