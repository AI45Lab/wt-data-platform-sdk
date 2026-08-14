"""Mark structurally selected trajectory rows as trainable."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Sequence

from ..exceptions import StageTransformError
from ..stage import ETLStage, Record, Session, SessionPatch, StageContext


class UpdateIsTrainableStage(ETLStage):
    """Mark the tail row of every append-only chain in a completed session.

    Inputs are grouped into append-only chains with exact message-prefix
    matching. Each chain tail contains the complete messages of one
    structurally separated trajectory. A later strict prefix of an active chain
    tail is treated as a retry reset, so the abandoned longer row is not a
    trainable tail. Identical message snapshots are separate occurrences rather
    than append operations. In a multi-chain session, every independent
    one-record chain is treated as a one-step subagent and is not trainable.
    Rows with an explicitly non-200 gateway status are excluded from chain
    detection without preventing the remaining rows from being processed. This
    stage copies the final session row's non-null ``reward`` to every trainable
    chain tail and assigns no semantic meaning to message contents.
    """

    name = "update_is_trainable"
    version = "1"
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
        del context
        if not _is_completed_session(session):
            return {}

        eligible_records = tuple(
            record for record in session if not _has_non_200_status_code(record)
        )
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


def _detect_trainable_record_ids(session: Sequence[Record]) -> set[str]:
    ordered = sorted(session, key=_step_sort_key)
    trie = _MessagePrefixTrie()
    chains: list[_Chain] = []
    latest_record_to_chain: dict[str, int] = {}
    record_positions: dict[str, int] = {}

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
        if matched_record_id is None:
            matched_record_id = trie.longest_eligible_strict_prefix_terminal(
                fingerprints,
                active_record_ids,
            )

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

    # Every append-only chain tail contains the complete messages of one
    # structurally separated trajectory. In a multi-chain session, a singleton
    # side chain is a one-step subagent and carries no trainable trajectory.
    return {
        chain.record_ids[-1]
        for chain in chains
        if not _is_single_record_side_chain(chain, len(chains))
    }


def _is_single_record_side_chain(
    chain: _Chain,
    chain_count: int,
) -> bool:
    return chain_count > 1 and len(chain.record_ids) == 1


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


def _is_completed_session(session: Sequence[Record]) -> bool:
    if not session:
        raise StageTransformError("session must contain at least one row")

    completed_step_id: int | None = None
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

    if completed_step_id is None:
        return False
    return completed_step_id == max_step_id


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
        message,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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
