"""Mark structurally selected trajectory rows as trainable."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Sequence

from ..exceptions import StageTransformError
from ..stage import ETLStage, Record, Session, SessionPatch, StageContext


class UpdateIsTrainableStage(ETLStage):
    """Mark the tail row of every append-only chain in a completed session.

    Inputs are grouped into append-only chains with exact message-prefix
    matching. Each chain tail contains the complete messages of one
    structurally separated trajectory. This stage records only
    ``is_trainable`` and assigns no semantic meaning to the topology.
    """

    name = "update_is_trainable"
    version = "1"
    required_fields = (
        "id",
        "step_id",
        "messages",
        "is_session_completed",
    )
    output_fields = ("is_trainable",)
    dependencies = ()

    def transform_session(
        self,
        session: Session,
        context: StageContext,
    ) -> SessionPatch:
        del context
        if not _is_completed_session(session):
            return {}

        trainable_ids = _detect_trainable_record_ids(session)
        patches: SessionPatch = {}
        for record in session:
            record_id = _record_id(record)
            patches[record_id] = {
                "is_trainable": record_id in trainable_ids,
            }
        return patches


@dataclass
class _TrieNode:
    children: dict[str, "_TrieNode"] = field(default_factory=dict)
    terminal_record_ids: list[str] = field(default_factory=list)


class _MessagePrefixTrie:
    """Trie of canonical message hashes with record IDs at input boundaries."""

    def __init__(self) -> None:
        self.root = _TrieNode()

    def longest_eligible_terminal(
        self,
        fingerprints: Sequence[str],
        eligible_record_ids: set[str],
    ) -> str | None:
        node = self.root
        matched_record_id = _latest_eligible_terminal(
            node, eligible_record_ids
        )
        for fingerprint in fingerprints:
            child = node.children.get(fingerprint)
            if child is None:
                break
            node = child
            candidate = _latest_eligible_terminal(node, eligible_record_ids)
            if candidate is not None:
                matched_record_id = candidate
        return matched_record_id

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

    for record in ordered:
        record_id = _record_id(record)
        messages = _decode_messages(record.get("messages"), record_id)
        fingerprints = [_message_fingerprint(message) for message in messages]
        matched_record_id = trie.longest_eligible_terminal(
            fingerprints,
            set(latest_record_to_chain),
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
    # structurally separated trajectory.
    return {chain.record_ids[-1] for chain in chains}


def _is_completed_session(session: Sequence[Record]) -> bool:
    if not session:
        raise StageTransformError("session must contain at least one row")

    completed_records: list[tuple[int, str]] = []
    for index, record in enumerate(session):
        record_id = _record_id(record)
        value = record.get("is_session_completed")
        if value is not None and not isinstance(value, bool):
            raise StageTransformError(
                "is_session_completed must be bool or null",
                record_id=record_id,
            )
        if value is True:
            completed_records.append((index, record_id))

    if not completed_records:
        return False
    completed_index, completed_record_id = completed_records[-1]
    if completed_index != len(session) - 1:
        raise StageTransformError(
            "is_session_completed must be set on the final session row",
            record_id=completed_record_id,
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
