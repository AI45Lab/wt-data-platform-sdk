"""Mark structurally selected trajectory rows as trainable."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any, Sequence

from ..exceptions import StageTransformError
from ..stage import ETLStage, Patch, Record, StageContext


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
        "is_trainable",
    )
    output_fields = ("is_trainable",)
    dependencies = ()

    def __init__(self) -> None:
        # PipelineDefinition invokes a stage once per row while providing the
        # same immutable StageContext.session. A one-session identity cache
        # avoids rebuilding the trie for every row without changing semantics.
        self._cached_session: object | None = None
        self._cached_trainable_ids: frozenset[str] | None = None

    def applies(self, record: Record, context: StageContext) -> bool:
        trainable_ids = self._trainable_ids(context)
        if trainable_ids is None:
            return False
        desired = _record_id(record) in trainable_ids
        return record.get("is_trainable") is not desired

    def transform(self, record: Record, context: StageContext) -> Patch:
        trainable_ids = self._trainable_ids(context)
        if trainable_ids is None:
            raise StageTransformError(
                "update_is_trainable requires a completed session"
            )
        return {"is_trainable": _record_id(record) in trainable_ids}

    def _trainable_ids(
        self,
        context: StageContext,
    ) -> frozenset[str] | None:
        session = context.session
        if self._cached_session is session:
            return self._cached_trainable_ids

        trainable_ids = (
            frozenset(_detect_trainable_record_ids(session))
            if _is_completed_session(session)
            else None
        )
        self._cached_session = session
        self._cached_trainable_ids = trainable_ids
        return trainable_ids


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

    completed_indexes: list[int] = []
    for index, record in enumerate(session):
        value = record.get("is_session_completed")
        if value is not None and not isinstance(value, bool):
            raise StageTransformError(
                "is_session_completed must be bool or null"
            )
        if value is True:
            completed_indexes.append(index)

    if not completed_indexes:
        return False
    if completed_indexes[-1] != len(session) - 1:
        raise StageTransformError(
            "is_session_completed must be set on the final session row"
        )
    return True


def _decode_messages(value: object, record_id: str) -> list[Any]:
    if not isinstance(value, str) or not value.strip():
        raise StageTransformError(
            f"messages must be a non-empty JSON string for record {record_id!r}"
        )
    try:
        messages = json.loads(value)
    except (TypeError, ValueError) as exc:
        raise StageTransformError(
            f"messages contains malformed JSON for record {record_id!r}"
        ) from exc
    if not isinstance(messages, list):
        raise StageTransformError(
            f"messages must be a JSON array for record {record_id!r}"
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
        raise StageTransformError(
            f"record {_record_id(record)!r} has invalid step_id: {value!r}"
        )
    return value
