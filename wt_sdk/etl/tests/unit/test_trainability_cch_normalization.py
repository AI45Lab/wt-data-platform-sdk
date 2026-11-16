"""Claude Code's per-request ``cch=<value>`` must not split one trajectory.

Every Claude Code request puts a fresh ``cch`` value into the
``x-anthropic-billing-header`` line at the top of its system prompt. Without
normalization each row's system message gets a unique fingerprint, so a
session degenerates into single-record chains that are all excluded as short
side chains.
"""

import json

from wt_sdk.etl.stages.trainability import (
    _detect_trainable_record_ids,
    _normalize_volatile_harness_text,
)

_SYSTEM_BODY = "\n\nYou are a Claude agent, built on Anthropic's Claude Agent SDK."


def _billing_system(cch: str, cc_version: str = "2.1.119.ef1") -> dict:
    return {
        "role": "system",
        "content": (
            f"x-anthropic-billing-header: cc_version={cc_version}; "
            f"cc_entrypoint=sdk-cli; cch={cch};{_SYSTEM_BODY}"
        ),
    }


def _row(row_id: str, step_id: int, messages: list) -> dict:
    return {"id": row_id, "step_id": step_id, "messages": json.dumps(messages)}


def _turns(count: int) -> list[dict]:
    return [
        {"role": "user", "content": f"question {index}"}
        if index % 2 == 0
        else {"role": "assistant", "content": [{"type": "text", "text": "answer"}]}
        for index in range(count)
    ]


def test_cch_field_is_removed_from_billing_header():
    text = _billing_system("4c602")["content"]

    assert _normalize_volatile_harness_text(text) == (
        f"x-anthropic-billing-header: cc_version=2.1.119.ef1; cc_entrypoint=sdk-cli;{_SYSTEM_BODY}"
    )


def test_cch_is_removed_from_text_content_blocks():
    first = [{"type": "text", "text": _billing_system("4c602")["content"]}]
    second = [{"type": "text", "text": _billing_system("d6755")["content"]}]
    session = [
        _row("row-1", 1, [{"role": "system", "content": first}, *_turns(1)]),
        _row("row-2", 2, [{"role": "system", "content": second}, *_turns(3)]),
    ]

    assert _detect_trainable_record_ids(session) == {"row-2"}


def test_rows_differing_only_in_cch_form_one_chain():
    session = [
        _row("row-1", 1, [_billing_system("4c602"), *_turns(1)]),
        _row("row-2", 2, [_billing_system("d6755"), *_turns(3)]),
        _row("row-3", 3, [_billing_system("8df83"), *_turns(5)]),
    ]

    diagnostics: dict = {}
    trainable_ids = _detect_trainable_record_ids(session, diagnostics=diagnostics)

    assert trainable_ids == {"row-3"}
    assert {evidence["chain_count"] for evidence in diagnostics.values()} == {1}


def test_other_billing_header_differences_still_split_chains():
    session = [
        _row("row-1", 1, [_billing_system("4c602", "2.1.119.ef1"), *_turns(1)]),
        _row("row-2", 2, [_billing_system("4c602", "2.1.119.2d4"), *_turns(3)]),
    ]

    diagnostics: dict = {}
    _detect_trainable_record_ids(session, diagnostics=diagnostics)

    assert diagnostics["row-2"]["relation"] == "new_chain"


def test_cch_must_start_a_word():
    assert _normalize_volatile_harness_text("xcch=1; cch=2;") == "xcch=1;"
