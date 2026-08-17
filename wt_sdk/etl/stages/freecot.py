"""Replay Claude encrypted reasoning into landing messages."""

from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from ..stage import ETLStage, Session, SessionPatch, StageContext


class FreeCotStage(ETLStage):
    """Decode each unique Claude ``encrypted_content`` in a session."""

    name = "freecot"
    version = "1"
    required_fields = ("agent_model", "messages", "meta_json", "session_id", "step_id")
    output_fields = ("messages", "meta_json")

    def __init__(
        self,
        replay_client: Any | None = None,
        *,
        env_path: Path | None = None,
        max_tokens: int = 128000,
    ) -> None:
        self._replay_client = replay_client
        self._env_path = env_path
        self._max_tokens = max_tokens
        self._secrets: tuple[str, ...] = ()

    def transform_session(self, session: Session, context: StageContext) -> SessionPatch:
        del context
        messages_by_id: dict[str, list[dict[str, Any]]] = {}
        signatures: dict[str, list[tuple[str, dict[str, Any]]]] = {}
        models: dict[str, str] = {}
        errors: dict[str, list[dict[str, str]]] = {}

        for record in session:
            record_id = str(record["id"])
            if "claude" not in str(record.get("agent_model") or "").lower():
                continue
            try:
                messages = _messages(record.get("messages"))
            except ValueError as exc:
                _add_error(errors, record_id, "invalid_messages", self._safe_message(exc))
                continue
            signed = [
                message
                for message in messages
                if isinstance(message.get("encrypted_content"), str)
                and message["encrypted_content"]
            ]
            if not signed:
                continue
            messages_by_id[record_id] = messages
            models[record_id] = str(
                record.get("agent_model") or "claude-opus-4-6"
            ).removesuffix("-thinking")
            for message in signed:
                signatures.setdefault(message["encrypted_content"], []).append(
                    (record_id, message)
                )

        for signature, targets in signatures.items():
            try:
                decoded = self._client().extract(
                    signature, models[targets[0][0]], self._max_tokens
                )
                if not isinstance(decoded, str) or not decoded.strip():
                    raise ValueError("replay service returned empty reasoning")
            except Exception as exc:
                for record_id, _ in targets:
                    _add_error(errors, record_id, "replay_failed", self._safe_message(exc))
                continue
            for _, message in targets:
                message["reasoning_content"] = decoded

        patches: SessionPatch = {}
        for record in session:
            record_id = str(record["id"])
            messages = messages_by_id.get(record_id)
            if messages is None:
                continue
            row_errors = errors.get(record_id, [])
            patches[record_id] = {
                "messages": _json_string(messages),
                "meta_json": _with_status(
                    record.get("meta_json"),
                    "partial" if row_errors else "success",
                    row_errors,
                ),
            }
        return patches

    def _client(self) -> Any:
        if self._replay_client is not None:
            return self._replay_client
        config = _environment(self._env_path)
        self._secrets = tuple(
            value
            for value in (
                config["ORIGIN_COT_SERVICE_URL"],
                config["ORIGIN_COT_WRAPPER_KEY"],
                config.get("NEW_API_KEY"),
            )
            if value
        )
        from freecot import HttpReplayClient

        self._replay_client = HttpReplayClient(
            config["ORIGIN_COT_SERVICE_URL"],
            config["ORIGIN_COT_WRAPPER_KEY"],
            upstream_api_key=config.get("NEW_API_KEY"),
        )
        return self._replay_client

    def _safe_message(self, exc: Exception) -> str:
        message = str(exc)
        for secret in self._secrets:
            message = message.replace(secret, "[redacted]")
        return message[:500] or type(exc).__name__


def _messages(value: object) -> list[dict[str, Any]]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("messages is not valid JSON") from exc
    if not isinstance(value, list) or not all(isinstance(item, Mapping) for item in value):
        raise ValueError("messages must be a JSON array of objects")
    return json.loads(_json_string(value))


def _with_status(value: object, status: str, errors: Sequence[dict[str, str]]) -> str:
    try:
        meta = _mapping(value)
    except ValueError:
        meta = {}
        if not errors:
            status = "failed"
            errors = [{"code": "invalid_meta_json", "message": "meta_json is not a JSON object"}]
    meta["freecot"] = {"status": status, "errors": list(errors)}
    return _json_string(meta)


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError as exc:
            raise ValueError("meta_json is not valid JSON") from exc
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("meta_json must be a JSON object")


def _add_error(
    errors: dict[str, list[dict[str, str]]], record_id: str, code: str, message: str
) -> None:
    errors.setdefault(record_id, []).append({"code": code, "message": message[:500]})


def _json_string(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _environment(env_path: Path | None) -> dict[str, str]:
    values = dict(os.environ)
    candidate = env_path or _nearest_dotenv()
    if candidate is not None and candidate.is_file():
        for line in candidate.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, separator, value = line.removeprefix("export ").strip().partition("=")
            if separator and key.strip() and key.strip() not in values:
                values[key.strip()] = value.strip().strip("'\"")
    required = ("ORIGIN_COT_SERVICE_URL", "ORIGIN_COT_WRAPPER_KEY")
    missing = [key for key in required if not values.get(key)]
    if missing:
        raise RuntimeError(f"missing FreeCoT configuration: {', '.join(missing)}")
    return {key: values[key] for key in (*required, "NEW_API_KEY") if values.get(key)}


def _nearest_dotenv() -> Path | None:
    for directory in (Path.cwd(), *Path.cwd().parents):
        candidate = directory / ".env"
        if candidate.is_file():
            return candidate
    return None
