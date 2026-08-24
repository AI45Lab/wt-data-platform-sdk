"""Replay Claude encrypted reasoning into landing messages."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from ..exceptions import StageTransformError
from ..stage import ETLStage, Session, SessionPatch, StageContext

_ANT_THINKING_PATTERN = re.compile(
    r"<antThinking>(?P<content>.*?)</antThinking>", re.DOTALL | re.IGNORECASE
)
_COT_TOKENS_PATTERN = re.compile(r"<!--\s*cot_tokens\s*:\s*\d+\s*-->", re.IGNORECASE)


class FreeCotStage(ETLStage):
    """Decode each unique Claude ``encrypted_content`` in a session."""

    name = "freecot"
    version = "1"
    required_fields = (
        "agent_model",
        "is_trainable",
        "is_session_completed",
        "messages",
        "session_id",
        "step_id",
    )
    output_fields = ("messages",)
    dependencies = ("update_is_trainable",)
    job_discovery_filter = "is_session_completed = true"

    def __init__(
        self,
        replay_client: Any | None = None,
        *,
        env_path: Path | None = None,
        max_tokens: int = 128000,
        timeout_seconds: float = 300.0,
        max_attempts: int = 3,
    ) -> None:
        if timeout_seconds <= 0 or max_attempts <= 0:
            raise ValueError("timeout_seconds and max_attempts must be positive")
        self._replay_client = replay_client
        self._env_path = env_path
        self._max_tokens = max_tokens
        self._timeout_seconds = timeout_seconds
        self._max_attempts = max_attempts
        self._secrets: tuple[str, ...] = ()

    def transform_session(self, session: Session, context: StageContext) -> SessionPatch:
        del context
        if not any(record.get("is_session_completed") is True for record in session):
            return {}
        if not any(record.get("is_trainable") is True for record in session):
            return {}
        decoded_by_signature: dict[str, str] = {}
        model_by_signature: dict[str, str] = {}

        for record in session:
            if "claude" not in str(record.get("agent_model") or "").lower():
                continue
            record_id = str(record["id"])
            try:
                messages = _messages(record.get("messages"))
            except ValueError as exc:
                raise StageTransformError(
                    self._safe_message(exc), record_id=record_id
                ) from exc
            model = str(
                record.get("agent_model") or "claude-opus-4-6"
            ).removesuffix("-thinking")
            for message in messages:
                signature = message.get("encrypted_content")
                if (
                    not isinstance(signature, str)
                    or not signature
                    or message.get("reasoning_content") is not None
                    or signature in decoded_by_signature
                ):
                    continue
                model_by_signature.setdefault(signature, model)
                try:
                    decoded = self._client().extract(
                        signature, model_by_signature[signature], self._max_tokens
                    )
                    decoded = _reasoning_content(decoded)
                    if not decoded:
                        raise ValueError("replay service returned empty reasoning")
                except Exception as exc:
                    raise StageTransformError(
                        f"FreeCoT replay failed: {self._safe_message(exc)}",
                        record_id=record_id,
                    ) from exc
                decoded_by_signature[signature] = decoded

        if not decoded_by_signature:
            return {}

        patches: SessionPatch = {}
        for record in session:
            if "claude" not in str(record.get("agent_model") or "").lower():
                continue
            record_id = str(record["id"])
            try:
                messages = _messages(record.get("messages"))
            except ValueError as exc:
                raise StageTransformError(
                    self._safe_message(exc), record_id=record_id
                ) from exc
            changed = False
            for message in messages:
                signature = message.get("encrypted_content")
                if (
                    not isinstance(signature, str)
                    or not signature
                    or message.get("reasoning_content") is not None
                ):
                    continue
                decoded = decoded_by_signature.get(signature)
                if decoded is None:
                    continue
                message["reasoning_content"] = decoded
                changed = True
            if changed:
                patches[record_id] = {"messages": _json_string(messages)}
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
            timeout_seconds=self._timeout_seconds,
            max_attempts=self._max_attempts,
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


def _reasoning_content(value: object) -> str:
    if not isinstance(value, str):
        return ""
    matched = _ANT_THINKING_PATTERN.search(value)
    content = matched.group("content") if matched is not None else value
    return _COT_TOKENS_PATTERN.sub("", content).strip()


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
