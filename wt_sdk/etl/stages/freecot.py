"""Replay encrypted reasoning into landing messages for Claude and GPT models."""

from __future__ import annotations

import json
import os
import re
import time
from collections.abc import Mapping
from typing import Any
from urllib import error, request

from ..exceptions import StageTransformError
from ..stage import ETLStage, Session, SessionPatch, StageContext

_ANT_THINKING_PATTERN = re.compile(
    r"<antThinking>(?P<content>.*?)</antThinking>", re.DOTALL | re.IGNORECASE
)
_COT_TOKENS_PATTERN = re.compile(r"<!--\s*cot_tokens\s*:\s*\d+\s*-->", re.IGNORECASE)

_RETRYABLE_HTTP_CODES = {408, 422, 429}

_CLAUDE_ENDPOINT = "extract-claude-cot"
_GPT_ENDPOINT = "extract-gpt-cot"


class _ReplayError(RuntimeError):
    pass


def _provider_family(agent_model: str) -> str | None:
    """Classify the provider family from an agent model name."""

    model = (agent_model or "").lower()
    if "claude" in model:
        return "claude"
    if "gpt" in model:
        return "gpt"
    return None


def _service_base_url(url: str) -> str:
    """Normalize a service URL to a base URL ending with ``/v1/``.

    Accepts either a bare base URL (``http://host:8080/v1/``) or a full
    endpoint URL (``http://host:8080/v1/extract-claude-cot``) and returns
    the base URL with a trailing slash.
    """

    if not isinstance(url, str) or not url:
        raise ValueError("service_url must be a non-empty string")
    stripped = url.rstrip("/")
    for endpoint in (_CLAUDE_ENDPOINT, _GPT_ENDPOINT, "extract-fable-cot"):
        suffix = f"/v1/{endpoint}"
        if stripped.endswith(suffix):
            stripped = stripped[: -len(endpoint)]
            break
    if not stripped.endswith("/v1"):
        raise ValueError(
            "service_url must end with /v1/ or /v1/<endpoint>"
        )
    return stripped + "/"


class _HttpReplayClient:
    """Minimal HTTP client for the Origin-CoT extraction service.

    Supports both Claude (``extract-claude-cot`` with ``x-api-key``) and
    GPT (``extract-gpt-cot`` with ``Authorization: Bearer``) families.
    """

    def __init__(
        self,
        service_url: str,
        wrapper_key: str,
        *,
        upstream_api_key: str | None = None,
        timeout_seconds: float = 300.0,
        max_attempts: int = 3,
    ) -> None:
        if not service_url.startswith(("http://", "https://")):
            raise ValueError("service_url must be an HTTP(S) URL")
        if not wrapper_key:
            raise ValueError("wrapper_key must not be empty")
        if timeout_seconds <= 0 or max_attempts <= 0:
            raise ValueError("timeout_seconds and max_attempts must be positive")
        self._base_url = _service_base_url(service_url)
        self.wrapper_key = wrapper_key
        self.upstream_api_key = upstream_api_key
        self.timeout_seconds = timeout_seconds
        self.max_attempts = max_attempts

    def extract(
        self,
        signature: str | list[dict[str, Any]],
        model: str,
        max_tokens: int,
        *,
        family: str,
    ) -> str:
        if family == "claude":
            endpoint = _CLAUDE_ENDPOINT
            auth_header = ("x-api-key", self.wrapper_key)
        elif family == "gpt":
            endpoint = _GPT_ENDPOINT
            auth_header = ("Authorization", f"Bearer {self.wrapper_key}")
        else:
            raise ValueError(f"unsupported provider family: {family!r}")

        payload: dict[str, object] = {
            "signature": signature,
            "model": model,
            "max_tokens": max_tokens,
        }
        if self.upstream_api_key:
            payload["api_key"] = self.upstream_api_key
        body = json.dumps(payload, separators=(",", ":")).encode()
        http_request = request.Request(
            self._base_url + endpoint,
            data=body,
            headers={
                "Content-Type": "application/json",
                auth_header[0]: auth_header[1],
            },
            method="POST",
        )
        for attempt in range(1, self.max_attempts + 1):
            try:
                with request.urlopen(
                    http_request,
                    timeout=self.timeout_seconds,
                ) as response:
                    text = response.read().decode("utf-8")
            except error.HTTPError as failure:
                retryable = (
                    failure.code in _RETRYABLE_HTTP_CODES or failure.code >= 500
                )
                if retryable and attempt < self.max_attempts:
                    time.sleep(2**attempt)
                    continue
                raise _ReplayError(
                    f"replay service returned HTTP {failure.code}"
                ) from failure
            except (error.URLError, TimeoutError) as failure:
                if attempt < self.max_attempts:
                    time.sleep(2**attempt)
                    continue
                raise _ReplayError("replay service is unavailable") from failure
            if not text.strip():
                raise _ReplayError("replay service returned empty reasoning")
            return text
        raise AssertionError("unreachable replay retry state")


class FreeCotStage(ETLStage):
    """Decode each unique ``encrypted_content`` in a session.

    Supports Claude models (encrypted reasoning inside assistant messages)
    and GPT models (encrypted reasoning inside ``type: "reasoning"`` items).
    """

    name = "freecot"
    version = "1"
    required_fields = (
        "id",
        "agent_model",
        "is_trainable",
        "is_session_completed",
        "messages",
    )
    output_fields = ("messages",)
    dependencies = ("update_is_trainable",)
    job_discovery_filter = "is_session_completed = true"

    def __init__(
        self,
        replay_client: Any | None = None,
        *,
        max_tokens: int = 128000,
        timeout_seconds: float = 300.0,
        max_attempts: int = 3,
    ) -> None:
        if timeout_seconds <= 0 or max_attempts <= 0:
            raise ValueError("timeout_seconds and max_attempts must be positive")
        self._replay_client = replay_client
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
            family = _provider_family(str(record.get("agent_model") or ""))
            if family is None:
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
                wire_signature = _wire_signature(signature, message, family)
                try:
                    decoded = self._client().extract(
                        wire_signature,
                        model_by_signature[signature],
                        self._max_tokens,
                        family=family,
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
            family = _provider_family(str(record.get("agent_model") or ""))
            if family is None:
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
        service_url = os.environ.get("ORIGIN_COT_SERVICE_URL")
        wrapper_key = os.environ.get("ORIGIN_COT_WRAPPER_KEY")
        upstream_api_key = os.environ.get("NEW_API_KEY")
        missing = [
            key
            for key, value in (
                ("ORIGIN_COT_SERVICE_URL", service_url),
                ("ORIGIN_COT_WRAPPER_KEY", wrapper_key),
            )
            if not value
        ]
        if missing:
            raise RuntimeError(
                f"missing FreeCoT configuration: {', '.join(missing)}"
            )
        self._secrets = tuple(
            value
            for value in (service_url, wrapper_key, upstream_api_key)
            if value
        )
        self._replay_client = _HttpReplayClient(
            service_url,
            wrapper_key,
            upstream_api_key=upstream_api_key,
            timeout_seconds=self._timeout_seconds,
            max_attempts=self._max_attempts,
        )
        return self._replay_client

    def _safe_message(self, exc: Exception) -> str:
        message = str(exc)
        for secret in self._secrets:
            message = message.replace(secret, "[redacted]")
        return message[:500] or type(exc).__name__


def _wire_signature(
    signature: str,
    message: Mapping[str, Any],
    family: str,
) -> str | list[dict[str, Any]]:
    """Build the wire signature payload for the extraction service.

    Claude signatures are plain strings; GPT signatures are ordered arrays
    of reasoning objects.
    """

    if family == "gpt":
        return [
            {
                "type": "reasoning",
                "encrypted_content": signature,
                "summary": list(message.get("summary") or []),
                "content": list(message.get("content") or []),
            }
        ]
    return signature


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
