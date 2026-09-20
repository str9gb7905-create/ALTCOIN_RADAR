"""Provider interface and Telegram implementation for outbound notifications."""

from __future__ import annotations

import json
import os
import socket
import urllib.error
import urllib.request
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, Callable

TELEGRAM_MESSAGE_LIMIT = 4096


class NotificationConfigurationError(RuntimeError):
    """Raised without including credential values."""


@dataclass(frozen=True)
class DispatchResult:
    status: str
    provider: str
    provider_message_id: str | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class NotificationDispatcher(ABC):
    @abstractmethod
    def send(self, message: str) -> DispatchResult:
        """Send one market-notification message."""

    @abstractmethod
    def send_health_alert(self, message: str) -> DispatchResult:
        """Send one health alert through the same provider."""


class TelegramDispatcher(NotificationDispatcher):
    """Telegram Bot API sender that never exposes credential values in results."""

    provider = "TELEGRAM"

    def __init__(self, bot_token: str, chat_id: str, *, timeout_seconds: float = 15.0,
                 opener: Callable[..., Any] = urllib.request.urlopen):
        if not bot_token or not chat_id:
            raise NotificationConfigurationError(
                "missing required environment variables: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID"
            )
        self._bot_token = bot_token
        self._chat_id = chat_id
        self._timeout_seconds = timeout_seconds
        self._opener = opener

    @classmethod
    def from_env(cls, **kwargs: Any) -> "TelegramDispatcher":
        token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
        chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
        missing = [name for name, value in (
            ("TELEGRAM_BOT_TOKEN", token), ("TELEGRAM_CHAT_ID", chat_id)
        ) if not value]
        if missing:
            raise NotificationConfigurationError(
                f"missing required environment variables: {', '.join(missing)}"
            )
        return cls(token, chat_id, **kwargs)

    def send(self, message: str) -> DispatchResult:
        if not message:
            return DispatchResult("FAILED_PERMANENT", self.provider, error="EMPTY_MESSAGE")
        if len(message) > TELEGRAM_MESSAGE_LIMIT:
            return DispatchResult("FAILED_PERMANENT", self.provider, error="MESSAGE_TOO_LONG")
        body = json.dumps(
            {"chat_id": self._chat_id, "text": message, "disable_web_page_preview": True}
        ).encode("utf-8")
        request = urllib.request.Request(
            f"https://api.telegram.org/bot{self._bot_token}/sendMessage",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with self._opener(request, timeout=self._timeout_seconds) as response:
                status_code = getattr(response, "status", 200)
                raw = response.read()
        except urllib.error.HTTPError as exc:
            return DispatchResult(
                "FAILED_RETRYABLE" if exc.code == 429 or exc.code >= 500 else "FAILED_PERMANENT",
                self.provider,
                error=f"HTTP_{exc.code}",
            )
        except (TimeoutError, socket.timeout):
            return DispatchResult("UNKNOWN_TIMEOUT", self.provider, error="TIMEOUT")
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, (TimeoutError, socket.timeout)):
                return DispatchResult("UNKNOWN_TIMEOUT", self.provider, error="TIMEOUT")
            return DispatchResult("UNKNOWN_TIMEOUT", self.provider, error="NETWORK_OUTCOME_UNKNOWN")
        except OSError:
            return DispatchResult("UNKNOWN_TIMEOUT", self.provider, error="NETWORK_OUTCOME_UNKNOWN")

        if status_code == 429 or status_code >= 500:
            return DispatchResult("FAILED_RETRYABLE", self.provider, error=f"HTTP_{status_code}")
        if status_code >= 400:
            return DispatchResult("FAILED_PERMANENT", self.provider, error=f"HTTP_{status_code}")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return DispatchResult("UNKNOWN_TIMEOUT", self.provider, error="INVALID_RESPONSE")
        if payload.get("ok") is not True:
            error_code = payload.get("error_code")
            retryable = error_code == 429 or (isinstance(error_code, int) and error_code >= 500)
            code = f"TELEGRAM_{error_code}" if isinstance(error_code, int) else "TELEGRAM_REJECTED"
            return DispatchResult(
                "FAILED_RETRYABLE" if retryable else "FAILED_PERMANENT",
                self.provider,
                error=code,
            )
        message_id = payload.get("result", {}).get("message_id")
        if not isinstance(message_id, int):
            return DispatchResult("UNKNOWN_TIMEOUT", self.provider, error="MISSING_MESSAGE_ID")
        return DispatchResult("SUCCESS", self.provider, provider_message_id=str(message_id))

    def send_health_alert(self, message: str) -> DispatchResult:
        return self.send(f"ALTCOIN RADAR HEALTH ALERT\n{message}")
