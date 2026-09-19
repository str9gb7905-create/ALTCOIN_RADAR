"""Replaceable persistent-state storage for ALTCOIN_RADAR."""

from __future__ import annotations

import json
import os
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any


class StateStore(ABC):
    """Minimal interface required by the cloud-readiness orchestration layer."""

    @abstractmethod
    def load(self, path: Path, default: Any = None) -> Any:
        """Load a JSON value, returning default only when the file is absent."""

    @abstractmethod
    def save_atomic(self, path: Path, payload: Any) -> None:
        """Atomically replace a JSON document."""


class FileSystemStateStore(StateStore):
    """Current local-filesystem implementation of StateStore."""

    def __init__(self, root: Path | None = None):
        self.root = root.resolve() if root is not None else None

    def _resolve(self, path: Path) -> Path:
        value = Path(path)
        return self.root / value if self.root is not None and not value.is_absolute() else value

    def load(self, path: Path, default: Any = None) -> Any:
        target = self._resolve(path)
        try:
            with target.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except FileNotFoundError:
            return default

    def save_atomic(self, path: Path, payload: Any) -> None:
        target = self._resolve(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_name(f".{target.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
        try:
            with temporary.open("w", encoding="utf-8", newline="\n") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, target)
        finally:
            if temporary.exists():
                temporary.unlink()
