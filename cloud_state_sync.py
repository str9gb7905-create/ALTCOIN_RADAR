"""Restore and export authoritative runtime state for the cloud workflow."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from radar_scan import PROJECT_ROOT
from state_store import FileSystemStateStore


MANIFEST_PATH = PROJECT_ROOT / "cloud_state_manifest.json"


def load_manifest(path: Path = MANIFEST_PATH) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    if not isinstance(manifest.get("files"), list):
        raise ValueError("cloud state manifest has no files list")
    return manifest


def durable_state_paths(path: Path = MANIFEST_PATH) -> list[Path]:
    """Return every durable JSON state file; transient locks are never included."""
    manifest = load_manifest(path)
    result = []
    for item in manifest["files"]:
        filename = item.get("filename")
        if not isinstance(filename, str):
            raise ValueError("cloud state manifest contains an invalid filename")
        candidate = Path(filename)
        if candidate.parts[:1] == ("state",) and candidate.suffix == ".json":
            result.append(candidate)
    required = {
        Path("state/radar_state.json"),
        Path("state/technical_state.json"),
        Path("state/divergence_state.json"),
        Path("state/heartbeat.json"),
        Path("state/run_status.json"),
        Path("state/notification_delivery.json"),
    }
    if not required.issubset(result):
        missing = sorted(str(item) for item in required - set(result))
        raise ValueError(f"manifest is missing durable state: {missing}")
    return sorted(set(result), key=str)


def _validated_json(raw: bytes, source: str) -> Any:
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"invalid JSON state from {source}") from exc


def restore_from_git(ref: str, destination: Path = PROJECT_ROOT) -> str | None:
    store = FileSystemStateStore(destination)
    previous_run_id = None
    for relative in durable_state_paths():
        source = f"{ref}:{relative.as_posix()}"
        completed = subprocess.run(
            ["git", "show", source],
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
        )
        if completed.returncode != 0:
            raise RuntimeError(f"required durable state is unavailable: {source}")
        payload = _validated_json(completed.stdout, source)
        store.save_atomic(relative, payload)
        if relative == Path("state/run_status.json") and isinstance(payload, dict):
            previous_run_id = payload.get("run_id")
    print(
        "STATE_RESTORE_SUCCESS"
        + (f" previous_run_id={previous_run_id}" if previous_run_id else "")
    )
    return previous_run_id


def export_to_directory(destination: Path, source: Path = PROJECT_ROOT) -> list[Path]:
    source_store = FileSystemStateStore(source)
    destination_store = FileSystemStateStore(destination)
    exported = []
    missing = object()
    for relative in durable_state_paths():
        payload = source_store.load(relative, missing)
        if payload is missing:
            raise RuntimeError(f"required durable state is missing: {relative}")
        destination_store.save_atomic(relative, payload)
        exported.append(relative)
    print(f"STATE_EXPORT_SUCCESS files={len(exported)}")
    return exported


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    restore = subparsers.add_parser("restore")
    restore.add_argument("--ref", required=True)

    export = subparsers.add_parser("export")
    export.add_argument("--destination", type=Path, required=True)

    args = parser.parse_args()
    if args.command == "restore":
        restore_from_git(args.ref)
    else:
        export_to_directory(args.destination)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"STATE_SYNC_FAILED: {exc}", file=sys.stderr)
        raise SystemExit(1)
