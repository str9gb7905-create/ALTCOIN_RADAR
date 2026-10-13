"""Drain notification parts, persisting each claim before any external send."""

from __future__ import annotations

import os
import subprocess
import tempfile
import time
from pathlib import Path

from notification_delivery import NotificationDeliveryLedger
from notification_dispatcher import TelegramDispatcher
from radar_scan import PROJECT_ROOT


def drain_queue(ledger, dispatcher, persist, *, pause=time.sleep, max_messages=256):
    """Never retry the same part twice in one run; preserve unknown outcomes."""
    attempted = set()
    failures = 0
    for _ in range(max_messages):
        record = ledger.claim_next(exclude_ids=attempted)
        # Includes recovery of an abandoned SENDING record when no send is due.
        persist("claim")
        if record is None:
            return {"claimed": bool(attempted), "sent": len(attempted),
                    "status": "SUCCESS" if failures == 0 else "FAILED"}
        notification_id = record["notification_id"]
        attempted.add(notification_id)
        result = ledger.dispatch_claimed(notification_id, dispatcher)
        persist("result")
        print(f"DELIVERY_{result['delivery_status']} notification_id={notification_id}")
        if result["delivery_status"] != "SUCCESS":
            failures += 1
        pause(1.1)
    return {"claimed": bool(attempted), "sent": len(attempted), "status": "FAILED"}


def persist_to_github(phase):
    """Use the existing state exporter and fixed state branch, without a shell."""
    with tempfile.TemporaryDirectory(prefix="radar-delivery-", dir=os.environ["RUNNER_TEMP"]) as root:
        output = Path(root) / "status.txt"
        subprocess.run([
            "bash", str(PROJECT_ROOT / ".github/scripts/persist_authoritative_state.sh"),
            str(Path(root) / "worktree"), str(output),
        ], check=True, cwd=PROJECT_ROOT)
        statuses = [line.partition("=")[2] for line in output.read_text().splitlines()
                    if line.startswith("status=")]
        if not statuses or statuses[-1] not in {"SUCCESS", "NO_CHANGE"}:
            raise RuntimeError("state persistence failed")


def main():
    result = {"claimed": False, "sent": 0, "status": "FAILED"}
    persistence = "FAILED"
    try:
        result = drain_queue(NotificationDeliveryLedger(), TelegramDispatcher.from_env(),
                             persist_to_github)
        persistence = "SUCCESS"
    except Exception:
        # Do not print arbitrary exception text while credentials are present.
        print("DELIVERY_BATCH_FAILED_OR_UNKNOWN")
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with open(output, "a", encoding="utf-8") as handle:
            handle.write(f"claimed={str(result['claimed']).lower()}\n")
            handle.write(f"status={result['status']}\n")
            handle.write(f"persistence={persistence}\n")
            handle.write(f"parts_attempted={result['sent']}\n")
    print(f"DELIVERY_BATCH_{result['status']} parts_attempted={result['sent']}")
    return 0 if result["status"] == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
