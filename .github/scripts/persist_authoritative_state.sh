#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: persist_authoritative_state.sh WORKTREE_PATH GITHUB_OUTPUT" >&2
  exit 2
fi

state_worktree="$1"
github_output="$2"
echo "status=STATE_PERSISTENCE_FAILED" >> "$github_output"

git fetch --no-tags origin radar-state:refs/remotes/origin/radar-state
git worktree add --detach "$state_worktree" origin/radar-state
cleanup() {
  git worktree remove --force "$state_worktree" >/dev/null 2>&1 || true
}
trap cleanup EXIT

python cloud_state_sync.py export --destination "$state_worktree"
cd "$state_worktree"
git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git add -f state/*.json

if git diff --cached --quiet; then
  persistence_status="NO_CHANGE"
else
  run_id=$(python -c 'import json; print(json.load(open("state/run_status.json", encoding="utf-8"))["run_id"])')
  git commit -m "radar state: $run_id"
  git push origin HEAD:radar-state
  persistence_status="SUCCESS"
fi

echo "status=$persistence_status" >> "$github_output"
echo "STATE_PERSISTENCE_$persistence_status"
