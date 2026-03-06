# tools/watch_snapshot.py
from __future__ import annotations

import time
from pathlib import Path

from tools.project_snapshot import iter_files, should_skip, main as build_snapshot

POLL_SECONDS = 1.0


def get_state(root: Path) -> dict[str, float]:
    state: dict[str, float] = {}
    for p in root.rglob("*"):
        if p.is_dir():
            continue
        if should_skip(p):
            continue
        try:
            state[p.as_posix()] = p.stat().st_mtime
        except Exception:
            pass
    return state


def watch():
    root = Path(".").resolve()
    print("👀 Watching for changes… (Ctrl+C to stop)")
    last = get_state(root)

    # build once at start
    build_snapshot()

    while True:
        time.sleep(POLL_SECONDS)
        cur = get_state(root)
        if cur != last:
            print("🔁 Change detected → regenerating snapshot…")
            build_snapshot()
            last = cur


if __name__ == "__main__":
    watch()
