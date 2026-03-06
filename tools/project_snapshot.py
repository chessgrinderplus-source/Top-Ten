# tools/project_snapshot.py
# Generates a "PROJECT SNAPSHOT" text file that lists every file + its contents,
# in the same format you pasted in chat:
#
# modules/cards.py:
# <contents>
#
# data/cards.json:
# <contents>
#
# Usage:
#   python3 tools/project_snapshot.py
#   python3 tools/project_snapshot.py --include-data
#   python3 tools/project_snapshot.py --out PROJECT_SNAPSHOT.txt
#
# Notes:
# - Uses America/Toronto time in the header.
# - Skips big/irrelevant folders by default (.git, .pythonlibs, __pycache__, etc.)
# - Skips binary files automatically.

from __future__ import annotations

import argparse
import os
from datetime import datetime
import zoneinfo

TORONTO = zoneinfo.ZoneInfo("America/Toronto")

DEFAULT_EXCLUDE_DIRS = {
    ".git",
    ".github",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "__pycache__",
    "node_modules",
    ".pythonlibs",   # Replit internal
    ".venv",
    "venv",
    "env",
    "dist",
    "build",
    ".cache",
}

# If you DON'T want secrets dumped, keep these out.
DEFAULT_EXCLUDE_FILES = {
    ".env",
    ".env.local",
    ".env.production",
}

# You can add more patterns here if needed
DEFAULT_EXCLUDE_EXTS = {
    ".png", ".jpg", ".jpeg", ".gif", ".webp",
    ".mp4", ".mov", ".avi", ".mkv",
    ".pdf",
    ".zip", ".7z", ".rar",
    ".exe", ".dll", ".so",
    ".db", ".sqlite", ".sqlite3",
}

# Hard safety: don't dump files bigger than this (bytes)
MAX_FILE_BYTES = 300_000  # 300 KB per file


def is_binary_bytes(b: bytes) -> bool:
    # Heuristic: if there are many NUL bytes, it's likely binary
    if not b:
        return False
    if b"\x00" in b:
        return True
    # If a lot of bytes are non-text, treat as binary-ish
    text_chars = bytearray({7, 8, 9, 10, 12, 13, 27} | set(range(0x20, 0x100)))
    nontext = sum(ch not in text_chars for ch in b[:2000])
    return nontext > 200


def should_exclude_path(rel_path: str, include_data: bool) -> bool:
    # Normalize slashes
    rel_path = rel_path.replace("\\", "/").strip("/")

    # Exclude specific directories anywhere in the path
    parts = rel_path.split("/")
    for p in parts:
        if p in DEFAULT_EXCLUDE_DIRS:
            return True

    # Optionally exclude data/
    if not include_data and (rel_path == "data" or rel_path.startswith("data/")):
        return True

    # Exclude known sensitive files
    base = os.path.basename(rel_path)
    if base in DEFAULT_EXCLUDE_FILES:
        return True

    # Exclude by extension
    _, ext = os.path.splitext(base.lower())
    if ext in DEFAULT_EXCLUDE_EXTS:
        return True

    return False


def read_text_file(full_path: str) -> str | None:
    try:
        size = os.path.getsize(full_path)
        if size > MAX_FILE_BYTES:
            return f"[SKIPPED: file too large ({size} bytes) > {MAX_FILE_BYTES}]"

        with open(full_path, "rb") as f:
            raw = f.read()

        if is_binary_bytes(raw):
            return "[SKIPPED: binary file]"

        # decode as utf-8, fallback to replace errors
        return raw.decode("utf-8", errors="replace")
    except Exception as e:
        return f"[SKIPPED: error reading file: {repr(e)}]"


def iter_project_files(root: str, include_data: bool) -> list[str]:
    all_files: list[str] = []

    for dirpath, dirnames, filenames in os.walk(root):
        # prune excluded directories in-place (important for speed)
        rel_dir = os.path.relpath(dirpath, root).replace("\\", "/")
        if rel_dir == ".":
            rel_dir = ""

        # Filter dirnames so os.walk won't traverse them
        kept = []
        for d in dirnames:
            rel = f"{rel_dir}/{d}" if rel_dir else d
            if should_exclude_path(rel, include_data):
                continue
            kept.append(d)
        dirnames[:] = kept

        for fn in filenames:
            rel = f"{rel_dir}/{fn}" if rel_dir else fn
            if should_exclude_path(rel, include_data):
                continue
            all_files.append(rel.replace("\\", "/"))

    all_files.sort(key=lambda s: s.lower())
    return all_files


def build_snapshot_text(root: str, include_data: bool) -> str:
    now = datetime.now(TORONTO)
    ts = now.strftime("%Y-%m-%d %I:%M:%S %p %Z")

    lines: list[str] = []
    lines.append(f"PROJECT SNAPSHOT (generated {ts})")
    lines.append(f"Root: {os.path.abspath(root)}")
    lines.append("")

    rel_files = iter_project_files(root, include_data=include_data)

    for rel in rel_files:
        full = os.path.join(root, rel)
        contents = read_text_file(full)
        if contents is None:
            continue

        # Match your format: "path: <contents>"
        lines.append(f"{rel}:")
        lines.append(contents.rstrip("\n"))
        lines.append("")  # blank line between files

    return "\n".join(lines).rstrip() + "\n"


def main():
    parser = argparse.ArgumentParser(description="Generate a full project snapshot text file.")
    parser.add_argument("--root", default=".", help="Project root directory (default: .)")
    parser.add_argument("--out", default="PROJECT_SNAPSHOT.txt", help="Output file name")
    parser.add_argument("--include-data", action="store_true", help="Include data/ folder")
    args = parser.parse_args()

    root = os.path.abspath(args.root)
    snapshot = build_snapshot_text(root=root, include_data=args.include_data)

    out_path = os.path.join(root, args.out)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(snapshot)

    print(f"✅ Wrote snapshot: {out_path}")
    print("Tip: Re-run this anytime to update it.")


if __name__ == "__main__":
    main()
