#!/usr/bin/env python3
# tools/pfc_generate.py
# ─────────────────────────────────────────────────────────────────────────────
# Perfect Fit Challenge — Weekly Challenge Generator
#
# Usage (run from your project root):
#   python tools/pfc_generate.py
#
# Output:
#   data/pfc_challenge.json   ← the bot reads this when you run /pfc-create
#
# REQUIRES: pip install beautifulsoup4
# ─────────────────────────────────────────────────────────────────────────────

import json
import os
import sys
from collections import defaultdict
from datetime import date

# ── Path resolution ───────────────────────────────────────────────────────────
# Works correctly regardless of which directory you run the script from.
# SCRIPT_DIR = the tools/ folder
# ROOT_DIR   = the project root (where bot.py and the data/ folder live)

SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR    = os.path.dirname(SCRIPT_DIR)
POOL_FILE   = os.path.join(SCRIPT_DIR, "pfc_players.json")
OUTPUT_FILE = os.path.join(ROOT_DIR, "data", "pfc_challenge.json")

# Make sure tools/ is importable
sys.path.insert(0, SCRIPT_DIR)
from pfc_categories import CATEGORIES, build_h2h_categories, build_season_categories


# ── Helpers ───────────────────────────────────────────────────────────────────

def clear():
    os.system("cls" if os.name == "nt" else "clear")

def hr(char="═", width=64):
    print(char * width)

def prompt_number(msg: str) -> int | float:
    while True:
        raw = input(msg).strip()
        try:
            return float(raw) if "." in raw else int(raw)
        except ValueError:
            print("  ⚠  Enter a valid number.")


# ── Category menu ─────────────────────────────────────────────────────────────

def _group_categories(cats: list[dict]) -> dict[str, list[dict]]:
    groups = defaultdict(list)
    for cat in cats:
        groups[cat["group"]].append(cat)
    return dict(groups)


def pick_categories(all_cats: list[dict]) -> list[dict]:
    chosen:     list[dict]   = []
    chosen_ids: set[str]     = set()

    while len(chosen) < 8:
        clear()
        hr()
        print(f"  PERFECT FIT CHALLENGE — PICK CATEGORIES  ({len(chosen)}/8 chosen)")
        hr()

        if chosen:
            print("\n  ✅ Chosen so far:")
            for i, c in enumerate(chosen, 1):
                print(f"     {i}. {c['display_name']}")
            print()

        remaining   = [c for c in all_cats if c["id"] not in chosen_ids]
        groups      = _group_categories(remaining)
        group_names = sorted(groups.keys())

        print("  GROUPS\n")
        for gi, gname in enumerate(group_names, 1):
            print(f"  {gi:>3}.  {gname}  ({len(groups[gname])} categories)")

        print()
        print("  H  — Add H2H categories (vs a reference player)")
        print("  S  — Add Season categories (for a specific year)")
        print("  R  — Remove a chosen category")
        if len(chosen) == 8:
            print("  D  — Done ✓")
        print()

        cmd = input("  > ").strip().lower()

        if cmd == "d" and len(chosen) == 8:
            break

        if cmd == "r":
            if not chosen:
                input("  Nothing chosen. Press Enter...")
                continue
            raw = input(f"  Remove which? (1–{len(chosen)}): ").strip()
            try:
                idx = int(raw) - 1
                if 0 <= idx < len(chosen):
                    removed = chosen.pop(idx)
                    chosen_ids.discard(removed["id"])
            except ValueError:
                pass
            continue

        if cmd == "h":
            ref_name = input("  Reference player name (e.g. Roger Federer): ").strip()
            ref_slug = input("  Their Wikipedia slug (e.g. Roger_Federer): ").strip()
            new_cats = build_h2h_categories(ref_name, ref_slug)
            existing = {x["id"] for x in all_cats}
            added    = [c for c in new_cats if c["id"] not in existing]
            all_cats.extend(added)
            print(f"  ✅ Added {len(added)} H2H categories for {ref_name}. Browse 'Head to Head' group.")
            input("  Press Enter...")
            continue

        if cmd == "s":
            raw = input("  Year (e.g. 2019): ").strip()
            try:
                year = int(raw)
            except ValueError:
                input("  Invalid year. Press Enter...")
                continue
            new_cats = build_season_categories(year)
            existing = {x["id"] for x in all_cats}
            added    = [c for c in new_cats if c["id"] not in existing]
            all_cats.extend(added)
            print(f"  ✅ Added {len(added)} categories for {year} season.")
            input("  Press Enter...")
            continue

        # Browse a group
        try:
            gi = int(cmd) - 1
            if not (0 <= gi < len(group_names)):
                raise ValueError
        except ValueError:
            input("  ⚠  Invalid input. Press Enter...")
            continue

        chosen_group = group_names[gi]
        group_cats   = groups[chosen_group]

        clear()
        hr()
        print(f"  {chosen_group.upper()}  ({len(group_cats)} categories)")
        hr()
        for ci, cat in enumerate(group_cats, 1):
            print(f"  {ci:>4}.  {cat['display_name']}")
        print(f"\n  Slots remaining: {8 - len(chosen)}")
        print("  Type numbers to add (space-separated), or Enter to go back.\n")

        raw = input("  > ").strip()
        if not raw:
            continue

        for p in raw.replace(",", " ").split():
            try:
                cat = group_cats[int(p) - 1]
            except (ValueError, IndexError):
                continue
            if len(chosen) >= 8:
                print("  ⚠  Already have 8. Remove one first (R).")
                break
            if cat["id"] in chosen_ids:
                print(f"  ⚠  '{cat['display_name']}' already chosen.")
                continue
            chosen.append(cat)
            chosen_ids.add(cat["id"])
            print(f"  ✅ Added: {cat['display_name']}")

        input("  Press Enter to continue...")

    return chosen


# ── Player picker ─────────────────────────────────────────────────────────────

def load_players() -> list[dict]:
    if not os.path.exists(POOL_FILE):
        print(f"  ❌ Player pool not found at: {POOL_FILE}")
        sys.exit(1)
    with open(POOL_FILE, "r", encoding="utf-8") as f:
        return json.load(f)["players"]


def pick_players(all_players: list[dict]) -> list[dict]:
    clear()
    hr()
    print("  PLAYER POOL")
    hr()
    for i, p in enumerate(all_players, 1):
        print(f"  {i:>4}.  {p['name']:<35}  [{p.get('tour','?')}]")
    print(f"\n  {len(all_players)} players total.")
    print("  A — Use ALL  |  or enter numbers separated by spaces\n")

    while True:
        cmd = input("  > ").strip().lower()
        if cmd == "a":
            return all_players
        parts = cmd.replace(",", " ").split()
        try:
            picks = [int(p) for p in parts]
        except ValueError:
            print("  ⚠  Enter numbers or A.")
            continue
        if any(p < 1 or p > len(all_players) for p in picks):
            print(f"  ⚠  Numbers must be 1–{len(all_players)}.")
            continue
        if len(set(picks)) != len(picks):
            print("  ⚠  No duplicates.")
            continue
        return [all_players[p - 1] for p in picks]


# ── Stat fetcher ──────────────────────────────────────────────────────────────

def fetch_all_stats(players: list[dict], categories: list[dict]) -> list[dict]:
    results = []
    total   = len(players) * len(categories)
    done    = 0

    clear()
    hr()
    print("  FETCHING STATS")
    hr()
    print(f"  {len(players)} players × {len(categories)} categories = {total} values")
    print(f"  Output file: {OUTPUT_FILE}\n")

    for player in players:
        player_stats: dict = {}
        bar = "─" * max(0, 40 - len(player["name"]))
        print(f"\n  ── {player['name']} {bar}")

        for cat in categories:
            done += 1
            prefix = f"  [{done:>{len(str(total))}}/{total}]"

            val = None
            if cat.get("source") != "manual":
                try:
                    val = cat["fetch_fn"](player)
                except Exception as e:
                    val = None

                if val is not None:
                    print(f"{prefix} ✅  {cat['display_name']}: {val}")
                    player_stats[cat["id"]] = val
                    continue
                else:
                    print(f"{prefix} ❌  {cat['display_name']} — auto-fetch failed, enter manually:")

            val = prompt_number(f"{prefix} ✏️   {cat['display_name']}: ")
            player_stats[cat["id"]] = val

        results.append({
            "name":  player["name"],
            "tour":  player.get("tour", ""),
            "stats": player_stats,
        })

    return results


# ── Save ──────────────────────────────────────────────────────────────────────

def save_challenge(categories: list[dict], players: list[dict]):
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    challenge = {
        "week":       str(date.today()),
        "categories": [{"id": c["id"], "display_name": c["display_name"]} for c in categories],
        "players":    players,
    }
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(challenge, f, indent=2, ensure_ascii=False)

    size = os.path.getsize(OUTPUT_FILE)
    print(f"\n  ✅ Saved to: {OUTPUT_FILE}  ({size:,} bytes)")
    print(f"     {len(players)} players, {len(categories)} categories")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n  Project root:  {ROOT_DIR}")
    print(f"  Output file:   {OUTPUT_FILE}\n")

    all_players = load_players()
    all_cats    = list(CATEGORIES)

    chosen_cats    = pick_categories(all_cats)

    clear()
    hr()
    print("  CONFIRMED — 8 CATEGORIES FOR THIS WEEK")
    hr()
    for i, c in enumerate(chosen_cats, 1):
        print(f"  {i}. {c['display_name']}")

    chosen_players = pick_players(all_players)
    print(f"\n  {len(chosen_players)} players selected.")
    input("  Press Enter to start fetching stats...")

    player_results = fetch_all_stats(chosen_players, chosen_cats)
    save_challenge(chosen_cats, player_results)

    print(f"\n  Done! In Discord, run: /pfc-create name:Your Challenge Name")
    print(f"  (The bot will load the file from: {OUTPUT_FILE})\n")


if __name__ == "__main__":
    main()