# modules/tournaments.py
"""
Full competition tournament system for matchsim bot.
"""
from __future__ import annotations

import json, math, os, random, re, uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

import config

# ─────────────────────────────────────────────────────────────────────────────
# Storage
# ─────────────────────────────────────────────────────────────────────────────
def _data_dir() -> str:
    return str(getattr(config, "DATA_DIR", "data"))

def _load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f: return json.load(f)
    except Exception: return default

def _save_json(path, data):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.{uuid.uuid4().hex}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False); f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)

CATS_PATH     = os.path.join(_data_dir(), "comp_categories.json")
COMP_PATH     = os.path.join(_data_dir(), "comp_tournaments.json")
RANKINGS_PATH = os.path.join(_data_dir(), "comp_rankings.json")
H2H_PATH      = os.path.join(_data_dir(), "comp_h2h.json")
STATS_PATH    = os.path.join(_data_dir(), "comp_stats.json")

OWNER_ID = 1279106601931899015  # Only this user may run /history-wipe

def _cats_db():      return _load_json(CATS_PATH,     {"categories": {}})
def _cats_save(db):  _save_json(CATS_PATH, db)
def _comp_db():      return _load_json(COMP_PATH,     {"tournaments": {}})
def _comp_save(db):  _save_json(COMP_PATH, db)
def _rank_db():      return _load_json(RANKINGS_PATH, {"guilds": {}})
def _rank_save(db):  _save_json(RANKINGS_PATH, db)
def _h2h_db():       return _load_json(H2H_PATH,      {"h2h": {}})
def _h2h_save(db):   _save_json(H2H_PATH, db)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────
VALID_BRACKET_SIZES = (2, 4, 8, 16, 32, 64, 128)

BRACKET_ROUNDS: Dict[int, List[str]] = {
    2:   ["F"],
    4:   ["SF", "F"],
    8:   ["QF", "SF", "F"],
    16:  ["R16", "QF", "SF", "F"],
    32:  ["R32", "R16", "QF", "SF", "F"],
    64:  ["R64", "R32", "R16", "QF", "SF", "F"],
    128: ["R128", "R64", "R32", "R16", "QF", "SF", "F"],
}

# Maps category field names to round labels
ROUND_TO_CAT_KEY: Dict[str, str] = {
    "R128": "r128_pts", "R64": "r64_pts", "R32": "r32_pts",
    "R16":  "r16_pts",  "QF":  "quarter_pts", "SF": "semi_pts",
    "F":    "finalist_pts", "W": "champion_pts",
}

ROUND_DISPLAY: Dict[str, str] = {
    "R128": "Round of 128", "R64": "Round of 64", "R32": "Round of 32",
    "R16":  "Round of 16",  "QF":  "Quarterfinal", "SF": "Semifinal",
    "F":    "Final",        "W":   "Winner",
}

def _rnd(r: str) -> str:
    """Return full display name for a round code."""
    return ROUND_DISPLAY.get(r, r)

COURT_KEYS_ORDERED = (
    ["main_stage", "stage_2", "stage_3", "stage_4"]
    + [f"other_{i}" for i in range(1, 11)]
)
COURT_DISPLAY: Dict[str, str] = {
    "main_stage": "Main Stage Court", "stage_2": "Stage Court 2",
    "stage_3": "Stage Court 3", "stage_4": "Stage Court 4",
    **{f"other_{i}": f"Other Court {i}" for i in range(1, 11)},
}

DEFAULT_DAY_SESSION   = "11:00"
DEFAULT_NIGHT_SESSION = "19:00"
STATUS_UPCOMING  = "upcoming"
STATUS_REG       = "registration"
STATUS_ACTIVE    = "active"
STATUS_COMPLETED = "completed"

# ─────────────────────────────────────────────────────────────────────────────
# Rankings helpers
# ─────────────────────────────────────────────────────────────────────────────
def _rank_guild(db, guild_id: int) -> dict:
    return db.setdefault("guilds", {}).setdefault(str(guild_id), {})

def _player_entry(g: dict, uid: int, name: str = "") -> dict:
    return g.setdefault(str(uid), {
        "user_id": uid, "name": name, "points": 0,
        "history": [], "rankings_snapshots": [],
        "career_high_pts": 0, "career_low_pts": None,
        "career_high_rank": None, "career_low_rank": None,
    })

def get_player_points(guild_id: int, uid: int) -> int:
    db = _rank_db(); g = _rank_guild(db, guild_id)
    return int(g.get(str(uid), {}).get("points", 0))

def get_rankings_sorted(guild_id: int) -> List[dict]:
    db = _rank_db(); g = _rank_guild(db, guild_id)
    rows = list(g.values())
    rows.sort(key=lambda r: int(r.get("points", 0)), reverse=True)
    return rows

def get_player_rank(guild_id: int, uid: int) -> int:
    for i, r in enumerate(get_rankings_sorted(guild_id)):
        if int(r.get("user_id", 0)) == uid:
            return i + 1
    return 99999

def _award_points(guild_id: int, uid: int, delta: int, tourn_id: str,
                  round_label: str, name: str = "", is_defense: bool = False) -> None:
    db = _rank_db(); g = _rank_guild(db, guild_id)
    e = _player_entry(g, uid, name)
    old = int(e.get("points", 0))
    new = max(0, old + delta)
    e["points"] = new
    if name: e["name"] = name
    if new > int(e.get("career_high_pts", 0)): e["career_high_pts"] = new
    if e.get("career_low_pts") is None or new < int(e.get("career_low_pts", 999999)):
        e["career_low_pts"] = new
    e.setdefault("history", []).append({
        "tournament_id": tourn_id, "round": round_label,
        "delta": delta, "is_defense": is_defense,
        "date": datetime.now(timezone.utc).isoformat(),
    })
    g[str(uid)] = e
    _rank_save(db)

def _snapshot_rankings(guild_id: int) -> None:
    db = _rank_db(); g = _rank_guild(db, guild_id)
    ranked = sorted(g.items(), key=lambda x: int(x[1].get("points", 0)), reverse=True)
    now = datetime.now(timezone.utc).isoformat()
    for rank, (uid, entry) in enumerate(ranked, 1):
        entry.setdefault("rankings_snapshots", []).append({"date": now, "rank": rank, "points": entry.get("points", 0)})
        ch = entry.get("career_high_rank")
        cl = entry.get("career_low_rank")
        if ch is None or rank < ch: entry["career_high_rank"] = rank
        if cl is None or rank > cl: entry["career_low_rank"] = rank
    _rank_save(db)

# ─────────────────────────────────────────────────────────────────────────────
# H2H helpers
# ─────────────────────────────────────────────────────────────────────────────
def _h2h_key(a: int, b: int) -> str:
    x, y = sorted([a, b]); return f"{x}:{y}"

def record_h2h(guild_id: int, winner_id: int, loser_id: int, score: str,
               tourn_id: str, round_label: str, venue_id: Optional[str], surface: str) -> None:
    db = _h2h_db()
    g  = db.setdefault("h2h", {}).setdefault(str(guild_id), {})
    key = _h2h_key(winner_id, loser_id)
    rec = g.setdefault(key, {"player_ids": sorted([winner_id, loser_id]), "matches": []})
    rec["matches"].append({
        "winner": winner_id, "loser": loser_id, "score": score,
        "tournament_id": tourn_id, "round": round_label,
        "venue_id": venue_id, "surface": surface,
        "date": datetime.now(timezone.utc).isoformat(),
    })
    _h2h_save(db)

# ─────────────────────────────────────────────────────────────────────────────
# Stats storage helpers
# ─────────────────────────────────────────────────────────────────────────────
def _stats_db():      return _load_json(STATS_PATH, {"guilds": {}})
def _stats_save(db):  _save_json(STATS_PATH, db)

def _stats_guild(db, guild_id: int) -> dict:
    return db.setdefault("guilds", {}).setdefault(str(guild_id), {})

def _stats_player(g: dict, uid: int) -> dict:
    return g.setdefault(str(uid), {
        "user_id": uid,
        # Serve
        "aces": 0, "double_faults": 0,
        "first_serve_in": 0, "first_serve_total": 0,
        "first_serve_pts_won": 0, "first_serve_pts_total": 0,
        "second_serve_pts_won": 0, "second_serve_pts_total": 0,
        "service_games": 0, "service_games_won": 0,
        "bp_faced": 0, "bp_saved": 0,
        # Return
        "return_games": 0, "return_games_won": 0,
        "bp_opportunities": 0, "bp_converted": 0,
        "first_return_pts_won": 0, "first_return_pts_total": 0,
        "second_return_pts_won": 0, "second_return_pts_total": 0,
        # Tiebreaks
        "tiebreaks_played": 0, "tiebreaks_won": 0,
        "tb_pts_played": 0, "tb_pts_won": 0,
        # Points / Games / Sets
        "total_points_played": 0, "total_points_won": 0,
        "total_games_played": 0, "total_games_won": 0,
        "total_sets_played": 0, "total_sets_won": 0,
        "bagels_won": 0, "bagels_conceded": 0,
        "breadsticks_won": 0, "breadsticks_conceded": 0,
        "deciding_set_played": 0, "deciding_set_won": 0,
        # Aggression
        "winners": 0, "unforced_errors": 0, "forced_errors": 0,
        "net_approaches": 0, "net_pts_won": 0, "net_pts_total": 0,
        # Match record
        "matches_played": 0, "matches_won": 0, "matches_lost": 0,
        "titles": 0, "finals": 0, "semis": 0, "quarters": 0, "r16": 0,
        # Surface splits  {surface: {played, won}}
        "surface": {},
        # Round record  {round: {played, won}}
        "round_record": {},
        # vs ranking tiers — overall, per surface, per round
        "vs_top5":   {"w": 0, "l": 0},
        "vs_top10":  {"w": 0, "l": 0},
        "vs_top25":  {"w": 0, "l": 0},
        "vs_top50":  {"w": 0, "l": 0},
        "vs_top100": {"w": 0, "l": 0},
        "vs_unranked": {"w": 0, "l": 0},
        # vs ranked by surface  {surface: {top10: {w,l}, top25: {w,l}, ...}}
        "vs_ranked_surface": {},
        # vs ranked by round  {round: {top10: {w,l}, top25: {w,l}, ...}}
        "vs_ranked_round": {},
        # Full opponent rank buckets for detailed breakdown
        "vs_rank_buckets": {
            "1":     {"w": 0, "l": 0},   # vs #1
            "2-5":   {"w": 0, "l": 0},
            "6-10":  {"w": 0, "l": 0},
            "11-20": {"w": 0, "l": 0},
            "21-50": {"w": 0, "l": 0},
            "51+":   {"w": 0, "l": 0},
        },
        # Best wins (list of {opponent_rank, opponent_name, round, tournament, score, date})
        "best_wins": [],
        # Streaks
        "current_win_streak": 0, "current_loss_streak": 0,
        "best_win_streak": 0,
        # Year records  {year: {matches_played, matches_won, titles, ...}}
        "year": {},
        # Tournament bests  {tourn_id: {best_round, count}}
        "tournament_bests": {},
    })

def record_match_stats(guild_id: int, uid: int, opponent_id: int,
                        won: bool, rnd: str, surface: str,
                        tourn_id: str, opponent_rank: int,
                        stats: Optional[dict] = None) -> None:
    """Record a single match outcome + optional detailed stats for a player."""
    import datetime as _dt
    db = _stats_db(); g = _stats_guild(db, guild_id)
    p  = _stats_player(g, uid)
    year = str(_dt.datetime.now(_dt.timezone.utc).year)

    p["matches_played"] += 1
    if won:
        p["matches_won"] += 1
        p["current_win_streak"]  = p.get("current_win_streak", 0) + 1
        p["current_loss_streak"] = 0
        if p["current_win_streak"] > p.get("best_win_streak", 0):
            p["best_win_streak"] = p["current_win_streak"]
    else:
        p["matches_lost"] += 1
        p["current_loss_streak"] = p.get("current_loss_streak", 0) + 1
        p["current_win_streak"]  = 0

    # Round milestones
    rnd_map = {"F": "finals", "SF": "semis", "QF": "quarters", "R16": "r16"}
    if rnd in rnd_map and won: p[rnd_map[rnd]] = p.get(rnd_map[rnd], 0) + 1
    if rnd == "W": p["titles"] = p.get("titles", 0) + 1

    # Round record
    rr = p.setdefault("round_record", {}).setdefault(rnd, {"played": 0, "won": 0})
    rr["played"] += 1
    if won: rr["won"] += 1

    # Surface
    ss = p.setdefault("surface", {}).setdefault(surface, {"played": 0, "won": 0})
    ss["played"] += 1
    if won: ss["won"] += 1

    # vs ranking tiers (cumulative — top10 also counts in top25, top50, etc.)
    rank_tiers = [(5,"vs_top5"),(10,"vs_top10"),(25,"vs_top25"),(50,"vs_top50"),(100,"vs_top100")]
    if opponent_rank:
        for tier, key in rank_tiers:
            if opponent_rank <= tier:
                rec = p.setdefault(key, {"w": 0, "l": 0})
                if won: rec["w"] += 1
                else:   rec["l"] += 1
        if opponent_rank > 100:
            rec = p.setdefault("vs_unranked", {"w": 0, "l": 0})
            if won: rec["w"] += 1
            else:   rec["l"] += 1
    else:
        rec = p.setdefault("vs_unranked", {"w": 0, "l": 0})
        if won: rec["w"] += 1
        else:   rec["l"] += 1

    # Rank bucket breakdown
    def _bucket(r):
        if not r: return "51+"
        if r == 1: return "1"
        if r <= 5: return "2-5"
        if r <= 10: return "6-10"
        if r <= 20: return "11-20"
        if r <= 50: return "21-50"
        return "51+"
    bkt = p.setdefault("vs_rank_buckets", {}).setdefault(
        _bucket(opponent_rank), {"w": 0, "l": 0})
    if won: bkt["w"] += 1
    else:   bkt["l"] += 1

    # vs ranked by surface
    if surface and opponent_rank:
        for tier, key in rank_tiers:
            if opponent_rank <= tier:
                sr = p.setdefault("vs_ranked_surface", {}).setdefault(
                    surface, {}).setdefault(key, {"w": 0, "l": 0})
                if won: sr["w"] += 1
                else:   sr["l"] += 1

    # vs ranked by round
    if opponent_rank:
        for tier, key in rank_tiers:
            if opponent_rank <= tier:
                rr2 = p.setdefault("vs_ranked_round", {}).setdefault(
                    rnd, {}).setdefault(key, {"w": 0, "l": 0})
                if won: rr2["w"] += 1
                else:   rr2["l"] += 1

    # Best wins list (top 20, sorted by opponent rank ascending)
    if won and opponent_rank and opponent_rank <= 50:
        bw = p.setdefault("best_wins", [])
        bw.append({
            "opponent_rank": opponent_rank,
            "opponent_id": opponent_id,
            "round": rnd,
            "tournament_id": tourn_id,
            "surface": surface,
            "date": _dt.datetime.now(_dt.timezone.utc).isoformat()[:10],
        })
        bw.sort(key=lambda x: x["opponent_rank"])
        p["best_wins"] = bw[:20]  # keep top 20

    # Year record
    yr = p.setdefault("year", {}).setdefault(year, {
        "matches_played": 0, "matches_won": 0, "titles": 0,
        "finals": 0, "semis": 0, "quarters": 0,
        "aces": 0, "double_faults": 0,
        "points_earned": 0, "tournaments": [],
    })
    yr["matches_played"] += 1
    if won: yr["matches_won"] += 1
    if tourn_id not in yr["tournaments"]: yr["tournaments"].append(tourn_id)

    # Tournament best round
    tb = p.setdefault("tournament_bests", {}).setdefault(tourn_id, {"best_round": None, "record": {"w": 0, "l": 0}})
    rnd_order = ["R128","R64","R32","R16","QF","SF","F","W"]
    cur_best  = tb.get("best_round")
    if cur_best is None or rnd_order.index(rnd) > rnd_order.index(cur_best if cur_best in rnd_order else "R128"):
        tb["best_round"] = rnd
    tb["record"]["w" if won else "l"] = tb["record"].get("w" if won else "l", 0) + 1

    # Deciding set
    if stats and stats.get("deciding_set"):
        p["deciding_set_played"] = p.get("deciding_set_played", 0) + 1
        if won: p["deciding_set_won"] = p.get("deciding_set_won", 0) + 1

    # Merge numeric stat fields
    if stats:
        for field in ["aces","double_faults","first_serve_in","first_serve_total",
                      "first_serve_pts_won","first_serve_pts_total",
                      "second_serve_pts_won","second_serve_pts_total",
                      "service_games","service_games_won","bp_faced","bp_saved",
                      "return_games","return_games_won","bp_opportunities","bp_converted",
                      "first_return_pts_won","first_return_pts_total",
                      "second_return_pts_won","second_return_pts_total",
                      "tiebreaks_played","tiebreaks_won","tb_pts_played","tb_pts_won",
                      "total_points_played","total_points_won",
                      "total_games_played","total_games_won",
                      "total_sets_played","total_sets_won",
                      "bagels_won","bagels_conceded","breadsticks_won","breadsticks_conceded",
                      "winners","unforced_errors","forced_errors",
                      "net_approaches","net_pts_won","net_pts_total"]:
            if field in stats:
                p[field] = p.get(field, 0) + int(stats[field])
        # Year aces/dfs
        if "aces" in stats: yr["aces"] = yr.get("aces", 0) + int(stats["aces"])
        if "double_faults" in stats: yr["double_faults"] = yr.get("double_faults", 0) + int(stats["double_faults"])

    g[str(uid)] = p
    _stats_save(db)

# ─────────────────────────────────────────────────────────────────────────────
# Category helpers
# ─────────────────────────────────────────────────────────────────────────────
def _get_cat(cid: str) -> Optional[dict]:
    return _cats_db().get("categories", {}).get(cid)

# ─────────────────────────────────────────────────────────────────────────────
# Comp tournament helpers
# ─────────────────────────────────────────────────────────────────────────────
def _get_comp(tid: str) -> Optional[dict]:
    return _comp_db().get("tournaments", {}).get(tid)

def _save_comp(tid: str, data: dict) -> None:
    db = _comp_db(); db.setdefault("tournaments", {})[tid] = data; _comp_save(db)

def _del_comp(tid: str) -> bool:
    db = _comp_db(); t = db.get("tournaments", {})
    if tid in t: del t[tid]; _comp_save(db); return True
    return False

def _is_admin(m: discord.Member) -> bool:
    return bool(getattr(m.guild_permissions, "administrator", False))

def _parse_date(s: str) -> Optional[datetime]:
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try: return datetime.strptime(s.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError: continue
    return None

def _fmt_dt(iso: Optional[str], style: str = "F") -> str:
    """Return a Discord timestamp string <t:unix:style> or '—' if missing."""
    if not iso: return "—"
    try:
        dt = datetime.fromisoformat(iso)
        return f"<t:{int(dt.timestamp())}:{style}>"
    except: return str(iso)

def _fmt_countdown(iso: Optional[str]) -> str:
    """Return 'in X / X ago' style relative timestamp, or blank."""
    if not iso: return ""
    try:
        dt = datetime.fromisoformat(iso)
        return f"<t:{int(dt.timestamp())}:R>"
    except: return ""

def _rounds(size: int) -> List[str]:
    return list(BRACKET_ROUNDS.get(size, ["F"]))

# ─────────────────────────────────────────────────────────────────────────────
# Draw generation
# ─────────────────────────────────────────────────────────────────────────────
def _seed_of(draw: List, seeded: List[int], uid: Optional[int]) -> Optional[int]:
    if uid is None: return None
    try: return seeded.index(uid) + 1
    except ValueError: return None

def generate_draw(bracket_size: int, ranked_players: List[int], num_seeds: int) -> Tuple[List, List[int]]:
    """Returns (draw: list of uid|None, seeded_players: list of uid)."""
    n        = bracket_size
    draw     = [None] * n
    taken    : set = set()
    seeded   = ranked_players[:min(num_seeds, len(ranked_players))]
    unseeded = list(ranked_players[min(num_seeds, len(ranked_players)):])
    random.shuffle(unseeded)

    def place(pos, player): draw[pos] = player; taken.add(pos)

    if len(seeded) >= 1: place(0, seeded[0])
    if len(seeded) >= 2: place(n - 1, seeded[1])

    tier_start = 3; num_sections = 4
    while tier_start <= len(seeded):
        tier_end     = min(len(seeded), tier_start + num_sections // 2 - 1)
        tier_players = list(seeded[tier_start - 1: tier_end])
        if not tier_players: break

        section_size = n // num_sections; half = n // 2
        top_pos: List[int] = []; bottom_pos: List[int] = []
        for sec in range(num_sections):
            ss = sec * section_size; se = ss + section_size - 1
            pos = se if sec < num_sections // 2 else ss
            if pos not in taken:
                (top_pos if pos < half else bottom_pos).append(pos)

        num_each = max(1, len(tier_players) // 2)
        random.shuffle(top_pos); random.shuffle(bottom_pos); random.shuffle(tier_players)
        for p, pos in zip(tier_players[:num_each], top_pos[:num_each]): place(pos, p)
        for p, pos in zip(tier_players[num_each:], bottom_pos[:len(tier_players[num_each:])]): place(pos, p)

        tier_start += num_sections // 2; num_sections *= 2

    it = iter(unseeded)
    for i in range(n):
        if draw[i] is None:
            try: draw[i] = next(it)
            except StopIteration: break

    return draw, seeded

# ─────────────────────────────────────────────────────────────────────────────
# Match slot builders
# ─────────────────────────────────────────────────────────────────────────────
def _build_all_match_slots(bracket_size: int, draw: List, seeded: List[int]) -> List[dict]:
    rounds_list = _rounds(bracket_size)
    matches: List[dict] = []

    # First round from draw
    r1 = rounds_list[0]
    for i in range(0, bracket_size, 2):
        p1 = draw[i]; p2 = draw[i + 1] if i + 1 < bracket_size else None
        mid = f"{r1}_{i // 2 + 1:03d}"
        matches.append(_match_stub(mid, r1, p1, p2, _seed_of(draw, seeded, p1), _seed_of(draw, seeded, p2), i, i + 1))

    # Stub slots for all later rounds
    for ridx in range(1, len(rounds_list)):
        rnd = rounds_list[ridx]
        prev_cnt = len([m for m in matches if m["round"] == rounds_list[ridx - 1]])
        for i in range(prev_cnt // 2):
            mid = f"{rnd}_{i + 1:03d}"
            matches.append(_match_stub(mid, rnd, None, None, None, None, None, None))

    return matches

def _match_stub(mid, rnd, p1, p2, s1, s2, dp1, dp2) -> dict:
    return {
        "match_id": mid, "round": rnd,
        "draw_pos_1": dp1, "draw_pos_2": dp2,
        "player1_id": p1, "player2_id": p2,
        "seed1": s1, "seed2": s2,
        "status": "scheduled" if p1 or p2 else "pending",
        "winner_id": None, "loser_id": None, "score": None,
        "day": None, "session": None,
        "court_key": None, "court_venue_id": None,
        "scheduled_time": None, "timing_type": "session_start",
    }

# ─────────────────────────────────────────────────────────────────────────────
# Scheduler
# ─────────────────────────────────────────────────────────────────────────────
def schedule_matches(tourn: dict, matches: List[dict]) -> List[dict]:
    bracket  = int(tourn["bracket_size"])
    duration = str(tourn.get("duration", "1week"))
    venues   = tourn.get("venues", {})
    rnds     = _rounds(bracket)

    courts = [k for k in COURT_KEYS_ORDERED if k in venues]
    if not courts: courts = list(venues.keys()) or ["main_stage"]

    def round_days(r: str) -> List[Tuple[int, str]]:
        """Returns [(day_number, 'top'|'bottom'|'all'), ...]"""
        idx = rnds.index(r) if r in rnds else 0
        n   = len(rnds)

        if duration == "2week":
            # Two-week: align from the back. F=day14, SF=day12, rest days 11&13
            # Day pairs from first round forward
            # Special overrides for late rounds
            if r == "F":   return [(14, "all")]
            if r == "SF":  return [(12, "all")]
            if r == "QF":  return [(9, "top"), (10, "bottom")]
            # Earlier rounds fill days 1-8 from first round forward
            early_pairs = [(1, 2), (3, 4), (5, 6), (7, 8)]
            # idx 0 = earliest round
            early_idx = n - 1 - idx - 3  # offset so QF(idx n-3) maps to last pair
            pair_idx = early_idx
            if 0 <= pair_idx < len(early_pairs):
                d1, d2 = early_pairs[pair_idx]
                return [(d1, "top"), (d2, "bottom")]
            return [(1, "all")]

        else:  # 1week
            if bracket == 64:
                m = {"R64": [(1,"top"),(2,"bottom")], "R32": [(3,"all")], "R16": [(4,"all")],
                     "QF":  [(5,"all")], "SF": [(6,"all")], "F": [(7,"all")]}
            elif bracket == 32:
                m = {"R32": [(1,"top"),(2,"bottom")], "R16": [(3,"top"),(4,"bottom")],
                     "QF": [(5,"all")], "SF": [(6,"all")], "F": [(7,"all")]}
            elif bracket == 16:
                m = {"R16": [(1,"top"),(2,"bottom")], "QF": [(3,"top"),(4,"bottom")],
                     "SF": [(6,"all")], "F": [(7,"all")]}
            elif bracket == 8:
                m = {"QF": [(1,"top"),(2,"bottom")], "SF": [(3,"all")],
                     "F": [(5,"all")]}
            elif bracket == 4:
                m = {"SF": [(1,"all")], "F": [(3,"all")]}
            else:
                m = {r2: [(i+1,"all")] for i, r2 in enumerate(rnds)}
            return m.get(r, [(1, "all")])

    by_round: Dict[str, List[dict]] = {}
    for m in matches:
        by_round.setdefault(m["round"], []).append(m)

    for r, rmatch in by_round.items():
        day_slots = round_days(r)
        half      = len(rmatch) // 2

        for slot_idx, (day, which) in enumerate(day_slots):
            if which == "top":
                slot = rmatch[:half]
            elif which == "bottom":
                slot = rmatch[half:]
            else:
                # "all" — if multiple all-slots, interleave
                slot = rmatch[slot_idx::len(day_slots)]

            # Sort by seed importance — higher seeds get bigger courts
            slot.sort(key=lambda m: (
                min((m.get("seed1") or 9999), (m.get("seed2") or 9999))
            ))

            n_courts  = len(courts)
            n_matches = len(slot)
            day_half  = slot[:math.ceil(n_matches / 2)]
            night_half = slot[math.ceil(n_matches / 2):]

            def _assign(s_matches: List[dict], session: str) -> None:
                court_last: Dict[str, str] = {}  # court_key -> last match_id on that court
                for idx, m in enumerate(s_matches):
                    ck = courts[idx % n_courts]
                    m["day"] = day; m["session"] = session
                    m["court_key"] = ck
                    m["court_venue_id"] = venues.get(ck)
                    if ck not in court_last:
                        m["scheduled_time"] = DEFAULT_DAY_SESSION if session == "day" else DEFAULT_NIGHT_SESSION
                        m["timing_type"]    = "session_start"
                    else:
                        m["scheduled_time"] = court_last[ck]
                        m["timing_type"]    = "next_on"
                    court_last[ck] = m["match_id"]

            _assign(day_half,   "day")
            _assign(night_half, "night")

    return matches

# ─────────────────────────────────────────────────────────────────────────────
# Text rendering
# ─────────────────────────────────────────────────────────────────────────────
def draw_text(draw: List, bracket_size: int, seeded: List[int], guild: discord.Guild,
              tourn: Optional[dict] = None) -> List[str]:
    lines = []; half = bracket_size // 2
    wcs = set(tourn.get("wildcard_entries", [])) if tourn else set()
    qls = set(tourn.get("qualifier_entries", [])) if tourn else set()
    for pos, uid in enumerate(draw):
        seed   = _seed_of(draw, seeded, uid)
        member = guild.get_member(uid) if uid else None
        name   = member.display_name if member else (f"UID:{uid}" if uid else "BYE")
        prefix = ""
        if uid and uid in wcs: prefix = "(W) "
        elif uid and uid in qls: prefix = "(Q) "
        elif seed: prefix = f"({seed}) "
        label  = "TOP" if pos < half else "BTM"
        lines.append(f"`{pos+1:>3}.` {label}  **{prefix}{name}**")
    return lines

def schedule_text(tourn: dict, guild: discord.Guild, day_filter: Optional[int] = None) -> List[str]:
    import datetime as _dt
    matches = tourn.get("matches", [])

    def _name(uid):
        if uid is None: return "BYE/TBD"
        m = guild.get_member(uid)
        return m.display_name if m else f"UID:{uid}"

    # Compute base date for day 1
    ts_iso = tourn.get("tournament_start_date")
    try:
        base_date = _dt.datetime.fromisoformat(ts_iso).replace(
            hour=0, minute=0, second=0, microsecond=0)
    except Exception:
        base_date = None

    def _session_ts(day: int, session: str, time_str: str) -> str:
        """Return Discord timestamp for a session start, or plain time_str."""
        if base_date is None: return time_str or "?"
        try:
            h, m2 = map(int, (time_str or "11:00").split(":")[:2])
            dt = base_date + _dt.timedelta(days=day - 1, hours=h, minutes=m2)
            return f"<t:{int(dt.timestamp())}:t>"
        except Exception:
            return time_str or "?"

    by_day: Dict[int, List[dict]] = {}
    for m in matches:
        d = int(m.get("day") or 0)
        by_day.setdefault(d, []).append(m)

    lines = [f"📅 **Schedule — {tourn.get('name','Tournament')}**"]
    if base_date:
        lines.append(f"Day 1 = {_fmt_dt(ts_iso)}")

    for day in sorted(by_day):
        if day_filter is not None and day != day_filter: continue
        # Day header with Discord date stamp
        if base_date:
            day_dt = base_date + _dt.timedelta(days=day - 1)
            day_ts = f"<t:{int(day_dt.timestamp())}:D>"
        else:
            day_ts = f"Day {day}"
        lines.append(f"\n**— Day {day} — {day_ts} —**")

        day_m = sorted(by_day[day],
                       key=lambda m: (m.get("session","day") != "day", m.get("match_id","")))
        cur_sess = None
        for m in day_m:
            sess = m.get("session","day")
            if sess != cur_sess:
                cur_sess = sess
                default_t = "11:00" if sess == "day" else "19:00"
                sess_ts   = _session_ts(day, sess, default_t)
                lines.append(f"  {'🌞 Day Session' if sess=='day' else '🌙 Night Session'} · {sess_ts}")
            p1 = _name(m.get("player1_id")); p2 = _name(m.get("player2_id"))
            s1 = m.get("seed1"); s2 = m.get("seed2")
            rnd = _rnd(m.get("round","?")); ct = COURT_DISPLAY.get(m.get("court_key",""),"?")
            tt  = m.get("timing_type","session_start"); st = m.get("scheduled_time","?")
            mid = m.get("match_id","?")
            if tt == "session_start":
                timing = _session_ts(day, sess, st)
            elif tt == "not_before":
                timing = f"Not Before {_session_ts(day, sess, st)}"
            else:
                timing = f"Next on {ct}"
            icon  = "✅" if m.get("status") == "completed" else ("🎾" if m.get("player1_id") else "⏳")
            score = f" · **{m['score']}**" if m.get("score") else ""
            win   = f" → **{_name(m.get('winner_id'))}** wins" if m.get("winner_id") else ""
            p1t   = f"({s1}) " if s1 else ""; p2t = f"({s2}) " if s2 else ""
            lines.append(f"  {icon} `{mid}` | **{rnd}** | {ct} | {timing}")
            lines.append(f"       {p1t}**{p1}** vs {p2t}**{p2}**{score}{win}")
    return lines

# ─────────────────────────────────────────────────────────────────────────────
# Google Sheets helpers
# ─────────────────────────────────────────────────────────────────────────────

# Bracket layout constants
_BK_ROWS_PER_PLAYER = 2   # name row + score row
_BK_MATCH_H  = 4          # rows per match (2 players × 2 rows)
_BK_GAP      = 2          # gap rows between matches in R0
_BK_STRIDE   = 6          # _BK_MATCH_H + _BK_GAP
_BK_NAME_W   = 3          # merged columns for player name
_BK_SCORE_W  = 1          # column for score
_BK_CONN_W   = 1          # connector column
_BK_CPR      = _BK_NAME_W + _BK_SCORE_W + _BK_CONN_W  # = 5 cols per round
_BK_DATA_ROW = 2          # row index (0-based) where bracket data starts (row 0=title, 1=headers)

def _bk_match_start(round_idx: int, match_idx: int) -> int:
    """0-based row index of a match's first player row, relative to DATA_ROW."""
    return match_idx * _BK_STRIDE * (2 ** round_idx) + 3 * (2 ** round_idx - 1)

def _bk_round_col(round_idx: int) -> int:
    """0-based column index of a round's name column."""
    return round_idx * _BK_CPR

def _hex_rgb(h: str) -> dict:
    h = h.lstrip("#")
    if len(h) == 3: h = "".join(c*2 for c in h)
    if len(h) != 6: return {"red": 0.9, "green": 0.9, "blue": 0.9}
    return {"red": int(h[0:2],16)/255, "green": int(h[2:4],16)/255, "blue": int(h[4:6],16)/255}

def _sheets_ok() -> bool:
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        print("[sheets] _sheets_ok: gspread + google-auth imported OK")
        return True
    except ImportError as e:
        print(f"[sheets] _sheets_ok FAILED — missing library: {e}")
        return False

def _gs_client():
    import gspread; from google.oauth2.service_account import Credentials
    sa = getattr(config, "GOOGLE_SERVICE_ACCOUNT_JSON", None)
    print(f"[sheets] _gs_client: GOOGLE_SERVICE_ACCOUNT_JSON = {sa!r}")
    if not sa: raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON not set in config.")
    import os
    if not os.path.exists(sa):
        raise FileNotFoundError(f"Service account JSON not found at path: {sa!r}")
    scopes = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(sa, scopes=scopes)
    print(f"[sheets] _gs_client: credentials loaded, authorizing…")
    client = gspread.authorize(creds)
    print(f"[sheets] _gs_client: authorized OK")
    return client

def _style(tourn: dict) -> dict:
    return tourn.get("sheets_config", {})

# ── Low-level batch-update request builders ──

def _solid(color: dict, width: int = 2) -> dict:
    return {"style": "SOLID", "width": width, "color": color}

def _no_border() -> dict:
    return {"style": "NONE"}

def _range(sid, r1, c1, r2, c2) -> dict:
    return {"sheetId": sid, "startRowIndex": r1, "endRowIndex": r2,
            "startColumnIndex": c1, "endColumnIndex": c2}

def _fmt_req(sid, r1, c1, r2, c2, fmt: dict) -> dict:
    fields = ",".join(f"userEnteredFormat.{k}" for k in fmt)
    return {"repeatCell": {"range": _range(sid, r1, c1, r2, c2),
                           "cell": {"userEnteredFormat": fmt}, "fields": fields}}

def _merge_req(sid, r1, c1, r2, c2) -> dict:
    return {"mergeCells": {"range": _range(sid, r1, c1, r2, c2), "mergeType": "MERGE_ALL"}}

def _col_width_req(sid, c1, c2, px: int) -> dict:
    return {"updateDimensionProperties": {
        "range": {"sheetId": sid, "dimension": "COLUMNS",
                  "startIndex": c1, "endIndex": c2},
        "properties": {"pixelSize": px}, "fields": "pixelSize"}}

def _row_height_req(sid, r1, r2, px: int) -> dict:
    return {"updateDimensionProperties": {
        "range": {"sheetId": sid, "dimension": "ROWS",
                  "startIndex": r1, "endIndex": r2},
        "properties": {"pixelSize": px}, "fields": "pixelSize"}}

def _border_req(sid, r1, c1, r2, c2, top=None, bottom=None, left=None, right=None) -> dict:
    req: dict = {"updateBorders": {"range": _range(sid, r1, c1, r2, c2)}}
    if top    is not None: req["updateBorders"]["top"]    = top
    if bottom is not None: req["updateBorders"]["bottom"] = bottom
    if left   is not None: req["updateBorders"]["left"]   = left
    if right  is not None: req["updateBorders"]["right"]  = right
    return req

def _gridlines_req(sid, hide: bool) -> dict:
    return {"updateSheetProperties": {
        "properties": {"sheetId": sid, "gridProperties": {"hideGridlines": hide}},
        "fields": "gridProperties.hideGridlines"}}

# ── Bracket sheet builder ──

def _player_display(uid, draw, seeded, guild) -> str:
    if uid is None: return "BYE"
    seed   = _seed_of(draw, seeded, uid)
    member = guild.get_member(uid) if guild else None
    name   = member.display_name if member else f"UID:{uid}"
    return f"({seed}) {name}" if seed else name

def _build_bracket_requests(ws_id: int, tourn: dict, guild) -> List[dict]:
    """Return all batchUpdate requests to render the bracket."""
    s       = _style(tourn)
    bg      = _hex_rgb(s.get("bg",  "#1a1a2e"))
    fc1     = _hex_rgb(s.get("fc1", "#ffffff"))   # regular name
    fc2     = _hex_rgb(s.get("fc2", "#00e676"))   # winner
    fc3     = _hex_rgb(s.get("fc3", "#ff5252"))   # loser
    sc1     = _hex_rgb(s.get("sc1", "#2a2a4a"))   # name-row bg
    sc2     = _hex_rgb(s.get("sc2", "#1a1a2e"))   # score-row bg (slightly darker)
    line_c  = _hex_rgb(s.get("fc1", "#ffffff"))   # bracket line color = fc1
    fn      = s.get("font", "Roboto Mono")

    bracket = int(tourn.get("bracket_size", 8))
    rnds    = _rounds(bracket)
    n_rnds  = len(rnds)
    draw    = tourn.get("draw", [])
    seeded  = tourn.get("seeded_players", [])

    # Build winner/loser lookup per player per round
    # match_result[uid] = {round: "W"|"L", score: str}
    match_result: dict = {}
    for m in tourn.get("matches", []):
        wid = m.get("winner_id"); lid = m.get("loser_id"); rnd = m.get("round",""); sc = m.get("score","")
        if wid: match_result.setdefault(wid, {})[rnd] = ("W", sc)
        if lid: match_result.setdefault(lid, {})[rnd] = ("L", sc)

    # Pre-compute which player appears at which (round, row) in the bracket
    # round_player[r][match_i] = (p1_uid, p2_uid)
    rounds_by_match: dict = {}
    for m in tourn.get("matches", []):
        rnd = m.get("round","")
        if rnd in rnds:
            ridx = rnds.index(rnd)
            prev_matches = sorted([mx for mx in tourn.get("matches",[]) if mx.get("round")==rnd],
                                   key=lambda mx: mx.get("match_id",""))
            for mi, mx in enumerate(prev_matches):
                rounds_by_match.setdefault(ridx, {})[mi] = mx

    reqs: List[dict] = []

    # Total rows and cols
    n_r0_matches = bracket // 2
    total_data_rows = _bk_match_start(0, n_r0_matches - 1) + _BK_MATCH_H + _BK_GAP + 4
    total_rows = _BK_DATA_ROW + total_data_rows + 4
    total_cols = (n_rnds + 1) * _BK_CPR + 4  # +1 for champion col

    # 1. Fill entire sheet with bg color
    reqs.append(_fmt_req(ws_id, 0, 0, total_rows, total_cols,
                         {"backgroundColor": bg,
                          "textFormat": {"fontFamily": fn, "foregroundColor": fc1}}))

    # 2. Title row (row 0)
    title_col_span = min(total_cols, 20)
    reqs.append(_fmt_req(ws_id, 0, 0, 1, title_col_span,
                         {"backgroundColor": sc1,
                          "textFormat": {"bold": True, "fontSize": 14,
                                         "fontFamily": fn, "foregroundColor": fc2}}))

    # 3. Round header row (row 1)
    for ridx, rnd in enumerate(rnds):
        col = _bk_round_col(ridx)
        reqs.append(_fmt_req(ws_id, 1, col, 2, col + _BK_NAME_W + _BK_SCORE_W,
                             {"backgroundColor": sc1,
                              "textFormat": {"bold": True, "fontSize": 10,
                                             "fontFamily": fn, "foregroundColor": fc1}}))
        reqs.append(_merge_req(ws_id, 1, col, 2, col + _BK_NAME_W + _BK_SCORE_W))
    # Champion header
    champ_col = _bk_round_col(n_rnds)
    reqs.append(_fmt_req(ws_id, 1, champ_col, 2, champ_col + _BK_NAME_W,
                         {"backgroundColor": sc1,
                          "textFormat": {"bold": True, "fontSize": 10,
                                         "fontFamily": fn, "foregroundColor": fc2}}))
    reqs.append(_merge_req(ws_id, 1, champ_col, 2, champ_col + _BK_NAME_W))

    # 4. For each round, draw all matches
    for ridx, rnd in enumerate(rnds):
        n_matches = bracket // (2 ** (ridx + 1))
        col       = _bk_round_col(ridx)
        name_col  = col
        score_col = col + _BK_NAME_W
        conn_col  = col + _BK_NAME_W + _BK_SCORE_W

        # Get match data for this round
        rnd_matches = sorted([m for m in tourn.get("matches",[]) if m.get("round") == rnd],
                              key=lambda m: m.get("match_id",""))

        for mi in range(n_matches):
            start = _BK_DATA_ROW + _bk_match_start(ridx, mi)
            p1_row = start;     p1_score_row = start + 1
            p2_row = start + 2; p2_score_row = start + 3

            # Get player info
            match_data = rnd_matches[mi] if mi < len(rnd_matches) else {}
            p1_uid = match_data.get("player1_id"); p2_uid = match_data.get("player2_id")
            wid    = match_data.get("winner_id");  lid    = match_data.get("loser_id")
            score  = match_data.get("score") or ""

            for player_row, score_row, uid in [(p1_row, p1_score_row, p1_uid),
                                               (p2_row, p2_score_row, p2_uid)]:
                is_winner = uid is not None and uid == wid
                is_loser  = uid is not None and uid == lid
                txt_color = fc2 if is_winner else (fc3 if is_loser else fc1)

                # Name row bg = sc1
                reqs.append(_fmt_req(ws_id, player_row, name_col,
                                     player_row + 1, score_col + 1,
                                     {"backgroundColor": sc1,
                                      "textFormat": {"fontFamily": fn, "bold": is_winner,
                                                     "foregroundColor": txt_color},
                                      "verticalAlignment": "MIDDLE",
                                      "horizontalAlignment": "LEFT"}))
                # Score row bg = sc2
                reqs.append(_fmt_req(ws_id, score_row, name_col,
                                     score_row + 1, score_col + 1,
                                     {"backgroundColor": sc2,
                                      "textFormat": {"fontFamily": fn, "italic": True,
                                                     "foregroundColor": txt_color},
                                      "verticalAlignment": "MIDDLE",
                                      "horizontalAlignment": "CENTER"}))

                # Merge name cells
                reqs.append(_merge_req(ws_id, player_row, name_col,
                                       player_row + 1, name_col + _BK_NAME_W))
                reqs.append(_merge_req(ws_id, score_row, name_col,
                                       score_row + 1, name_col + _BK_NAME_W))

                # Right border on score col cells (horizontal bracket arm)
                reqs.append(_border_req(ws_id, player_row, score_col,
                                        score_row + 1, score_col + 1,
                                        right=_solid(line_c)))

            # Connector column borders (vertical bracket line + horizontal to next round)
            # Left border spanning full match height
            reqs.append(_border_req(ws_id, start, conn_col, start + _BK_MATCH_H, conn_col + 1,
                                    left=_solid(line_c),
                                    top=_solid(line_c),
                                    bottom=_solid(line_c)))
            # Right border on middle 2 rows (horizontal line → next round)
            reqs.append(_border_req(ws_id, p1_score_row, conn_col,
                                    p2_row + 1, conn_col + 1,
                                    right=_solid(line_c)))

        # 5. Column widths for this round
        reqs.append(_col_width_req(ws_id, name_col, name_col + _BK_NAME_W, 140))
        reqs.append(_col_width_req(ws_id, score_col, score_col + 1, 70))
        reqs.append(_col_width_req(ws_id, conn_col, conn_col + 1, 24))

    # 6. Champion column
    champ_uid = tourn.get("champion_id")
    champ_col = _bk_round_col(n_rnds)
    champ_row = _BK_DATA_ROW + _bk_match_start(n_rnds - 1, 0)  # center of final match
    reqs.append(_fmt_req(ws_id, champ_row, champ_col, champ_row + _BK_ROWS_PER_PLAYER,
                         champ_col + _BK_NAME_W,
                         {"backgroundColor": fc2,
                          "textFormat": {"bold": True, "fontSize": 11,
                                         "fontFamily": fn, "foregroundColor": bg},
                          "verticalAlignment": "MIDDLE",
                          "horizontalAlignment": "CENTER"}))
    reqs.append(_merge_req(ws_id, champ_row, champ_col,
                           champ_row + _BK_ROWS_PER_PLAYER, champ_col + _BK_NAME_W))
    reqs.append(_col_width_req(ws_id, champ_col, champ_col + _BK_NAME_W, 160))

    # 7. Row heights
    reqs.append(_row_height_req(ws_id, 0, 1, 30))       # title
    reqs.append(_row_height_req(ws_id, 1, 2, 22))       # headers
    reqs.append(_row_height_req(ws_id, 2, total_rows, 20))  # data rows

    return reqs

def _write_bracket_values(ws, tourn: dict, guild) -> None:
    """Write cell text values for the bracket (separate from formatting)."""
    rnds   = _rounds(int(tourn.get("bracket_size", 8)))
    draw   = tourn.get("draw", [])
    seeded = tourn.get("seeded_players", [])

    updates = []
    # Title
    updates.append({"range": "A1", "values": [[f"🏆 {tourn.get('name','Tournament')}"]]})
    # Round headers
    for ridx, rnd in enumerate(rnds):
        col_letter = _col_letter(_bk_round_col(ridx))
        updates.append({"range": f"{col_letter}2", "values": [[rnd]]})
    champ_letter = _col_letter(_bk_round_col(len(rnds)))
    updates.append({"range": f"{champ_letter}2", "values": [["🏆 CHAMPION"]]})

    # Player names + scores per round
    for ridx, rnd in enumerate(rnds):
        n_matches = int(tourn["bracket_size"]) // (2 ** (ridx + 1))
        rnd_matches = sorted([m for m in tourn.get("matches",[]) if m.get("round") == rnd],
                              key=lambda m: m.get("match_id",""))
        name_col  = _bk_round_col(ridx)
        score_col = name_col + _BK_NAME_W

        for mi in range(n_matches):
            start = _BK_DATA_ROW + _bk_match_start(ridx, mi)
            match_data = rnd_matches[mi] if mi < len(rnd_matches) else {}
            p1_uid = match_data.get("player1_id"); p2_uid = match_data.get("player2_id")
            wid    = match_data.get("winner_id");  score  = match_data.get("score") or ""

            for offset, uid in [(0, p1_uid), (2, p2_uid)]:
                row       = start + offset
                score_row = row + 1
                name_str  = _player_display(uid, draw, seeded, guild) if uid else ("BYE" if ridx == 0 else "")
                score_str = score if uid and uid == wid else ("" if not wid else score)
                if name_str:
                    nc = _col_letter(name_col); sc = _col_letter(score_col)
                    updates.append({"range": f"{nc}{row+1}", "values": [[name_str]]})
                    if score_str:
                        updates.append({"range": f"{sc}{score_row+1}", "values": [[score_str]]})

    # Champion
    champ_uid = tourn.get("champion_id")
    if champ_uid:
        champ_col = _bk_round_col(len(rnds))
        champ_row = _BK_DATA_ROW + _bk_match_start(len(rnds) - 1, 0)
        champ_name = _player_display(champ_uid, draw, seeded, guild)
        cl = _col_letter(champ_col)
        updates.append({"range": f"{cl}{champ_row+1}", "values": [[f"🏆 {champ_name}"]]})

    if updates:
        ws.batch_update(updates)

def _col_letter(col_idx: int) -> str:
    """Convert 0-based column index to A1-notation letter(s)."""
    result = ""
    col_idx += 1
    while col_idx:
        col_idx, rem = divmod(col_idx - 1, 26)
        result = chr(65 + rem) + result
    return result

def _setup_schedule_sheet(ss, ws2_id: int, tourn: dict, s: dict) -> None:
    """Populate and format the Schedule sheet."""
    fn  = s.get("font", "Roboto Mono")
    bg  = _hex_rgb(s.get("bg",  "#1a1a2e"))
    sc1 = _hex_rgb(s.get("sc1", "#2a2a4a"))
    fc1 = _hex_rgb(s.get("fc1", "#ffffff"))
    fc2 = _hex_rgb(s.get("fc2", "#00e676"))
    fc3 = _hex_rgb(s.get("fc3", "#ff5252"))

    hdrs = ["Day","Session","Round","Court","Time","P1","S1","P2","S2","Status","Score","Winner"]
    srows: List[List] = []
    for m in sorted(tourn.get("matches",[]),
                    key=lambda x: (int(x.get("day") or 0), x.get("session","day"))):
        tt = m.get("timing_type",""); st = m.get("scheduled_time","")
        timing = st if tt=="session_start" else (f"NB {st}" if tt=="not_before" else f"→{st}")
        srows.append([
            m.get("day",""), m.get("session",""), m.get("round",""),
            COURT_DISPLAY.get(m.get("court_key",""),""), timing,
            str(m.get("player1_id","")), str(m.get("seed1","")),
            str(m.get("player2_id","")), str(m.get("seed2","")),
            m.get("status",""), m.get("score",""),
            str(m["winner_id"]) if m.get("winner_id") else "",
        ])

    ws2 = ss.worksheet("Schedule")
    ws2.clear()
    all_rows = [hdrs] + srows
    ws2.update("A1", all_rows)

    n_data = len(srows)
    reqs = [
        _gridlines_req(ws2_id, True),
        _fmt_req(ws2_id, 0, 0, 1, len(hdrs),
                 {"backgroundColor": sc1,
                  "textFormat": {"bold": True, "fontFamily": fn, "foregroundColor": fc2}}),
        _fmt_req(ws2_id, 1, 0, 1 + n_data, len(hdrs),
                 {"backgroundColor": bg,
                  "textFormat": {"fontFamily": fn, "foregroundColor": fc1}}),
    ]
    # Highlight completed rows in fc2/fc3
    for ri, m in enumerate(sorted(tourn.get("matches",[]),
                                   key=lambda x:(int(x.get("day") or 0), x.get("session","day")))):
        if m.get("winner_id"):
            row_i = 1 + ri
            reqs.append(_fmt_req(ws2_id, row_i, 11, row_i + 1, 12,
                                 {"textFormat": {"bold": True, "fontFamily": fn,
                                                 "foregroundColor": fc2}}))
    ss.batch_update({"requests": reqs})

def create_sheet(tourn: dict, guild=None) -> Optional[str]:
    print(f"[sheets] create_sheet called for {tourn.get('name','?')!r}")
    if not _sheets_ok():
        print("[sheets] create_sheet: _sheets_ok() returned False, aborting")
        return None
    try:
        s = _style(tourn)
        folder_id = getattr(config, "GOOGLE_DRIVE_FOLDER_ID", None)
        print(f"[sheets] GOOGLE_DRIVE_FOLDER_ID = {folder_id!r}")

        # Build Drive API client using same service account credentials
        import googleapiclient.discovery as _gd
        from google.oauth2.service_account import Credentials as _Creds
        sa = getattr(config, "GOOGLE_SERVICE_ACCOUNT_JSON", None)
        scopes = ["https://spreadsheets.google.com/feeds",
                  "https://www.googleapis.com/auth/drive"]
        creds = _Creds.from_service_account_file(sa, scopes=scopes)
        drive = _gd.build("drive", "v3", credentials=creds, cache_discovery=False)

        # Create the spreadsheet directly inside the user's Drive folder
        # This never touches the service account's storage quota at all
        print(f"[sheets] creating spreadsheet directly in folder {folder_id!r}…")
        file_meta = {
            "name": f"[LIVE] {tourn.get('name','Tournament')}",
            "mimeType": "application/vnd.google-apps.spreadsheet",
        }
        if folder_id:
            file_meta["parents"] = [folder_id]
        created = drive.files().create(body=file_meta, fields="id").execute()
        file_id = created["id"]
        print(f"[sheets] spreadsheet created with id: {file_id}")

        # Open with gspread using the file id
        import gspread as _gs
        gc = _gs.authorize(creds)
        ss = gc.open_by_key(file_id)
        print(f"[sheets] opened via gspread OK — {ss.url}")

        ws  = ss.get_worksheet(0); ws.update_title("Bracket")
        ws2 = ss.add_worksheet("Schedule", rows=500, cols=12)

        # Build all formatting requests for bracket sheet
        reqs = [_gridlines_req(ws.id, True)]
        reqs += _build_bracket_requests(ws.id, tourn, guild)

        # Schedule sheet setup
        reqs.append(_gridlines_req(ws2.id, True))
        ss.batch_update({"requests": reqs})

        # Write text values
        _write_bracket_values(ws, tourn, guild)
        _setup_schedule_sheet(ss, ws2.id, tourn, s)

        ss.share(None, perm_type="anyone", role="reader")
        return ss.url
    except Exception as e:
        import traceback
        print(f"[sheets] create_sheet error: {e}")
        traceback.print_exc()
        return None

def update_sheet(tourn: dict, guild=None) -> None:
    if not _sheets_ok() or not tourn.get("sheet_url"): return
    try:
        gc  = _gs_client()
        m   = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", tourn["sheet_url"])
        if not m: return
        ss  = gc.open_by_key(m.group(1)); s = _style(tourn)
        ws  = ss.worksheet("Bracket")

        # Full re-render bracket formatting + values
        reqs = _build_bracket_requests(ws.id, tourn, guild)
        ss.batch_update({"requests": reqs})
        _write_bracket_values(ws, tourn, guild)

        # Refresh schedule
        try:
            ws2 = ss.worksheet("Schedule")
            _setup_schedule_sheet(ss, ws2.id, tourn, s)
        except Exception: pass
    except Exception as e:
        print(f"[sheets] update_sheet error: {e}")

def archive_sheet(tourn: dict) -> None:
    if not _sheets_ok() or not tourn.get("sheet_url"): return
    try:
        gc = _gs_client()
        m  = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", tourn["sheet_url"])
        if not m: return
        ss = gc.open_by_key(m.group(1))
        ss.update_title(f"[ARCHIVE] {tourn.get('name','Tournament')}")
        try:
            aw = ss.add_worksheet("Archive Info", rows=5, cols=2)
            aw.update("A1", [["Tournament", tourn.get("name","")],
                              ["Completed", tourn.get("completed_at","")],
                              ["Champion",  tourn.get("champion_name","")]])
        except Exception: pass
    except Exception as e:
        print(f"[sheets] archive_sheet error: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Paginator view
# ─────────────────────────────────────────────────────────────────────────────
class PageView(discord.ui.View):
    def __init__(self, lines: List[str], per: int = 25, timeout: int = 180):
        super().__init__(timeout=timeout)
        self.pages = [lines[i:i+per] for i in range(0, max(1,len(lines)), per)]
        self.page  = 0; self._upd()

    def _upd(self):
        self._prev.disabled = (self.page == 0)
        self._next.disabled = (self.page >= len(self.pages) - 1)

    def content(self) -> str:
        return "\n".join(self.pages[self.page]) if self.pages else "—"

    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary)
    async def _prev(self, i: discord.Interaction, _):
        self.page = max(0, self.page-1); self._upd()
        await i.response.edit_message(content=self.content(), view=self)

    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary)
    async def _next(self, i: discord.Interaction, _):
        self.page = min(len(self.pages)-1, self.page+1); self._upd()
        await i.response.edit_message(content=self.content(), view=self)

class WildcardView(discord.ui.View):
    def __init__(self, candidates: List[discord.Member], spots: int):
        super().__init__(timeout=180)
        self.spots = spots; self.selected: List[int] = []; self.confirmed = False
        opts = [discord.SelectOption(label=m.display_name[:100], value=str(m.id)) for m in candidates[:25]]
        self._sel = discord.ui.Select(placeholder=f"Select up to {spots} wildcard(s)…",
                                      min_values=1, max_values=min(spots, len(opts)), options=opts)
        self._sel.callback = self._pick; self.add_item(self._sel)
        self._btn = discord.ui.Button(label="✅ Confirm Wildcards",
                                      style=discord.ButtonStyle.success, disabled=True)
        self._btn.callback = self._confirm; self.add_item(self._btn)

    async def _pick(self, i: discord.Interaction):
        self.selected = [int(v) for v in self._sel.values]
        self._btn.disabled = False
        await i.response.edit_message(content=f"Selected {len(self.selected)} — press Confirm.", view=self)

    async def _confirm(self, i: discord.Interaction):
        self.confirmed = True
        for c in self.children: c.disabled = True
        await i.response.edit_message(content="✅ Wildcards confirmed.", view=self); self.stop()
# tournaments_p2.py  — paste below Part 1 (Cog definition)
# This is combined with Part 1 into modules/tournaments.py

# ─────────────────────────────────────────────────────────────────────────────
# Interaction reply helper — safe regardless of ack state
# ─────────────────────────────────────────────────────────────────────────────
async def _reply(i: discord.Interaction, content: str = None, embed: discord.Embed = None,
                 view: discord.ui.View = None, ephemeral: bool = False) -> None:
    """Send a response or followup depending on whether interaction is already acked."""
    kwargs = {k: v for k, v in
              {"content": content, "embed": embed, "view": view, "ephemeral": ephemeral}.items()
              if v is not None}
    try:
        if i.response.is_done():
            await i.followup.send(**kwargs)
        else:
            await i.response.send_message(**kwargs)
    except discord.errors.HTTPException:
        try:
            await i.followup.send(**kwargs)
        except Exception:
            pass

# ─────────────────────────────────────────────────────────────────────────────
# Autocomplete helpers
# ─────────────────────────────────────────────────────────────────────────────
def _safe_ac(fn):
    """Decorator that silently returns [] if an autocomplete interaction has expired."""
    import functools
    @functools.wraps(fn)
    async def wrapper(i: discord.Interaction, cur: str):
        try:
            return await fn(i, cur)
        except Exception:
            return []
    return wrapper

@_safe_ac
async def _ac_cat(i: discord.Interaction, cur: str) -> List[app_commands.Choice[str]]:
    c = cur.lower(); out = []
    for cid, row in _cats_db().get("categories",{}).items():
        if c in cid or c in row.get("name","").lower() or not c:
            out.append(app_commands.Choice(name=f"{row.get('name',cid)} ({cid})"[:100], value=cid))
        if len(out)>=25: break
    return out

@_safe_ac
async def _ac_comp_all(i: discord.Interaction, cur: str) -> List[app_commands.Choice[str]]:
    c = cur.lower(); out = []
    for tid, t in _comp_db().get("tournaments",{}).items():
        if c in tid.lower() or c in t.get("name","").lower() or not c:
            out.append(app_commands.Choice(name=f"{t.get('name',tid)} [{t.get('status','?')}]"[:100], value=tid))
        if len(out)>=25: break
    return out

@_safe_ac
async def _ac_comp_open(i: discord.Interaction, cur: str) -> List[app_commands.Choice[str]]:
    c = cur.lower(); out = []
    for tid, t in _comp_db().get("tournaments",{}).items():
        if t.get("status") not in (STATUS_UPCOMING, STATUS_REG, STATUS_ACTIVE): continue
        if c in tid.lower() or c in t.get("name","").lower() or not c:
            out.append(app_commands.Choice(name=f"{t.get('name',tid)} [{t.get('status','?')}]"[:100], value=tid))
        if len(out)>=25: break
    return out

@_safe_ac
async def _ac_comp_done(i: discord.Interaction, cur: str) -> List[app_commands.Choice[str]]:
    c = cur.lower(); out = []
    for tid, t in _comp_db().get("tournaments",{}).items():
        if t.get("status") != STATUS_COMPLETED: continue
        if c in tid.lower() or c in t.get("name","").lower() or not c:
            out.append(app_commands.Choice(name=f"{t.get('name',tid)}"[:100], value=tid))
        if len(out)>=25: break
    return out

@_safe_ac
async def _ac_match(i: discord.Interaction, cur: str) -> List[app_commands.Choice[str]]:
    tid = getattr(i.namespace, "tournament_id", None); c = cur.lower(); out = []
    if tid:
        t = _get_comp(tid)
        if t:
            for m in t.get("matches",[]):
                mid = str(m.get("match_id",""))
                if c in mid.lower() or not c:
                    p1 = m.get("player1_id","?"); p2 = m.get("player2_id","?")
                    out.append(app_commands.Choice(
                        name=f"{mid} | {_rnd(m.get('round','?'))} | {p1} vs {p2}"[:100], value=mid))
                if len(out)>=25: break
    return out

@_safe_ac
async def _ac_user(i: discord.Interaction, cur: str) -> List[app_commands.Choice[str]]:
    if not i.guild: return []
    c = cur.lower(); out = []
    for m in i.guild.members[:200]:
        if m.bot: continue
        if c in m.display_name.lower() or c in str(m.id) or not c:
            out.append(app_commands.Choice(name=f"{m.display_name}"[:100], value=str(m.id)))
        if len(out)>=25: break
    return out

COMMON_FONTS = [
    "Arial", "Roboto", "Roboto Mono", "Open Sans", "Lato", "Montserrat",
    "Raleway", "Oswald", "Merriweather", "Playfair Display", "Ubuntu",
    "Source Code Pro", "Courier New", "Georgia", "Times New Roman",
    "Trebuchet MS", "Verdana", "Impact", "Nunito", "Poppins",
    "Bebas Neue", "Barlow", "Exo 2", "Orbitron", "Rajdhani",
    "Teko", "Anton", "Jost", "Inter", "Black Han Sans",
]

@_safe_ac
async def _ac_font(i: discord.Interaction, cur: str) -> List[app_commands.Choice[str]]:
    c = cur.lower()
    return [app_commands.Choice(name=f, value=f) for f in COMMON_FONTS if c in f.lower()][:25]

@_safe_ac
async def _ac_venue(i: discord.Interaction, cur: str) -> List[app_commands.Choice[str]]:
    """Autocomplete venue IDs from venues.json."""
    out = []; c = cur.lower()
    try:
        from modules.venues import _load_venues
        venues = _load_venues()
    except Exception:
        try:
            import json, os
            data_dir = str(getattr(config, "DATA_DIR", "data"))
            path = os.path.join(data_dir, "venues.json")
            venues = json.load(open(path)).get("venues", {})
        except Exception:
            venues = {}
    for vid, v in venues.items():
        name = v.get("name") or v.get("title") or vid
        label = f"{name} ({vid})"[:100]
        if c in label.lower() or not c:
            out.append(app_commands.Choice(name=label, value=vid))
        if len(out) >= 25: break
    return out

# ─────────────────────────────────────────────────────────────────────────────
# Cog
# ─────────────────────────────────────────────────────────────────────────────
class TournamentsCog(commands.Cog):
    tournament = app_commands.Group(name="tournament", description="Competition tournament system")
    category   = app_commands.Group(name="category",   description="Tournament point categories")
    rankings   = app_commands.Group(name="rankings",   description="Player rankings and leaderboards")
    stats      = app_commands.Group(name="stats",      description="Player match and career statistics")
    admin      = app_commands.Group(name="admin",      description="Admin-only server management tools")

    def __init__(self, bot: commands.Bot): self.bot = bot

    # ═══════════════════════════════════════════════════════════════════════
    # /category commands
    # ═══════════════════════════════════════════════════════════════════════
    @category.command(name="create", description="(Admin) Create a points category.")
    @app_commands.guild_only()
    async def cat_create(self, i: discord.Interaction, category_id: str, name: str,
                         champion_pts: int, finalist_pts: int, semi_pts: int,
                         quarter_pts: int, r16_pts: int, r32_pts: int,
                         r64_pts: int = 0, r128_pts: int = 0):
        if not isinstance(i.user, discord.Member) or not _is_admin(i.user):
            return await _reply(i, "❌ Admin only.", ephemeral=True)
        cid = category_id.strip().lower().replace(" ", "_")
        db  = _cats_db(); cats = db.setdefault("categories",{})
        if cid in cats: return await _reply(i, "❌ Already exists.", ephemeral=True)
        cats[cid] = {"id": cid, "name": name.strip(),
                     "champion_pts": champion_pts, "finalist_pts": finalist_pts,
                     "semi_pts": semi_pts, "quarter_pts": quarter_pts,
                     "r16_pts": r16_pts, "r32_pts": r32_pts,
                     "r64_pts": r64_pts, "r128_pts": r128_pts,
                     "created_by": i.user.id,
                     "created_at": datetime.now(timezone.utc).isoformat()}
        _cats_save(db)
        emb = discord.Embed(title=f"✅ Category: {name}", color=discord.Color.gold())
        emb.add_field(name="ID", value=f"`{cid}`")
        for lbl,val in [("Champion",champion_pts),("Finalist",finalist_pts),
                         ("SF",semi_pts),("QF",quarter_pts),("R16",r16_pts),
                         ("R32",r32_pts),("R64",r64_pts),("R128",r128_pts)]:
            emb.add_field(name=lbl, value=str(val), inline=True)
        await _reply(i, embed=emb)

    @category.command(name="edit", description="(Admin) Edit a category.")
    @app_commands.guild_only()
    @app_commands.autocomplete(category_id=_ac_cat)
    async def cat_edit(self, i: discord.Interaction, category_id: str,
                       name: Optional[str]=None, champion_pts: Optional[int]=None,
                       finalist_pts: Optional[int]=None, semi_pts: Optional[int]=None,
                       quarter_pts: Optional[int]=None, r16_pts: Optional[int]=None,
                       r32_pts: Optional[int]=None, r64_pts: Optional[int]=None,
                       r128_pts: Optional[int]=None):
        if not isinstance(i.user, discord.Member) or not _is_admin(i.user):
            return await _reply(i, "❌ Admin only.", ephemeral=True)
        db  = _cats_db(); row = db.get("categories",{}).get(category_id)
        if not row: return await _reply(i, "❌ Not found.", ephemeral=True)
        for attr, val in [("name",name),("champion_pts",champion_pts),("finalist_pts",finalist_pts),
                           ("semi_pts",semi_pts),("quarter_pts",quarter_pts),("r16_pts",r16_pts),
                           ("r32_pts",r32_pts),("r64_pts",r64_pts),("r128_pts",r128_pts)]:
            if val is not None: row[attr] = val.strip() if isinstance(val,str) else int(val)
        db["categories"][category_id] = row; _cats_save(db)
        await _reply(i, f"✅ Category `{category_id}` updated.")

    @category.command(name="delete", description="(Admin) Delete a category.")
    @app_commands.guild_only()
    @app_commands.autocomplete(category_id=_ac_cat)
    async def cat_delete(self, i: discord.Interaction, category_id: str):
        if not isinstance(i.user, discord.Member) or not _is_admin(i.user):
            return await _reply(i, "❌ Admin only.", ephemeral=True)
        for tid, t in _comp_db().get("tournaments",{}).items():
            if t.get("category_id") == category_id:
                return await _reply(i, f"❌ Tournament `{tid}` uses this.", ephemeral=True)
        db = _cats_db(); cats = db.get("categories",{})
        if category_id not in cats: return await _reply(i, "❌ Not found.", ephemeral=True)
        del cats[category_id]; _cats_save(db)
        await _reply(i, f"🗑️ Category `{category_id}` deleted.")

    @category.command(name="list", description="List all categories.")
    @app_commands.guild_only()
    async def cat_list(self, i: discord.Interaction):
        cats = list(_cats_db().get("categories",{}).values())
        if not cats: return await _reply(i, "No categories yet.", ephemeral=True)
        emb = discord.Embed(title="📋 Categories", color=discord.Color.gold())
        for c in cats[:10]:
            emb.add_field(name=f"{c.get('name','?')} (`{c.get('id','?')}`)",
                value=(f"🏆{c.get('champion_pts',0)} / 🥈{c.get('finalist_pts',0)} / "
                       f"SF:{c.get('semi_pts',0)} / QF:{c.get('quarter_pts',0)} / "
                       f"R16:{c.get('r16_pts',0)} / R32:{c.get('r32_pts',0)}"), inline=False)
        await _reply(i, embed=emb)

    # ═══════════════════════════════════════════════════════════════════════
    # /tournament create
    # ═══════════════════════════════════════════════════════════════════════
    @tournament.command(name="create", description="(Admin) Create a competition tournament.")
    @app_commands.guild_only()
    @app_commands.autocomplete(
        category_id=_ac_cat,
        main_stage_court=_ac_venue, stage_court_2=_ac_venue, stage_court_3=_ac_venue,
        stage_court_4=_ac_venue, other_court_1=_ac_venue, other_court_2=_ac_venue,
        other_court_3=_ac_venue, other_court_4=_ac_venue, other_court_5=_ac_venue,
    )
    @app_commands.choices(
        bracket_size=[app_commands.Choice(name=str(n), value=n) for n in VALID_BRACKET_SIZES],
        duration=[app_commands.Choice(name="1 Week (max 64)", value="1week"),
                  app_commands.Choice(name="2 Weeks (max 128)", value="2week")],
        best_of=[app_commands.Choice(name="Best of 3", value=3),
                 app_commands.Choice(name="Best of 5", value=5)],
    )
    async def tourn_create(self, i: discord.Interaction,
                           name: str, category_id: str, bracket_size: int,
                           duration: str, registration_start: str,
                           registration_close: str, tournament_start: str,
                           main_stage_court: str,
                           seeds: int = 8, wildcards: int = 0, best_of: int = 3,
                           stage_court_2: Optional[str] = None,
                           stage_court_3: Optional[str] = None,
                           stage_court_4: Optional[str] = None,
                           other_court_1: Optional[str] = None,
                           other_court_2: Optional[str] = None,
                           other_court_3: Optional[str] = None,
                           other_court_4: Optional[str] = None,
                           other_court_5: Optional[str] = None,
                           other_court_6: Optional[str] = None,
                           other_court_7: Optional[str] = None,
                           other_court_8: Optional[str] = None,
                           other_court_9: Optional[str] = None,
                           other_court_10: Optional[str] = None):
        if not isinstance(i.user, discord.Member) or not _is_admin(i.user):
            return await _reply(i, "❌ Admin only.", ephemeral=True)
        if bracket_size not in VALID_BRACKET_SIZES:
            return await _reply(i, f"❌ Bracket size must be one of {VALID_BRACKET_SIZES}.", ephemeral=True)
        if duration == "1week" and bracket_size > 64:
            return await _reply(i, "❌ 1-week max draw size is 64.", ephemeral=True)
        if not _get_cat(category_id):
            return await _reply(i, "❌ Category not found.", ephemeral=True)
        rs = _parse_date(registration_start); rc = _parse_date(registration_close); ts = _parse_date(tournament_start)
        if not all([rs, rc, ts]):
            return await _reply(i, "❌ Dates must be YYYY-MM-DD or YYYY-MM-DD HH:MM.", ephemeral=True)
        if rc <= rs: return await _reply(i, "❌ Reg close must be after reg start.", ephemeral=True)
        if ts <= rc: return await _reply(i, "❌ Tournament start must be after reg close.", ephemeral=True)

        venues: Dict[str, str] = {"main_stage": main_stage_court.strip()}
        for key, val in [("stage_2",stage_court_2),("stage_3",stage_court_3),("stage_4",stage_court_4),
                          ("other_1",other_court_1),("other_2",other_court_2),("other_3",other_court_3),
                          ("other_4",other_court_4),("other_5",other_court_5),("other_6",other_court_6),
                          ("other_7",other_court_7),("other_8",other_court_8),("other_9",other_court_9),
                          ("other_10",other_court_10)]:
            if val: venues[key] = val.strip()

        slug = re.sub(r"[^a-z0-9]","", name.lower())[:12]
        tid  = f"T_{slug}_{uuid.uuid4().hex[:6].upper()}"

        sc = {"bg":  "#1a1a2e",
              "fc1": "#ffffff",  "fc2": "#00e676",  "fc3": "#ff5252",
              "sc1": "#2a2a4a",  "sc2": "#1a1a2e",  "font": "Roboto Mono"}

        data = {"id": tid, "guild_id": i.guild.id, "name": name.strip(),
                "category_id": category_id, "bracket_size": bracket_size,
                "duration": duration, "seeds": max(0, min(bracket_size, seeds)),
                "wildcards": max(0, wildcards), "best_of": best_of, "venues": venues,
                "registration_start_date": rs.isoformat(),
                "registration_close_date": rc.isoformat(),
                "tournament_start_date": ts.isoformat(),
                "status": STATUS_UPCOMING, "registrations": [],
                "wildcard_entries": [], "draw": [], "seeded_players": [],
                "matches": [], "completed_at": None,
                "champion_id": None, "champion_name": None,
                "sheet_url": None, "sheets_config": sc,
                "awarded_points": {},   # uid -> {round: pts}
                "point_defense_applied": False}
        _save_comp(tid, data)

        cat = _get_cat(category_id) or {}
        emb = discord.Embed(title=f"🏆 Tournament Created: {name}",
                            color=discord.Color.gold(), description=f"ID: `{tid}`")
        emb.add_field(name="Category",  value=cat.get("name","?"),   inline=True)
        emb.add_field(name="Draw",      value=str(bracket_size),     inline=True)
        emb.add_field(name="Duration",  value=duration,              inline=True)
        emb.add_field(name="Seeds",     value=str(seeds),            inline=True)
        emb.add_field(name="Wildcards", value=str(wildcards),        inline=True)
        emb.add_field(name="Best Of",   value=f"Bo{best_of}",        inline=True)
        emb.add_field(name="Courts",    value=", ".join(
            COURT_DISPLAY.get(k, k) for k in venues), inline=False)
        emb.add_field(name="Reg Opens",  value=_fmt_dt(rs.isoformat()), inline=True)
        emb.add_field(name="Reg Closes", value=_fmt_dt(rc.isoformat()), inline=True)
        emb.add_field(name="T. Start",   value=_fmt_dt(ts.isoformat()), inline=True)
        await _reply(i, embed=emb)

    # ── /tournament edit ──────────────────────────────────────────────────
    @tournament.command(name="edit", description="(Admin) Edit tournament settings.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=_ac_comp_all)
    async def tourn_edit(self, i: discord.Interaction, tournament_id: str,
                         name: Optional[str]=None, seeds: Optional[int]=None,
                         wildcards: Optional[int]=None, best_of: Optional[int]=None,
                         duration: Optional[str]=None,
                         registration_start: Optional[str]=None,
                         registration_close: Optional[str]=None,
                         tournament_start: Optional[str]=None):
        if not isinstance(i.user, discord.Member) or not _is_admin(i.user):
            return await _reply(i, "❌ Admin only.", ephemeral=True)
        t = _get_comp(tournament_id)
        if not t: return await _reply(i, "❌ Not found.", ephemeral=True)
        if t.get("status") == STATUS_COMPLETED:
            return await _reply(i, "❌ Already completed.", ephemeral=True)
        if name:     t["name"]     = name.strip()
        if seeds is not None:    t["seeds"]    = max(0, seeds)
        if wildcards is not None: t["wildcards"]= max(0, wildcards)
        if best_of is not None:  t["best_of"]  = best_of
        if duration: t["duration"] = duration
        for field, raw in [("registration_start_date",registration_start),
                            ("registration_close_date",registration_close),
                            ("tournament_start_date",tournament_start)]:
            if raw:
                dt = _parse_date(raw)
                if not dt: return await _reply(i, f"❌ Bad date: {raw}", ephemeral=True)
                t[field] = dt.isoformat()
        _save_comp(tournament_id, t)
        await _reply(i, f"✅ Tournament `{tournament_id}` updated.")

    # ── /tournament delete ────────────────────────────────────────────────
    @tournament.command(name="delete", description="(Admin) Delete a tournament.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=_ac_comp_all)
    async def tourn_delete(self, i: discord.Interaction, tournament_id: str):
        if not isinstance(i.user, discord.Member) or not _is_admin(i.user):
            return await _reply(i, "❌ Admin only.", ephemeral=True)
        t = _get_comp(tournament_id)
        if not t: return await _reply(i, "❌ Not found.", ephemeral=True)
        if t.get("status") == STATUS_ACTIVE:
            return await _reply(i, "❌ Cannot delete an active tournament.", ephemeral=True)
        _del_comp(tournament_id)
        await _reply(i, f"🗑️ Tournament `{tournament_id}` deleted.")

    # ── /tournament view ──────────────────────────────────────────────────
    @tournament.command(name="view", description="View a tournament.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=_ac_comp_all)
    async def tourn_view(self, i: discord.Interaction, tournament_id: str):
        t = _get_comp(tournament_id)
        if not t: return await _reply(i, "❌ Not found.", ephemeral=True)
        cat   = _get_cat(t.get("category_id","")) or {}
        total = len(t.get("registrations",[])) + len(t.get("wildcard_entries",[]))
        now = datetime.now(timezone.utc)
        def _event(label: str, iso: Optional[str]) -> str:
            if not iso: return ""
            try:
                dt = datetime.fromisoformat(iso)
                rel = _fmt_countdown(iso)
                if dt > now:
                    return f"⏳ **{label}** {_fmt_dt(iso)} ({rel})"
                else:
                    return f"✅ **{label}** {_fmt_dt(iso)}"
            except: return ""

        status = t.get("status","?").upper()
        emb = discord.Embed(title=f"🏆 {t.get('name','Tournament')}",
                            color=discord.Color.gold(),
                            description=f"**{status}**")
        emb.add_field(name="Category",  value=cat.get("name","?"),           inline=True)
        emb.add_field(name="Draw",      value=str(t.get("bracket_size","?")),inline=True)
        emb.add_field(name="Duration",  value=t.get("duration","?"),         inline=True)
        emb.add_field(name="Seeds",     value=str(t.get("seeds",0)),         inline=True)
        emb.add_field(name="Wildcards", value=str(t.get("wildcards",0)),     inline=True)
        emb.add_field(name="Best Of",   value=f"Bo{t.get('best_of',3)}",     inline=True)
        emb.add_field(name="Players",   value=f"{total}/{t.get('bracket_size','?')}", inline=True)
        # Timeline — show each step with relative countdown if in the future
        timeline = []
        for lbl, key in [("Registration Opens",  "registration_start_date"),
                          ("Registration Closes", "registration_close_date"),
                          ("Tournament Starts",   "tournament_start_date"),
                          ("Completed",           "completed_at")]:
            ev = _event(lbl, t.get(key))
            if ev: timeline.append(ev)
        if timeline:
            emb.add_field(name="📅 Timeline", value="\n".join(timeline), inline=False)
        if t.get("champion_name"):
            emb.add_field(name="🏆 Champion", value=f"**{t['champion_name']}**", inline=False)
        if t.get("sheet_url"):
            emb.add_field(name="📊 Live Bracket", value=f"[Open Sheet]({t['sheet_url']})", inline=False)
        await _reply(i, embed=emb)

    # ── /tournament list ──────────────────────────────────────────────────
    @tournament.command(name="list", description="List all tournaments.")
    @app_commands.guild_only()
    async def tourn_list(self, i: discord.Interaction):
        db = _comp_db().get("tournaments",{})
        if not db: return await _reply(i, "No tournaments yet.", ephemeral=True)
        emb = discord.Embed(title="📋 Tournaments", color=discord.Color.blurple())
        for tid, t in list(db.items())[:15]:
            total  = len(t.get("registrations",[])) + len(t.get("wildcard_entries",[]))
            status = t.get("status","?").upper()
            rs  = _fmt_dt(t.get("registration_start_date"))
            rc  = _fmt_dt(t.get("registration_close_date"))
            ts  = _fmt_dt(t.get("tournament_start_date"))
            rsr = _fmt_countdown(t.get("registration_start_date"))
            rcr = _fmt_countdown(t.get("registration_close_date"))
            tsr = _fmt_countdown(t.get("tournament_start_date"))
            val = (f"**{status}** · Draw: {t.get('bracket_size','?')} · Players: {total}\n"
                   f"📋 Reg Opens: {rs} {rsr}\n"
                   f"🔒 Reg Closes: {rc} {rcr}\n"
                   f"🎾 Starts: {ts} {tsr}")
            if t.get("champion_name"): val += f"\n🏆 Champion: **{t['champion_name']}**"
            if t.get("sheet_url"): val += f"\n📊 [Live Bracket]({t['sheet_url']})"
            emb.add_field(name=t.get("name","?"), value=val, inline=False)
        await _reply(i, embed=emb)

    # ── /tournament join ──────────────────────────────────────────────────
    @tournament.command(name="join", description="Register for an upcoming tournament.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=_ac_comp_open)
    async def tourn_join(self, i: discord.Interaction, tournament_id: str):
        if not isinstance(i.user, discord.Member):
            return await _reply(i, "❌ Guild only.", ephemeral=True)
        t = _get_comp(tournament_id)
        if not t: return await _reply(i, "❌ Not found.", ephemeral=True)
        # Status-based check — admins control status so this is the source of truth
        if t.get("status") == STATUS_COMPLETED:
            return await _reply(i, "❌ This tournament has already concluded.", ephemeral=True)
        if t.get("status") == STATUS_ACTIVE:
            return await _reply(i, "❌ Draw has already been generated — registration is closed.", ephemeral=True)
        # Advisory date check — warn but don't hard-block (admin controls status)
        now = datetime.now(timezone.utc)
        rs  = datetime.fromisoformat(t["registration_start_date"]) if t.get("registration_start_date") else None
        rc  = datetime.fromisoformat(t["registration_close_date"]) if t.get("registration_close_date") else None
        if rs and now < rs:
            return await _reply(i, 
                f"❌ Registration hasn't opened yet — opens {_fmt_dt(t.get('registration_start_date'))} "
                f"({_fmt_countdown(t.get('registration_start_date'))}).", ephemeral=True)
        if rc and now > rc:
            return await _reply(i, 
                f"❌ Registration closed {_fmt_countdown(t.get('registration_close_date'))}.", ephemeral=True)
        uid = i.user.id; regs = t.setdefault("registrations",[])
        if uid in regs or uid in t.get("wildcard_entries",[]):
            return await _reply(i, "❌ Already registered.", ephemeral=True)
        regs.append(uid); t["status"] = STATUS_REG
        _save_comp(tournament_id, t)
        # Ensure in rankings db
        db = _rank_db(); g = _rank_guild(db, i.guild.id)
        e = _player_entry(g, uid, i.user.display_name); e["name"] = i.user.display_name
        _rank_save(db)
        total = len(regs) + len(t.get("wildcard_entries",[]))
        await _reply(i, 
            f"✅ **{i.user.display_name}** joined **{t.get('name')}**! "
            f"({total}/{t.get('bracket_size','?')} spots filled)", ephemeral=False)

    # ── /tournament leave ─────────────────────────────────────────────────
    @tournament.command(name="leave", description="Withdraw from a tournament before registration closes.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=_ac_comp_open)
    async def tourn_leave(self, i: discord.Interaction, tournament_id: str):
        if not isinstance(i.user, discord.Member):
            return await _reply(i, "❌ Guild only.", ephemeral=True)
        t = _get_comp(tournament_id)
        if not t: return await _reply(i, "❌ Not found.", ephemeral=True)
        rc = datetime.fromisoformat(t["registration_close_date"]) if t.get("registration_close_date") else None
        if rc and datetime.now(timezone.utc) > rc:
            return await _reply(i, "❌ Registration closed — cannot withdraw.", ephemeral=True)
        regs = t.get("registrations",[])
        if i.user.id not in regs:
            return await _reply(i, "❌ You're not registered.", ephemeral=True)
        regs.remove(i.user.id); _save_comp(tournament_id, t)
        await _reply(i, f"✅ **{i.user.display_name}** withdrew from **{t.get('name')}**.")

    # ── /tournament register-user ─────────────────────────────────────────
    @tournament.command(name="register-user", description="(Admin) Register any user into a tournament.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=_ac_comp_all)
    async def tourn_register_user(self, i: discord.Interaction,
                                   tournament_id: str, user: discord.Member):
        if not isinstance(i.user, discord.Member) or not _is_admin(i.user):
            return await _reply(i, "❌ Admin only.", ephemeral=True)
        t = _get_comp(tournament_id)
        if not t: return await _reply(i, "❌ Tournament not found.", ephemeral=True)
        if t.get("status") == STATUS_COMPLETED:
            return await _reply(i, "❌ Tournament already completed.", ephemeral=True)
        if user.bot:
            return await _reply(i, "❌ Cannot register bots.", ephemeral=True)
        regs = t.setdefault("registrations", [])
        wcs  = t.setdefault("wildcard_entries", [])
        if user.id in regs or user.id in wcs:
            return await _reply(i, f"❌ **{user.display_name}** is already registered.", ephemeral=True)
        regs.append(user.id)
        if t.get("status") == STATUS_UPCOMING:
            t["status"] = STATUS_REG
        _save_comp(tournament_id, t)
        db = _rank_db(); g = _rank_guild(db, i.guild.id)
        e = _player_entry(g, user.id, user.display_name); e["name"] = user.display_name
        _rank_save(db)
        total = len(regs) + len(wcs)
        await _reply(i, 
            f"✅ **{user.display_name}** registered in **{t.get('name')}** by admin. "
            f"({total}/{t.get('bracket_size','?')} spots filled)")

    # ── /tournament unregister-user ───────────────────────────────────────
    @tournament.command(name="unregister-user", description="(Admin) Remove any user from a tournament.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=_ac_comp_all)
    async def tourn_unregister_user(self, i: discord.Interaction,
                                     tournament_id: str, user: discord.Member):
        if not isinstance(i.user, discord.Member) or not _is_admin(i.user):
            return await _reply(i, "❌ Admin only.", ephemeral=True)
        t = _get_comp(tournament_id)
        if not t: return await _reply(i, "❌ Tournament not found.", ephemeral=True)
        if t.get("status") == STATUS_ACTIVE:
            return await _reply(i, "❌ Cannot remove players from an active tournament.", ephemeral=True)
        regs = t.get("registrations", [])
        wcs  = t.get("wildcard_entries", [])
        if user.id not in regs and user.id not in wcs:
            return await _reply(i, f"❌ **{user.display_name}** is not registered.", ephemeral=True)
        if user.id in regs: regs.remove(user.id)
        if user.id in wcs:  wcs.remove(user.id)
        _save_comp(tournament_id, t)
        await _reply(i, f"✅ **{user.display_name}** removed from **{t.get('name')}**.")

    # ── /tournament wildcard-assign ───────────────────────────────────────
    @tournament.command(name="wildcard-assign", description="(Admin) Assign wildcard spots.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=_ac_comp_all)
    async def tourn_wildcard(self, i: discord.Interaction, tournament_id: str):
        if not isinstance(i.user, discord.Member) or not _is_admin(i.user):
            return await _reply(i, "❌ Admin only.", ephemeral=True)
        t = _get_comp(tournament_id)
        if not t: return await _reply(i, "❌ Not found.", ephemeral=True)
        bracket  = int(t.get("bracket_size", 8))
        wc_spots = int(t.get("wildcards", 0))
        regs     = t.get("registrations", [])
        wcs      = t.setdefault("wildcard_entries", [])
        if wc_spots == 0:
            return await _reply(i, "❌ No wildcard spots.", ephemeral=True)
        spots_left = wc_spots - len(wcs)
        if spots_left <= 0:
            return await _reply(i, "❌ All wildcard spots already filled.", ephemeral=True)
        ranked = sorted(regs, key=lambda u: get_player_rank(i.guild.id, u))
        remaining = [u for u in ranked[bracket - wc_spots:] if u not in wcs]
        candidates = [m for m in (i.guild.get_member(u) for u in remaining) if m]
        if not candidates:
            return await _reply(i, "❌ No remaining candidates.", ephemeral=True)
        view = WildcardView(candidates[:25], spots_left)
        await _reply(i, 
            f"**{spots_left}** wildcard spot(s). Select from {len(candidates)} candidate(s):", view=view)
        await view.wait()
        if not view.confirmed: return
        for uid in view.selected:
            if uid not in wcs: wcs.append(uid)
        _save_comp(tournament_id, t)
        names = [i.guild.get_member(u).display_name for u in view.selected if i.guild.get_member(u)]
        await _reply(i, f"✅ Wildcards: **{', '.join(names)}**")

    # ── /tournament draw-generate ─────────────────────────────────────────
    @tournament.command(name="draw-generate", description="(Admin) Generate draw and full schedule.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=_ac_comp_all)
    async def tourn_draw_gen(self, i: discord.Interaction, tournament_id: str):
        # Defer FIRST — before any I/O — to claim the 3-second window immediately
        try:
            if not i.response.is_done():
                await i.response.defer()
        except (discord.errors.NotFound, discord.errors.HTTPException):
            # Interaction expired before we could defer (Replit idle / slow cold start)
            # Nothing we can do — silently drop it. User just needs to run the command again.
            return
        if not isinstance(i.user, discord.Member) or not _is_admin(i.user):
            return await _reply(i, "❌ Admin only.", ephemeral=True)
        t = _get_comp(tournament_id)
        if not t: return await _reply(i, "❌ Not found.", ephemeral=True)
        if t.get("status") == STATUS_COMPLETED:
            return await _reply(i, "❌ Already completed.", ephemeral=True)
        bracket = int(t["bracket_size"]); seeds = int(t.get("seeds",0))
        regs    = list(t.get("registrations",[])); wcs = list(t.get("wildcard_entries",[]))
        all_p   = regs + [u for u in wcs if u not in regs]
        if len(all_p) < 2:
            return await _reply(i, "❌ Need at least 2 players.", ephemeral=True)
        ranked = sorted(all_p, key=lambda u: get_player_rank(i.guild.id, u))[:bracket]
        draw, seeded = generate_draw(bracket, ranked, seeds)
        matches = _build_all_match_slots(bracket, draw, seeded)
        matches = schedule_matches(t, matches)
        t["draw"] = draw; t["seeded_players"] = seeded
        t["matches"] = matches; t["status"] = STATUS_ACTIVE
        _save_comp(tournament_id, t)

        # Send the draw first so Discord doesn't time out while sheets is working
        lines = [f"🎾 **Draw Generated — {t.get('name')}**\n"]
        lines += draw_text(draw, bracket, seeded, i.guild, tourn=t)
        lines.append(f"\n📅 {len(matches)} total match slots across {len(_rounds(bracket))} rounds.")
        pv = PageView(lines, per=25)
        await _reply(i, content=pv.content(), view=pv)

        # Now do the slow Google Sheets work — followup webhooks stay valid for 15 min
        if not _sheets_ok():
            await _reply(i, 
                "⚠️ Google Sheets not configured — install `gspread` and `google-auth`, "
                "and set `GOOGLE_SERVICE_ACCOUNT_JSON` in config to generate live bracket sheets.",
                ephemeral=True)
            return
        await _reply(i, "⏳ Creating Google Sheet…", ephemeral=True)
        url = create_sheet(t, guild=i.guild)
        if url:
            t["sheet_url"] = url; _save_comp(tournament_id, t)
            emb = discord.Embed(title="📊 Live Bracket Sheet",
                                color=discord.Color.gold(),
                                description=f"[Open Google Sheets Bracket]({url})")
            emb.add_field(name="Tournament", value=t.get("name","?"), inline=True)
            emb.add_field(name="Draw Size",  value=str(bracket),      inline=True)
            await _reply(i, embed=emb)
        else:
            await _reply(i, 
                "❌ Sheet creation failed — check the console for the full error.", ephemeral=True)

    # ── /tournament draw-view ─────────────────────────────────────────────
    @tournament.command(name="draw-view", description="Open the live bracket Google Sheet.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=_ac_comp_all)
    async def tourn_draw_view(self, i: discord.Interaction, tournament_id: str):
        t = _get_comp(tournament_id)
        if not t: return await _reply(i, "❌ Not found.", ephemeral=True)
        if not t.get("draw"):
            return await _reply(i, "❌ Draw not generated yet.", ephemeral=True)
        url = t.get("sheet_url")
        if not url:
            return await _reply(i, 
                "❌ No sheet available — Google Sheets is not configured or the draw was generated "
                "before a service account was set up.", ephemeral=True)
        emb = discord.Embed(title=f"📊 Live Bracket — {t.get('name','Tournament')}",
                            color=discord.Color.gold(),
                            description=f"[Open Google Sheets Bracket]({url})")
        emb.add_field(name="Draw Size", value=str(t.get("bracket_size","?")), inline=True)
        emb.add_field(name="Status",    value=t.get("status","?").upper(),    inline=True)
        await _reply(i, embed=emb)

    # ── /tournament schedule-view ─────────────────────────────────────────
    @tournament.command(name="schedule-view", description="View the match schedule.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=_ac_comp_all)
    async def tourn_sched(self, i: discord.Interaction, tournament_id: str,
                          day: Optional[int] = None):
        t = _get_comp(tournament_id)
        if not t: return await _reply(i, "❌ Not found.", ephemeral=True)
        if not t.get("matches"):
            return await _reply(i, "❌ No schedule yet — generate the draw first.", ephemeral=True)
        lines = schedule_text(t, i.guild, day)
        pv = PageView(lines, per=20)
        await _reply(i, content=pv.content(), view=pv)

    # ── /tournament match-edit ────────────────────────────────────────────
    @tournament.command(name="match-edit", description="(Admin) Edit a match's time or court.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=_ac_comp_all, match_id=_ac_match)
    async def tourn_match_edit(self, i: discord.Interaction,
                               tournament_id: str, match_id: str,
                               new_time: Optional[str] = None,
                               not_before: Optional[str] = None,
                               next_on_court: Optional[str] = None,
                               new_court_key: Optional[str] = None):
        if not isinstance(i.user, discord.Member) or not _is_admin(i.user):
            return await _reply(i, "❌ Admin only.", ephemeral=True)
        t = _get_comp(tournament_id)
        if not t: return await _reply(i, "❌ Tournament not found.", ephemeral=True)
        match = next((m for m in t.get("matches",[]) if m["match_id"] == match_id), None)
        if not match: return await _reply(i, "❌ Match not found.", ephemeral=True)
        if match.get("status") == "completed":
            return await _reply(i, "❌ Match already completed.", ephemeral=True)

        changes = []
        if new_court_key:
            if new_court_key not in t.get("venues",{}):
                return await _reply(i, f"❌ Court `{new_court_key}` not in this tournament.", ephemeral=True)
            # Check no other scheduled match on same court at same time (basic check)
            conflict = next((m for m in t.get("matches",[])
                             if m["match_id"] != match_id
                             and m.get("court_key") == new_court_key
                             and m.get("day") == match.get("day")
                             and m.get("session") == match.get("session")
                             and m.get("timing_type") == "session_start"
                             and m.get("scheduled_time") == match.get("scheduled_time")
                             and m.get("status") != "completed"), None)
            if conflict:
                return await _reply(i, 
                    f"❌ Conflict: Match `{conflict['match_id']}` is already on that court at that time.", ephemeral=True)
            match["court_key"]      = new_court_key
            match["court_venue_id"] = t["venues"].get(new_court_key)
            changes.append(f"Court → {COURT_DISPLAY.get(new_court_key, new_court_key)}")
        if new_time:
            match["scheduled_time"] = new_time.strip()
            match["timing_type"]    = "session_start"; changes.append(f"Time → {new_time}")
        elif not_before:
            match["scheduled_time"] = not_before.strip()
            match["timing_type"]    = "not_before";    changes.append(f"Not Before → {not_before}")
        elif next_on_court:
            match["scheduled_time"] = next_on_court.strip()
            match["timing_type"]    = "next_on";       changes.append(f"Next on → {COURT_DISPLAY.get(next_on_court, next_on_court)}")

        _save_comp(tournament_id, t)
        await _reply(i, f"✅ `{match_id}` updated: {', '.join(changes) or 'no changes.'}")

    # ── /tournament match-result ──────────────────────────────────────────
    @tournament.command(name="match-result", description="(Admin) Record a match result and award points.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=_ac_comp_all, match_id=_ac_match)
    async def tourn_result(self, i: discord.Interaction,
                           tournament_id: str, match_id: str,
                           winner_id: str, score: str):
        if not isinstance(i.user, discord.Member) or not _is_admin(i.user):
            return await _reply(i, "❌ Admin only.", ephemeral=True)
        t = _get_comp(tournament_id)
        if not t: return await _reply(i, "❌ Tournament not found.", ephemeral=True)
        match = next((m for m in t.get("matches",[]) if m["match_id"] == match_id), None)
        if not match: return await _reply(i, "❌ Match not found.", ephemeral=True)
        if match.get("status") == "completed":
            return await _reply(i, "❌ Already completed.", ephemeral=True)
        try:
            wid = int(winner_id)
        except ValueError:
            return await _reply(i, "❌ winner_id must be a numeric user ID.", ephemeral=True)

        p1 = match.get("player1_id"); p2 = match.get("player2_id")
        if wid not in (p1, p2):
            return await _reply(i, "❌ Winner must be one of the two players.", ephemeral=True)
        lid = p2 if wid == p1 else p1

        match["winner_id"] = wid; match["loser_id"] = lid
        match["score"] = score; match["status"] = "completed"

        # Award loser's round exit points
        cat = _get_cat(t.get("category_id","")) or {}
        rnd = match.get("round","")
        cat_key = ROUND_TO_CAT_KEY.get(rnd)
        loser_pts = int(cat.get(cat_key, 0)) if cat_key else 0

        guild_id = i.guild.id
        loser_m  = i.guild.get_member(lid)  if lid  else None
        winner_m = i.guild.get_member(wid)

        if lid and loser_pts > 0:
            _award_points(guild_id, lid, loser_pts, tournament_id, rnd,
                          name=loser_m.display_name if loser_m else "")
            t.setdefault("awarded_points",{}).setdefault(str(lid),{})[rnd] = loser_pts

        # Propagate winner into next round
        rnds = _rounds(int(t.get("bracket_size",8)))
        ridx = rnds.index(rnd) if rnd in rnds else -1
        if ridx >= 0 and ridx + 1 < len(rnds):
            next_rnd = rnds[ridx + 1]
            # Find which next-round slot this feeds into
            prev_rnd_matches = [m for m in t.get("matches",[]) if m["round"] == rnd]
            prev_rnd_matches.sort(key=lambda m: m["match_id"])
            match_idx = next((idx for idx, m in enumerate(prev_rnd_matches) if m["match_id"] == match_id), None)
            if match_idx is not None:
                next_slot_idx = match_idx // 2
                next_rnd_matches = sorted([m for m in t.get("matches",[]) if m["round"] == next_rnd],
                                          key=lambda m: m["match_id"])
                if next_slot_idx < len(next_rnd_matches):
                    slot = next_rnd_matches[next_slot_idx]
                    if match_idx % 2 == 0:
                        slot["player1_id"] = wid; slot["seed1"] = match.get("seed1") if wid == p1 else match.get("seed2")
                    else:
                        slot["player2_id"] = wid; slot["seed2"] = match.get("seed1") if wid == p1 else match.get("seed2")
                    if slot["player1_id"] and slot["player2_id"]:
                        slot["status"] = "scheduled"

        # H2H record
        from modules.venues import _get_venue
        venue_id = match.get("court_venue_id")
        surface  = "hard"
        if venue_id:
            try: v = _get_venue(venue_id); surface = v.get("surface","hard") if v else "hard"
            except Exception: pass
        record_h2h(guild_id, wid, lid, score, tournament_id, rnd, venue_id, surface)

        # Record match stats for both players
        winner_rank = get_player_rank(guild_id, wid)
        loser_rank  = get_player_rank(guild_id, lid) if lid else 99999
        if lid:
            record_match_stats(guild_id, wid, lid, True,  rnd, surface, tournament_id, loser_rank)
            record_match_stats(guild_id, lid, wid, False, rnd, surface, tournament_id, winner_rank)

        _save_comp(tournament_id, t)
        _snapshot_rankings(guild_id)

        # Update Google Sheet
        update_sheet(t, guild=i.guild if hasattr(i, "guild") else None)

        wname = winner_m.display_name if winner_m else f"UID:{wid}"
        lname = loser_m.display_name  if loser_m  else (f"UID:{lid}" if lid else "BYE")
        msg   = (f"✅ **{_rnd(rnd)}** result recorded: **{wname}** def. **{lname}** {score}\n"
                 f"🏅 {lname} awarded **{loser_pts}pts**" if loser_pts else
                 f"✅ **{_rnd(rnd)}** result: **{wname}** def. **{lname}** {score}")
        await _reply(i, msg)

    # ── /tournament complete ──────────────────────────────────────────────
    @tournament.command(name="complete", description="(Admin) Mark tournament finished and award champion points.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=_ac_comp_all)
    async def tourn_complete(self, i: discord.Interaction, tournament_id: str,
                              champion_id: str, finalist_score: str = ""):
        if not isinstance(i.user, discord.Member) or not _is_admin(i.user):
            return await _reply(i, "❌ Admin only.", ephemeral=True)
        t = _get_comp(tournament_id)
        if not t: return await _reply(i, "❌ Not found.", ephemeral=True)
        if t.get("status") == STATUS_COMPLETED:
            return await _reply(i, "❌ Already completed.", ephemeral=True)
        try: cid = int(champion_id)
        except ValueError: return await _reply(i, "❌ champion_id must be a user ID.", ephemeral=True)

        cat = _get_cat(t.get("category_id","")) or {}
        champ_pts    = int(cat.get("champion_pts", 0))
        finalist_pts = int(cat.get("finalist_pts", 0))
        guild_id     = i.guild.id
        champ_m      = i.guild.get_member(cid)
        champ_name   = champ_m.display_name if champ_m else f"UID:{cid}"

        # Award champion
        _award_points(guild_id, cid, champ_pts, tournament_id, "W", name=champ_name)
        t.setdefault("awarded_points",{}).setdefault(str(cid),{})["W"] = champ_pts

        # Award finalist (the other finalist from the Final match)
        final_m = next((m for m in t.get("matches",[]) if m["round"] == "F"), None)
        if final_m:
            lid = final_m.get("player1_id") if final_m.get("player2_id") == cid else final_m.get("player2_id")
            if lid and lid != cid:
                lm = i.guild.get_member(lid)
                _award_points(guild_id, lid, finalist_pts, tournament_id, "F",
                              name=lm.display_name if lm else "")
                t.setdefault("awarded_points",{}).setdefault(str(lid),{})["F"] = finalist_pts

        t["status"]        = STATUS_COMPLETED
        t["champion_id"]   = cid
        t["champion_name"] = champ_name
        t["completed_at"]  = datetime.now(timezone.utc).isoformat()
        _save_comp(tournament_id, t)
        _snapshot_rankings(guild_id)

        # Archive sheet
        archive_sheet(t)

        emb = discord.Embed(title=f"🏆 Tournament Complete: {t.get('name')}", color=discord.Color.gold())
        emb.add_field(name="Champion", value=f"**{champ_name}** (+{champ_pts}pts)", inline=False)
        if finalist_score: emb.add_field(name="Final Score", value=finalist_score, inline=False)
        await _reply(i, embed=emb)

    # ── /tournament set-style ─────────────────────────────────────────────
    @tournament.command(name="set-style", description="(Admin) Set Google Sheets bracket colours and font.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=_ac_comp_all, font=_ac_font)
    async def tourn_set_style(self, i: discord.Interaction, tournament_id: str,
                              background_color:  Optional[str] = None,
                              name_color:        Optional[str] = None,
                              winner_color:      Optional[str] = None,
                              loser_color:       Optional[str] = None,
                              name_bg_color:     Optional[str] = None,
                              score_bg_color:    Optional[str] = None,
                              font:              Optional[str] = None):
        if not isinstance(i.user, discord.Member) or not _is_admin(i.user):
            return await _reply(i, "❌ Admin only.", ephemeral=True)
        t = _get_comp(tournament_id)
        if not t: return await _reply(i, "❌ Not found.", ephemeral=True)
        sc = t.setdefault("sheets_config", {})
        for k, v in [("bg", background_color), ("fc1", name_color), ("fc2", winner_color),
                     ("fc3", loser_color), ("sc1", name_bg_color), ("sc2", score_bg_color),
                     ("font", font)]:
            if v is not None: sc[k] = v.strip()
        _save_comp(tournament_id, t)
        labels = {"bg":"Background","fc1":"Name Colour","fc2":"Winner Colour",
                  "fc3":"Loser Colour","sc1":"Name-Row BG","sc2":"Score-Row BG","font":"Font"}
        emb = discord.Embed(title="🎨 Style Updated", color=discord.Color.blurple())
        for k, v in sc.items(): emb.add_field(name=labels.get(k,k), value=v, inline=True)
        await _reply(i, embed=emb)

    # ═══════════════════════════════════════════════════════════════════════
    # /tournament point-defense
    # ═══════════════════════════════════════════════════════════════════════
    @tournament.command(name="point-defense",
                        description="(Admin) Strip everyone's points from a past tournament.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=_ac_comp_done)
    async def point_defense(self, i: discord.Interaction, tournament_id: str):
        if not isinstance(i.user, discord.Member) or not _is_admin(i.user):
            return await _reply(i, "❌ Admin only.", ephemeral=True)
        t = _get_comp(tournament_id)
        if not t: return await _reply(i, "❌ Not found.", ephemeral=True)
        if t.get("status") != STATUS_COMPLETED:
            return await _reply(i, "❌ Tournament not completed yet.", ephemeral=True)
        if t.get("point_defense_applied"):
            return await _reply(i, "❌ Point defense already applied for this tournament.", ephemeral=True)

        awarded = t.get("awarded_points", {})
        guild_id = i.guild.id; removed = []
        for uid_str, rounds in awarded.items():
            try: uid = int(uid_str)
            except ValueError: continue
            m = i.guild.get_member(uid)
            total = sum(int(v) for v in rounds.values())
            _award_points(guild_id, uid, -total, tournament_id, "defense",
                          name=m.display_name if m else "", is_defense=True)
            removed.append(f"{m.display_name if m else uid_str}: -{total}pts")

        t["point_defense_applied"] = True
        _save_comp(tournament_id, t)
        _snapshot_rankings(guild_id)

        emb = discord.Embed(title=f"🛡️ Point Defense Applied: {t.get('name','?')}",
                            color=discord.Color.red())
        emb.description = "\n".join(removed[:20]) or "No points were on record."
        if len(removed) > 20: emb.set_footer(text=f"…and {len(removed)-20} more.")
        await _reply(i, embed=emb)

    # ═══════════════════════════════════════════════════════════════════════
    # /rankings view
    # ═══════════════════════════════════════════════════════════════════════
    @rankings.command(name="view", description="View the current player rankings.")
    @app_commands.guild_only()
    async def rankings_view(self, i: discord.Interaction, page: int = 1):
        rows    = get_rankings_sorted(i.guild.id)
        per     = 20; start = (page-1)*per
        total_p = math.ceil(len(rows)/per) if rows else 1
        page    = max(1, min(page, total_p))
        start   = (page-1)*per
        chunk   = rows[start:start+per]
        emb = discord.Embed(title=f"🏆 Rankings — Page {page}/{total_p}", color=discord.Color.gold())
        lines = []
        for rank, row in enumerate(chunk, start+1):
            name  = row.get("name") or f"UID:{row.get('user_id','?')}"
            pts   = int(row.get("points",0))
            lines.append(f"**#{rank}** — {name} — **{pts}pts**")
        emb.description = "\n".join(lines) or "No rankings yet."
        await _reply(i, embed=emb)

    # ═══════════════════════════════════════════════════════════════════════
    # /rankings history
    # ═══════════════════════════════════════════════════════════════════════
    @rankings.command(name="history",
                      description="View a player's full rankings history and career stats.")
    @app_commands.guild_only()
    @app_commands.autocomplete(user_id=_ac_user)
    async def rankings_history(self, i: discord.Interaction,
                                user_id: Optional[str] = None):
        if not i.guild: return await _reply(i, "❌ Guild only.", ephemeral=True)
        try:
            uid = int(user_id) if user_id else i.user.id
        except ValueError:
            uid = i.user.id
        db = _rank_db(); g = _rank_guild(db, i.guild.id)
        entry = g.get(str(uid))
        if not entry:
            return await _reply(i, "❌ No rankings data for that player.", ephemeral=True)

        m     = i.guild.get_member(uid)
        name  = m.display_name if m else entry.get("name", f"UID:{uid}")
        pts   = int(entry.get("points",0))
        rank  = get_player_rank(i.guild.id, uid)
        ch_p  = int(entry.get("career_high_pts",0))
        cl_p  = entry.get("career_low_pts"); cl_p = int(cl_p) if cl_p is not None else 0
        ch_r  = entry.get("career_high_rank","—")
        cl_r  = entry.get("career_low_rank","—")

        snaps = entry.get("rankings_snapshots",[])
        hist  = entry.get("history",[])

        emb = discord.Embed(title=f"📊 Rankings History — {name}", color=discord.Color.blurple())
        emb.add_field(name="Current Points", value=f"**{pts}pts** (Rank #{rank})", inline=True)
        emb.add_field(name="Career High Pts",  value=f"**{ch_p}pts**",              inline=True)
        emb.add_field(name="Career Low Pts",   value=f"**{cl_p}pts**",              inline=True)
        emb.add_field(name="Best Rank",        value=f"**#{ch_r}**",                inline=True)
        emb.add_field(name="Worst Rank",       value=f"**#{cl_r}**",                inline=True)
        emb.add_field(name="Total Entries",    value=str(len(hist)),                inline=True)

        # Rankings trajectory (last 10 snapshots)
        if snaps:
            traj = snaps[-10:]
            traj_lines = [f"#{s['rank']} ({s['points']}pts) — {s['date'][:10]}" for s in traj]
            emb.add_field(name="📈 Recent Rankings Trajectory",
                          value="\n".join(traj_lines), inline=False)

        # Recent tournament history (last 10)
        if hist:
            hl = []
            for h in hist[-10:][::-1]:
                d_flag = " *(defense)*" if h.get("is_defense") else ""
                sign   = "+" if int(h.get("delta",0)) >= 0 else ""
                hl.append(f"{h.get('tournament_id','?')} | {h.get('round','?')} | "
                           f"**{sign}{h.get('delta',0)}pts**{d_flag} — {h.get('date','')[:10]}")
            emb.add_field(name="🏆 Recent History", value="\n".join(hl), inline=False)

        await _reply(i, embed=emb)

    # ═══════════════════════════════════════════════════════════════════════
    # /rankings h2h
    # ═══════════════════════════════════════════════════════════════════════
    @rankings.command(name="h2h", description="Head-to-head stats between two players.")
    @app_commands.guild_only()
    @app_commands.autocomplete(player1_id=_ac_user, player2_id=_ac_user)
    async def h2h(self, i: discord.Interaction, player1_id: str, player2_id: str):
        if not i.guild: return await _reply(i, "❌ Guild only.", ephemeral=True)
        try:
            uid1 = int(player1_id); uid2 = int(player2_id)
        except ValueError:
            return await _reply(i, "❌ Invalid player IDs.", ephemeral=True)
        if uid1 == uid2:
            return await _reply(i, "❌ Select two different players.", ephemeral=True)

        m1 = i.guild.get_member(uid1); m2 = i.guild.get_member(uid2)
        n1 = m1.display_name if m1 else f"UID:{uid1}"
        n2 = m2.display_name if m2 else f"UID:{uid2}"

        db  = _h2h_db(); g = db.get("h2h",{}).get(str(i.guild.id),{})
        key = _h2h_key(uid1, uid2); rec = g.get(key)

        emb = discord.Embed(title=f"⚔️ H2H — {n1} vs {n2}", color=discord.Color.purple())

        if not rec or not rec.get("matches"):
            emb.description = "No matches recorded between these players."
            return await _reply(i, embed=emb)

        matches = rec["matches"]
        w1 = sum(1 for m in matches if int(m.get("winner",0)) == uid1)
        w2 = len(matches) - w1
        emb.add_field(name="Overall",
                      value=f"**{n1}**: {w1} wins | **{n2}**: {w2} wins", inline=False)

        # By surface
        surf_stats: Dict[str, Dict] = {}
        for m in matches:
            s = m.get("surface","hard")
            ss = surf_stats.setdefault(s, {"w1":0,"w2":0})
            if int(m.get("winner",0)) == uid1: ss["w1"]+=1
            else: ss["w2"]+=1
        surf_lines = [f"**{s.title()}**: {ss['w1']}–{ss['w2']}" for s, ss in surf_stats.items()]
        if surf_lines: emb.add_field(name="By Surface", value="\n".join(surf_lines), inline=True)

        # By tournament
        tourn_stats: Dict[str, Dict] = {}
        for m in matches:
            tid = str(m.get("tournament_id","?"))
            ts  = tourn_stats.setdefault(tid, {"w1":0,"w2":0,"name":tid})
            ct  = _get_comp(tid)
            if ct: ts["name"] = ct.get("name",tid)
            if int(m.get("winner",0)) == uid1: ts["w1"]+=1
            else: ts["w2"]+=1
        tl = [f"**{v['name']}**: {v['w1']}–{v['w2']}" for v in list(tourn_stats.values())[:5]]
        if tl: emb.add_field(name="By Tournament", value="\n".join(tl), inline=True)

        # By round
        rnd_stats: Dict[str, Dict] = {}
        for m in matches:
            r = m.get("round","?")
            rs = rnd_stats.setdefault(r, {"w1":0,"w2":0})
            if int(m.get("winner",0)) == uid1: rs["w1"]+=1
            else: rs["w2"]+=1
        rl = [f"**{r}**: {v['w1']}–{v['w2']}" for r, v in rnd_stats.items()]
        if rl: emb.add_field(name="By Round", value="\n".join(rl), inline=True)

        # Recent matches
        recent = matches[-5:][::-1]
        rl2 = []
        for m in recent:
            winner_name = n1 if int(m.get("winner",0)) == uid1 else n2
            rl2.append(f"{m.get('tournament_id','?')} | {m.get('round','?')} | "
                       f"**{winner_name}** {m.get('score','')} — {m.get('date','')[:10]}")
        if rl2: emb.add_field(name="Recent Matches", value="\n".join(rl2), inline=False)

        await _reply(i, embed=emb)


    # ═══════════════════════════════════════════════════════════════════════
    # /stats career
    # ═══════════════════════════════════════════════════════════════════════
    @stats.command(name="career", description="View a player's full career stats.")
    @app_commands.guild_only()
    @app_commands.autocomplete(user_id=_ac_user)
    async def player_stats(self, i: discord.Interaction, user_id: Optional[str] = None):
        if not i.guild: return await _reply(i, "❌ Guild only.", ephemeral=True)
        try: uid = int(user_id) if user_id else i.user.id
        except ValueError: uid = i.user.id
        db = _stats_db(); g = _stats_guild(db, i.guild.id)
        p  = g.get(str(uid))
        m  = i.guild.get_member(uid)
        name = m.display_name if m else f"UID:{uid}"
        if not p:
            return await _reply(i, f"❌ No stats recorded for **{name}** yet.", ephemeral=True)

        mp = int(p.get("matches_played", 0)); mw = int(p.get("matches_won", 0))
        ml = int(p.get("matches_lost", 0));   wp = round(mw / mp * 100, 1) if mp else 0

        emb = discord.Embed(title=f"📊 Career Stats — {name}", color=discord.Color.blurple())
        emb.add_field(name="Overall Record",
                      value=(f"**{mw}–{ml}** ({wp}%)\n"
                            f"🏆 Titles: **{p.get('titles',0)}** | Finals: {p.get('finals',0)} | "
                            f"Semis: {p.get('semis',0)} | QF: {p.get('quarters',0)})"),
                      inline=False)

        # Serve
        fsi = p.get("first_serve_in",0); fst = p.get("first_serve_total",1)
        fspw = p.get("first_serve_pts_won",0); fspt = p.get("first_serve_pts_total",1)
        sspw = p.get("second_serve_pts_won",0); sspt = p.get("second_serve_pts_total",1)
        sg   = p.get("service_games",1); sgw = p.get("service_games_won",0)
        bpf  = p.get("bp_faced",0); bps = p.get("bp_saved",0)
        emb.add_field(name="🎾 Serve",
                      value=(f"Aces: **{p.get('aces',0)}** | DFs: {p.get('double_faults',0)}\n"
                            f"1st Serve: **{round(fsi/fst*100,1) if fst else 0}%** | "
                            f"1st Pts Won: **{round(fspw/fspt*100,1) if fspt else 0}%**\n"
                            f"2nd Pts Won: **{round(sspw/sspt*100,1) if sspt else 0}%**\n"
                            f"Hold%: **{round(sgw/sg*100,1) if sg else 0}%** | "
                            f"BP Saved: **{bps}/{bpf}** ({round(bps/bpf*100,1) if bpf else 0}%)"),
                      inline=True)

        # Return
        rg  = p.get("return_games",1); rgw = p.get("return_games_won",0)
        bpo = p.get("bp_opportunities",0); bpc = p.get("bp_converted",0)
        emb.add_field(name="↩️ Return",
                      value=(f"Return Games Won: **{round(rgw/rg*100,1) if rg else 0}%**\n"
                            f"BP Conv: **{bpc}/{bpo}** ({round(bpc/bpo*100,1) if bpo else 0}%)\n"
                            f"1st Return Pts Won: **{round(p.get('first_return_pts_won',0)/max(p.get('first_return_pts_total',1),1)*100,1)}%**\n"
                            f"2nd Return Pts Won: **{round(p.get('second_return_pts_won',0)/max(p.get('second_return_pts_total',1),1)*100,1)}%**"),
                      inline=True)

        # Tiebreaks + clutch
        tbp = p.get("tiebreaks_played",0); tbw = p.get("tiebreaks_won",0)
        dsp = p.get("deciding_set_played",0); dsw = p.get("deciding_set_won",0)
        emb.add_field(name="⚡ Pressure",
                      value=(f"Tiebreaks: **{tbw}/{tbp}** ({round(tbw/tbp*100,1) if tbp else 0}%)\n"
                            f"Deciding Set: **{dsw}/{dsp}** ({round(dsw/dsp*100,1) if dsp else 0}%)"),
                      inline=True)

        # Points/Games/Sets
        tpp = p.get("total_points_played",0); tpw = p.get("total_points_won",0)
        tgp = p.get("total_games_played",0);  tgw = p.get("total_games_won",0)
        tsp = p.get("total_sets_played",0);   tsw = p.get("total_sets_won",0)
        emb.add_field(name="📈 Points / Games / Sets",
                      value=(f"Points: **{tpw}/{tpp}** ({round(tpw/tpp*100,1) if tpp else 0}%)\n"
                            f"Games: **{tgw}/{tgp}** ({round(tgw/tgp*100,1) if tgp else 0}%)\n"
                            f"Sets: **{tsw}/{tsp}** ({round(tsw/tsp*100,1) if tsp else 0}%)\n"
                            f"Bagels W/C: {p.get('bagels_won',0)}/{p.get('bagels_conceded',0)}"),
                      inline=True)

        # Aggression
        w = p.get("winners",0); ue = p.get("unforced_errors",0)
        na = p.get("net_approaches",0); npw = p.get("net_pts_won",0); npt = p.get("net_pts_total",0)
        emb.add_field(name="💥 Aggression",
                      value=(f"Winners: **{w}** | UFE: {ue}\n"
                            f"W/UE Ratio: **{round(w/ue,2) if ue else '∞'}**\n"
                            f"Net: {npw}/{npt} ({round(npw/npt*100,1) if npt else 0}%)"),
                      inline=True)

        # vs Ranked
        def _tier(key):
            if ":" in key:
                outer, inner = key.split(":", 1)
                r = p.get(outer, {}).get(inner, {"w":0,"l":0})
            else:
                r = p.get(key, {"w":0,"l":0})
            tot = r["w"]+r["l"]
            return f"{r['w']}–{r['l']} ({round(r['w']/tot*100,1) if tot else 0}%)"
        emb.add_field(name="🆚 vs Ranked Players",
                      value=(f"vs #1: **{_tier('vs_rank_buckets:1')}**  "
                             f"vs #2–5: **{_tier('vs_rank_buckets:2-5')}**\n"
                             f"vs Top 5: **{_tier('vs_top5')}**  "
                             f"vs Top 10: **{_tier('vs_top10')}**\n"
                             f"vs Top 25: **{_tier('vs_top25')}**  "
                             f"vs Top 50: **{_tier('vs_top50')}**\n"
                             f"vs Top 100: **{_tier('vs_top100')}**  "
                             f"vs Unranked: **{_tier('vs_unranked')}**"),
                      inline=False)

        # Rank bucket breakdown
        buckets = p.get("vs_rank_buckets", {})
        if buckets:
            blines = []
            for bkt in ["1","2-5","6-10","11-20","21-50","51+"]:
                r = buckets.get(bkt, {"w":0,"l":0}); tot = r["w"]+r["l"]
                if tot:
                    blines.append(f"**#{bkt}**: {r['w']}–{r['l']} "
                                  f"({round(r['w']/tot*100,1)}%)")
            if blines:
                emb.add_field(name="📊 Wins by Opponent Rank",
                              value="  ".join(blines), inline=False)

        # vs Ranked by Surface
        vrs = p.get("vs_ranked_surface", {})
        if vrs:
            slines = []
            for surf, tiers in vrs.items():
                t10 = tiers.get("vs_top10", {"w":0,"l":0})
                tot10 = t10["w"]+t10["l"]
                t50 = tiers.get("vs_top50", {"w":0,"l":0})
                tot50 = t50["w"]+t50["l"]
                if tot50:
                    slines.append(f"{surf.title()} — T10: **{t10['w']}–{t10['l']}**  "
                                  f"T50: **{t50['w']}–{t50['l']}**")
            if slines:
                emb.add_field(name="🌍 vs Ranked by Surface",
                              value="\n".join(slines), inline=False)

        # vs Ranked by Round
        vrr = p.get("vs_ranked_round", {})
        if vrr:
            rlines = []
            for rnd2 in ["Final","Semifinal","Quarterfinal","Round of 16","Round of 32"]:
                # find matching key
                rnd_key = next((k for k in vrr if _rnd(k) == rnd2), None)
                if not rnd_key: continue
                t10 = vrr[rnd_key].get("vs_top10", {"w":0,"l":0})
                tot = t10["w"]+t10["l"]
                if tot:
                    rlines.append(f"{rnd2}: **{t10['w']}–{t10['l']}** vs Top 10")
            if rlines:
                emb.add_field(name="🏆 vs Top 10 by Round",
                              value="\n".join(rlines), inline=False)

        # Best wins
        bw = p.get("best_wins", [])
        if bw:
            bwlines = []
            for win in bw[:5]:
                opp = i.guild.get_member(int(win.get("opponent_id",0)))
                oname = opp.display_name if opp else f"UID:{win.get('opponent_id','?')}"
                bwlines.append(f"**#{win['opponent_rank']}** {oname} — "
                               f"{_rnd(win.get('round','?'))} · {win.get('surface','?').title()} · "
                               f"{win.get('date','')}")
            emb.add_field(name="⭐ Best Wins (by Opponent Rank)",
                          value="\n".join(bwlines), inline=False)

        # Streaks
        emb.add_field(name="🔥 Streaks",
                      value=(f"Current Win Streak: **{p.get('current_win_streak',0)}**\n"
                            f"Current Loss Streak: **{p.get('current_loss_streak',0)}**\n"
                            f"Best Win Streak: **{p.get('best_win_streak',0)}**"),
                      inline=True)

        # Surface splits
        surf = p.get("surface", {})
        if surf:
            slines = []
            for s, rec in surf.items():
                sp = rec.get("played",0); sw = rec.get("won",0)
                slines.append(f"{s.title()}: **{sw}–{sp-sw}** ({round(sw/sp*100,1) if sp else 0}%)")
            emb.add_field(name="🌍 By Surface", value="\n".join(slines), inline=True)

        await _reply(i, embed=emb)

    # ═══════════════════════════════════════════════════════════════════════
    # /stats year
    # ═══════════════════════════════════════════════════════════════════════
    @stats.command(name="year", description="View a player's stats for a specific year.")
    @app_commands.guild_only()
    @app_commands.autocomplete(user_id=_ac_user)
    async def stats_year(self, i: discord.Interaction,
                         year: int,
                         user_id: Optional[str] = None):
        if not i.guild: return await _reply(i, "❌ Guild only.", ephemeral=True)
        try: uid = int(user_id) if user_id else i.user.id
        except ValueError: uid = i.user.id
        db = _stats_db(); g = _stats_guild(db, i.guild.id)
        p  = g.get(str(uid)); m = i.guild.get_member(uid)
        name = m.display_name if m else f"UID:{uid}"
        if not p:
            return await _reply(i, f"❌ No stats for **{name}**.", ephemeral=True)
        yr = p.get("year", {}).get(str(year))
        if not yr:
            return await _reply(i, f"❌ No data for **{name}** in **{year}**.", ephemeral=True)
        mp = yr.get("matches_played",0); mw = yr.get("matches_won",0)
        emb = discord.Embed(title=f"📅 {year} Stats — {name}", color=discord.Color.green())
        emb.add_field(name="Record", value=f"**{mw}–{mp-mw}** ({round(mw/mp*100,1) if mp else 0}%)", inline=True)
        emb.add_field(name="Titles", value=str(yr.get("titles",0)), inline=True)
        emb.add_field(name="Finals", value=str(yr.get("finals",0)), inline=True)
        emb.add_field(name="Semis",  value=str(yr.get("semis",0)),  inline=True)
        emb.add_field(name="QF",     value=str(yr.get("quarters",0)), inline=True)
        emb.add_field(name="Tournaments", value=str(len(yr.get("tournaments",[]))), inline=True)
        emb.add_field(name="Aces",         value=str(yr.get("aces",0)),         inline=True)
        emb.add_field(name="Double Faults",value=str(yr.get("double_faults",0)),inline=True)
        emb.add_field(name="Points Earned",value=str(yr.get("points_earned",0)),inline=True)
        await _reply(i, embed=emb)

    # ═══════════════════════════════════════════════════════════════════════
    # /rankings leaderboard
    # ═══════════════════════════════════════════════════════════════════════
    @rankings.command(name="leaderboard", description="View various player leaderboards.")
    @app_commands.guild_only()
    @app_commands.choices(category=[
        app_commands.Choice(name="Titles",          value="titles"),
        app_commands.Choice(name="Win %",           value="win_pct"),
        app_commands.Choice(name="Aces",            value="aces"),
        app_commands.Choice(name="Double Faults",   value="double_faults"),
        app_commands.Choice(name="Winners",         value="winners"),
        app_commands.Choice(name="Break Points Converted", value="bp_conv"),
        app_commands.Choice(name="Tiebreaks Won",   value="tiebreaks_won"),
        app_commands.Choice(name="Best Win Streak", value="best_win_streak"),
        app_commands.Choice(name="vs Top 10 Wins",  value="vs_top10"),
    ])
    async def leaderboard(self, i: discord.Interaction, category: str = "titles"):
        if not i.guild: return await _reply(i, "❌ Guild only.", ephemeral=True)
        db = _stats_db(); g = _stats_guild(db, i.guild.id)

        def _sort_key(p):
            if category == "titles":        return int(p.get("titles", 0))
            if category == "win_pct":
                mp = int(p.get("matches_played", 0))
                return round(int(p.get("matches_won",0)) / mp * 100, 2) if mp >= 10 else -1
            if category == "aces":          return int(p.get("aces", 0))
            if category == "double_faults": return int(p.get("double_faults", 0))
            if category == "winners":       return int(p.get("winners", 0))
            if category == "bp_conv":
                bpo = int(p.get("bp_opportunities", 0))
                return round(int(p.get("bp_converted", 0)) / bpo * 100, 2) if bpo else -1
            if category == "tiebreaks_won": return int(p.get("tiebreaks_won", 0))
            if category == "best_win_streak": return int(p.get("best_win_streak", 0))
            if category == "vs_top10":      return int(p.get("vs_top10", {}).get("w", 0))
            return 0

        rows = sorted(g.values(), key=_sort_key, reverse=True)[:15]
        cat_labels = {"titles":"Titles","win_pct":"Win %","aces":"Aces",
                      "double_faults":"Double Faults","winners":"Winners",
                      "bp_conv":"BP Conv %","tiebreaks_won":"Tiebreaks Won",
                      "best_win_streak":"Best Win Streak","vs_top10":"vs Top 10 Wins"}
        emb = discord.Embed(title=f"🏆 Leaderboard — {cat_labels.get(category,category)}",
                            color=discord.Color.gold())
        lines = []
        medals = ["🥇","🥈","🥉"]
        for rank, p in enumerate(rows, 1):
            uid  = int(p.get("user_id", 0))
            mem  = i.guild.get_member(uid)
            name = mem.display_name if mem else f"UID:{uid}"
            val  = _sort_key(p)
            if val < 0: continue
            icon = medals[rank-1] if rank <= 3 else f"**{rank}.**"
            lines.append(f"{icon} **{name}** — {val}{'%' if category in ('win_pct','bp_conv') else ''}")
        emb.description = "\n".join(lines) or "No data yet."
        await _reply(i, embed=emb)

    # ═══════════════════════════════════════════════════════════════════════
    # /admin history-wipe  (OWNER ONLY)
    # ═══════════════════════════════════════════════════════════════════════
    @admin.command(name="history-wipe",
                   description="[OWNER ONLY] Wipe ALL history: rankings, stats, H2H, points, everything.")
    @app_commands.guild_only()
    async def history_wipe(self, i: discord.Interaction, confirm: str = ""):
        if i.user.id != OWNER_ID:
            return await _reply(i, "❌ This command is restricted to the bot owner.", ephemeral=True)
        if confirm.strip().lower() != "wipe everything":
            return await _reply(i, 
                '⚠️ To confirm, run this command with `confirm: wipe everything`\n'
                '**This will permanently delete:**\n'
                '• All rankings & points\n• All career stats\n• All H2H records\n'
                '• All rankings history & snapshots\n• All year records\n'
                '• All awarded points in tournaments\n• All point defense records',
                ephemeral=True)

        guild_id = str(i.guild.id)
        wiped = []

        # Rankings
        db = _rank_db()
        if guild_id in db.get("guilds", {}):
            db["guilds"][guild_id] = {}
            _rank_save(db); wiped.append("Rankings & points history")

        # Stats
        db = _stats_db()
        if guild_id in db.get("guilds", {}):
            db["guilds"][guild_id] = {}
            _stats_save(db); wiped.append("Career stats")

        # H2H
        db = _h2h_db()
        if guild_id in db.get("h2h", {}):
            db["h2h"][guild_id] = {}
            _h2h_save(db); wiped.append("Head-to-head records")

        # Tournament awarded_points + point_defense flags
        db = _comp_db()
        count = 0
        for t in db.get("tournaments", {}).values():
            if str(t.get("guild_id","")) == guild_id:
                t["awarded_points"] = {}
                t["point_defense_applied"] = False
                count += 1
        if count: _comp_save(db); wiped.append(f"Awarded points in {count} tournament(s)")

        emb = discord.Embed(title="🗑️ History Wiped", color=discord.Color.red())
        emb.description = "**Wiped:**\n" + "\n".join(f"• {w}" for w in wiped)
        emb.set_footer(text=f"Executed by {i.user.display_name}")
        await _reply(i, embed=emb)


async def setup(bot: commands.Bot):
    await bot.add_cog(TournamentsCog(bot))