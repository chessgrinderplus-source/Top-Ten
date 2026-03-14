# modules/matchsim.py
from __future__ import annotations

import asyncio
import json
import math
import os
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
import discord
import time
from discord import app_commands
from discord.ext import commands
from typing import Dict, Optional, Tuple, List, TYPE_CHECKING, Union, Any

from time import monotonic
from datetime import datetime, timezone

from modules.players import ensure_player_for_member, set_fatigue_for_user_id, set_player_row_by_id
from modules.academy import academy_can_challenge
from dataclasses import dataclass
from modules.venues import _get_venues, _get_user_inv, _get_venue, _tourn_db
from modules.economy import get_balance, remove_balance, add_balance
from modules.gear import gear_get_equipped, gear_get_equipped_strung_count_for_frame, gear_has_shoes_equipped

KMH_TO_MPH = 0.621371
# Fatigue recovery: fitness=1 → 100 fatigue gone in ~72h (3 days); fitness=99 → ~18h
# Formula: decay_rate = lerp(100/72, 100/18, fitness_t) per hour
_FATIGUE_DECAY_MIN = 100.0 / 72.0    # ~1.39/hr at fitness=1
_FATIGUE_DECAY_MAX = 100.0 / 18.0    # ~5.56/hr at fitness=99

MATCH_SPEED_MULT = 0.25  # 1.0 = normal, 0.5 = 2x faster, 0.25 = 4x faster
RECOVERY_COOLDOWN_HOURS = 6

_bot_names_cache: Optional[Tuple[float, List[str]]] = None
_bot_names_cache_ttl = 5.0

PENDING_MATCHES: dict[str, dict] = {}
InteractionT = "Interaction"

_BALLS_FIRST_CHANGE_GAMES = 7
_BALLS_INTERVAL_GAMES = 9


def _balls_change_times_upto(max_games: int) -> List[int]:
    out: List[int] = []
    t = _BALLS_FIRST_CHANGE_GAMES
    while t < max_games:
        out.append(t)
        t += _BALLS_INTERVAL_GAMES
    return out


def _max_games_for_best_of(best_of: int) -> int:
    return best_of * 13


def strung_rackets_needed_for_match(best_of: int) -> int:
    max_games = _max_games_for_best_of(best_of)
    changes = len(_balls_change_times_upto(max_games))
    return 1 + changes


def _cooldown_remaining_seconds(last_iso: Optional[str], hours: int) -> int:
    if not last_iso:
        return 0
    dt = _parse_iso(last_iso)
    if not dt:
        return 0
    now = _now_utc()
    elapsed = (now - dt).total_seconds()
    remain = (hours * 3600) - elapsed
    return int(remain) if remain > 0 else 0

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)

def _parse_iso(s: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _fatigue_decay_per_hour(fitness: float) -> float:
    """Fitness-scaled fatigue recovery rate.
    fitness=1  → 100 fatigue clears in ~72h (3 days)
    fitness=99 → 100 fatigue clears in ~18h
    """
    t = max(0.0, min(1.0, (float(fitness) - 1.0) / 98.0))
    return _FATIGUE_DECAY_MIN + t * (_FATIGUE_DECAY_MAX - _FATIGUE_DECAY_MIN)


def apply_passive_fatigue_decay(row: Dict[str, Any]) -> Dict[str, Any]:
    fatigue = float(row.get("fatigue", 0) or 0)
    fatigue = max(0.0, min(100.0, fatigue))
    last = _parse_iso(str(row.get("fatigue_updated_at", "")))
    now = _now_utc()
    if last is None:
        row["fatigue_updated_at"] = now.isoformat()
        return row
    hours = max(0.0, (now - last).total_seconds() / 3600.0)
    if hours <= 0:
        return row
    fitness = float(row.get("fitness", 1) or 1)
    rate = _fatigue_decay_per_hour(fitness)
    fatigue = max(0.0, fatigue - rate * hours)
    row["fatigue"] = fatigue
    row["fatigue_updated_at"] = now.isoformat()
    return row


def _gear_defaults():
    return {
        "racket_power": 50.0,
        "racket_spin": 50.0,
        "racket_control": 50.0,
        "shoe_footwork": 50.0,
    }

def _read_gear_for_user(guild_id: int, user_id: int) -> Dict[str, float]:
    d = _gear_defaults()
    try:
        racket, shoes = gear_get_equipped(guild_id, user_id)
        if isinstance(racket, dict):
            d["racket_power"] = float(racket.get("power", d["racket_power"]))
            d["racket_spin"] = float(racket.get("spin", d["racket_spin"]))
            d["racket_control"] = float(racket.get("control", d["racket_control"]))
            d["strung_pattern"] = racket.get("strung_pattern", "")
            d["strung_tension"] = float(racket.get("strung_tension", 55))
            d["strung_weight"] = racket.get("strung_weight", "")
        if isinstance(shoes, dict):
            d["shoe_footwork"] = float(shoes.get("footwork_impact", shoes.get("footwork", d["shoe_footwork"])))
    except Exception:
        pass
    return d

def _is_enabled(row: dict) -> bool:
    if row is None:
        return False
    if "enabled" not in row and "disabled" in row:
        return not bool(row.get("disabled"))
    v = row.get("enabled", True)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "1", "yes", "y", "on", "enabled"):
            return True
        if s in ("false", "0", "no", "n", "off", "disabled"):
            return False
        return True
    if v is None:
        return True
    return bool(v)

# =========================
# Tuning knobs
# =========================
# ──────────────────────────────────────────────────────────────────────────
# RNG KNOBS — easy to tune
# ──────────────────────────────────────────────────────────────────────────
# CHAOS_RATE   : Fraction of outcomes that are completely random (frame shots,
#                net cords, lucky calls). 0.0 = pure skill, 0.30 = very wild.
CHAOS_RATE     = 0.18

# TILT_RATE    : Fraction of outcomes where a player briefly chokes regardless
#                of skill. Adds drama and upsets. 0.0 = no chokes, 0.05 = frequent.
TILT_RATE      = 0.05

# BETA_SHAPE   : Shape param for the beta distribution noise (same value used
#                for both alpha and beta). Lower = more extreme (0/1 outcomes),
#                0.5 = moderate, 1.0 = no noise. Recommended range: 0.20–0.70.
BETA_SHAPE     = 0.30

# SKILL_WEIGHT : How much true probability anchors the result (vs pure chaos).
#                0.0 = skill irrelevant, 1.0 = pure skill. Works with BETA_SHAPE.
SKILL_WEIGHT   = 0.30   # 38% skill, 62% chaotic beta noise

# MOMENTUM_GAIN  : How much momentum a player gains per point won (0.0–0.5).
MOMENTUM_GAIN  = 0.15

# MOMENTUM_LOSS  : How much momentum drops per point lost (0.0–0.5).
MOMENTUM_LOSS  = 0.10

# MOMENTUM_DECAY : Multiplier applied each point — keeps streaks from lasting forever.
#                  0.90 = fades quickly, 0.97 = very persistent streaks.
MOMENTUM_DECAY = 0.94

# MOMENTUM_EFFECT : Max probability shift from momentum (±value). 0.30 = ±30%.
MOMENTUM_EFFECT = 0.50

P_MIN, P_MAX = 0.0, 1.0  # full clamp range

SLEEP_MIN = 0.001
SLEEP_MAX = 0.005

STAMINA_START = 100.0
STAMINA_MIN = 0.0

CHALLENGE_TIMEOUT = 30


def _as_bool(v, default: bool = True) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true", "t", "yes", "y", "1", "on", "enabled"):
            return True
        if s in ("false", "f", "no", "n", "0", "off", "disabled"):
            return False
    return default

def _data_dir() -> str:
    try:
        import config  # type: ignore
        d = getattr(config, "DATA_DIR", "data")
        return str(d)
    except Exception:
        return "data"


BOTS_PATH = os.path.join(_data_dir(), "matchsim_bots.json")
LOADOUT_PRESETS_PATH = os.path.join(_data_dir(), "loadout_presets.json")
LOADOUT_INV_PATH = os.path.join(_data_dir(), "loadout_inventory.json")

def _loadout_presets_db() -> Dict[str, Any]:
    raw = _load_json(LOADOUT_PRESETS_PATH, {})
    if isinstance(raw, dict) and "presets" in raw and isinstance(raw["presets"], dict):
        return {"presets": raw["presets"]}
    if isinstance(raw, dict):
        return {"presets": raw}
    return {"presets": {}}

def _loadout_inv_db() -> Dict[str, Any]:
    return _load_json(LOADOUT_INV_PATH, {"inv": {}})

def _loadout_inv_row(guild_id: int, user_id: int) -> Dict[str, Any]:
    db = _loadout_inv_db()
    g = db.setdefault("inv", {}).setdefault(str(guild_id), {})
    return g.setdefault(str(user_id), {
        "has_custom": False,
        "custom_name": "Custom Loadout",
        "custom_sliders": {k: 50 for k in SLIDER_KEYS},
    })

def _normalize_sliders(sliders: Dict[str, Any]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    sliders = sliders or {}
    for k in SLIDER_KEYS:
        try:
            v = int(sliders.get(k, 50))
        except Exception:
            v = 50
        out[k] = max(0, min(100, v))
    return out

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path: str, data) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = f"{path}.{uuid.uuid4().hex}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _bots_db() -> Dict:
    return _load_json(BOTS_PATH, {"bots": {}})


def _bots_save(db: Dict) -> None:
    _save_json(BOTS_PATH, db)


def _bot_get(name: str) -> Optional[Dict]:
    db = _bots_db()
    bots = db.get("bots", {})
    changed = False
    for _, row in bots.items():
        if isinstance(row, dict):
            new_val = _as_bool(row.get("enabled", True), True)
            if row.get("enabled") != new_val:
                row["enabled"] = new_val
                changed = True
    if changed:
        _bots_save(db)
    return db.get("bots", {}).get(name)


def _bot_set(name: str, bot_row: Dict) -> None:
    db = _bots_db()
    db.setdefault("bots", {})[name] = bot_row
    _bots_save(db)


def _bot_delete(name: str) -> bool:
    db = _bots_db()
    bots = db.get("bots", {})
    if name in bots:
        del bots[name]
        _bots_save(db)
        return True
    return False


def _bot_names(enabled_only: bool = False) -> List[str]:
    db = _bots_db()
    bots = db.get("bots", {})
    names = []
    for n, row in bots.items():
        if enabled_only and not _as_bool(row.get("enabled", True), True):
            continue
        names.append(n)
    return sorted(names, key=lambda s: s.lower())

async def _bot_autocomplete(interaction: discord.Interaction, current: str):
    try:
        db = _bots_db()
        bots = db.get("bots", {}) or {}
    except Exception:
        bots = {}
    cur = (current or "").strip().lower()
    out: list[app_commands.Choice[str]] = []
    for name, row in bots.items():
        if not isinstance(row, dict):
            continue
        if not _as_bool(row.get("enabled", True), True):
            continue
        sname = str(name)
        if cur and cur not in sname.lower():
            continue
        out.append(app_commands.Choice(name=sname[:100], value=sname))
        if len(out) >= 25:
            break
    out.sort(key=lambda c: c.name.lower())
    return out

async def _venue_autocomplete_admin(
    interaction: discord.Interaction, current: str
) -> list[app_commands.Choice[str]]:
    """Autocomplete all venues (admin — not filtered by ownership)."""
    try:
        from modules.venues import _get_venues
        venues = _get_venues()
    except Exception:
        venues = {}
    cur = (current or "").strip().lower()
    out: list[app_commands.Choice[str]] = []
    for vid, row in venues.items():
        if not isinstance(row, dict):
            continue
        name  = str(row.get("name", vid))
        tid   = str(row.get("tournament_id", ""))
        label = f"{name} [{tid}]" if tid else name
        if cur and cur not in label.lower() and cur not in vid.lower():
            continue
        out.append(app_commands.Choice(name=label[:100], value=vid))
        if len(out) >= 25:
            break
    return out


# -------------------------
# Loadout sliders (0..100)
# -------------------------
def resolve_loadout_sliders(choice: str | None, guild_id: int, user_id: int) -> dict:
    if not choice or choice == "__BALANCED__":
        return balanced_sliders()
    if choice == "__CUSTOM__":
        inv = _loadout_inv_row(guild_id, user_id)
        sliders = inv.get("custom_sliders") or {}
        return _normalize_sliders(sliders)
    presets = _loadout_presets_db().get("presets", {}) or {}
    return resolve_preset_sliders(choice, presets)

def _rest_seconds_from_slider(v: int) -> float:
    v = max(0, min(100, int(v)))
    return 6.0 + (20.0 * (v / 100.0))

# Canonical SLIDER_KEYS – must match loadouts.py exactly
SLIDER_KEYS: list[str] = [
    "fh_power",            # Forehand Power       – pace vs risk
    "fh_spin",             # Forehand Spin        – margin vs pace
    "bh_power",            # Backhand Power
    "bh_spin",             # Backhand Spin
    "serve_power",         # Serve Power
    "serve_spin",          # Serve Spin
    "shot_dir_risk",       # Shot Direction Risk  – DTL frequency/risk
    "serve_variety",       # Serve Variety
    "drop_frequency",      # Drop Shot Frequency
    "deuce_spin",          # Deuce Serve Spin
    "deuce_place",         # Deuce Serve Placement (0=Wide 34=Center 67=T)
    "ad_spin",             # Ad Serve Spin
    "ad_place",            # Ad Serve Placement
    "pressure_play_risk",  # Pressure Play Risk
    "return_position",     # Return Position (high=inside, low=deep)
    "movement_aggression", # Movement Aggression
    "time_btwn_points",    # Time Between Points (server's service games)
    "slice_usage",         # Slice Usage
]

def balanced_sliders():
    return {k: 50 for k in SLIDER_KEYS}

# ── loadout deviation helpers ─────────────────────────────────────────────────
def _lo(sl: dict, key: str) -> float:
    """Normalized deviation: -1.0 (slider=0) … 0 (slider=50) … +1.0 (slider=100)."""
    return (float(sl.get(key, 50)) - 50.0) / 50.0

_MAX_LO = 20.0   # ±20 stat-point maximum effect from any single loadout slider

def _serve_place_bucket(v: float) -> str:
    """0-33 → Wide, 34-66 → Center, 67-100 → T"""
    if v <= 33:  return "Wide"
    if v <= 66:  return "Center"
    return "T"

def _return_pos_y(sl: dict) -> float:
    """Maps return_position slider to court Y (0=net, 1=deep behind baseline).
    High slider (inside) → y≈0.65; low slider (deep) → y≈1.05"""
    v = float(sl.get("return_position", 50)) / 100.0
    return 1.05 + (0.65 - 1.05) * v   # lerp(1.05, 0.65, v)

def _baseline_y(sl: dict) -> float:
    """Maps movement_aggression to preferred baseline rally depth.
    High aggression → y≈0.75 (inside baseline); low → y≈1.05 (behind baseline)"""
    v = float(sl.get("movement_aggression", 50)) / 100.0
    return 1.05 + (0.75 - 1.05) * v   # lerp(1.05, 0.75, v)

def resolve_preset_sliders(preset_id: str | None, presets: dict) -> dict:
    if not preset_id:
        return balanced_sliders()
    p = presets.get(preset_id)
    if not p or "sliders" not in p:
        return balanced_sliders()
    out = {}
    for k in SLIDER_KEYS:
        v = int(p["sliders"].get(k, 50))
        out[k] = max(0, min(100, v))
    return out

def apply_loadout_to_profile(p: "PlayerProfile", sliders: dict) -> None:
    """Map canonical SLIDER_KEYS onto PlayerProfile loadout mirror fields."""
    s = sliders or {}
    def _s(key: str, default: float = 50.0) -> float:
        try:   return float(s.get(key, default))
        except: return default
    p.lo_fh_power            = _s("fh_power")
    p.lo_fh_spin             = _s("fh_spin")
    p.lo_bh_power            = _s("bh_power")
    p.lo_bh_spin             = _s("bh_spin")
    p.lo_serve_power         = _s("serve_power")
    p.lo_serve_spin          = _s("serve_spin")
    p.lo_dir_risk            = _s("shot_dir_risk")
    p.lo_serve_variety       = _s("serve_variety")
    p.lo_drop_frequency      = _s("drop_frequency")
    p.lo_deuce_spin          = _s("deuce_spin")
    p.lo_deuce_place         = _s("deuce_place")
    p.lo_ad_spin             = _s("ad_spin")
    p.lo_ad_place            = _s("ad_place")
    p.lo_pressure_play_risk  = _s("pressure_play_risk")
    p.lo_return_position     = _s("return_position")
    p.lo_movement_aggression = _s("movement_aggression")
    p.lo_time_btwn_points    = _s("time_btwn_points")
    p.lo_slice_usage         = _s("slice_usage")

# Units
def _c_to_f(c: float) -> float:
    return (c * 9.0 / 5.0) + 32.0

def _kmh_to_mph(kmh: float) -> float:
    return kmh * 0.621371

def _m_to_ft(m: float) -> float:
    return m * 3.28084

def _tournament_units_for(conditions) -> tuple[str, str, str]:
    weather_unit, speed_unit, altitude_unit = "C", "KMH", "M"
    tid = getattr(conditions, "tournament_id", None)
    if tid:
        try:
            tdb = _tourn_db().get("tournaments", {})
            row = tdb.get(tid, {}) or {}
            weather_unit = str(row.get("weather_unit", "C")).upper()
            speed_unit = str(row.get("speed_unit", "KMH")).upper()
            altitude_unit = str(row.get("altitude_unit", "M")).upper()
        except Exception:
            pass
    return weather_unit, speed_unit, altitude_unit

# =========================
# Utility math
# =========================

def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def sigmoid(x: float) -> float:
    return 1.0 / (1.0 + math.exp(-x))


def lerp(a: float, b: float, t: float) -> float:
    return a + (b - a) * t


def bounded_rng(p: float, hot: float = 0.0) -> float:
    """
    Randomized probability function. Tune using the RNG KNOBS above.

    1. TILT_RATE   chance: player chokes — output collapses toward failure.
    2. CHAOS_RATE  chance: completely random outcome (frame shot, net cord, etc.)
    3. Otherwise: blend of true probability (SKILL_WEIGHT) + beta noise (1-SKILL_WEIGHT)
       with momentum shifting the probability by up to ±MOMENTUM_EFFECT.
    """
    r = random.random()

    # Tilt event — choke regardless of skill
    if r < TILT_RATE:
        return float(clamp(random.betavariate(0.20, 2.5), 0.0, 0.35))

    # Pure chaos event — frame shot, net cord, lucky call, etc.
    if r < TILT_RATE + CHAOS_RATE:
        return float(random.random())

    # Momentum-adjusted probability
    p_adj = float(clamp(p + hot * MOMENTUM_EFFECT, 0.0, 1.0))

    # Beta noise — U-shaped when BETA_SHAPE < 1
    try:
        beta_noise = random.betavariate(BETA_SHAPE, BETA_SHAPE)
    except Exception:
        beta_noise = random.random()

    result = p_adj * SKILL_WEIGHT + beta_noise * (1.0 - SKILL_WEIGHT)
    return float(clamp(result, 0.0, 1.0))


# ─── Momentum tracking ────────────────────────────────────────────────
# Module-level vars: set before each point by _run_match_loop so
# bounded_rng() can pick them up without threading state everywhere.
_P1_HOT: float = 0.0   # -1.0 (ice cold) to +1.0 (scorching)
_P2_HOT: float = 0.0

def _momentum_hot(server_idx: int, is_server: bool) -> float:
    """Return hot factor for the active player."""
    global _P1_HOT, _P2_HOT
    idx = server_idx if is_server else (1 - server_idx)
    return _P1_HOT if idx == 0 else _P2_HOT

def _update_momentum(winner_idx: int) -> None:
    """Update momentum after each point using the RNG KNOBS above."""
    global _P1_HOT, _P2_HOT
    delta_hot  =  random.uniform(MOMENTUM_GAIN * 0.5, MOMENTUM_GAIN * 1.5)
    delta_cold = -random.uniform(MOMENTUM_LOSS * 0.5, MOMENTUM_LOSS * 1.5)
    if winner_idx == 0:
        _P1_HOT = float(clamp(_P1_HOT + delta_hot,  -1.0, 1.0))
        _P2_HOT = float(clamp(_P2_HOT + delta_cold, -1.0, 1.0))
    else:
        _P2_HOT = float(clamp(_P2_HOT + delta_hot,  -1.0, 1.0))
        _P1_HOT = float(clamp(_P1_HOT + delta_cold, -1.0, 1.0))
    _P1_HOT *= MOMENTUM_DECAY
    _P2_HOT *= MOMENTUM_DECAY


# =========================
# Tennis scoring helpers
# =========================
POINTS_STR = ["0", "15", "30", "40"]


def is_pressure_point(
    is_tiebreak: bool,
    server_points: int,
    returner_points: int,
    game_points: Tuple[int, int],
    games_in_set: Tuple[int, int],
) -> bool:
    if is_tiebreak:
        return (server_points >= 5 or returner_points >= 5)
    a, b = game_points
    return (a >= 3 and b >= 3) or (a == 3 and b <= 2) or (b == 3 and a <= 2) or (max(games_in_set) >= 5)


def game_point_label(a: int, b: int) -> str:
    if a >= 3 and b >= 3:
        if a == b:
            return "Deuce"
        if a == b + 1:
            return "Ad-In"
        if b == a + 1:
            return "Ad-Out"
    if a < 4 and b < 4:
        return f"{POINTS_STR[a]}–{POINTS_STR[b]}"
    return f"{a}–{b}"

def game_point_label_server(server_points: int, returner_points: int) -> str:
    a, b = server_points, returner_points
    if a >= 3 and b >= 3:
        if a == b:
            return "Deuce"
        if a == b + 1:
            return "Ad-In"
        if b == a + 1:
            return "Ad-Out"
    if a < 4 and b < 4:
        return f"{POINTS_STR[a]}–{POINTS_STR[b]}"
    return f"{a}–{b}"

def deuce_or_ad_side(point_a: int, point_b: int) -> str:
    total = point_a + point_b
    return "Deuce" if total % 2 == 0 else "Ad"


# =========================
# Data structures
# =========================
class MatchSimLobbyView(discord.ui.View):
    def __init__(self, match_id: str, p1_id: int, p2_id: int):
        super().__init__(timeout=120)
        self.match_id = match_id
        self.p1_id = p1_id
        self.p2_id = p2_id

    async def _pick(self, interaction: discord.Interaction, which: str):
        data = PENDING_MATCHES.get(self.match_id)
        if not data:
            return await interaction.response.send_message("❌ This match lobby expired.", ephemeral=True)
        if which == "p1" and interaction.user.id != self.p1_id:
            return await interaction.response.send_message("❌ Only Player 1 can pick this.", ephemeral=True)
        if which == "p2" and interaction.user.id != self.p2_id:
            return await interaction.response.send_message("❌ Only Player 2 can pick this.", ephemeral=True)
        presets = data["presets"]
        custom = data["custom"]
        view = LoadoutSelectView(chooser_id=interaction.user.id, guild_id=interaction.guild.id, user_id=interaction.user.id, timeout=30)
        await interaction.response.send_message("🎚 Choose your loadout:", view=view, ephemeral=True)
        timed_out = await view.wait()
        if timed_out or view.choice is None:
            return
        data[which + "_choice"] = view.choice
        p1m = f"<@{self.p1_id}>"
        p2m = f"<@{self.p2_id}>"
        p1_ready = "✅" if data.get("p1_choice") else "🎚"
        p2_ready = "✅" if data.get("p2_choice") else "🎚"
        content = f"{p1_ready} {p1m} is {'ready' if data.get('p1_choice') else 'picking their loadout'}.\n" \
                  f"{p2_ready} {p2m} is {'ready' if data.get('p2_choice') else 'picking their loadout'}."
        try:
            await data["lobby_msg"].edit(content=content, view=self)
        except Exception:
            pass
        if data.get("p1_choice") and data.get("p2_choice"):
            self.stop()

    @discord.ui.button(label="Pick loadout (P1)", style=discord.ButtonStyle.primary)
    async def pick_p1(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._pick(interaction, "p1")

    @discord.ui.button(label="Pick loadout (P2)", style=discord.ButtonStyle.primary)
    async def pick_p2(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._pick(interaction, "p2")

class PlayerStatsView(discord.ui.View):
    def __init__(self, member: discord.Member, stats: Dict[str, Any], gear_info: Dict[str, Any], timeout: int = 180):
        super().__init__(timeout=timeout)
        self.member = member
        self.stats = stats
        self.gear_info = gear_info
        self.current_page = 0
        self.categories = {
            "Overview": {
                "Forehand": stats.get("forehand", 1),
                "Backhand": stats.get("backhand", 1),
                "Serve": stats.get("serve", 1),
                "Return": stats.get("return", 1),
                "Touch": stats.get("touch", 1),
                "Fitness": stats.get("fitness", 1),
                "Mental": stats.get("mental", 1),
                "Fatigue": stats.get("fatigue", 0),
            },
            "Forehand": {
                "Forehand (Overall)": stats.get("forehand", 1),
                "Power": stats.get("fh_power", 1),
                "Accuracy": stats.get("fh_accuracy", 1),
                "Timing": stats.get("fh_timing", 1),
            },
            "Backhand": {
                "Backhand (Overall)": stats.get("backhand", 1),
                "Power": stats.get("bh_power", 1),
                "Accuracy": stats.get("bh_accuracy", 1),
                "Timing": stats.get("bh_timing", 1),
            },
            "Serve": {
                "Serve (Overall)": stats.get("serve", 1),
                "1st Serve Speed": stats.get("fs_speed", 1),
                "1st Serve Accuracy": stats.get("fs_accuracy", 1),
                "1st Serve Spin": stats.get("fs_spin", 1),
                "2nd Serve Speed": stats.get("ss_speed", 1),
                "2nd Serve Accuracy": stats.get("ss_accuracy", 1),
                "2nd Serve Spin": stats.get("ss_spin", 1),
            },
            "Return": {
                "Return (Overall)": stats.get("return", 1),
                "Return Accuracy": stats.get("return_accuracy", 1),
                "Return Speed": stats.get("return_speed", 1),
            },
            "Touch": {
                "Touch (Overall)": stats.get("touch", 1),
                "Volley": stats.get("volley", 1),
                "Half-Volley": stats.get("half_volley", 1),
                "Drop Shot": stats.get("drop_shot_effectivity", 1),
                "Slice": stats.get("slice", 1),
                "Lob": stats.get("lob", 1),
            },
            "Fitness": {
                "Fitness (Overall)": stats.get("fitness", 1),
                "Footwork": stats.get("footwork", 1),
                "Speed": stats.get("speed", 1),
                "Stamina": stats.get("stamina", 1),
            },
            "Mental": {
                "Mental (Overall)": stats.get("mental", 1),
                "Focus": stats.get("focus", 1),
                "Tennis IQ": stats.get("tennis_iq", 1),
                "Mental Stamina": stats.get("mental_stamina", 1),
            },
        }
        self.page_names = list(self.categories.keys())
        self.update_buttons()

    def create_embed(self) -> discord.Embed:
        page_name = self.page_names[self.current_page]
        page_stats = self.categories[page_name]
        embed = discord.Embed(
            title=f"📊 {self.member.display_name}'s Stats",
            description=f"**{page_name}**",
            color=discord.Color.blue()
        )
        embed.set_thumbnail(url=self.member.display_avatar.url)
        lines = []
        for stat_name, value in page_stats.items():
            lines.append(f"{stat_name}: **{int(value)}**")
        embed.add_field(name="Stats", value="\n".join(lines), inline=False)
        if page_name == "Overview":
            # ── Handedness line ─────────────────────────────────────────────
            hand  = self.stats.get("handedness")
            bhstyle = self.stats.get("backhand_style")
            if hand and bhstyle:
                hand_label = "Right-handed" if hand == "right" else "Left-handed"
                bh_label   = "2HBH" if bhstyle == "two_handed" else "1HBH"
                embed.add_field(
                    name="Playing Style",
                    value=f"🎾 **{hand_label}, {bh_label}**",
                    inline=False,
                )
            unspent = int(self.stats.get("unspent_points", 0))
            embed.add_field(name="Unspent Points", value=f"**{unspent}** points available", inline=False)
            racket_name = self.gear_info.get("racket_name", "None")
            shoes_name = self.gear_info.get("shoes_name", "None")
            gear_text = f"Racket: {racket_name}\n Shoes: {shoes_name}"
            embed.add_field(name="Equipped Gear", value=gear_text, inline=False)
            try:
                from modules.training import (
                    _xp_key, xp_needed_for_level,
                    _XP_PAGE_ORDER, CATEGORY_COMPONENTS,
                )
                from modules.players import BASE_STAT as _BASE_STAT
                xp_lines = []
                for cat in _XP_PAGE_ORDER:
                    parts   = CATEGORY_COMPONENTS.get(cat, ())
                    cat_val = int(self.stats.get(cat, _BASE_STAT))
                    best_pct = 0.0
                    all_max  = True
                    for s in parts:
                        sv    = int(self.stats.get(s, _BASE_STAT))
                        if sv < 99:
                            all_max = False
                            xnow  = float(self.stats.get(_xp_key(s), 0.0))
                            xneed = xp_needed_for_level(sv)
                            pct   = xnow / xneed * 100 if xneed else 0
                            if pct > best_pct:
                                best_pct = pct
                    if all_max:
                        xp_lines.append(f"**{cat.title()}** `{cat_val}` — ⭐ **ALL MAX**")
                    else:
                        xp_lines.append(f"**{cat.title()}** `{cat_val}` — best sub-stat: *{best_pct:.0f}% to next*")
                embed.add_field(
                    name="🏋️ Training XP",
                    value="\n".join(xp_lines) + "\n*Use `/training xp-view` for full detail*",
                    inline=False,
                )
            except Exception:
                pass
        embed.set_footer(text=f"Page {self.current_page + 1}/{len(self.page_names)}")
        return embed

    def update_buttons(self):
        self.previous_button.disabled = (self.current_page == 0)
        self.next_button.disabled = (self.current_page == len(self.page_names) - 1)

    @discord.ui.button(label="◀", style=discord.ButtonStyle.primary)
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = max(0, self.current_page - 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.create_embed(), view=self)

    @discord.ui.button(label="▶", style=discord.ButtonStyle.primary)
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page = min(len(self.page_names) - 1, self.current_page + 1)
        self.update_buttons()
        await interaction.response.edit_message(embed=self.create_embed(), view=self)

@dataclass
class MatchConditions:
    venue_id: str | None = None
    venue_name: str = "Default"
    tournament_id: str | None = None
    surface: str = "hard"
    roof: bool = False
    roof_closed: bool = False
    cpi_effective: int = 35
    bounce_effective: int = 50
    altitude_m: int = 0
    temp_c: int = 20
    wind_kmh: int = 5
    is_raining: bool = False
    rain_delay_min: int = 0
    humidity_pct: int = 50
    venue_capacity: int = 5000  # seating capacity for experience multiplier

def _inv_only_venues(guild_id: int, user_id: int) -> List[str]:
    try:
        from modules.venues import _get_user_inv  # type: ignore
        return list(_get_user_inv(guild_id, user_id))
    except Exception:
        return []

def _roll_conditions_for_venue(guild_id: int, venue_id: Optional[str]) -> MatchConditions:
    cond = MatchConditions()
    if not venue_id:
        return cond
    v = _get_venue(venue_id)
    # If lookup by ID failed, try matching by name across all venues
    if not v:
        try:
            all_venues = _get_venues()
            for vid, vdata in all_venues.items():
                if (vdata.get("name") or "").lower() == venue_id.lower():
                    v = vdata
                    venue_id = vid
                    break
        except Exception:
            pass
    if not v:
        return cond
    cond.venue_id = venue_id
    raw_name = str(v.get("name", "") or "")
    if raw_name and raw_name not in ("Default", ""):
        cond.venue_name = raw_name
    elif venue_id and venue_id.startswith("venue-"):
        parts = venue_id.split("-")
        cond.venue_name = " ".join(p.capitalize() for p in parts[2:]) if len(parts) >= 3 else venue_id.replace("-", " ").title()
    else:
        cond.venue_name = venue_id.replace("-", " ").title() if venue_id else "Unknown"
    cond.tournament_id = str(v.get("tournament_id")) if v.get("tournament_id") else None
    cond.surface = str(v.get("surface", "hard"))
    cond.roof = bool(v.get("roof", False))
    cond.cpi_effective = int(v.get("cpi_base", 35))
    cond.bounce_effective = int(v.get("bounce_height_base", 50))
    cond.venue_capacity = int(v.get("capacity", 5000))

    alt_m = 0
    if cond.tournament_id:
        tdb = _tourn_db().get("tournaments", {})
        trow = tdb.get(cond.tournament_id, {})
        alt_m = int(trow.get("altitude_m", 0))
        hmin = int(trow.get("humidity_pct_min", 30))
        hmax = int(trow.get("humidity_pct_max", 70))
        if hmax < hmin:
            hmin, hmax = hmax, hmin
        cond.humidity_pct = random.randint(max(0, min(100, hmin)), max(0, min(100, hmax)))
    cond.altitude_m = alt_m

    w = v.get("weather", {}) or {}
    tmin = int(w.get("temp_c_min", 10))
    tmax = int(w.get("temp_c_max", 28))
    wmin = int(w.get("wind_kmh_min", 0))
    wmax = int(w.get("wind_kmh_max", 18))
    rain_pct = int(w.get("rain_chance_pct", 10))

    if tmax < tmin: tmin, tmax = tmax, tmin
    if wmax < wmin: wmin, wmax = wmax, wmin

    cond.temp_c = random.randint(tmin, tmax)
    cond.wind_kmh = random.randint(wmin, wmax)
    cond.is_raining = (random.randint(1, 100) <= max(0, min(100, rain_pct)))

    if cond.is_raining and cond.roof:
        cond.roof_closed = True
    elif cond.is_raining and not cond.roof:
        cond.roof_closed = False
        cond.rain_delay_min = random.randint(10, 60)

    return cond

class VenueSelectView(discord.ui.View):
    def __init__(self, chooser_id: int, guild_id: int, user_id: int, timeout: int = 25):
        super().__init__(timeout=timeout)
        self.chooser_id = chooser_id
        self.guild_id = guild_id
        self.user_id = user_id
        self.venue_id: Optional[str] = None

        owned_ids = _inv_only_venues(guild_id, user_id)
        venues_map = _get_venues()

        if not owned_ids:
            self.venue_id = None
            self.select = discord.ui.Select(
                placeholder="No venues owned",
                options=[discord.SelectOption(label="Buy a venue first: /venue shop", value="none")],
                disabled=True,
            )
            self.add_item(self.select)
            return

        options: List[discord.SelectOption] = []
        for vid in owned_ids[:24]:
            row = venues_map.get(vid, {})
            label = f"{row.get('name','(unknown)')} ({row.get('surface','?')}, CPI {row.get('cpi_base','?')})"
            options.append(discord.SelectOption(label=label[:100], value=vid))
        options.append(discord.SelectOption(label="No venue (default conditions)", value="__NONE__"))

        self.select = discord.ui.Select(
            placeholder="Select a venue for this match…",
            min_values=1, max_values=1, options=options,
        )
        self.select.callback = self._on_select  # type: ignore
        self.add_item(self.select)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.chooser_id:
            await interaction.response.send_message("❌ Only the match challenger can pick the venue.", ephemeral=True)
            return False
        return True

    async def _on_select(self, interaction: discord.Interaction):
        val = self.select.values[0]
        self.venue_id = None if val in ("__NONE__", "none") else val
        label = "Default conditions" if self.venue_id is None else val
        for child in self.children:
            child.disabled = True
        try:
            await interaction.response.edit_message(content=f"✅ Venue set: **{label}**", view=None)
        except Exception:
            pass
        self.stop()


class LoadoutSelectView(discord.ui.View):
    def __init__(self, chooser_id: int, guild_id: int, user_id: int, timeout: int = 30):
        super().__init__(timeout=timeout)
        self.chooser_id = chooser_id
        self.guild_id = guild_id
        self.user_id = user_id
        self.choice: str | None = None

        presets = _loadout_presets_db().get("presets", {}) or {}
        options: list[discord.SelectOption] = []
        options.append(discord.SelectOption(label="Balanced (default)", value="__BALANCED__"))
        for pid, row in list(presets.items())[:23]:
            if not isinstance(row, dict):
                continue
            display = str(row.get("title") or row.get("name") or pid)
            options.append(discord.SelectOption(label=display[:100], value=pid))
        inv = _loadout_inv_row(guild_id, user_id)
        if inv.get("has_custom", False):
            custom_label = str(inv.get("custom_name") or "Custom Loadout")[:100]
            options.append(discord.SelectOption(label=custom_label, value="__CUSTOM__"))

        self.select = discord.ui.Select(
            placeholder="Select a loadout…", min_values=1, max_values=1, options=options
        )
        self.select.callback = self._on_select  # type: ignore
        self.add_item(self.select)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.chooser_id:
            await interaction.response.send_message("❌ Only the correct player can pick this loadout.", ephemeral=True)
            return False
        return True

    async def _on_select(self, interaction: discord.Interaction):
        self.choice = self.select.values[0]
        if self.choice == "__BALANCED__":
            label = "Balanced"
        elif self.choice == "__CUSTOM__":
            inv = _loadout_inv_row(self.guild_id, self.user_id)
            label = str(inv.get("custom_name") or "Custom Loadout")
        else:
            presets = _loadout_presets_db().get("presets", {}) or {}
            prow = presets.get(self.choice) if isinstance(presets, dict) else None
            label = str(prow.get("title") or prow.get("name") or self.choice) if isinstance(prow, dict) else str(self.choice)
        await interaction.response.edit_message(content=f"✅ Loadout locked: **{label}**", view=None)
        self.stop()


# =========================
# PlayerProfile dataclass
# =========================
@dataclass
class PlayerProfile:
    name: str
    user_id: Optional[int] = None

    # Groundstroke stats
    fh_power: float    = 50
    fh_accuracy: float = 50
    fh_timing: float   = 50
    bh_power: float    = 50
    bh_accuracy: float = 50
    bh_timing: float   = 50

    # Return
    return_accuracy: float = 50
    return_speed: float    = 50

    # Serve
    fs_speed: float    = 50
    fs_accuracy: float = 50
    fs_spin: float     = 50
    ss_speed: float    = 45
    ss_accuracy: float = 55
    ss_spin: float     = 45

    # Touch / net game
    touch: float               = 50
    volley: float              = 50
    half_volley: float         = 50
    drop_shot_effectivity: float = 50
    slice: float               = 50
    lob: float                 = 50

    # Fitness / movement
    fitness: float     = 50
    footwork: float    = 50
    speed: float       = 50
    stamina_stat: float = 50

    # Mental
    focus: float          = 50
    tennis_iq: float      = 50
    mental_stamina: float = 50

    # Loadout mirrors (set by apply_loadout_to_profile – all default neutral=50)
    lo_fh_power:            float = 50  # Forehand Power
    lo_fh_spin:             float = 50  # Forehand Spin (margin/safety)
    lo_bh_power:            float = 50  # Backhand Power
    lo_bh_spin:             float = 50  # Backhand Spin
    lo_serve_power:         float = 50  # Serve Power
    lo_serve_spin:          float = 50  # Serve Spin
    lo_dir_risk:            float = 50  # Shot Direction Risk
    lo_serve_variety:       float = 50  # Serve Variety
    lo_drop_frequency:      float = 50  # Drop Shot Frequency
    lo_deuce_spin:          float = 50  # Deuce Serve Spin
    lo_deuce_place:         float = 50  # Deuce Serve Placement (Wide/Center/T)
    lo_ad_spin:             float = 50  # Ad Serve Spin
    lo_ad_place:            float = 50  # Ad Serve Placement
    lo_pressure_play_risk:  float = 50  # Pressure Play Risk
    lo_return_position:     float = 50  # Return Position (high=inside, low=deep)
    lo_movement_aggression: float = 50  # Movement Aggression
    lo_time_btwn_points:    float = 50  # Time Between Points
    lo_slice_usage:         float = 50  # Slice Usage

    # Gear
    racket_power: float   = 50.0
    racket_spin: float    = 50.0
    racket_control: float = 50.0
    shoe_footwork: float  = 50.0
    strung_pattern: str   = ""
    strung_tension: float = 55.0
    strung_weight: str    = ""

    # Playing style — confirmed on first match
    handedness: str    = "right"       # "right" | "left"
    backhand_style: str = "two_handed"  # "two_handed" | "one_handed"

    # Runtime
    stamina: float = STAMINA_START
    is_bot: bool   = False

    # Pre-match multipliers (set by build_pre_match_multipliers)
    sharpness_mult:  float = 1.0   # 0.94–1.06 based on training recency
    venue_exp_mult:  float = 1.0   # 1.00–1.05 based on venue experience


@dataclass
class MatchState:
    p1: PlayerProfile
    p2: PlayerProfile
    best_of: int = 3
    sets: List[Tuple[int, int]] = field(default_factory=list)
    set_tb_loser_points: List[Optional[int]] = field(default_factory=list)
    current_games: Tuple[int, int] = (0, 0)
    game_points: Tuple[int, int] = (0, 0)
    in_tiebreak: bool = False
    tiebreak_points: Tuple[int, int] = (0, 0)
    server_idx: int = 0
    last_point_desc: str = "—"
    last_serve_kmh: Optional[float] = None
    last_rally_shots: Optional[int] = None
    conditions: MatchConditions | None = None
    started_at: float = field(default_factory=monotonic)
    match_id: Optional[str] = None
    # Tournament context
    is_tournament_match: bool = False
    tournament_name: str = ""
    tournament_round: str = ""
    draw_snapshot: str = ""
    # (min_sec, max_sec) override for tournament point delays; None = use normal MATCH_SPEED_MULT
    point_delay_range: Optional[Tuple[float, float]] = None
    # Crucial point tallies — per player, reset at appropriate scope
    bp_tally: List[int] = field(default_factory=lambda: [0, 0])   # reset each game
    sp_tally: List[int] = field(default_factory=lambda: [0, 0])   # reset each set
    mp_tally: List[int] = field(default_factory=lambda: [0, 0])   # whole match
    # "Serving for…" context — set at game start, shown all game
    serving_context: str = ""

@dataclass
class MatchStats:
    aces: List[int] = field(default_factory=lambda: [0, 0])
    double_faults: List[int] = field(default_factory=lambda: [0, 0])
    first_serves_in: List[int] = field(default_factory=lambda: [0, 0])
    first_serves_total: List[int] = field(default_factory=lambda: [0, 0])
    first_serve_pts_won: List[int] = field(default_factory=lambda: [0, 0])
    first_serve_pts_total: List[int] = field(default_factory=lambda: [0, 0])
    second_serve_pts_won: List[int] = field(default_factory=lambda: [0, 0])
    second_serve_pts_total: List[int] = field(default_factory=lambda: [0, 0])
    return_pts_won: List[int] = field(default_factory=lambda: [0, 0])
    break_pts_saved: List[int] = field(default_factory=lambda: [0, 0])
    break_pts_faced: List[int] = field(default_factory=lambda: [0, 0])
    break_pts_converted: List[int] = field(default_factory=lambda: [0, 0])
    break_pts_chances: List[int] = field(default_factory=lambda: [0, 0])
    total_points_won: List[int] = field(default_factory=lambda: [0, 0])
    total_points_played: int = 0
    rally_shots_total: int = 0
    rally_points: int = 0
    winners: Dict[str, List[int]] = field(default_factory=lambda: {
        "forehand": [0, 0], "backhand": [0, 0], "overhead": [0, 0],
        "volley": [0, 0], "drop_shot": [0, 0], "lob": [0, 0],
        "slice": [0, 0], "half_volley": [0, 0], "serve": [0, 0],
    })
    unforced_errors: Dict[str, List[int]] = field(default_factory=lambda: {
        "forehand": [0, 0], "backhand": [0, 0], "serve": [0, 0], "other": [0, 0],
    })
    forced_errors: Dict[str, List[int]] = field(default_factory=lambda: {
        "forehand": [0, 0], "backhand": [0, 0], "other": [0, 0],
    })
    sets: List[Dict[str, Any]] = field(default_factory=list)

    def ensure_set(self, set_idx: int) -> None:
        while len(self.sets) <= set_idx:
            self.sets.append({
                "aces": [0, 0], "double_faults": [0, 0],
                "first_serves_in": [0, 0], "first_serves_total": [0, 0],
                "first_serve_pts_won": [0, 0], "first_serve_pts_total": [0, 0],
                "second_serve_pts_won": [0, 0], "second_serve_pts_total": [0, 0],
                "return_pts_won": [0, 0], "points_won": [0, 0], "points_played": 0,
                "break_pts_saved": [0, 0], "break_pts_faced": [0, 0],
                "break_pts_converted": [0, 0], "break_pts_chances": [0, 0],
                "rally_points": 0, "rally_shots_total": 0,
                "winners": {
                    "forehand": [0, 0], "backhand": [0, 0], "overhead": [0, 0],
                    "volley": [0, 0], "drop_shot": [0, 0], "lob": [0, 0],
                    "slice": [0, 0], "half_volley": [0, 0], "serve": [0, 0],
                },
                "unforced_errors": {"forehand": [0, 0], "backhand": [0, 0], "serve": [0, 0], "other": [0, 0]},
                "forced_errors": {"forehand": [0, 0], "backhand": [0, 0], "other": [0, 0]},
            })


# =========================
# Core simulation helpers
# =========================
def fatigue_factor(p: PlayerProfile) -> float:
    return 1.0 - (p.stamina / 100.0)

def apply_fatigue_penalties(value: float, p: PlayerProfile, penalty_scale: float) -> float:
    return value - (fatigue_factor(p) * penalty_scale)

def pressure_risk_multiplier(p: PlayerProfile, pressure: bool) -> float:
    """Under pressure: high pressure_play_risk → more aggressive (>1), low → safer (<1).
    At neutral (50): no change.
    At max (100): +35% risk — more winners but also more errors.
    At min (0): −30% risk — safer but capped upside.
    """
    if not pressure:
        return 1.0
    t = clamp(p.lo_pressure_play_risk / 100.0, 0.0, 1.0)
    return lerp(0.70, 1.35, t)

def pressure_quality_variance(p: PlayerProfile, pressure: bool, shot_quality: float) -> float:
    """High pressure_play_risk under pressure: amplify the quality deviation so clutch
    players hit bigger winners but also make bigger errors. Returns adjusted quality."""
    if not pressure:
        return shot_quality
    t = _lo({"v": p.lo_pressure_play_risk}, "v")   # -1..+1
    # High risk → amplify deviation from average (50)
    # Low risk  → compress toward 50 (safe, consistent, but limited ceiling)
    deviation  = shot_quality - 50.0
    factor     = 1.0 + t * 0.40                    # ±40% stretch/compress
    return float(clamp(50.0 + deviation * factor, 0.0, 100.0))

MATCH_SPEED_MULT = 0.25
MATCH_SLEEP_CAP = 0.18

def point_delay_seconds(rally_shots: int, pressure: bool) -> float:
    base = lerp(SLEEP_MIN, SLEEP_MAX, clamp(rally_shots / 14.0, 0.0, 1.0))
    if pressure:
        base = min(SLEEP_MAX, base + 0.5)
    base *= MATCH_SPEED_MULT
    return min(MATCH_SLEEP_CAP, max(0.0, base))

def stamina_cost(rally_shots: int, ended_on_serve: bool) -> float:
    if ended_on_serve:
        return 0.12
    rs = min(18, max(1, int(rally_shots)))
    return 0.10 + 0.06 * rs

def _placement_bh_factor(server_hand: str, returner_hand: str, side: str, placement: str) -> float:
    """
    Returns a multiplier showing how much the serve placement targets the returner's
    backhand (hardest to handle). >1.0 = BH targeted more = harder = better ace chance.

    Coordinate reality:
      Deuce court, Wide: goes to outer-right corner → righty returner's BACKHAND,
                                                       lefty returner's FOREHAND
      Deuce court, T: goes to center → righty returner's FOREHAND, lefty's BACKHAND
      Ad court, Wide: goes to outer-left corner → righty's BACKHAND, lefty's FOREHAND
      Ad court, T: goes to center → righty's FOREHAND, lefty's BACKHAND

    Lefty server twist:
      Deuce Wide: lefty's natural swing curves ball to the LEFT, going to righty's FOREHAND
                  (opposite of righty server's wide) — LESS effective vs righty
      Ad Wide: lefty's natural kick sweeps far outside to righty's BACKHAND with huge
               angle — the FAMOUS lefty ad-wide ace. Extra bonus.
      Deuce T:  lefty's T naturally targets righty's BACKHAND — bonus.
      Ad T:     lefty's ad-T goes to righty's FOREHAND — penalty.
    """
    if placement == "Center":
        return 1.0  # body serve, neutral

    righty_ret = (returner_hand == "right")
    lefty_srv  = (server_hand == "left")
    is_wide    = (placement == "Wide")

    if not lefty_srv:
        # Standard righty server: Wide always to returner's BH (if righty), T to FH
        targets_bh = righty_ret if is_wide else not righty_ret
        return 1.35 if targets_bh else 0.72
    else:
        # Lefty server — geometry inverts for deuce wide/T
        if side == "Deuce" and is_wide:
            # Lefty deuce wide curves TO returner's forehand if righty
            targets_bh = not righty_ret
            return (1.35 if targets_bh else 0.72)
        elif side == "Deuce" and not is_wide:  # T
            # Lefty deuce T naturally attacks righty's backhand
            targets_bh = righty_ret
            return 1.30 if targets_bh else 0.75
        elif side == "Ad" and is_wide:
            # FAMOUS lefty ad wide — devastating angle to righty's backhand
            if righty_ret:
                return 1.80  # extremely effective
            else:
                return 0.65  # lefty vs lefty on ad wide = less special
        else:  # Ad T, lefty
            targets_bh = not righty_ret
            return 1.28 if targets_bh else 0.78


def serve_scores(
    server: PlayerProfile,
    returner: PlayerProfile,
    second_serve: bool,
    side: str,
    pressure: bool,
) -> Tuple[float, float, float]:
    """
    Returns (p_ace, p_fault, p_in_play).
    Serve placement (Wide/Center/T) and spin preference affect both ace chance
    and how hard the return will be for the returner.
    """
    if second_serve:
        spd, acc, spin_stat = server.ss_speed, server.ss_accuracy, server.ss_spin
    else:
        spd, acc, spin_stat = server.fs_speed, server.fs_accuracy, server.fs_spin

    # ── Loadout sliders ──────────────────────────────────────────────────
    sl_spin_dev  = _lo({"v": server.lo_deuce_spin if side == "Deuce" else server.lo_ad_spin}, "v")
    sl_pow_dev   = _lo({"v": server.lo_serve_power},  "v")
    sl_spin2_dev = _lo({"v": server.lo_serve_spin},   "v")
    sl_var_dev   = _lo({"v": server.lo_serve_variety},"v")
    place_val    = server.lo_deuce_place if side == "Deuce" else server.lo_ad_place
    placement    = _serve_place_bucket(place_val)

    # ── Serve Power effect: +_MAX_LO quality but needs accuracy ─────────
    # If accuracy < 60, amplify fault risk; if accuracy ≥ 70, power is safe
    acc_norm   = clamp((acc - 50.0) / 50.0, -1.0, 1.0)
    pow_bonus  = sl_pow_dev * _MAX_LO * max(0.3, (1.0 + acc_norm) / 2.0)

    # ── Serve Spin effect: -pace but +margin (fewer faults) ─────────────
    # Requires good spin stat to be effective
    spin_norm  = clamp((spin_stat - 50.0) / 50.0, -1.0, 1.0)
    spin_bonus = sl_spin2_dev * _MAX_LO * max(0.3, (1.0 + spin_norm) / 2.0)

    # ── Side-specific spin (deuce_spin / ad_spin) ────────────────────────
    side_spin_bonus = sl_spin_dev * _MAX_LO * 0.5

    # ── Combined effective serve stats ───────────────────────────────────
    racket_pow = (server.racket_power  - 50.0) * 0.18
    racket_ctl = (server.racket_control - 50.0) * 0.22
    racket_spn = (server.racket_spin   - 50.0) * 0.18

    # Power goes up with pow_bonus, spin reduces pace but adds margin
    spd_eff  = (apply_fatigue_penalties(spd,      server, 10)
                + pow_bonus  * 0.60
                - spin_bonus * 0.30     # spin takes off pace
                + racket_pow)
    acc_eff  = (apply_fatigue_penalties(acc,      server, 12)
                + spin_bonus * 0.50     # spin adds control/margin
                - pow_bonus  * 0.20     # too much power hurts control
                + racket_ctl
                + sl_var_dev * (-3.0))  # variety trades slight accuracy
    spin_eff = (apply_fatigue_penalties(spin_stat, server, 8)
                + side_spin_bonus
                + racket_spn)

    # ── Placement modifier — handedness-aware ───────────────────────────
    # How much the placement targets returner's backhand is the key driver.
    # Lefty servers get different advantages on different sides/placements.
    bh_factor = _placement_bh_factor(
        server_hand   = getattr(server,   "handedness", "right"),
        returner_hand = getattr(returner, "handedness", "right"),
        side          = side,
        placement     = placement,
    )
    if placement == "Wide":
        placement_ace_bonus   =  0.045 * bh_factor - 0.045  # net 0 at bh_factor=1
        placement_fault_bonus =  0.015
    elif placement == "T":
        placement_ace_bonus   =  0.035 * bh_factor - 0.035
        placement_fault_bonus =  0.008
    else:  # Center
        placement_ace_bonus   = -0.025
        placement_fault_bonus = -0.010

    # Serve variety: adds unpredictability (better ace chance) at small accuracy cost
    variety_ace_bonus = sl_var_dev * 0.03

    # ── Risk multiplier under pressure ───────────────────────────────────
    risk = pressure_risk_multiplier(server, pressure)

    # ── Returner difficulty ──────────────────────────────────────────────
    ret_eff = (
        apply_fatigue_penalties(returner.return_accuracy, returner, 10)
        + apply_fatigue_penalties(returner.return_speed, returner, 8) * 0.5
        + (returner.tennis_iq - 50) * 0.15
    )

    serve_adv = (0.45 * spd_eff + 0.45 * acc_eff + 0.10 * spin_eff) - ret_eff

    p_ace = clamp(
        sigmoid((serve_adv - 18) / 10) * (0.18 if not second_serve else 0.08)
        + placement_ace_bonus + variety_ace_bonus,
        0.0, 0.28
    )

    # ── Fault probability ────────────────────────────────────────────────
    base_fault = sigmoid((50 - acc_eff) / 10) * (0.22 if not second_serve else 0.12)
    # Power without accuracy → more faults; spin adds margin → fewer faults
    if sl_pow_dev > 0 and acc < 60:
        base_fault *= (1.0 + sl_pow_dev * 0.25)
    if sl_spin2_dev > 0:
        base_fault *= (1.0 - sl_spin2_dev * 0.18)
    # Pressure risk: aggressive under pressure → slightly more faults
    if pressure:
        if risk > 1.0:
            base_fault *= 1.0 + (risk - 1.0) * 0.5
        elif risk < 1.0:
            base_fault *= 1.0 - (1.0 - risk) * 0.3
    base_fault += placement_fault_bonus
    p_fault = clamp(base_fault, 0.01, 0.38)

    p_in_play = clamp(1.0 - p_ace - p_fault, 0.0, 1.0)
    return p_ace, p_fault, p_in_play


def _serve_return_difficulty(
    server: PlayerProfile,
    returner: PlayerProfile,
    second_serve: bool,
    side: str,
    p_ace: float,
) -> float:
    """
    Returns an initial difficulty [0,1] for the returner, factoring in:
    - Serve speed/depth via p_ace proxy
    - Return position (inside=high slider / deep=low slider)
    - Placement (Wide/T vs Center)
    - Side spin (kick/slice effect at Wide or T)

    Return position spec:
      INSIDE (high slider ≈ 0.65 court Y):
        • Less time on fast serves → harder on fast/powerful serves
        • Center serves cramp them very badly
        • Wide/T with spin can still pull them off
        • Allows aggressive return attacks on weaker serves (net approach)

      DEEP (low slider ≈ 1.05 court Y):
        • More time → easier on flat fast serves
        • Wide/T with kick/slice drag them much further off court
        • Has to run farther on short angled balls
        • Center serves are manageable (plenty of room)
    """
    place_val   = server.lo_deuce_place if side == "Deuce" else server.lo_ad_place
    placement   = _serve_place_bucket(place_val)
    sl_spin_dev = _lo({"v": server.lo_deuce_spin if side == "Deuce" else server.lo_ad_spin}, "v")
    sl_pow_dev  = _lo({"v": server.lo_serve_power}, "v")

    # Base difficulty from serve quality (fast, heavy serves are harder)
    base_diff = clamp(0.28 + p_ace * 2.6, 0.18, 0.82)
    # Power slider makes the serve more penetrating
    base_diff += sl_pow_dev * 0.06

    # Return position
    ret_y = _return_pos_y(returner_sliders_to_rp(returner))
    # ret_y: 0.65 = inside, 1.05 = deep behind baseline
    inside_score = 1.0 - (ret_y - 0.65) / 0.40   # 1.0=fully inside, 0.0=fully deep

    if placement == "Wide" or placement == "T":
        if inside_score > 0.5:   # inside returner
            # Less reaction time on fast/heavy serves — harder
            diff_adj = 0.05 + sl_pow_dev * 0.07 + sl_spin_dev * 0.06
        else:                    # deep returner
            # More time for the serve, but kick/slice drags them way off court
            diff_adj = -0.04 + sl_spin_dev * 0.16
    else:  # Center
        if inside_score > 0.5:   # inside returner: center serve jams the body hard
            diff_adj = 0.14 + sl_spin_dev * 0.04
        else:                    # deep returner: plenty of room to handle center
            diff_adj = -0.05

    # Fatigued returner has slower first step
    diff_adj += _fatigue_penalty_mult(returner) * 0.15

    return float(clamp(base_diff + diff_adj, 0.14, 0.92))

def returner_sliders_to_rp(returner: PlayerProfile) -> dict:
    """Extract return_position slider from a PlayerProfile."""
    return {"return_position": returner.lo_return_position}

def _serve_speed_kmh(server: PlayerProfile, second_serve: bool, sl: Optional[dict] = None) -> float:
    sl = sl or {}
    if second_serve:
        spd, acc = server.ss_speed, server.ss_accuracy
        lo, hi = 120.0, 205.0
        spin_bonus = (server.ss_spin - 50.0) * 0.08
    else:
        spd, acc = server.fs_speed, server.fs_accuracy
        lo, hi = 150.0, 245.0
        spin_bonus = (server.fs_spin - 50.0) * 0.06

    base  = lo + (hi - lo) * clamp((spd - 1.0) / 99.0, 0.0, 1.0)
    base += (server.racket_power - 50.0) * 0.55
    base += (server.racket_control - 50.0) * 0.10
    base += spin_bonus
    base -= fatigue_factor(server) * (18.0 if not second_serve else 12.0)

    # serve_power slider boosts speed; serve_spin slider reduces it
    pow_dev  = _lo({"v": server.lo_serve_power}, "v")
    spin_dev = _lo({"v": server.lo_serve_spin},  "v")
    base += pow_dev  * 15.0   # up to ±15 km/h from power
    base -= spin_dev * 10.0   # up to -10 km/h from spin

    scatter = lerp(18.0, 8.0, clamp((acc - 1.0) / 99.0, 0.0, 1.0))
    if second_serve:
        base -= 2.0
    v = random.gauss(0.0, scatter)
    return clamp(base + v, 90.0, 265.0)


# =========================
# RALLY ENGINE — Multiplier weights
# Stats matter but are NOT deterministic. A 40-pt stat gap ≈ ±11% shift.
# RNG noise still dominates so every match is alive.
# =========================
_W_STAT        = 0.14   # per 50-pt stat gap → ±14% outcome shift
_W_FATIGUE     = 0.18   # full exhaustion → −18% quality
_W_SHARPNESS   = 0.06   # ±6% from training recency
_W_VENUE_EXP   = 0.05   # up to +5% at familiar large venues
_W_CONDITIONS  = 0.12   # extreme CPI / weather ±12%
_W_LOADOUT     = 0.20   # loadout biases shot selection ±20%
_BASE_RNG_SPREAD = 0.55  # was 0.30 — wider shot quality variance
_MAX_RALLY_SHOTS = 35

# --- Shot & position dataclasses ---
@dataclass
class RallyShot:
    shot_type: str   # forehand | backhand | forehand_volley | backhand_volley |
                     # overhead | drop_shot | lob | half_volley |
                     # slice_backhand | slice_forehand | swinging_volley
    direction: str   # cross_court | dtl | body | inside_out | inside_in
    spin: str        # flat | topspin | heavy_topspin | slice | kick
    pace: float      # 0=soft, 100=max
    height: float    # 0=net-skimmer, 100=lob arc
    depth: float     # 0=drop-short, 100=baseline
    court_x: float   # 0.0–1.0 horizontal landing (0=left, 1=right)
    court_y: float   # 0.0–1.0 depth (0=net, 1=baseline from hitter's side)
    quality: float   # 0–100 execution quality

@dataclass
class CourtPosition:
    x: float = 0.50   # 0=left sideline, 0.5=centre, 1=right
    y: float = 0.90   # 0=net, 1=deep behind baseline
    role: str = "neutral"  # offense | neutral | defense

# --- CONDITIONS ENGINE ---
def compute_effective_cpi(cond: MatchConditions) -> float:
    """Adjust venue CPI for temperature, altitude, humidity, and indoor/outdoor.

    Base variance: max +15 / max -15 from the venue base CPI.
    Each factor contributes a share of that ±15 envelope:

    CPI rules:
      • Higher humidity  → LESS CPI   (heavy ball, slower)
      • Higher altitude  → MORE CPI   (thinner air, faster)
      • Higher temp      → MORE CPI   (hot air, livelier ball)
      • Indoor           → MORE CPI   (no wind/damp, faster conditions)

    Reference points (neutral):
      humidity=50%, altitude=0m, temp=20°C, outdoor
    """
    if cond is None:
        return 35.0

    cpi = float(cond.cpi_effective)
    is_indoor = cond.roof and cond.roof_closed

    # ── Humidity: 0% → +5 CPI, 100% → −5 CPI  (±5 max share) ──────────
    # humidity_pct 0..100; neutral=50 → 0 delta
    hum_delta = -(float(cond.humidity_pct) - 50.0) * (5.0 / 50.0)   # −5..+5

    # ── Altitude: 0m → 0, 3000m → +7 CPI  (0..+7 share) ────────────────
    # Caps at 3000m for practical venue range
    alt_delta = min(7.0, float(cond.altitude_m) / 3000.0 * 7.0)     # 0..+7

    # ── Temperature: 5°C → −5, 40°C → +3 CPI  (−5..+3 share) ──────────
    # Neutral=20°C; cold air is denser (slower), hot air less dense (faster)
    temp_norm  = (float(cond.temp_c) - 20.0) / 20.0   # −1 at 0°C, +1 at 40°C
    temp_delta = float(clamp(temp_norm * 4.0, -5.0, 3.0))

    # ── Indoor: flat +3 CPI bonus (no wind, no damp) ─────────────────────
    indoor_delta = 3.0 if is_indoor else 0.0

    # ── Rain (outdoor only): slows ball down −4 CPI ──────────────────────
    rain_delta = -4.0 if (cond.is_raining and not is_indoor) else 0.0

    # ── Wind (outdoor only): adds pace/unpredictability, slight +CPI ─────
    wind_delta = 0.0
    if not is_indoor and cond.wind_kmh > 10:
        wind_delta = min(2.0, (float(cond.wind_kmh) - 10.0) * 0.10)

    total_delta = hum_delta + alt_delta + temp_delta + indoor_delta + rain_delta + wind_delta
    # Hard clamp to ±15 variance from base
    total_delta = float(clamp(total_delta, -15.0, 15.0))

    return float(max(1.0, min(100.0, cpi + total_delta)))


def compute_effective_bounce(cond: MatchConditions) -> float:
    """Compute dynamic bounce height from base + conditions. Separate from CPI.

    Base variance: max +15 / max -15 from the venue base bounce.
    Each factor contributes a share of that ±15 envelope:

    Bounce rules:
      • Indoor           → LOWER bounce  (controlled environment, less trampoline effect)
      • Higher humidity  → LESS bounce   (heavier felt absorbs more energy)
      • Higher altitude  → MORE bounce   (thinner air, ball travels further, rises higher)
      • Higher temp      → MORE bounce   (rubber/felt more elastic, court surface livelier)
    """
    if cond is None:
        return 50.0

    bounce = float(cond.bounce_effective)
    is_indoor = cond.roof and cond.roof_closed

    # ── Indoor: flat −5 bounce (roof dampens the outdoor "trampoline" effect) ──
    indoor_delta = -5.0 if is_indoor else 0.0

    # ── Humidity: 0% → +4, 100% → −4  (±4 max share) ───────────────────
    # Dry felt = more elastic; damp felt = heavier, less lively
    hum_delta = -(float(cond.humidity_pct) - 50.0) * (4.0 / 50.0)   # −4..+4

    # ── Altitude: 0m → 0, 3000m → +6  (0..+6 share) ────────────────────
    # Thinner air → less drag on ball → ball keeps more energy through bounce
    alt_delta = min(6.0, float(cond.altitude_m) / 3000.0 * 6.0)     # 0..+6

    # ── Temperature: 5°C → −4, 40°C → +4  (−4..+4 share) ───────────────
    # Cold = rubber stiffens, less bounce; hot = rubber/felt more elastic
    temp_norm  = (float(cond.temp_c) - 20.0) / 20.0   # −1 at 0°C, +1 at 40°C
    temp_delta = float(clamp(temp_norm * 4.0, -4.0, 4.0))

    # ── Rain (outdoor only): wet surface = ball skids lower −3 ──────────
    rain_delta = -3.0 if (cond.is_raining and not is_indoor) else 0.0

    total_delta = indoor_delta + hum_delta + alt_delta + temp_delta + rain_delta
    # Hard clamp to ±15 variance from base
    total_delta = float(clamp(total_delta, -15.0, 15.0))

    return float(max(1.0, min(100.0, bounce + total_delta)))


def conditions_error_bias(cond: MatchConditions, effective_cpi: float) -> float:
    """Extra error probability nudge from extreme conditions. Range 0..0.12."""
    bias = 0.0
    if cond is None:
        return bias
    is_indoor = cond.roof and cond.roof_closed
    if not is_indoor and cond.wind_kmh > 15:
        bias += min(0.08, (float(cond.wind_kmh) - 15.0) * 0.003)
    if effective_cpi >= 70:
        bias += (effective_cpi - 70.0) / 30.0 * 0.05
    if effective_cpi <= 20:
        bias += (20.0 - effective_cpi) / 20.0 * 0.03
    if cond.is_raining:
        bias += 0.04
    return min(0.12, bias)

# --- SHARPNESS & VENUE EXPERIENCE ---
def build_pre_match_multipliers(
    p: PlayerProfile,
    guild_id: int,
    venue_id: Optional[str],
    venue_capacity: int = 5000,
) -> None:
    """Set p.sharpness_mult and p.venue_exp_mult once before the match loop."""
    # Sharpness: peaks at +6% if trained within 24h, decays to −6% after 7 days
    sharpness = 1.0
    try:
        from modules.training import get_last_training_at  # type: ignore
        iso = get_last_training_at(guild_id, p.user_id) if p.user_id else None
        if iso:
            last_dt  = datetime.fromisoformat(iso)
            now_dt   = datetime.now(timezone.utc)
            hours_ago = max(0.0, (now_dt - last_dt).total_seconds() / 3600.0)
            sharpness = (
                1.0
                + 0.06 * (1.0 - min(1.0, hours_ago / 24.0))
                - 0.06 * max(0.0, min(1.0, (hours_ago - 24.0) / 144.0))
            )
    except Exception:
        sharpness = 1.0
    p.sharpness_mult = float(max(0.94, min(1.06, sharpness)))

    # Venue experience: only matters at large venues (>10k seats)
    exp_mult = 1.0
    try:
        from modules.players import get_player_row_by_id  # type: ignore
        row = get_player_row_by_id(guild_id, p.user_id) if p.user_id else None
        if row and venue_id:
            vp = row.get("venues_played") or {}
            if isinstance(vp, str):
                vp = json.loads(vp)
            matches_here = int(vp.get(str(venue_id), 0))
            size_factor  = max(0.0, min(1.0, (venue_capacity - 5000) / 45000.0))
            exp_bonus    = size_factor * _W_VENUE_EXP * min(1.0, math.log1p(matches_here) / math.log1p(20))
            exp_mult     = 1.0 + exp_bonus
    except Exception:
        exp_mult = 1.0
    p.venue_exp_mult = float(max(1.0, min(1.05, exp_mult)))

def record_venue_experience(guild_id: int, user_id: int, venue_id: str) -> None:
    """Increment venues_played[venue_id] in player DB at match end."""
    if not user_id or not venue_id:
        return
    try:
        from modules.players import get_player_row_by_id, set_player_row_by_id as _srbi  # type: ignore
        row = get_player_row_by_id(guild_id, user_id)
        if not row:
            return
        vp = row.get("venues_played") or {}
        if isinstance(vp, str):
            vp = json.loads(vp)
        vp[str(venue_id)] = int(vp.get(str(venue_id), 0)) + 1
        row["venues_played"] = vp
        _srbi(guild_id, user_id, row)
    except Exception:
        pass

# ─── LOADOUT HELPERS ────────────────────────────────────────────────────────

def _net_approach_freq(sl: dict, movement_aggression: float = 50.0) -> float:
    """How often hitter rushes net after a short ball.
    movement_aggression high → more net-rushing; low → stay back."""
    agg = float(sl.get("movement_aggression", movement_aggression))
    return float(max(0.02, min(0.75, 0.06 + (agg - 50) / 50.0 * 0.25)))

def _drop_freq(sl: dict) -> float:
    """How often a drop shot is chosen from a short ball."""
    df = float(sl.get("drop_frequency", 50))
    return float(max(0.01, min(0.35, 0.05 + (df - 50) / 50.0 * 0.15)))

def _slice_freq(sl: dict) -> float:
    """How often a slice is chosen instead of a regular groundstroke."""
    su = float(sl.get("slice_usage", 50))
    return float(max(0.04, min(0.40, 0.10 + (su - 50) / 50.0 * 0.18)))

def _dtl_prob(sl: dict) -> float:
    """Base probability of going down-the-line in a neutral rally."""
    dr = float(sl.get("shot_dir_risk", 50))
    return float(max(0.20, min(0.55, 0.35 + (dr - 50) / 50.0 * 0.12)))

def _pace_bias(sl: dict) -> float:
    """Overall pace multiplier from power sliders."""
    fp = float(sl.get("fh_power",   50))
    bp = float(sl.get("bh_power",   50))
    sp = float(sl.get("serve_power",50))
    return float(max(0.75, min(1.30, 1.0 + ((fp + bp + sp) / 3.0 - 50.0) / 50.0 * _W_LOADOUT)))

def _fatigue_penalty_mult(p: PlayerProfile) -> float:
    """Fatigue penalty for shot quality. Non-linear: small effect until ~70 stamina,
    then grows steeply. Full exhaustion (0 stamina) → _W_FATIGUE penalty."""
    t = 1.0 - (p.stamina / STAMINA_START)    # 0=fresh, 1=exhausted
    # Quadratic: light fatigue barely matters, heavy fatigue compounds hard
    return (t ** 1.6) * _W_FATIGUE


# --- SHOT SELECTION ---

def _ball_on_fh_side(hitter: PlayerProfile, incoming_court_x: float, hitter_x: float) -> bool:
    """
    Determine if the ball is on the hitter's FOREHAND side.
    For a righty: ball at high x (far right) = backhand; low x = forehand when they're at x~0.5.
    For a lefty: INVERTED — high x = forehand, low x = backhand.
    We compare ball x vs hitter x: ball is on FH side if it's in the direction of their FH.
    """
    is_lefty = (getattr(hitter, "handedness", "right") == "left")
    # If ball lands to the player's right (ball_x > hitter_x), righties play BH, lefties play FH
    ball_right_of_player = incoming_court_x > hitter_x
    if is_lefty:
        return ball_right_of_player        # lefty: right = forehand side
    else:
        return not ball_right_of_player    # righty: right = backhand side (from deep baseline)


def _invert_direction_for_lefty(direction: str, hitter: PlayerProfile) -> str:
    """
    Cross-court and DTL are relative to handedness.
    A righty's forehand cross-court is a lefty's backhand cross-court from the same position.
    When a lefty hits from the same corner as a righty would hit FH CC, geometrically it's BH CC.
    We don't invert the direction label (it's court-absolute), but we track for 1HBH logic.
    Actually, court direction labels (cross_court/dtl) are ABSOLUTE — no inversion needed here.
    This function is a no-op for now but kept for future per-shot spin/direction logic.
    """
    return direction


def choose_shot(
    hitter: PlayerProfile,
    sliders: dict,
    hitter_pos: CourtPosition,
    incoming: RallyShot,
    defender: PlayerProfile,
    pressure: bool,
    cpi: float,
) -> Tuple[str, str, str]:
    """Returns (shot_type, direction, spin)."""
    rng = random.random

    at_net     = hitter_pos.y < 0.40
    in_front   = hitter_pos.y < 0.65
    short_ball = incoming.depth < 40
    high_ball  = incoming.height > 65
    low_slice  = incoming.spin == "slice" and incoming.height < 30

    net_freq   = _net_approach_freq(sliders)
    drop_f     = _drop_freq(sliders)
    slice_f    = _slice_freq(sliders)
    dtl_p      = _dtl_prob(sliders)
    risk_dev   = _lo(sliders, "shot_dir_risk")   # -1..+1
    press_mult = pressure_risk_multiplier(hitter, pressure)
    tiq        = float(hitter.tennis_iq)

    # ── Shot type ────────────────────────────────────────────────────────
    if at_net:
        if high_ball and rng() < 0.70 + (tiq - 50)/100.0 * 0.20:
            shot_type = "overhead"
        elif low_slice and rng() < 0.50:
            shot_type = "half_volley"
        else:
            shot_type = "forehand_volley" if rng() < 0.55 + (hitter.fh_power - hitter.bh_power)/200.0 else "backhand_volley"

    elif short_ball and in_front:
        if high_ball and rng() < 0.60:
            shot_type = "overhead"
        elif rng() < drop_f * 0.5:
            shot_type = "drop_shot"
        else:
            shot_type = "forehand" if rng() < 0.55 else "backhand"

    elif incoming.depth < 30:
        if rng() < drop_f * 1.2:
            shot_type = "drop_shot"
        elif rng() < 0.70:
            # slice_usage can push toward slice here
            if rng() < slice_f * 0.8:
                shot_type = "slice_forehand" if rng() < 0.40 else "slice_backhand"
            else:
                shot_type = "forehand" if rng() < 0.55 else "backhand"
        else:
            shot_type = "slice_forehand" if rng() < 0.40 else "slice_backhand"

    elif high_ball:
        if in_front and rng() < 0.55:
            shot_type = "overhead"
        elif rng() < 0.25:
            shot_type = "lob"
        else:
            shot_type = "forehand" if rng() < 0.50 else "backhand"

    elif low_slice:
        if rng() < 0.30:
            shot_type = "half_volley"
        elif rng() < 0.35:
            shot_type = "slice_backhand" if rng() < 0.55 else "slice_forehand"
        else:
            shot_type = "forehand" if rng() < 0.50 else "backhand"

    elif hitter_pos.role == "defense":
        if rng() < 0.18 + (hitter.lob - 50)/100.0 * 0.15:
            shot_type = "lob"
        elif rng() < slice_f * 1.4:
            shot_type = "slice_backhand" if rng() < 0.60 else "slice_forehand"
        else:
            shot_type = "forehand" if rng() < 0.40 else "backhand"

    else:
        if rng() < drop_f and tiq > 55:
            shot_type = "drop_shot"
        elif rng() < slice_f:
            shot_type = "slice_forehand" if rng() < 0.45 else "slice_backhand"
        else:
            # Handedness-aware run-around: lefty runs around to their FH on the RIGHT
            is_lefty   = (getattr(hitter, "handedness", "right") == "left")
            fh_side_x  = hitter_pos.x > 0.30 if not is_lefty else hitter_pos.x < 0.70
            runround   = max(0.0, min(0.20, (hitter.fh_power - hitter.bh_power)/200.0*0.30 + (tiq-50)/200.0))
            ball_on_fh = _ball_on_fh_side(hitter, incoming.court_x, hitter_pos.x)
            if rng() < runround and fh_side_x:
                shot_type = "forehand"
            elif ball_on_fh:
                # Ball is naturally on the FH side — play forehand more often
                shot_type = "forehand" if rng() < 0.72 else "backhand"
            elif rng() < 0.55 + (hitter.fh_power - hitter.bh_power)/400.0:
                shot_type = "forehand"
            else:
                shot_type = "backhand"

    # ── 1HBH vs 2HBH adjustments ────────────────────────────────────────
    bh_style = getattr(hitter, "backhand_style", "two_handed")
    is_1hbh  = (bh_style == "one_handed")
    is_bh_shot = shot_type in ("backhand", "slice_backhand", "backhand_volley")

    if is_bh_shot and is_1hbh:
        if incoming.height > 65:
            # 1HBH on high balls: very awkward, high error tendency → force slice
            if rng() < 0.55 + (hitter.slice - 50) / 200.0:
                shot_type = "slice_backhand"   # natural defensive response
            elif rng() < 0.30:
                shot_type = "lob"              # bail out with lob
        elif incoming.height < 28:
            # 1HBH on very low balls: good slice/half-volley
            if rng() < 0.40 + (hitter.slice - 50) / 150.0:
                shot_type = "slice_backhand"
        # 1HBH loves down-the-line (more natural swing path)
        # This gets applied in the direction logic below

    # ── Direction ────────────────────────────────────────────────────────
    def_fh = (defender.fh_accuracy + defender.fh_timing) / 2.0
    def_bh = (defender.bh_accuracy + defender.bh_timing) / 2.0
    exploit = max(0.0, min(0.50, (tiq - 50)/50.0*0.30 + risk_dev*0.15))

    # Pressure adjusts risk: aggressive under pressure → more DTL
    effective_dtl = clamp(dtl_p * press_mult, 0.15, 0.60)

    if shot_type in ("overhead", "drop_shot"):
        direction = "cross_court" if rng() < 0.65 else "dtl"
    elif shot_type == "lob":
        direction = "dtl" if rng() < 0.55 else "cross_court"
    elif shot_type in ("forehand_volley", "backhand_volley"):
        direction = "cross_court" if rng() < 0.55 else "dtl"
    elif hitter_pos.role == "offense":
        r = rng()
        if r < 0.10 + risk_dev * 0.10:
            direction = "body"
        elif r < 0.45 + risk_dev * 0.10:
            direction = "dtl"
        else:
            direction = "cross_court"
    elif rng() < exploit:
        # Attack the weaker wing — handedness-aware cross-court targeting
        # When same-handed: CC forehand goes to opponent's backhand naturally
        # When opposite-handed: CC forehand goes to opponent's forehand
        hitter_hand   = getattr(hitter,   "handedness", "right")
        defender_hand = getattr(defender, "handedness", "right")
        same_handed   = (hitter_hand == defender_hand)

        # If CC naturally goes to defender BH (same-handed) AND BH is weaker → CC
        if same_handed:
            if def_bh < def_fh:
                direction = "cross_court"   # CC → their BH (weaker)
            else:
                direction = "dtl"           # DTL → their FH (weaker) if same-handed
        else:
            # Opposite-handed: CC goes to their FH, DTL goes to their BH
            if def_bh < def_fh:
                direction = "dtl"           # DTL → their BH (weaker)
            else:
                direction = "cross_court"   # CC → their FH (weaker)
    elif shot_type == "forehand" and hitter_pos.x < 0.40:
        direction = "inside_out" if rng() < 0.65 else "inside_in"
    elif rng() < effective_dtl:
        direction = "dtl"
    elif is_bh_shot and is_1hbh and rng() < 0.28:
        # 1HBH strongly favors DTL — the inside-out swing path is natural.
        # Federer/Henin ran around FH rather than open up with 1HBH CC.
        direction = "dtl"
    elif is_bh_shot and not is_1hbh:
        # 2HBH: strong CC (heavy topspin angle) is their WEAPON
        direction = "dtl" if rng() < 0.12 else "cross_court"
    elif rng() < 0.08:
        direction = "body"
    else:
        direction = "cross_court"

    # ── Spin ─────────────────────────────────────────────────────────────
    slice_stat = float(hitter.slice)
    spin_stat  = float(hitter.racket_spin)

    # fh_spin / bh_spin sliders → more topspin vs flat preference
    is_fh     = "forehand" in shot_type
    spin_dev  = _lo(sliders, "fh_spin" if is_fh else "bh_spin")

    if shot_type == "lob":
        spin = "heavy_topspin"
    elif shot_type in ("slice_backhand", "slice_forehand"):
        spin = "slice"
    elif shot_type == "overhead":
        spin = "flat"
    elif shot_type == "drop_shot":
        spin = "slice" if rng() < 0.60 else "topspin"
    elif shot_type in ("forehand_volley", "backhand_volley"):
        spin = "flat" if rng() < 0.55 else "slice"
    elif hitter_pos.role == "defense":
        spin = "slice" if rng() < 0.55 else "topspin"
    else:
        r = rng()
        # Higher slice_usage slider → more slice overall
        slice_chance = max(0.04, min(0.35, 0.08 + (slice_stat - 50)/100.0*0.10 + (float(sliders.get("slice_usage",50))-50)/50.0*0.12))
        # Higher fh_spin/bh_spin → more topspin preference, less flat
        heavy_chance = max(0.05, min(0.40, 0.18 + (spin_stat - 50)/100.0*0.15 + spin_dev * 0.10))
        topspin_chance = max(0.20, min(0.60, 0.45 + spin_dev * 0.12))
        if r < slice_chance:
            spin = "slice"
        elif r < slice_chance + heavy_chance:
            spin = "heavy_topspin"
        elif r < slice_chance + heavy_chance + topspin_chance:
            spin = "topspin"
        else:
            spin = "flat"

    return shot_type, direction, spin

# --- BALL DIFFICULTY ---
def rate_ball_difficulty(
    incoming: RallyShot,
    hitter_pos: CourtPosition,
    hitter: PlayerProfile,
    cpi: float,
) -> float:
    """Returns difficulty [0,1] for the incoming ball."""
    diff = 0.0
    cpi_norm = cpi / 100.0
    diff += (incoming.pace / 100.0) * (0.25 + cpi_norm * 0.20)

    dx = abs(incoming.court_x - hitter_pos.x)
    if incoming.spin == "lob" or incoming.height > 70:
        dy = abs((1.0 - incoming.court_y) - hitter_pos.y)
    else:
        dy = max(0.0, (1.0 - incoming.court_y) - 0.10)
    run_dist  = math.sqrt(dx*dx + dy*dy*0.5)
    coverage  = ((hitter.speed + hitter.footwork) / 2.0) / 100.0
    diff += min(0.35, max(0.0, run_dist - 0.10) * (1.0 - coverage * _W_STAT * 4))

    if incoming.spin == "slice":
        diff += max(0.0, (30.0 - incoming.height) / 100.0 * 0.15)
    elif incoming.spin in ("heavy_topspin", "kick"):
        diff += max(0.0, (incoming.height - 55.0) / 100.0 * 0.12)

    if incoming.depth > 80:
        diff += (incoming.depth - 80.0) / 200.0 * 0.10
    elif incoming.depth < 25:
        diff -= 0.05

    return float(max(0.0, min(1.0, diff)))

# --- SHOT QUALITY ---
def compute_shot_quality(
    hitter: PlayerProfile,
    shot_type: str,
    direction: str,
    spin: str,
    sliders: dict,
    incoming_diff: float,
    hitter_pos: CourtPosition,
    cpi: float,
    pressure: bool,
) -> float:
    """
    Returns [0,100] execution quality.
    Power sliders boost quality but require accuracy/timing; spin sliders reduce
    raw quality (pace) but reduce error risk (handled in shot_outcome_probs).
    ±MAX_LO (20) is the maximum any single slider can move the quality.
    """
    # ── Base stat ────────────────────────────────────────────────────────
    if shot_type == "forehand":
        stat      = hitter.fh_power*0.45 + hitter.fh_accuracy*0.35 + hitter.fh_timing*0.20
        pow_dev   = _lo(sliders, "fh_power")
        spin_dev  = _lo(sliders, "fh_spin")
        acc_norm  = clamp((hitter.fh_accuracy + hitter.fh_timing - 100.0) / 100.0, -1.0, 1.0)
        pow_bonus = pow_dev * _MAX_LO * max(0.3, (1.0 + acc_norm) / 2.0)
        # Spin adds margin (doesn't change quality but flattens errors in shot_outcome_probs)
        # Spin quality effect: spin requires power to be effective
        spin_quality = spin_dev * _MAX_LO * 0.35 * max(0.3, (1.0 + clamp((hitter.fh_power-50)/50.0,-1,1))/2.0)
        lo_boost  = pow_bonus / 100.0 + spin_quality / 100.0

    elif shot_type == "backhand":
        stat      = hitter.bh_power*0.45 + hitter.bh_accuracy*0.35 + hitter.bh_timing*0.20
        pow_dev   = _lo(sliders, "bh_power")
        spin_dev  = _lo(sliders, "bh_spin")
        acc_norm  = clamp((hitter.bh_accuracy + hitter.bh_timing - 100.0) / 100.0, -1.0, 1.0)
        pow_bonus = pow_dev * _MAX_LO * max(0.3, (1.0 + acc_norm) / 2.0)
        spin_quality = spin_dev * _MAX_LO * 0.35 * max(0.3, (1.0 + clamp((hitter.bh_power-50)/50.0,-1,1))/2.0)
        lo_boost  = pow_bonus / 100.0 + spin_quality / 100.0

    elif shot_type in ("forehand_volley", "swinging_volley"):
        stat     = hitter.volley*0.55 + hitter.fh_power*0.25 + hitter.touch*0.20
        lo_boost = _lo(sliders, "fh_power") * _W_LOADOUT * 0.5

    elif shot_type == "backhand_volley":
        stat     = hitter.volley*0.55 + hitter.bh_power*0.25 + hitter.touch*0.20
        lo_boost = _lo(sliders, "bh_power") * _W_LOADOUT * 0.5

    elif shot_type == "overhead":
        stat     = hitter.touch*0.45 + hitter.fh_power*0.35 + hitter.tennis_iq*0.20
        lo_boost = 0.0

    elif shot_type == "drop_shot":
        stat     = hitter.drop_shot_effectivity*0.55 + hitter.touch*0.30 + hitter.fh_accuracy*0.15
        drop_dev = _lo(sliders, "drop_frequency")
        lo_boost = drop_dev * _W_LOADOUT * 0.5

    elif shot_type == "lob":
        stat     = hitter.lob*0.50 + hitter.touch*0.30 + hitter.tennis_iq*0.20
        lo_boost = 0.0

    elif shot_type == "half_volley":
        stat     = hitter.half_volley*0.60 + hitter.touch*0.25 + hitter.fitness*0.15
        lo_boost = 0.0

    elif shot_type in ("slice_backhand", "slice_forehand"):
        stat     = hitter.slice*0.55 + hitter.bh_accuracy*0.25 + hitter.racket_control*0.20
        su_dev   = _lo(sliders, "slice_usage")
        lo_boost = su_dev * _W_LOADOUT * 0.4   # more slice practice → slightly better slice

    else:
        stat     = (hitter.fh_accuracy + hitter.bh_accuracy) / 2.0
        lo_boost = 0.0

    # ── Direction risk ───────────────────────────────────────────────────
    risk_dev = _lo(sliders, "shot_dir_risk")
    acc_t_norm = clamp((getattr(hitter,"fh_accuracy",50)+getattr(hitter,"fh_timing",50)-100)/100.0,-1.0,1.0)
    # Power direction shots require accuracy; penalty if accuracy low
    if direction == "dtl":
        # risk_dev > 0 → more DTL, quality hit unless accuracy is good
        quality_penalty = risk_dev * _MAX_LO * (0.5 - 0.4 * max(0.0, acc_t_norm))
        quality = stat - quality_penalty
    elif direction in ("inside_out", "inside_in"):
        quality_penalty = risk_dev * _MAX_LO * 0.30
        quality = stat - quality_penalty
    elif direction == "body":
        quality = stat + 2.0
    else:
        quality = stat

    # ── Apply loadout power/spin modifier ───────────────────────────────
    quality *= (1.0 + lo_boost)

    # ── Difficulty of incoming ball ──────────────────────────────────────
    quality *= (1.0 - incoming_diff * 0.30)

    # ── Fatigue: per-point stamina level ────────────────────────────────
    quality *= (1.0 - _fatigue_penalty_mult(hitter))

    # ── Sharpness + venue experience ────────────────────────────────────
    quality *= hitter.sharpness_mult
    quality *= hitter.venue_exp_mult

    # ── Pressure play risk: high risk amplifies quality deviation ───────
    quality = pressure_quality_variance(hitter, pressure, quality)

    # ── RNG noise ───────────────────────────────────────────────────────
    quality += random.gauss(0.0, _BASE_RNG_SPREAD * 24.0)  # was *15.0

    return float(max(0.0, min(100.0, quality)))

# --- BALL PHYSICS ---
def compute_ball_physics(
    hitter: PlayerProfile,
    shot_type: str,
    spin: str,
    shot_quality: float,
    sliders: dict,
    cpi: float,
) -> Tuple[float, float, float]:
    """Returns (pace, height, depth) all 0-100."""
    cpi_norm  = cpi / 100.0
    q_norm    = shot_quality / 100.0
    pace_bias = _pace_bias(sliders)

    if shot_type in ("drop_shot", "lob"):
        pace = 5.0 + q_norm * 20.0
    elif shot_type == "overhead":
        pace = 70.0 + q_norm * 25.0
    elif shot_type in ("forehand_volley", "backhand_volley"):
        pace = 55.0 + q_norm * 30.0
    elif shot_type in ("slice_backhand", "slice_forehand"):
        pace = 30.0 + q_norm * 35.0
    elif shot_type == "half_volley":
        pace = 40.0 + q_norm * 30.0
    else:
        power = hitter.fh_power if "forehand" in shot_type else hitter.bh_power
        pace  = 30.0 + (power/100.0)*45.0 + cpi_norm*10.0
        pace  *= pace_bias
        pace  += random.gauss(0.0, 5.0)

    if shot_type == "lob":
        height = random.uniform(80.0, 98.0)
    elif shot_type == "overhead":
        height = random.uniform(5.0, 20.0)
    elif shot_type in ("forehand_volley", "backhand_volley"):
        height = random.uniform(10.0, 35.0)
    elif shot_type == "drop_shot":
        height = random.uniform(8.0, 22.0)
    elif spin == "slice":
        height = random.uniform(12.0, 32.0)
    elif spin in ("heavy_topspin", "kick"):
        height = random.uniform(50.0, 78.0)
    elif spin == "topspin":
        height = random.uniform(30.0, 58.0)
    else:
        height = random.uniform(18.0, 38.0)
    height -= cpi_norm * 8.0

    if shot_type == "drop_shot":
        depth = random.uniform(5.0, 25.0)
    elif shot_type == "lob":
        depth = random.uniform(75.0, 98.0)
    elif shot_type in ("forehand_volley", "backhand_volley", "overhead"):
        depth = random.uniform(40.0, 72.0)
    else:
        depth = 50.0 + q_norm*40.0 - random.gauss(0.0, 7.0)

    return (
        float(max(0.0, min(100.0, pace))),
        float(max(0.0, min(100.0, height))),
        float(max(0.0, min(100.0, depth))),
    )

# --- LANDING POSITION ---
def compute_landing_pos(
    direction: str,
    hitter_pos: CourtPosition,
    shot_quality: float,
    depth: float,
) -> Tuple[float, float]:
    q_norm  = shot_quality / 100.0
    sx = random.gauss(0.0, (1.0 - q_norm) * 0.15)
    sy = random.gauss(0.0, (1.0 - q_norm) * 0.10)

    if direction == "cross_court":
        base_x = 0.20 + random.uniform(0.0, 0.20)
        target_x = base_x if hitter_pos.x < 0.50 else 1.0 - base_x
    elif direction == "dtl":
        target_x = 0.85 + random.uniform(0.0, 0.10) if hitter_pos.x < 0.50 else 0.05 + random.uniform(0.0, 0.10)
    elif direction == "inside_out":
        target_x = 0.15 + random.uniform(0.0, 0.15)
    elif direction == "inside_in":
        target_x = 0.75 + random.uniform(0.0, 0.15)
    elif direction == "body":
        target_x = 0.45 + random.uniform(0.0, 0.10)
    else:
        target_x = 0.50

    court_x = float(max(0.0, min(1.0, target_x + sx)))
    court_y = float(max(0.0, min(1.0, depth / 100.0 + sy)))
    return court_x, court_y

# --- POSITION UPDATES ---
def update_positions_after_shot(
    hitter_pos: CourtPosition,
    def_pos: CourtPosition,
    shot: RallyShot,
    hitter: PlayerProfile,
    defender: PlayerProfile,
    sliders_hitter: dict,
    sliders_defender: dict,
) -> Tuple[CourtPosition, CourtPosition, str]:
    net_freq       = _net_approach_freq(sliders_hitter)
    already_at_net = shot.shot_type in ("forehand_volley", "backhand_volley", "overhead", "swinging_volley")
    can_approach   = (
        hitter_pos.y < 0.65
        and shot.depth < 45
        and hitter_pos.role == "offense"
        and shot.shot_type not in ("drop_shot", "lob")
    )
    came_to_net = already_at_net or (can_approach and random.random() < net_freq)

    if came_to_net:
        new_hitter_x, new_hitter_y = 0.50, 0.25
    elif shot.shot_type == "drop_shot":
        new_hitter_x, new_hitter_y = 0.50, 0.30
    elif shot.shot_type == "lob":
        new_hitter_x, new_hitter_y = 0.50, 0.95
    else:
        # movement_aggression: high → stay close to baseline
        new_hitter_x = 0.50
        new_hitter_y = _baseline_y(sliders_hitter)

    dx    = shot.court_x - def_pos.x
    reach = ((defender.speed + defender.footwork) / 2.0) / 100.0
    new_def_x = def_pos.x + dx * min(1.0, reach + 0.30)
    # Defender recovers toward their preferred baseline depth
    new_def_y = max(shot.court_y, _baseline_y(sliders_defender) * 0.85)

    new_def_x = float(max(0.02, min(0.98, new_def_x)))
    new_def_y = float(max(0.05, min(1.05, new_def_y)))

    horiz_stretch = abs(new_def_x - 0.50) > 0.30
    ball_short    = shot.depth < 40

    if horiz_stretch and shot.depth > 75:
        next_role = "defense"
    elif ball_short and not horiz_stretch:
        next_role = "offense"
    elif horiz_stretch:
        next_role = "defense"
    else:
        next_role = "neutral"

    new_hitter = CourtPosition(x=new_hitter_x, y=new_hitter_y, role="neutral")
    new_def    = CourtPosition(x=new_def_x,    y=new_def_y,    role=next_role)
    return new_hitter, new_def, next_role

# --- OUTCOME PROBABILITIES ---
def shot_outcome_probs(
    shot: RallyShot,
    shot_quality: float,
    def_pos: CourtPosition,
    hitter: PlayerProfile,
    defender: PlayerProfile,
    incoming_diff: float,
    sliders_hitter: dict,
    pressure: bool,
    cpi: float,
    cond_error_bias: float,
    shot_num: int,
) -> Tuple[float, float, float]:
    """Returns (p_winner, p_unforced_error, p_forced_error).

    Fatigue is applied PER SHOT — later shots in a rally degrade quality further.
    Spin sliders (fh_spin / bh_spin) add a safety margin that reduces UE probability.
    Power sliders add pace but push up error risk when accuracy is insufficient.
    """
    q_norm = shot_quality / 100.0

    # ── Per-shot fatigue accumulation ────────────────────────────────────
    # Fatigue penalty grows shot-by-shot during the rally (not just once at point start)
    shot_fatigue = _fatigue_penalty_mult(hitter)
    # Long rallies compound fatigue: each shot in a rally after shot 3 adds exponentially
    rally_fatigue_extra = min(0.18, max(0, shot_num - 3) * 0.012 * (1.0 + shot_fatigue * 3.0))
    total_fatigue_error = shot_fatigue + rally_fatigue_extra

    # ── Pressure play risk: split winner vs error under pressure ─────────
    pressure_risk_t = _lo({"v": hitter.lo_pressure_play_risk}, "v")   # -1..+1
    # High risk: more winner chance AND more error chance (variance amplifier)
    # Low risk:  less winner chance but much fewer errors (safe play)
    pressure_winner_bonus = pressure * pressure_risk_t * 0.06
    pressure_error_bonus  = pressure * abs(pressure_risk_t) * 0.05   # both extremes add slight error

    # ── Direction/power risk ─────────────────────────────────────────────
    risk_dev = _lo(sliders_hitter, "shot_dir_risk")
    risk = max(0.0, risk_dev)   # only positive risk matters for errors

    # Power without accuracy: if fh/bh_power slider high and accuracy < 60, amplify errors
    is_fh = "forehand" in shot.shot_type
    pow_dev  = _lo(sliders_hitter, "fh_power" if is_fh else "bh_power")
    acc_stat = (hitter.fh_accuracy + hitter.fh_timing) / 2.0 if is_fh else (hitter.bh_accuracy + hitter.bh_timing) / 2.0
    # Power error penalty: bigger if accuracy/timing is weak, caps at ±20 effect
    power_err_penalty = max(0.0, pow_dev) * _MAX_LO * 0.010 * max(0.0, (65.0 - acc_stat) / 65.0)

    # ── 1HBH high-ball penalty ───────────────────────────────────────────
    # One-handed backhand struggles severely on high balls — add error risk
    bh_style_1h  = (getattr(hitter, "backhand_style", "two_handed") == "one_handed")
    is_bh_shot2  = "backhand" in shot.shot_type
    high_ball2   = (getattr(shot, "height", 50) > 62)
    if bh_style_1h and is_bh_shot2 and high_ball2:
        # Scale penalty with how high the ball is (80 = nightmare, 65 = manageable)
        height_excess = clamp((shot.height - 62) / 38.0, 0.0, 1.0)
        power_err_penalty += 0.08 * height_excess   # up to +8% extra UE on very high balls

    # ── 1HBH slice slice bonus (1HBH players are very effective slicing) ─
    is_slice = "slice" in shot.shot_type
    if bh_style_1h and is_slice and is_bh_shot2:
        spin_safety_from_1hbh = 0.04 * clamp((hitter.slice - 40) / 59.0, 0.0, 1.0)
        # Gets applied below as part of spin_safety
    else:
        spin_safety_from_1hbh = 0.0

    # Spin safety bonus: fh_spin / bh_spin slider reduces UE risk
    # Spin requires good power to be fully effective (spin off a weak ball doesn't help as much)
    spin_dev  = _lo(sliders_hitter, "fh_spin" if is_fh else "bh_spin")
    pow_stat  = hitter.fh_power if is_fh else hitter.bh_power
    spin_safety = (
        max(0.0, spin_dev) * _MAX_LO * 0.008 * max(0.3, (1.0 + clamp((pow_stat-50)/50.0,-1,1))/2.0)
        + spin_safety_from_1hbh
    )

    # ── Court position ───────────────────────────────────────────────────
    dx  = abs(shot.court_x - def_pos.x)
    dy  = abs(shot.court_y - def_pos.y)
    def_dist = math.sqrt(dx*dx + dy*dy)
    coverage = ((defender.speed + defender.footwork) / 2.0) / 100.0
    offense_bonus = 0.08 if def_pos.role == "defense" else 0.0

    p_winner = (
        (q_norm - 0.50) * 0.25
        + max(0.0, def_dist - 0.20) * 0.30
        - coverage * 0.12
        + risk * 0.06
        + offense_bonus
        + (cpi / 100.0) * 0.04
        + float(pressure_winner_bonus)
    )
    if shot.shot_type == "overhead":
        p_winner += 0.12
    elif shot.shot_type in ("forehand_volley", "backhand_volley", "swinging_volley"):
        p_winner += 0.06
    elif shot.shot_type == "drop_shot":
        p_winner += max(0.0, def_pos.y - 0.60) * 0.25
    elif shot.shot_type == "lob":
        p_winner += 0.15 if def_pos.y < 0.40 else -0.10
    p_winner = float(max(0.02, min(0.32, p_winner)))

    quality_error_risk = max(0.0, 0.60 - q_norm)
    p_ue = (
        quality_error_risk * 0.55 * (1.0 - incoming_diff)
        + risk * 0.10
        + cond_error_bias * 0.60
        + total_fatigue_error * 0.32    # fatigue builds strongly in rally
        + power_err_penalty             # power without accuracy
        - spin_safety                   # spin adds safety margin
        + float(pressure_error_bonus)
    )
    if shot.shot_type in ("drop_shot",):
        p_ue += 0.04
    p_ue = float(max(0.02, min(0.28, p_ue)))

    p_fe = (
        quality_error_risk * 0.45 * incoming_diff
        + incoming_diff * 0.08
        + risk * 0.05
        + cond_error_bias * 0.40
        + shot_fatigue * 0.12          # tired players get forced off the ball more
    )
    p_fe = float(max(0.01, min(0.22, p_fe)))

    total_bad = p_winner + p_ue + p_fe
    if total_bad > 0.68:
        s = 0.68 / total_bad
        p_winner *= s; p_ue *= s; p_fe *= s

    return p_winner, p_ue, p_fe

def _error_wing_label(shot: RallyShot) -> str:
    if shot.shot_type in ("forehand", "forehand_volley", "slice_forehand"):
        return "forehand"
    elif shot.shot_type in ("backhand", "backhand_volley", "slice_backhand"):
        return "backhand"
    return "other"


# --- FULL SHOT-BY-SHOT RALLY ENGINE ---
def run_rally_engine(
    server: PlayerProfile,
    returner: PlayerProfile,
    server_sliders: dict,
    returner_sliders: dict,
    effective_cpi: float,
    cond_error_bias: float,
    pressure: bool,
    serve_depth: float,
    serve_return_diff: float = 0.40,   # initial returner difficulty from serve
    cond: Optional[MatchConditions] = None,
    serve_spin_type: str = "flat",     # "flat" | "kick" | "slice"
) -> Tuple[int, str, int, str, Dict[str, Any]]:
    """
    Simulates a full rally shot-by-shot.
    Returns (winner_rel, typ, shots, used_side, shot_log)
    winner_rel: 0=server wins, 1=returner wins

    return_position slider → returner starting Y (inside vs deep)
    movement_aggression slider → both players' preferred baseline depth
    serve_spin_type → determines the initial bounce shape the returner faces
    """
    # ── Starting positions ───────────────────────────────────────────────
    agg_y_server   = _baseline_y(server_sliders)
    ret_y_pos      = _return_pos_y({"return_position": returner.lo_return_position})

    server_pos   = CourtPosition(x=0.50, y=agg_y_server, role="neutral")
    returner_pos = CourtPosition(x=0.50, y=ret_y_pos,    role="neutral")

    # High serve_return_diff → returner starts in defense
    returner_pos.role = "defense" if serve_return_diff > 0.50 else "neutral"
    # Inside returner slightly wider initial X (anticipates more)
    returner_pos.x = 0.50 + random.uniform(-0.12, 0.12)

    shot_log: Dict[str, Any] = {
        "shots": [], "winner_side": None, "winner_type": None,
        "unforced_side": None, "forced_side": None,
    }

    # ── Initial "ball" = the serve the returner is facing ────────────────
    # Kick serve: high, heavy, pulls returner; harder to drive
    # Slice serve: low, skidding, wide; must dig it up or slice back
    # Flat serve: fast, low, penetrating; pace dominant
    if serve_spin_type == "kick":
        init_height = random.uniform(55.0, 80.0)   # high bounce
        init_spin   = "kick"
        init_pace   = 45.0 + (serve_depth / 100.0) * 30.0   # slightly less raw pace
    elif serve_spin_type == "slice":
        init_height = random.uniform(12.0, 28.0)   # stays very low
        init_spin   = "slice"
        init_pace   = 40.0 + (serve_depth / 100.0) * 35.0
    else:  # flat
        init_height = random.uniform(18.0, 40.0)
        init_spin   = "flat"
        init_pace   = 55.0 + (serve_depth / 100.0) * 35.0

    incoming = RallyShot(
        shot_type="forehand", direction="cross_court",
        spin=init_spin,
        pace=init_pace,
        height=init_height,
        depth=serve_depth,
        court_x=0.50 + random.uniform(-0.25, 0.25),
        court_y=0.75 + (serve_depth / 100.0) * 0.20,
        quality=serve_depth,
    )

    # Returner starts with elevated difficulty from serve quality
    # Inside returner is more cramped by center serves, wider by wide/T serves
    incoming_diff_override = serve_return_diff

    shot_count = 0
    hitter, defender             = returner, server
    hitter_pos, def_pos          = returner_pos, server_pos
    hitter_sliders, def_sliders  = returner_sliders, server_sliders
    incoming_diff                = incoming_diff_override

    # ── Per-shot stamina drain constants ────────────────────────────────
    # Increased from 0.04 to make fatigue matter across a match.
    # Aggressive movement (high movement_aggression) costs more.
    STAMINA_PER_SHOT_BASE = 0.10   # base drain per shot exchange

    while shot_count < _MAX_RALLY_SHOTS:
        shot_count += 1

        # Re-rate difficulty from actual ball position (overrides initial on shot 1)
        if shot_count > 1:
            incoming_diff = rate_ball_difficulty(incoming, hitter_pos, hitter, effective_cpi)

        stype, sdirection, sspin = choose_shot(
            hitter, hitter_sliders, hitter_pos, incoming, defender, pressure, effective_cpi
        )
        quality = compute_shot_quality(
            hitter, stype, sdirection, sspin, hitter_sliders,
            incoming_diff, hitter_pos, effective_cpi, pressure
        )
        pace, height, depth = compute_ball_physics(
            hitter, stype, sspin, quality, hitter_sliders, effective_cpi
        )
        court_x, court_y = compute_landing_pos(sdirection, hitter_pos, quality, depth)

        outgoing = RallyShot(
            shot_type=stype, direction=sdirection, spin=sspin,
            pace=pace, height=height, depth=depth,
            court_x=court_x, court_y=court_y, quality=quality,
        )

        p_win, p_ue, p_fe = shot_outcome_probs(
            outgoing, quality, def_pos, hitter, defender,
            incoming_diff, hitter_sliders, pressure, effective_cpi,
            cond_error_bias, shot_count
        )

        hitter_is_server = (hitter is server)
        hitter_rel   = 0 if hitter_is_server else 1
        defender_rel = 1 - hitter_rel

        shot_log["shots"].append({
            "shot_num": shot_count, "hitter": "server" if hitter_is_server else "returner",
            "shot_type": stype, "direction": sdirection, "spin": sspin,
            "pace": round(pace, 1), "height": round(height, 1),
            "depth": round(depth, 1), "quality": round(quality, 1),
            "role": hitter_pos.role,
            "stamina": round(hitter.stamina, 1),
        })

        r = random.random()
        wing = _error_wing_label(outgoing)

        if r < p_win:
            shot_log["winner_side"] = wing
            shot_log["winner_type"] = stype
            typ = "Unreturned serve" if shot_count == 1 else "Winner"
            return hitter_rel, typ, shot_count, wing, shot_log

        elif r < p_win + p_ue:
            shot_log["unforced_side"] = wing
            return defender_rel, "Unforced error", shot_count, wing, shot_log

        elif r < p_win + p_ue + p_fe:
            shot_log["forced_side"] = wing
            return defender_rel, "Forced error", shot_count, wing, shot_log

        # ── Per-shot stamina drain ───────────────────────────────────────
        # Movement aggression → more ground covered → more drain
        agg_drain_h = 1.0 + max(0.0, _lo(hitter_sliders,  "movement_aggression")) * 0.35
        agg_drain_d = 1.0 + max(0.0, _lo(def_sliders,     "movement_aggression")) * 0.35
        # Long rallies are exponentially more taxing (conditioning effect)
        rally_compound = 1.0 + max(0, shot_count - 6) * 0.06
        drain_h = STAMINA_PER_SHOT_BASE * agg_drain_h * rally_compound
        drain_d = STAMINA_PER_SHOT_BASE * agg_drain_d * rally_compound * 0.65
        hitter.stamina   = clamp(hitter.stamina   - drain_h, STAMINA_MIN, STAMINA_START)
        defender.stamina = clamp(defender.stamina - drain_d, STAMINA_MIN, STAMINA_START)

        new_hitter_pos, new_def_pos, _ = update_positions_after_shot(
            hitter_pos, def_pos, outgoing, hitter, defender, hitter_sliders, def_sliders
        )
        hitter, defender              = defender, hitter
        hitter_sliders, def_sliders  = def_sliders, hitter_sliders
        hitter_pos, def_pos          = new_def_pos, new_hitter_pos
        incoming                     = outgoing
        incoming_diff                = rate_ball_difficulty(incoming, hitter_pos, hitter, effective_cpi)

    # Safety cap: weighted coin by stamina
    srv_s = server.stamina / STAMINA_START
    ret_s = returner.stamina / STAMINA_START
    winner_rel = 0 if random.random() < srv_s / (srv_s + ret_s + 1e-9) else 1
    used_side  = "forehand" if random.random() < 0.55 else "backhand"
    return winner_rel, "Forced error", shot_count, used_side, shot_log



# --- MAIN simulate_point ---
def simulate_point(state: MatchState) -> Tuple[int, str, int, Dict[str, Any]]:
    server   = state.p1 if state.server_idx == 0 else state.p2
    returner = state.p2 if state.server_idx == 0 else state.p1

    gp1, gp2 = state.game_points
    side = deuce_or_ad_side(gp1, gp2)

    pressure = is_pressure_point(
        state.in_tiebreak,
        state.tiebreak_points[0], state.tiebreak_points[1],
        state.game_points, state.current_games,
    )

    meta: Dict[str, Any] = {
        "server_idx":    state.server_idx,
        "second_serve":  False,
        "event":         "rally",
        "winner_side":   None,
        "winner_type":   None,
        "unforced_side": None,
        "forced_side":   None,
        "serve_kmh":     None,
        "shot_log":      None,
    }

    # Conditions
    cond          = getattr(state, "conditions", None)
    eff_cpi       = compute_effective_cpi(cond) if cond else 35.0
    cond_err_bias = conditions_error_bias(cond, eff_cpi) if cond else 0.0

    # ── Serve spin type ─────────────────────────────────────────────────
    # deuce_spin / ad_spin slider → high = kick/topspin, low = slice/flat
    side_spin_val = server.lo_deuce_spin if side == "Deuce" else server.lo_ad_spin
    serve_spin_dev = _lo({"v": side_spin_val}, "v")      # -1=flat/slice, +1=kick
    serve_spin_dev2 = _lo({"v": server.lo_serve_spin}, "v")  # general spin
    combined_spin = (serve_spin_dev + serve_spin_dev2) / 2.0

    if combined_spin > 0.3:
        serve_spin_type = "kick"       # high bounce, pulls returner, harder to attack
    elif combined_spin < -0.3:
        serve_spin_type = "slice"      # skids low, wide angles, harder to drive
    else:
        serve_spin_type = "flat"       # pace-dominant, pure speed

    # Serve phase
    p_ace, p_fault, _ = serve_scores(server, returner, second_serve=False, side=side, pressure=pressure)
    spd_kmh = _serve_speed_kmh(server, second_serve=False)
    meta["serve_kmh"] = spd_kmh

    # Serve depth: deep/penetrating serves are harder to return
    serve_depth = float(max(20.0, min(95.0, 45.0 + p_ace * 300.0)))
    # Kick serves tend to sit up but bounce unpredictably → slightly harder
    # Slice serves stay low but slow down → slightly easier on clay, harder on grass
    surface_cpi = eff_cpi
    if serve_spin_type == "kick":
        serve_depth += (surface_cpi / 100.0) * 5.0    # high bounce on slow courts
    elif serve_spin_type == "slice":
        serve_depth -= (1.0 - surface_cpi / 100.0) * 4.0  # skids on fast courts

    # ── Momentum-adjusted RNG ────────────────────────────────────────────
    srv_hot = _P1_HOT if state.server_idx == 0 else _P2_HOT
    ret_hot = _P2_HOT if state.server_idx == 0 else _P1_HOT

    roll = random.random()
    if roll < bounded_rng(p_ace, hot=srv_hot):
        meta["event"] = "ace"
        return state.server_idx, "Ace", 1, meta

    if roll < bounded_rng(p_ace + p_fault, hot=-srv_hot):  # hot server less likely to fault
        meta["second_serve"] = True
        spd2_kmh = _serve_speed_kmh(server, second_serve=True)
        meta["serve_kmh"] = spd2_kmh
        p2_ace, p2_fault, _ = serve_scores(server, returner, second_serve=True, side=side, pressure=pressure)
        # Second serve usually kick-heavy (safer, higher margin)
        serve_spin_type = "kick" if combined_spin > -0.5 else "slice"
        serve_depth = float(max(10.0, min(85.0, 35.0 + p2_ace * 250.0)))
        roll2 = random.random()
        if roll2 < bounded_rng(p2_ace, hot=srv_hot * 0.7):
            meta["event"] = "ace"
            return state.server_idx, "Ace (2nd serve)", 1, meta
        if roll2 < bounded_rng(p2_ace + p2_fault, hot=-srv_hot * 0.5):
            meta["event"] = "double_fault"
            meta["unforced_side"] = "serve"
            return 1 - state.server_idx, "Double fault", 1, meta

    # Pull sliders from profile (_match_sliders set at match start)
    _empty_sl = {k: 50 for k in SLIDER_KEYS}
    server_sl   = getattr(server,   "_match_sliders", _empty_sl)
    returner_sl = getattr(returner, "_match_sliders", _empty_sl)

    # Compute initial returner difficulty from the serve
    serve_return_diff = _serve_return_difficulty(server, returner, second_serve=False, side=side, p_ace=p_ace)

    # Run rally engine — pass serve_spin_type so the first return ball is correct
    winner_rel, typ, shots, used_side, shot_log = run_rally_engine(
        server=server, returner=returner,
        server_sliders=server_sl, returner_sliders=returner_sl,
        effective_cpi=eff_cpi, cond_error_bias=cond_err_bias,
        pressure=pressure, serve_depth=serve_depth,
        serve_return_diff=serve_return_diff, cond=cond,
        serve_spin_type=serve_spin_type,
    )

    meta["shot_log"] = shot_log
    winner_idx = state.server_idx if winner_rel == 0 else 1 - state.server_idx

    wsl = shot_log.get("winner_side") or used_side
    wtp = shot_log.get("winner_type") or "regular"
    usl = shot_log.get("unforced_side") or used_side
    fsl = shot_log.get("forced_side")  or used_side

    def _wlabel(s: str, t: str) -> str:
        sv = s.replace("_", " ")
        return f"{sv} winner" if t in ("regular", "forehand", "backhand", "other") else f"{sv} {t.replace('_',' ')} winner"

    if winner_idx == state.server_idx:
        if typ == "Unreturned serve":
            meta["event"] = "service_winner"
            return winner_idx, "Unreturned serve", shots, meta
        if typ == "Winner":
            meta["event"] = "winner"; meta["winner_side"] = wsl; meta["winner_type"] = wtp
            return winner_idx, _wlabel(wsl, wtp), shots, meta
        if typ == "Unforced error":
            meta["event"] = "unforced_error"; meta["unforced_side"] = usl
            return winner_idx, "Unforced error", shots, meta
        meta["event"] = "forced_error"; meta["forced_side"] = fsl
        return winner_idx, "Forced error", shots, meta
    else:
        srv_spd = f" ({spd_kmh:.0f} km/h | {spd_kmh*KMH_TO_MPH:.0f} mph)"
        if typ == "Winner":
            meta["event"] = "winner"; meta["winner_side"] = wsl; meta["winner_type"] = wtp
            lbl = _wlabel(wsl, wtp)
            return winner_idx, f"{lbl}{srv_spd} ({shots} shots)", shots, meta
        if typ == "Unforced error":
            # Server made an unforced error — attribute it correctly as a UE, not a winner
            meta["event"] = "unforced_error"; meta["unforced_side"] = usl
            return winner_idx, f"Unforced error{srv_spd} ({shots} shots)", shots, meta
        # Forced error (server couldn't handle returner's quality shot)
        meta["event"] = "forced_error"; meta["forced_side"] = fsl
        return winner_idx, f"Forced error{srv_spd} ({shots} shots)", shots, meta


# =========================
# Scoring engine
# =========================
def tb_target_for_set(best_of: int, set_index: int) -> int:
    if best_of == 5 and set_index == 4:
        return 10
    return 7

def match_sets_needed(best_of: int) -> int:
    return best_of // 2 + 1

def tiebreak_won(p1: int, p2: int, target: int) -> Optional[int]:
    if (p1 >= target or p2 >= target) and abs(p1 - p2) >= 2:
        return 0 if p1 > p2 else 1
    return None

def game_won(pa: int, pb: int) -> Optional[int]:
    if (pa >= 4 or pb >= 4) and abs(pa - pb) >= 2:
        return 0 if pa > pb else 1
    return None

def tiebreak_server_index(point_number: int, initial_server_idx: int) -> int:
    if point_number == 0:
        return initial_server_idx
    block = (point_number - 1) // 2
    return 1 - initial_server_idx if block % 2 == 0 else initial_server_idx

# =========================
# Formatting
# =========================

# Maps shot_type from rally engine → winners stat bucket.
# Avoids double-counting when both winner_side and winner_type are set.
_SHOT_TO_WINNER_KEY: Dict[str, str] = {
    "forehand":        "forehand",
    "backhand":        "backhand",
    "forehand_volley": "volley",
    "backhand_volley": "volley",
    "swinging_volley": "volley",
    "overhead":        "overhead",
    "drop_shot":       "drop_shot",
    "lob":             "lob",
    "slice_forehand":  "slice",
    "slice_backhand":  "slice",
    "half_volley":     "half_volley",
}
def format_sets(sets: List[Tuple[int, int]], current: Tuple[int, int], include_current: bool = True) -> str:
    parts = [f"{a}-{b}" for a, b in sets]
    if include_current:
        parts.append(f"{current[0]}-{current[1]}")
    return " | ".join(parts) if parts else "—"

_SUP_TRANS = str.maketrans("0123456789", "⁰¹²³⁴⁵⁶⁷⁸⁹")

def _to_sup(n) -> str:
    return str(n).translate(_SUP_TRANS)

def format_completed_sets_winner_labeled(state: MatchState) -> str:
    parts = []
    for i, (a, b) in enumerate(state.sets):
        tb = state.set_tb_loser_points[i] if i < len(state.set_tb_loser_points) else None
        def fmt(x: int, y: int, tb_loser: Optional[int]) -> str:
            s = f"{x}-{y}"
            if tb_loser is not None and ((x == 7 and y == 6) or (x == 6 and y == 7)):
                s += f"({tb_loser})"
            return s
        if a > b:
            parts.append(f"{state.p1.name} {fmt(a,b,tb)}")
        else:
            parts.append(f"{state.p2.name} {fmt(b,a,tb)}")
    return ", ".join(parts) if parts else "—"

def render_match_stats_text(state: MatchState, set_idx: Optional[int] = None) -> str:
    st: Optional[MatchStats] = getattr(state, "stats", None)  # type: ignore
    if not st:
        return "📊 No stats yet."

    def frac(a: int, b: int) -> str:
        return f"{a}/{b}" if b > 0 else "0/0"

    if set_idx is None:
        aces = st.aces; dfs = st.double_faults
        first_in = st.first_serves_in; first_tot = st.first_serves_total
        fs_w = st.first_serve_pts_won; fs_t = st.first_serve_pts_total
        ss_w = st.second_serve_pts_won; ss_t = st.second_serve_pts_total
        ret_w = st.return_pts_won
        bp_saved = st.break_pts_saved; bp_faced = st.break_pts_faced
        bp_conv = st.break_pts_converted; bp_ch = st.break_pts_chances
        winners = st.winners; ues = st.unforced_errors
        fes = getattr(st, "forced_errors", {"forehand":[0,0],"backhand":[0,0],"other":[0,0]})
        pw = st.total_points_won; played = st.total_points_played
    else:
        b = st.sets[set_idx]
        aces = b["aces"]; dfs = b["double_faults"]
        first_in = b["first_serves_in"]; first_tot = b["first_serves_total"]
        fs_w = b["first_serve_pts_won"]; fs_t = b["first_serve_pts_total"]
        ss_w = b["second_serve_pts_won"]; ss_t = b["second_serve_pts_total"]
        ret_w = b["return_pts_won"]
        bp_saved = b["break_pts_saved"]; bp_faced = b["break_pts_faced"]
        bp_conv = b["break_pts_converted"]; bp_ch = b["break_pts_chances"]
        winners = b["winners"]; ues = b["unforced_errors"]
        fes = b["forced_errors"]
        pw = b["points_won"]; played = b["points_played"]

    elapsed = max(0.0, monotonic() - state.started_at)
    mm = int(elapsed // 60); ss_e = int(elapsed % 60)

    rp = getattr(st, "rally_points", 0) if set_idx is None else int(b.get("rally_points", 0))
    rs = getattr(st, "rally_shots_total", 0) if set_idx is None else int(b.get("rally_shots_total", 0))
    avg_rally = (rs / rp) if rp > 0 else 0.0

    p1i, p2i = 0, 1
    title = "📊 **Match Stats (Overall)**" if set_idx is None else f"📊 **Match Stats (Set {set_idx+1})**"

    def pct_in(i: int) -> float:
        return (100.0 * first_in[i] / first_tot[i]) if first_tot[i] else 0.0

    w1 = (f"Winners: FH {winners['forehand'][p1i]}, BH {winners['backhand'][p1i]}, "
          f"OH {winners['overhead'][p1i]}, V {winners['volley'][p1i]}, "
          f"DS {winners['drop_shot'][p1i]}, Lob {winners['lob'][p1i]}, "
          f"Slc {winners['slice'][p1i]}, HV {winners['half_volley'][p1i]}, "
          f"Srv {winners.get('serve', [0,0])[p1i]}")
    w2 = (f"Winners: FH {winners['forehand'][p2i]}, BH {winners['backhand'][p2i]}, "
          f"OH {winners['overhead'][p2i]}, V {winners['volley'][p2i]}, "
          f"DS {winners['drop_shot'][p2i]}, Lob {winners['lob'][p2i]}, "
          f"Slc {winners['slice'][p2i]}, HV {winners['half_volley'][p2i]}, "
          f"Srv {winners.get('serve', [0,0])[p2i]}")
    ue1 = (f"Errors (UF/F): FH {ues['forehand'][p1i]}/{fes['forehand'][p1i]}, "
           f"BH {ues['backhand'][p1i]}/{fes['backhand'][p1i]}, "
           f"Srv {ues['serve'][p1i]}, Oth {ues.get('other',[0,0])[p1i]}")
    ue2 = (f"Errors (UF/F): FH {ues['forehand'][p2i]}/{fes['forehand'][p2i]}, "
           f"BH {ues['backhand'][p2i]}/{fes['backhand'][p2i]}, "
           f"Srv {ues['serve'][p2i]}, Oth {ues.get('other',[0,0])[p2i]}")

    return (
        f"{title}\n"
        f"**{state.p1.name}** — Aces **{aces[p1i]}**, DF **{dfs[p1i]}**, "
        f"1st In **{pct_in(p1i):.0f}%**, "
        f"1st Pts **{frac(fs_w[p1i], fs_t[p1i])}**, "
        f"2nd Pts **{frac(ss_w[p1i], ss_t[p1i])}**, "
        f"Ret Pts **{ret_w[p1i]}**, "
        f"BP Saved **{frac(bp_saved[p1i], bp_faced[p1i])}**, "
        f"BP Won **{frac(bp_conv[p1i], bp_ch[p1i])}**, "
        f"Pts **{pw[p1i]}**\n"
        f"{w1}\n{ue1}\n\n"
        f"**{state.p2.name}** — Aces **{aces[p2i]}**, DF **{dfs[p2i]}**, "
        f"1st In **{pct_in(p2i):.0f}%**, "
        f"1st Pts **{frac(fs_w[p2i], fs_t[p2i])}**, "
        f"2nd Pts **{frac(ss_w[p2i], ss_t[p2i])}**, "
        f"Ret Pts **{ret_w[p2i]}**, "
        f"BP Saved **{frac(bp_saved[p2i], bp_faced[p2i])}**, "
        f"BP Won **{frac(bp_conv[p2i], bp_ch[p2i])}**, "
        f"Pts **{pw[p2i]}**\n"
        f"{w2}\n{ue2}\n"
        f"\nPoints Played: **{played}**"
        f"\nMatch Time: **{mm}:{ss_e:02d}**"
        f"\nAvg Rally Length: **{avg_rally:.1f}** shots"
    )

def server_name(state: MatchState) -> str:
    return state.p1.name if state.server_idx == 0 else state.p2.name

def _break_point_label(state: MatchState) -> Optional[str]:
    if state.in_tiebreak:
        return None
    pa, pb = state.game_points
    server_idx = state.server_idx
    sp = pa if server_idx == 0 else pb
    rp = pb if server_idx == 0 else pa
    if (rp >= 3) and (rp - sp >= 1) and (rp >= 4 or rp == 3):
        returner_idx = 1 - server_idx
        n = state.bp_tally[returner_idx] + 1
        return f"Break Point #{n}" if n > 1 else "Break Point"
    return None

def _set_or_match_point_label(state: MatchState) -> Optional[str]:
    sets_needed = match_sets_needed(state.best_of)
    won1 = sum(1 for a, b in state.sets if a > b)
    won2 = sum(1 for a, b in state.sets if b > a)
    near_match_p1 = (won1 == sets_needed - 1)
    near_match_p2 = (won2 == sets_needed - 1)
    g1, g2 = state.current_games
    pa, pb = state.game_points

    # Championship Point label for tournament Finals
    is_final = (state.tournament_round == "F")

    def _match_label(player_idx: int) -> str:
        n = state.mp_tally[player_idx] + 1
        suffix = f" #{n}" if n > 1 else ""
        return ("Championship Point" if is_final else "Match Point") + suffix

    def _set_label(player_idx: int) -> str:
        n = state.sp_tally[player_idx] + 1
        return f"Set Point #{n}" if n > 1 else "Set Point"

    if state.in_tiebreak:
        t1, t2 = state.tiebreak_points
        target = tb_target_for_set(state.best_of, len(state.sets))
        if t1 >= target - 1 and t1 > t2:
            return _match_label(0) if near_match_p1 else _set_label(0)
        if t2 >= target - 1 and t2 > t1:
            return _match_label(1) if near_match_p2 else _set_label(1)
        return None

    bp = _break_point_label(state)
    if bp:
        if state.server_idx == 0:
            new_g1, new_g2 = g1, g2 + 1
            if new_g2 >= 6 and abs(new_g2 - new_g1) >= 2:
                return _match_label(1) if near_match_p2 else _set_label(1)
        else:
            new_g1, new_g2 = g1 + 1, g2
            if new_g1 >= 6 and abs(new_g1 - new_g2) >= 2:
                return _match_label(0) if near_match_p1 else _set_label(0)
    return None

def _compute_serving_context(state: MatchState) -> str:
    """Return a persistent game-long label for serving context, or empty string."""
    sets_needed = match_sets_needed(state.best_of)
    won1 = sum(1 for a, b in state.sets if a > b)
    won2 = sum(1 for a, b in state.sets if b > a)
    g1, g2 = state.current_games
    s = state.server_idx
    is_final = (state.tournament_round == "F")
    match_label = "Championship" if is_final else "Match"

    # p1 serving
    if s == 0:
        # Serving for the match
        if won1 == sets_needed - 1 and g1 >= 5 and g1 >= g2:
            return f"Serving for the {match_label}"
        # Serving for the set
        if g1 >= 5 and g1 > g2:
            return "Serving for the Set"
        # Serving to stay in set (facing set point)
        if g2 >= 5 and g2 > g1:
            return "Serving to Stay in the Set"
        # Serving to stay in match
        if won2 == sets_needed - 1 and g2 >= 5 and g2 > g1:
            return f"Serving to Stay in the {match_label}"
    else:
        # p2 serving
        if won2 == sets_needed - 1 and g2 >= 5 and g2 >= g1:
            return f"Serving for the {match_label}"
        if g2 >= 5 and g2 > g1:
            return "Serving for the Set"
        if g1 >= 5 and g1 > g2:
            return "Serving to Stay in the Set"
        if won1 == sets_needed - 1 and g1 >= 5 and g1 > g2:
            return f"Serving to Stay in the {match_label}"
    return ""


def build_score_text(state: MatchState) -> str:
    sets_needed = match_sets_needed(state.best_of)
    won1 = sum(1 for a, b in state.sets if a > b)
    won2 = sum(1 for a, b in state.sets if b > a)
    match_over = (won1 >= sets_needed) or (won2 >= sets_needed)

    # Tournament context header
    tourn_name  = state.tournament_name
    tourn_round = state.tournament_round
    _ROUND_DISPLAY = {
        "R128": "Round of 128", "R64": "Round of 64", "R32": "Round of 32",
        "R16":  "Round of 16",  "QF":  "Quarterfinal", "SF": "Semifinal",
        "F":    "Final",        "W":   "Winner",
    }
    if tourn_name:
        rnd_label = _ROUND_DISPLAY.get(tourn_round, tourn_round or "") if tourn_round else ""
        header = f"🏆 **{tourn_name}** — {rnd_label}\n"
        header += f"**{state.p1.name}** vs **{state.p2.name}**\n"
        header += f"Format: Best of {state.best_of} (First to {sets_needed} sets)\n\n"
    else:
        header = f"🎾 **Match Simulation**\n**{state.p1.name}** vs **{state.p2.name}**\n"
        header += f"Format: Best of {state.best_of} (First to {sets_needed} sets)\n\n"

    cond = getattr(state, "conditions", None)
    if cond:
        wu, su, au = _tournament_units_for(cond)
        temp_val  = float(cond.temp_c)
        temp_unit = "°C"
        if wu == "F":
            temp_val  = _c_to_f(temp_val)
            temp_unit = "°F"
        wind_val  = float(cond.wind_kmh)
        wind_unit = "km/h"
        if su == "MPH":
            wind_val  = _kmh_to_mph(wind_val)
            wind_unit = "mph"
        alt_val  = float(cond.altitude_m)
        alt_unit = "m"
        if au == "FT":
            alt_val  = _m_to_ft(alt_val)
            alt_unit = "ft"
        eff_cpi    = compute_effective_cpi(cond)
        eff_bounce = compute_effective_bounce(cond)
        header += (
            f"🏟️ Venue: **{cond.venue_name}** ({cond.surface}) | CPI **{eff_cpi:.0f}** (base {cond.cpi_effective}) | Bounce **{eff_bounce:.0f}/100** (base {cond.bounce_effective})\n"
            f"🌦️ Weather: **{temp_val:.0f}{temp_unit}**, Wind **{wind_val:.0f} {wind_unit}**, Humidity **{cond.humidity_pct}%**"
            + (" | ☔ Rain" if cond.is_raining else "")
            + (" | Roof closed" if cond.roof_closed else "")
            + (f" | ⏳ Delay {cond.rain_delay_min}m" if cond.rain_delay_min else "")
            + f" | Alt **{alt_val:.0f} {alt_unit}**\n\n"
        )

    set_line  = f"Sets: {won1}–{won2}\n"
    completed = format_completed_sets_winner_labeled(state)
    games_line = f"Set Scores: {completed}\n"
    if not match_over:
        games_line += f"Current Set: {state.current_games[0]}-{state.current_games[1]}\n"

    if state.in_tiebreak:
        tb = state.tiebreak_points
        game_line = f"Tiebreak: {tb[0]}–{tb[1]}\n"
        side = "Deuce" if (sum(tb) % 2 == 0) else "Ad"
    else:
        gp = state.game_points
        sp, rp = (gp[0], gp[1]) if state.server_idx == 0 else (gp[1], gp[0])
        game_line = f"Game: {game_point_label_server(sp, rp)}\n"
        side = deuce_or_ad_side(gp[0], gp[1])

    big = _set_or_match_point_label(state) or _break_point_label(state)
    big_line  = f"🔥 **{big}**\n" if big and not match_over else ""
    ctx = state.serving_context
    ctx_line  = f"⚡ *{ctx}*\n" if ctx and not match_over else ""
    serve_line = f"Server: ➡️ {server_name(state)} ({side} side)\n"

    serve_info = ""
    if state.last_serve_kmh is not None:
        su = "KMH"
        cond2 = getattr(state, "conditions", None)
        if cond2:
            _, su, _ = _tournament_units_for(cond2)
        if su == "MPH":
            serve_info = f"\nServe Speed: **{_kmh_to_mph(float(state.last_serve_kmh)):.0f} mph**"
        else:
            serve_info = f"\nServe Speed: **{state.last_serve_kmh:.0f} km/h**"

    rally_info = f"\nRally Length: **{state.last_rally_shots}** shot(s)" if state.last_rally_shots is not None else ""
    last = f"\nLast Point: {state.last_point_desc}{serve_info}{rally_info}\n"

    draw_snap = state.draw_snapshot if not state.is_tournament_match else ""
    draw_part = f"\n{draw_snap}" if draw_snap else ""
    return header + set_line + games_line + game_line + big_line + ctx_line + serve_line + last + draw_part

def bot_toss_choice(bot_row: Optional[Dict[str, Any]]) -> str:
    if not bot_row or not isinstance(bot_row, dict):
        return "serve" if random.random() < 0.5 else "receive"
    pref = str(bot_row.get("toss_preference") or "").strip().lower()
    if pref in ("serve", "receive"):
        return pref if random.random() < 0.75 else ("receive" if pref == "serve" else "serve")
    return "serve" if random.random() < 0.5 else "receive"


# =========================
# Win reward
# =========================
def calculate_match_win_reward(winner: "PlayerProfile", loser: "PlayerProfile") -> int:
    """Award 1-100 unspent stat points to the winner.
    Beating a stronger opponent gives more points. Beating a much weaker one gives fewer.
    """
    def _avg(p: "PlayerProfile") -> float:
        stats = [
            p.fh_power, p.fh_accuracy, p.fh_timing,
            p.bh_power, p.bh_accuracy, p.bh_timing,
            p.fs_speed, p.fs_accuracy,
            p.touch, p.fitness, p.tennis_iq, p.mental_stamina,
        ]
        return sum(stats) / max(1, len(stats))

    winner_avg = _avg(winner)
    loser_avg  = _avg(loser)

    # ratio > 1 means loser is stronger (upset); ratio < 1 means expected win
    ratio = loser_avg / max(1.0, winner_avg)

    # Base reward 3–15. Bonus scales with how much stronger loser was.
    base  = random.randint(3, 15)
    bonus = int(clamp((ratio - 0.5) * 55.0, 0, 85))
    total = clamp(base + bonus + random.randint(0, 8), 1, 100)
    return total


# =========================
# Profile builders
# =========================
def _to_profile_from_row(
    name: str,
    raw: Dict,
    is_bot: bool,
    user_id: Optional[int],
    gear: Optional[Dict[str, float]] = None,
) -> PlayerProfile:
    def g(key: str, default: float = 60.0) -> float:
        v = raw.get(key, default)
        return float(v) if isinstance(v, (int, float)) else float(default)

    fh_base = clamp(g("forehand", 1), 1, 99)
    bh_base = clamp(g("backhand", 1), 1, 99)
    sv_base = clamp(g("serve", 1), 1, 99)
    to_base = clamp(g("touch", 1), 1, 99)
    fi_base = clamp(g("fitness", 1), 1, 99)

    fatigue = clamp(g("fatigue", 0), 0.0, 100.0)
    stamina = clamp(100.0 - fatigue, STAMINA_MIN, STAMINA_START)
    gear    = gear or _gear_defaults()

    return PlayerProfile(
        name=name,
        user_id=user_id,

        fh_power   = g("fh_power",   fh_base),
        fh_accuracy= g("fh_accuracy",fh_base),
        fh_timing  = g("fh_timing",  fh_base),
        bh_power   = g("bh_power",   bh_base),
        bh_accuracy= g("bh_accuracy",bh_base),
        bh_timing  = g("bh_timing",  bh_base),

        return_accuracy = g("return_accuracy", (fh_base+bh_base)/2.0),
        return_speed    = g("return_speed", fi_base),

        fs_speed   = g("fs_speed",   sv_base),
        fs_accuracy= g("fs_accuracy",sv_base),
        fs_spin    = g("fs_spin",    sv_base),
        ss_speed   = g("ss_speed",   clamp(sv_base-5, 1, 100)),
        ss_accuracy= g("ss_accuracy",clamp(sv_base+5, 1, 100)),
        ss_spin    = g("ss_spin",    clamp(sv_base-5, 1, 100)),

        touch              = to_base,
        volley             = g("volley",             to_base),
        half_volley        = g("half_volley",        to_base),
        drop_shot_effectivity = g("drop_shot_effectivity", to_base),
        slice              = g("slice",              to_base),
        lob                = g("lob",                to_base),

        fitness    = fi_base,
        footwork   = g("footwork",   fi_base),
        speed      = g("speed",      fi_base),
        stamina_stat = g("stamina",  fi_base),

        focus         = g("focus",         50),
        tennis_iq     = g("tennis_iq",     50),
        mental_stamina= g("mental_stamina",50),

        racket_power  = float(gear.get("racket_power",  50.0)),
        racket_spin   = float(gear.get("racket_spin",   50.0)),
        racket_control= float(gear.get("racket_control",50.0)),
        shoe_footwork = float(gear.get("shoe_footwork", 50.0)),
        strung_pattern= str(gear.get("strung_pattern",  "")),
        strung_tension= float(gear.get("strung_tension",55.0)),
        strung_weight = str(gear.get("strung_weight",   "")),

        handedness   = str(raw.get("handedness", "right") or "right"),
        backhand_style = str(raw.get("backhand_style", "two_handed") or "two_handed"),

        stamina=stamina,
        is_bot=is_bot,
    )

def _to_profile_user(guild: discord.Guild, member: discord.Member) -> PlayerProfile:
    from modules.players import ensure_player_for_member as _epfm
    row  = _epfm(guild, member)
    row  = apply_passive_fatigue_decay(row)
    gear = _read_gear_for_user(guild.id, member.id)
    return _to_profile_from_row(member.display_name, row, is_bot=False, user_id=member.id, gear=gear)

# Full set of sub-stat keys that bots can have (same as players)
_BOT_FULL_STAT_KEYS: tuple = (
    "fh_power", "fh_accuracy", "fh_timing",
    "bh_power", "bh_accuracy", "bh_timing",
    "fs_speed", "fs_accuracy", "fs_spin",
    "ss_speed", "ss_accuracy", "ss_spin",
    "return_accuracy", "return_speed",
    "volley", "half_volley", "drop_shot_effectivity", "slice", "lob",
    "footwork", "speed", "stamina",
    "focus", "tennis_iq", "mental_stamina",
)


def _bot_ensure_full_stats(row: Dict[str, Any]) -> Dict[str, Any]:
    """Fill in any missing sub-stats from category bases. Does not overwrite existing values."""
    fh = int(row.get("forehand", 1))
    bh = int(row.get("backhand", 1))
    sv = int(row.get("serve",    1))
    to = int(row.get("touch",    1))
    fi = int(row.get("fitness",  1))

    defaults: Dict[str, Any] = {
        "fh_power": fh, "fh_accuracy": fh, "fh_timing": fh,
        "bh_power": bh, "bh_accuracy": bh, "bh_timing": bh,
        "fs_speed": sv, "fs_accuracy": sv, "fs_spin": sv,
        "ss_speed": max(1, sv - 5), "ss_accuracy": min(99, sv + 5), "ss_spin": max(1, sv - 5),
        "return_accuracy": int((fh + bh) / 2), "return_speed": fi,
        "volley": to, "half_volley": to, "drop_shot_effectivity": to, "slice": to, "lob": to,
        "footwork": fi, "speed": fi, "stamina": fi,
        "focus": 50, "tennis_iq": 50, "mental_stamina": 50,
    }
    for k, v in defaults.items():
        if k not in row:
            row[k] = v
    return row


def _to_profile_bot(bot_name: str) -> Optional[PlayerProfile]:
    row = _bot_get(bot_name)
    if not row or not _as_bool(row.get("enabled", True), True):
        return None

    raw = dict(row)
    raw["fatigue"] = 0
    raw = _bot_ensure_full_stats(raw)

    prof = _to_profile_from_row(bot_name, raw, is_bot=True, user_id=None, gear=None)
    prof.is_bot = True
    return prof


# =========================
# UI: Stats filter / Coin toss / Challenge
# =========================
# =========================
# Handedness Confirmation
# =========================
class HandednessView(discord.ui.View):
    """One-time permanent playing style selection. Cannot be changed after confirmation."""
    def __init__(self, user_id: int):
        super().__init__(timeout=120)
        self.user_id        = user_id
        self.handedness: Optional[str]     = None  # "right" | "left"
        self.backhand_style: Optional[str] = None  # "two_handed" | "one_handed"
        self.confirmed      = False

        self._hand_select = discord.ui.Select(
            placeholder="1️⃣  Choose dominant hand…",
            options=[
                discord.SelectOption(label="Right-Handed", value="right", emoji="🎾"),
                discord.SelectOption(label="Left-Handed",  value="left",  emoji="🎾"),
            ],
            row=0,
        )
        self._hand_select.callback = self._on_hand
        self.add_item(self._hand_select)

        self._bh_select = discord.ui.Select(
            placeholder="2️⃣  Choose backhand style…",
            options=[
                discord.SelectOption(label="Two-Handed Backhand (2HBH)", value="two_handed"),
                discord.SelectOption(label="One-Handed Backhand (1HBH)", value="one_handed"),
            ],
            row=1,
        )
        self._bh_select.callback = self._on_backhand
        self.add_item(self._bh_select)

        self._confirm_btn = discord.ui.Button(
            label="⚠️ Confirm — PERMANENT, cannot be changed!",
            style=discord.ButtonStyle.danger,
            row=2,
            disabled=True,
        )
        self._confirm_btn.callback = self._on_confirm
        self.add_item(self._confirm_btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Not your confirmation.", ephemeral=True)
            return False
        return True

    async def _on_hand(self, interaction: discord.Interaction):
        self.handedness = self._hand_select.values[0]
        self._confirm_btn.disabled = not (self.handedness and self.backhand_style)
        await interaction.response.edit_message(view=self)

    async def _on_backhand(self, interaction: discord.Interaction):
        self.backhand_style = self._bh_select.values[0]
        self._confirm_btn.disabled = not (self.handedness and self.backhand_style)
        await interaction.response.edit_message(view=self)

    async def _on_confirm(self, interaction: discord.Interaction):
        if not (self.handedness and self.backhand_style):
            return await interaction.response.send_message("❌ Select both options first.", ephemeral=True)
        self.confirmed = True
        for child in self.children:
            child.disabled = True
        hand_label = "Right" if self.handedness == "right" else "Left"
        bh_label   = "2HBH"  if self.backhand_style == "two_handed" else "1HBH"
        await interaction.response.edit_message(
            content=(
                f"✅ **Confirmed:** {hand_label}-handed, {bh_label}\n"
                f"*This is permanently locked to your profile and shown on `/player`.*"
            ),
            view=self,
        )
        self.stop()


async def _gate_handedness(
    interaction: discord.Interaction,
    guild: discord.Guild,
    member: discord.Member,
) -> bool:
    """
    Ensures the player has confirmed their handedness before playing.
    Sends a public channel message mentioning the member so it always
    reaches the right person regardless of who triggered the interaction.
    Returns True if already set (or just set). Returns False on timeout/skip.
    """
    from modules.players import get_player_row_by_id, set_player_row_by_id as _srbi
    row = get_player_row_by_id(member.id)
    if row and row.get("handedness") and row.get("backhand_style"):
        return True  # already confirmed

    view = HandednessView(member.id)
    content = (
        f"{member.mention} 🎾 **Before your first match, confirm your playing style.**\n"
        "⚠️ **This choice is PERMANENT and cannot ever be changed.**\n\n"
        "Your dominant hand and backhand style will affect how every rally, serve, "
        "and shot direction works in the simulation.\n"
        "Select your options below and press **Confirm** when ready."
    )

    try:
        await interaction.followup.send(content, view=view, ephemeral=False)
    except Exception:
        try:
            await interaction.response.send_message(content, view=view, ephemeral=False)
        except Exception:
            return False

    await view.wait()

    if not view.confirmed or not view.handedness or not view.backhand_style:
        fail_msg = (
            f"❌ {member.mention} did not confirm their playing style in time. Match cancelled."
            if member.id != interaction.user.id
            else "❌ You must confirm your playing style before playing a match."
        )
        try:
            await interaction.followup.send(fail_msg, ephemeral=False)
        except Exception:
            pass
        return False

    row = row or {}
    row["handedness"]     = view.handedness
    row["backhand_style"] = view.backhand_style
    try:
        _srbi(guild, member.id, row)
    except Exception:
        pass

    hand_lbl = "Right-handed" if view.handedness == "right" else "Left-handed"
    bh_lbl   = "2HBH" if view.backhand_style == "two_handed" else "1HBH"
    try:
        await interaction.followup.send(
            f"✅ {member.mention} confirmed playing style: **{hand_lbl}, {bh_lbl}** — locked in permanently. 🎾",
            ephemeral=False,
        )
    except Exception:
        pass

    return True


class StatsFilterView(discord.ui.View):
    def __init__(self, state: MatchState, show_winner_line: bool = True):
        super().__init__(timeout=12)
        self.state = state
        self.show_winner_line = show_winner_line

        options = [discord.SelectOption(label="Overall", value="overall")]
        for i, (a, b) in enumerate(getattr(state, "sets", [])):
            if a > b:
                label = f"Set {i+1}: {state.p1.name} {a}-{b}"
            else:
                label = f"Set {i+1}: {state.p2.name} {b}-{a}"
            options.append(discord.SelectOption(label=label[:100], value=str(i)))

        self.select = discord.ui.Select(placeholder="Filter stats…", options=options)
        self.select.callback = self._on_select  # type: ignore
        self.add_item(self.select)

    async def _on_select(self, interaction):
        v = self.select.values[0]
        set_idx = None if v == "overall" else int(v)
        content = build_score_text(self.state)
        if self.show_winner_line:
            won1 = sum(1 for a, b in self.state.sets if a > b)
            won2 = sum(1 for a, b in self.state.sets if b > a)
            winner_name = self.state.p1.name if won1 > won2 else self.state.p2.name
            content += f"\n✅ Winner: **{winner_name}**\n\n"
        else:
            content += "\n\n"
        content += render_match_stats_text(self.state, set_idx=set_idx)
        await interaction.response.edit_message(content=content, view=self)


class CoinTossChoiceView(discord.ui.View):
    def __init__(self, chooser_id: int, timeout: int = 20):
        super().__init__(timeout=timeout)
        self.chooser_id = chooser_id
        self.choice: Optional[str] = None

    async def interaction_check(self, interaction) -> bool:
        if interaction.user.id != self.chooser_id:
            await interaction.response.send_message("❌ Only the coin-toss winner can choose.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Serve", style=discord.ButtonStyle.primary)
    async def choose_serve(self, interaction, button: discord.ui.Button):
        self.choice = "serve"
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="✅ Choice locked: **Serve**", view=self)
        self.stop()

    @discord.ui.button(label="Receive", style=discord.ButtonStyle.secondary)
    async def choose_receive(self, interaction, button: discord.ui.Button):
        self.choice = "receive"
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="✅ Choice locked: **Receive**", view=self)
        self.stop()


class ChallengeView(discord.ui.View):
    def __init__(self, opponent_id: int, timeout: int = CHALLENGE_TIMEOUT):
        super().__init__(timeout=timeout)
        self.opponent_id = opponent_id
        self.result: Optional[bool] = None
        self.opponent_interaction: Optional[discord.Interaction] = None  # for P2 ephemeral

    async def interaction_check(self, interaction) -> bool:
        if interaction.user.id != self.opponent_id:
            await interaction.response.send_message("❌ Only the challenged user can press these buttons.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Accept", style=discord.ButtonStyle.success)
    async def accept(self, interaction, button: discord.ui.Button):
        self.result = True
        self.opponent_interaction = interaction   # ← store so P2 can receive ephemerals
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="✅ Challenge accepted. Starting match...", view=self)
        self.stop()

    @discord.ui.button(label="Decline", style=discord.ButtonStyle.danger)
    async def decline(self, interaction, button: discord.ui.Button):
        self.result = False
        for child in self.children:
            child.disabled = True
        await interaction.response.edit_message(content="❌ Challenge declined.", view=self)
        self.stop()


# =========================
# Real-time match simulation timing
# =========================

# =========================
# BotStatsView — paginated read-only stats view for bots (like /player but stats-only)
# =========================
class BotStatsView(discord.ui.View):
    """Read-only paginated stat viewer for a bot. Mirrors PlayerStatsView but no gear/XP."""

    _PAGE_NAMES = ["Overview", "Forehand", "Backhand", "Serve", "Return", "Touch", "Fitness", "Mental"]

    def __init__(self, bot_name: str, row: Dict[str, Any], timeout: int = 180):
        super().__init__(timeout=timeout)
        self.bot_name    = bot_name
        self.row         = _bot_ensure_full_stats(dict(row))
        self.current_page = 0
        self._update_buttons()

    def _update_buttons(self) -> None:
        self._prev_btn.disabled = (self.current_page == 0)
        self._next_btn.disabled = (self.current_page == len(self._PAGE_NAMES) - 1)

    def _bar(self, val: int, width: int = 10) -> str:
        filled = round(val / 99 * width)
        return "█" * filled + "░" * (width - filled)

    def _stat_row(self, label: str, val: int) -> str:
        return f"`{self._bar(val)}` **{label}**: {val}"

    def _embed(self) -> discord.Embed:
        row  = self.row
        page = self._PAGE_NAMES[self.current_page]
        enabled  = "✅ Enabled" if _as_bool(row.get("enabled", True), True) else "⛔ Disabled"
        reward   = int(row.get("reward", 250) or 0)
        hand     = row.get("handedness", "right")
        bhs      = row.get("backhand_style", "two_handed")
        hand_lbl = ("Right" if hand == "right" else "Left") + "-handed"
        bhs_lbl  = "2HBH" if bhs == "two_handed" else "1HBH"

        e = discord.Embed(
            title=f"🤖 {self.bot_name}",
            color=discord.Color.blurple(),
        )
        e.set_footer(text=f"Page {self.current_page + 1}/{len(self._PAGE_NAMES)} | {enabled} | Reward: {reward} coins | {hand_lbl}, {bhs_lbl}")

        def g(k: str, fallback: int = 1) -> int:
            return int(row.get(k, fallback))

        if page == "Overview":
            cats = [
                ("Forehand",  g("forehand")),
                ("Backhand",  g("backhand")),
                ("Serve",     g("serve")),
                ("Return",    int((g("return_accuracy") + g("return_speed")) / 2)),
                ("Touch",     g("touch")),
                ("Fitness",   g("fitness")),
                ("Mental",    int((g("focus") + g("tennis_iq") + g("mental_stamina")) / 3)),
            ]
            lines = [self._stat_row(lbl, v) for lbl, v in cats]
            lines.append(f"\n**Fatigue:** {g('fatigue', 0)}/100")
            toss = row.get("toss_preference", "none")
            lines.append(f"**Toss preference:** {toss.title()}")
            lid = row.get("main_loadout_preset_id") or row.get("loadout_preset_id")
            lines.append(f"**Loadout ID:** `{lid or 'Balanced (default)'}`")
            e.add_field(name="📊 Overview", value="\n".join(lines), inline=False)

        elif page == "Forehand":
            e.add_field(name="🎯 Forehand", value="\n".join([
                self._stat_row("Overall", g("forehand")),
                self._stat_row("Power",   g("fh_power")),
                self._stat_row("Accuracy",g("fh_accuracy")),
                self._stat_row("Timing",  g("fh_timing")),
            ]), inline=False)

        elif page == "Backhand":
            bh_type = "One-Handed BH (1HBH)" if bhs_lbl == "1HBH" else "Two-Handed BH (2HBH)"
            e.add_field(name=f"🎯 Backhand ({bh_type})", value="\n".join([
                self._stat_row("Overall", g("backhand")),
                self._stat_row("Power",   g("bh_power")),
                self._stat_row("Accuracy",g("bh_accuracy")),
                self._stat_row("Timing",  g("bh_timing")),
            ]), inline=False)

        elif page == "Serve":
            e.add_field(name="🎾 First Serve", value="\n".join([
                self._stat_row("Overall", g("serve")),
                self._stat_row("Speed",   g("fs_speed")),
                self._stat_row("Accuracy",g("fs_accuracy")),
                self._stat_row("Spin",    g("fs_spin")),
            ]), inline=True)
            e.add_field(name="🔄 Second Serve", value="\n".join([
                self._stat_row("Speed",   g("ss_speed")),
                self._stat_row("Accuracy",g("ss_accuracy")),
                self._stat_row("Spin",    g("ss_spin")),
            ]), inline=True)

        elif page == "Return":
            e.add_field(name="↩️ Return", value="\n".join([
                self._stat_row("Accuracy", g("return_accuracy")),
                self._stat_row("Speed",    g("return_speed")),
            ]), inline=False)

        elif page == "Touch":
            e.add_field(name="✋ Touch / Net", value="\n".join([
                self._stat_row("Touch Overall",g("touch")),
                self._stat_row("Volley",       g("volley")),
                self._stat_row("Half-Volley",  g("half_volley")),
                self._stat_row("Drop Shot",    g("drop_shot_effectivity")),
                self._stat_row("Slice",        g("slice")),
                self._stat_row("Lob",          g("lob")),
            ]), inline=False)

        elif page == "Fitness":
            e.add_field(name="💪 Fitness", value="\n".join([
                self._stat_row("Overall",  g("fitness")),
                self._stat_row("Footwork", g("footwork")),
                self._stat_row("Speed",    g("speed")),
                self._stat_row("Stamina",  g("stamina")),
            ]), inline=False)

        elif page == "Mental":
            e.add_field(name="🧠 Mental", value="\n".join([
                self._stat_row("Focus",          g("focus")),
                self._stat_row("Tennis IQ",      g("tennis_iq")),
                self._stat_row("Mental Stamina", g("mental_stamina")),
            ]), inline=False)

        return e

    @discord.ui.button(label="◀", style=discord.ButtonStyle.primary)
    async def _prev_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        self.current_page = max(0, self.current_page - 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.button(label="▶", style=discord.ButtonStyle.primary)
    async def _next_btn(self, interaction: discord.Interaction, _: discord.ui.Button):
        self.current_page = min(len(self._PAGE_NAMES) - 1, self.current_page + 1)
        self._update_buttons()
        await interaction.response.edit_message(embed=self._embed(), view=self)


# =========================
# BotAllocateView — interactive bot stat editor (like /player-admin)
# =========================
_BOT_ALLOCATABLE: Dict[str, list] = {
    "forehand": ["fh_power", "fh_accuracy", "fh_timing"],
    "backhand": ["bh_power", "bh_accuracy", "bh_timing"],
    "serve":    ["fs_speed", "fs_accuracy", "fs_spin", "ss_speed", "ss_accuracy", "ss_spin"],
    "return":   ["return_accuracy", "return_speed"],
    "touch":    ["volley", "half_volley", "drop_shot_effectivity", "slice", "lob"],
    "fitness":  ["footwork", "speed", "stamina"],
    "mental":   ["focus", "tennis_iq", "mental_stamina"],
}

_BOT_STAT_NAMES: Dict[str, str] = {
    "fh_power": "FH Power", "fh_accuracy": "FH Accuracy", "fh_timing": "FH Timing",
    "bh_power": "BH Power", "bh_accuracy": "BH Accuracy", "bh_timing": "BH Timing",
    "fs_speed": "1st Serve Speed", "fs_accuracy": "1st Serve Accuracy", "fs_spin": "1st Serve Spin",
    "ss_speed": "2nd Serve Speed", "ss_accuracy": "2nd Serve Accuracy", "ss_spin": "2nd Serve Spin",
    "return_accuracy": "Return Accuracy", "return_speed": "Return Speed",
    "volley": "Volley", "half_volley": "Half-Volley", "drop_shot_effectivity": "Drop Shot",
    "slice": "Slice", "lob": "Lob",
    "footwork": "Footwork", "speed": "Speed", "stamina": "Stamina",
    "focus": "Focus", "tennis_iq": "Tennis IQ", "mental_stamina": "Mental Stamina",
}


class _BotSetStatModal(discord.ui.Modal, title="Set Bot Stat Value"):
    value = discord.ui.TextInput(label="Value (1–99)", required=True, max_length=2)

    def __init__(self, on_set):
        super().__init__()
        self._on_set = on_set

    async def on_submit(self, interaction: discord.Interaction):
        try:
            v = max(1, min(99, int(str(self.value.value).strip())))
        except Exception:
            v = 1
        await self._on_set(interaction, v)


class BotAllocateView(discord.ui.View):
    """Interactive sub-stat editor for a bot (admin only). Same UX as PlayerAllocateView."""

    def __init__(self, invoker_id: int, bot_name: str, row: Dict[str, Any]):
        super().__init__(timeout=300)
        self.invoker_id  = invoker_id
        self.bot_name    = bot_name
        self.row         = dict(row)
        # Ensure all sub-stats exist
        self.row = _bot_ensure_full_stats(self.row)

        self.selected_category = list(_BOT_ALLOCATABLE.keys())[0]
        self.selected_stat     = _BOT_ALLOCATABLE[self.selected_category][0]

        self._cat_select:  Optional[discord.ui.Select] = None
        self._stat_select: Optional[discord.ui.Select] = None
        self._build_selects()

    def _build_selects(self):
        self._cat_select = discord.ui.Select(
            placeholder="Category…", min_values=1, max_values=1,
            options=[
                discord.SelectOption(label=cat.title(), value=cat, default=(cat == self.selected_category))
                for cat in _BOT_ALLOCATABLE.keys()
            ],
            row=0,
        )
        self._cat_select.callback = self._on_cat
        self.add_item(self._cat_select)
        self._rebuild_stat_select()

    def _rebuild_stat_select(self):
        if self._stat_select is not None:
            self.remove_item(self._stat_select)
        stats = _BOT_ALLOCATABLE[self.selected_category]
        self._stat_select = discord.ui.Select(
            placeholder="Stat…", min_values=1, max_values=1,
            options=[
                discord.SelectOption(
                    label=_BOT_STAT_NAMES.get(k, k),
                    value=k,
                    description=f"Current: {self.row.get(k, 1)}",
                    default=(k == self.selected_stat),
                )
                for k in stats
            ],
            row=1,
        )
        self._stat_select.callback = self._on_stat
        self.add_item(self._stat_select)

    def _embed(self) -> discord.Embed:
        cur_val = int(self.row.get(self.selected_stat, 1))
        e = discord.Embed(
            title=f"🔧 Bot Editor — {self.bot_name}",
            color=discord.Color.gold(),
        )
        # Category overview
        cat_lines = []
        for cat in _BOT_ALLOCATABLE.keys():
            parts = _BOT_ALLOCATABLE[cat]
            avg   = round(sum(int(self.row.get(k, 1)) for k in parts) / len(parts))
            marker = "▶" if cat == self.selected_category else "◦"
            cat_lines.append(f"{marker} **{cat.title()}** — {avg}")
        e.add_field(name="Categories", value="\n".join(cat_lines), inline=True)

        stat_lines = []
        for k in _BOT_ALLOCATABLE[self.selected_category]:
            marker = "▶ " if k == self.selected_stat else "    "
            stat_lines.append(f"{marker}**{_BOT_STAT_NAMES.get(k,k)}** — {self.row.get(k, 1)}")
        e.add_field(name=f"{self.selected_category.title()} Sub-stats", value="\n".join(stat_lines), inline=True)

        e.add_field(
            name="Editing",
            value=(
                f"**{_BOT_STAT_NAMES.get(self.selected_stat, self.selected_stat)}**: `{cur_val}` / 99\n"
                "*(Admin — no point cost)*"
            ),
            inline=False,
        )
        # Top-level stats summary
        e.set_footer(text=(
            f"FH {self.row.get('forehand',1)}  BH {self.row.get('backhand',1)}  "
            f"Srv {self.row.get('serve',1)}  Touch {self.row.get('touch',1)}  "
            f"Fit {self.row.get('fitness',1)}"
        ))
        return e

    async def _check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.invoker_id:
            return True
        await interaction.response.send_message("❌ Admin panel.", ephemeral=True)
        return False

    async def _on_cat(self, interaction: discord.Interaction):
        if not await self._check(interaction): return
        self.selected_category = self._cat_select.values[0]
        self.selected_stat     = _BOT_ALLOCATABLE[self.selected_category][0]
        for opt in self._cat_select.options:
            opt.default = (opt.value == self.selected_category)
        self._rebuild_stat_select()
        await interaction.response.edit_message(embed=self._embed(), view=self)

    async def _on_stat(self, interaction: discord.Interaction):
        if not await self._check(interaction): return
        self.selected_stat = self._stat_select.values[0]
        for opt in self._stat_select.options:
            opt.default = (opt.value == self.selected_stat)
        await interaction.response.edit_message(embed=self._embed(), view=self)

    async def _apply(self, interaction: discord.Interaction, delta: int):
        if not await self._check(interaction): return
        # Re-read fresh from disk
        fresh = _bot_get(self.bot_name)
        if fresh:
            self.row = _bot_ensure_full_stats(dict(fresh))
        old_val = int(self.row.get(self.selected_stat, 1))
        new_val = max(1, min(99, old_val + delta))
        if new_val == old_val:
            msg = "❌ Already at max (99)." if delta > 0 else "❌ Already at min (1)."
            return await interaction.response.send_message(msg, ephemeral=True)
        self.row[self.selected_stat] = new_val
        # Recompute category averages
        for cat, parts in _BOT_ALLOCATABLE.items():
            vals = [int(self.row.get(k, 1)) for k in parts]
            self.row[cat] = round(sum(vals) / len(parts))
        _bot_set(self.bot_name, self.row)
        for opt in self._stat_select.options:
            if opt.value == self.selected_stat:
                opt.description = f"Current: {new_val}"
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.button(label="+1",     style=discord.ButtonStyle.success,   row=2)
    async def add1(self, i, _): await self._apply(i, +1)
    @discord.ui.button(label="+5",     style=discord.ButtonStyle.success,   row=2)
    async def add5(self, i, _): await self._apply(i, +5)
    @discord.ui.button(label="+10",    style=discord.ButtonStyle.primary,   row=2)
    async def add10(self, i, _): await self._apply(i, +10)

    @discord.ui.button(label="Set…",   style=discord.ButtonStyle.secondary, row=2)
    async def set_exact(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not await self._check(interaction): return
        async def _on_set(ix: discord.Interaction, v: int):
            cur = int(self.row.get(self.selected_stat, 1))
            await self._apply(ix, v - cur)
        await interaction.response.send_modal(_BotSetStatModal(_on_set))

    @discord.ui.button(label="Done",   style=discord.ButtonStyle.danger,    row=2)
    async def done(self, interaction: discord.Interaction, _: discord.ui.Button):
        self.stop()
        await interaction.response.edit_message(
            content=f"✅ Done editing **{self.bot_name}**.", embed=None, view=None)

    @discord.ui.button(label="−1",     style=discord.ButtonStyle.secondary, row=3)
    async def sub1(self, i, _): await self._apply(i, -1)
    @discord.ui.button(label="−5",     style=discord.ButtonStyle.secondary, row=3)
    async def sub5(self, i, _): await self._apply(i, -5)
    @discord.ui.button(label="Reset→1",style=discord.ButtonStyle.danger,    row=3)
    async def reset_stat(self, interaction: discord.Interaction, _: discord.ui.Button):
        cur = int(self.row.get(self.selected_stat, 1))
        await self._apply(interaction, 1 - cur)


# =========================
# Cog
# =========================
class MatchSimCog(commands.Cog):

    @staticmethod
    def _total_games_played(state: "MatchState") -> int:
        done = 0
        for a, b in getattr(state, "sets", []):
            done += int(a) + int(b)
        g1, g2 = getattr(state, "current_games", (0, 0))
        done += int(g1) + int(g2)
        return done

    def _consume_racket_on_new_balls(self, guild_id: int, state: MatchState) -> None:
        from modules.gear import _db as gear_db, _save_db as gear_save, _inv_row
        try:
            db = gear_db()
            def _consume_for_player(user_id: int) -> None:
                inv = _inv_row(guild_id, user_id)
                eq_racket = inv.get("equipped_racket")
                if not isinstance(eq_racket, dict):
                    return
                frame_id    = str(eq_racket.get("frame_id", ""))
                if not frame_id:
                    return
                eq_pattern  = str(eq_racket.get("pattern", ""))
                eq_tension  = str(eq_racket.get("tension", ""))
                eq_weight   = str(eq_racket.get("weight", ""))
                strung = list(inv.get("strung_rackets", []))
                for i, r in enumerate(strung):
                    if (isinstance(r, dict)
                            and str(r.get("frame_id", "")) == frame_id
                            and str(r.get("pattern", "")) == eq_pattern
                            and str(r.get("tension", "")) == eq_tension
                            and str(r.get("weight", "")) == eq_weight):
                        strung.pop(i)
                        break
                inv["strung_rackets"] = strung
                db.setdefault("inv", {}).setdefault(str(guild_id), {})[str(user_id)] = inv

            if state.p1.user_id is not None:
                _consume_for_player(state.p1.user_id)
            if not state.p2.is_bot and state.p2.user_id is not None:
                _consume_for_player(state.p2.user_id)
            gear_save(db)
        except Exception as e:
            print(f"[gear] Error consuming racket on ball change: {e}")

    @staticmethod
    def _reserve_strung_rackets_or_reason(guild_id: int, user_id: int, best_of: int) -> tuple[bool, int, str]:
        if not gear_has_shoes_equipped(guild_id, user_id):
            return (False, 0,
                "❌ You don't have shoes equipped.\n"
                "Use `/shoe-shop` to browse, `/shoe-buy` to purchase, then `/gear-equip` to equip them.")
        frame_id, count = gear_get_equipped_strung_count_for_frame(guild_id, user_id)
        if frame_id is None:
            return (False, 0,
                "❌ You don't have a racket equipped.\n"
                "Use `/string-racket` to string a frame, then `/gear-equip` to equip it.")
        needed = strung_rackets_needed_for_match(best_of)
        if count < needed:
            return (False, count,
                f"❌ You need **{needed}** strung rackets of your equipped frame type for a Best of {best_of}.\n"
                f"You only have **{count}** strung of that type.\n"
                f"Use `/string-racket` to string more frames, or use `/gear-equip` to equip a different racket or frame.")
        return (True, count, "")

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._active_users: set[int] = set()
        if not hasattr(bot, "active_match_users"):
            bot.active_match_users = set()

    def _is_admin(self, member: discord.Member) -> bool:
        return bool(getattr(member.guild_permissions, "administrator", False))

    # ---------- bot admin ----------
    @app_commands.command(name="bot-create", description="(Admin) Create a match-sim bot with custom stats.")
    @app_commands.guild_only()
    async def ms_bot_create_cmd(self, interaction: discord.Interaction, name: str,
        forehand: int=1, backhand: int=1, serve: int=1, touch: int=1, fitness: int=1,
        enabled: bool=True, reward: int=250, main_loadout_preset_id: Optional[str]=None):
        if not isinstance(interaction.user, discord.Member) or not self._is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        if _bot_get(name):
            return await interaction.response.send_message("❌ A bot with that name already exists.", ephemeral=True)
        _bot_set(name, {
            "enabled": bool(enabled), "created_by": interaction.user.id, "created_at": _utc_now_iso(),
            "forehand": int(forehand), "backhand": int(backhand), "serve": int(serve),
            "touch": int(touch), "fitness": int(fitness), "reward": int(max(0, reward)),
            "toss_preference": "none", "main_loadout_preset_id": main_loadout_preset_id,
        })
        await interaction.response.send_message(f"🤖✅ Bot **{name}** created.", ephemeral=False)

    @app_commands.command(name="bot-edit", description="(Admin) Interactively edit a bot's sub-stats and settings.")
    @app_commands.guild_only()
    @app_commands.autocomplete(name=_bot_autocomplete)
    @app_commands.choices(toss_preference=[
        app_commands.Choice(name="Serve",   value="serve"),
        app_commands.Choice(name="Receive", value="receive"),
        app_commands.Choice(name="None",    value="none"),
    ])
    async def ms_bot_edit_cmd(
        self,
        interaction: discord.Interaction,
        name: str,
        enabled: Optional[bool] = None,
        toss_preference: Optional[str] = None,
        reward: Optional[int] = None,
        main_loadout_preset_id: Optional[str] = None,
    ):
        if not isinstance(interaction.user, discord.Member) or not self._is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        row = _bot_get(name)
        if not row:
            return await interaction.response.send_message("❌ Bot not found.", ephemeral=True)

        changes: list[str] = []
        if enabled is not None:
            row["enabled"] = bool(enabled)
            changes.append(f"Enabled → **{enabled}**")
        if toss_preference is not None:
            row["toss_preference"] = toss_preference
            changes.append(f"Toss pref → **{toss_preference}**")
        if reward is not None:
            row["reward"] = int(max(0, reward))
            changes.append(f"Reward → **{reward}**")
        if main_loadout_preset_id is not None:
            s = str(main_loadout_preset_id).strip()
            row["main_loadout_preset_id"] = s if s else None
            changes.append(f"Loadout preset → **{s or 'None'}**")
        if changes:
            _bot_set(name, row)

        # Open interactive sub-stat editor
        view = BotAllocateView(invoker_id=interaction.user.id, bot_name=name, row=row)
        header = ("✅ Applied: " + ", ".join(changes) + "\n\n") if changes else ""
        await interaction.response.send_message(
            content=f"{header}Editing **{name}** sub-stats:",
            embed=view._embed(),
            view=view,
            ephemeral=False,
        )

    @app_commands.command(name="bot-enable-all", description="(Admin) Enable ALL match-sim bots.")
    @app_commands.guild_only()
    async def ms_bot_enable_all_cmd(self, interaction: discord.Interaction):
        if not isinstance(interaction.user, discord.Member) or not self._is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        db = _bots_db(); bots = db.get("bots", {})
        if not bots:
            return await interaction.response.send_message("No bots to update.", ephemeral=True)
        changed = 0
        for n, row in bots.items():
            if isinstance(row, dict) and not _as_bool(row.get("enabled", True), True):
                row["enabled"] = True; changed += 1
        _bots_save(db)
        await interaction.response.send_message(f"✅ Enabled {changed} bot(s).", ephemeral=False)

    @app_commands.command(name="bot-delete", description="(Admin) Delete a match-sim bot.")
    @app_commands.guild_only()
    @app_commands.autocomplete(name=_bot_autocomplete)
    async def ms_bot_delete_cmd(self, interaction: discord.Interaction, name: str):
        if not isinstance(interaction.user, discord.Member) or not self._is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        ok = _bot_delete(name)
        if not ok:
            return await interaction.response.send_message("❌ Bot not found.", ephemeral=True)
        await interaction.response.send_message(f"🗑️ Bot **{name}** deleted.", ephemeral=False)

    @app_commands.command(name="bot-list", description="List all match-sim bots.")
    @app_commands.guild_only()
    async def ms_bot_list_cmd(self, interaction: discord.Interaction):
        db = _bots_db(); bots = db.get("bots", {})
        if not bots:
            return await interaction.response.send_message("No bots created yet.", ephemeral=True)
        enabled = []; disabled = []
        for n, row in bots.items():
            (enabled if _as_bool(row.get("enabled", True), True) else disabled).append(n)
        enabled.sort(key=lambda s: s.lower()); disabled.sort(key=lambda s: s.lower())
        lines = []
        if enabled:   lines.append("**Enabled Bots**");   lines.extend([f"• {n}" for n in enabled])
        if disabled:  lines.append("\n**Disabled Bots**"); lines.extend([f"• {n}" for n in disabled])
        await interaction.response.send_message("\n".join(lines)[:1900], ephemeral=False)

    @app_commands.command(name="bot-view", description="View a match-sim bot's full stats (paginated).")
    @app_commands.guild_only()
    @app_commands.autocomplete(name=_bot_autocomplete)
    async def ms_bot_view_cmd(self, interaction: discord.Interaction, name: str):
        row = _bot_get(name)
        if not row:
            return await interaction.response.send_message("❌ Bot not found.", ephemeral=True)
        view = BotStatsView(bot_name=name, row=row)
        await interaction.response.send_message(embed=view._embed(), view=view, ephemeral=False)

    # ---------- recovery ----------
    @app_commands.command(name="recovery", description="Recover fatigue (reduces fatigue).")
    @app_commands.guild_only()
    async def recovery_cmd(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        row = ensure_player_for_member(interaction.guild, interaction.user)
        row = apply_passive_fatigue_decay(row)
        remain = _cooldown_remaining_seconds(row.get("recovery_last_at"), RECOVERY_COOLDOWN_HOURS)
        if remain > 0:
            h = remain // 3600; m = (remain % 3600) // 60
            return await interaction.response.send_message(
                f"⏳ Recovery is on cooldown. Try again in **{h}h {m}m**.", ephemeral=True)
        fitness = int(float(row.get("fitness", 50) or 50))
        bonus = max(0, min(10, fitness // 10)); remove = min(20, 10 + bonus)
        old = float(row.get("fatigue", 0) or 0); new = max(0.0, old - remove)
        row["fatigue"] = float(new)
        row["fatigue_updated_at"] = _now_utc().isoformat()
        row["recovery_last_at"] = _now_utc().isoformat()
        set_player_row_by_id(interaction.guild, interaction.user.id, row)
        await interaction.response.send_message(
            f"🧊 Recovered **{int(old-new)}** fatigue. New fatigue: **{int(new)}**.", ephemeral=False)


    # ---------- matches ----------
    @app_commands.command(name="match-sim", description="Challenge a user to a tennis match simulation (they must accept).")
    @app_commands.guild_only()
    async def match_sim(self, interaction: discord.Interaction, opponent: discord.Member, best_of: int = 3, wager: int = 0):
        ok, reason = academy_can_challenge(interaction.guild.id, interaction.user.id, opponent.id)
        if not ok:
            return await interaction.response.send_message(reason, ephemeral=True)
        if best_of not in (1, 3, 5):
            return await interaction.response.send_message("❌ best_of must be 1, 3, or 5.", ephemeral=True)
        if opponent.bot:
            return await interaction.response.send_message("❌ Use `/match-sim-bot` to play bots.", ephemeral=True)
        if interaction.user.id in self._active_users or opponent.id in self._active_users:
            return await interaction.response.send_message("❌ One of you already has a match running.", ephemeral=True)
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        ok1, _, r1 = self._reserve_strung_rackets_or_reason(interaction.guild.id, interaction.user.id, best_of)
        if not ok1:
            return await interaction.response.send_message(r1, ephemeral=True)
        ok2, _, r2 = self._reserve_strung_rackets_or_reason(interaction.guild.id, opponent.id, best_of)
        if not ok2:
            return await interaction.response.send_message(
                f"❌ **{opponent.display_name}** can't play this match:\n{r2}", ephemeral=True)

        p1 = _to_profile_user(interaction.guild, interaction.user)
        p2 = _to_profile_user(interaction.guild, opponent)

        view = ChallengeView(opponent_id=opponent.id, timeout=CHALLENGE_TIMEOUT)
        wager_line = f"\n💰 **Wager:** {wager} coins each" if wager > 0 else ""
        await interaction.response.send_message(
            f"🎾 **Match Challenge**\n{opponent.mention} — {interaction.user.mention} challenged you to **Best of {best_of}**.{wager_line}\n"
            f"Press a button below (expires in {CHALLENGE_TIMEOUT}s).",
            view=view, ephemeral=False,
        )
        timed_out = await view.wait()
        if timed_out or view.result is None:
            try:
                for child in view.children: child.disabled = True
                await interaction.edit_original_response(content="⌛ Challenge expired.", view=view)
            except Exception:
                pass
            return
        if view.result is False:
            return

        # ----- Wager handling -----
        wager = int(max(0, wager))
        if wager > 0:
            bal1 = int(get_balance(interaction.user.id))
            bal2 = int(get_balance(opponent.id))
            if bal1 < wager or bal2 < wager:
                return await interaction.followup.send(
                    f"❌ Wager failed. Both players must have **{wager}** coins.\n"
                    f"You: **{bal1}**, Opponent: **{bal2}**", ephemeral=True)
            ok1 = remove_balance(interaction.user.id, wager)
            ok2 = remove_balance(opponent.id, wager)
            if not (ok1 and ok2):
                if ok1: add_balance(interaction.user.id, wager)
                if ok2: add_balance(opponent.id, wager)
                return await interaction.followup.send("❌ Wager failed (insufficient funds).", ephemeral=True)

        # ----- Handedness gate (both players must have confirmed) -----
        if not await _gate_handedness(interaction, interaction.guild, interaction.user):
            return
        if not await _gate_handedness(interaction, interaction.guild, opponent):
            return await interaction.followup.send(
                f"❌ {opponent.mention} hasn't confirmed their playing style yet. "
                f"They need to play `/match-sim` first to set it.", ephemeral=True
            )

        state = MatchState(p1=p1, p2=p2, best_of=best_of)
        state.wager = wager  # type: ignore[attr-defined]

        # ----- Venue pick -----
        try:
            vview = VenueSelectView(
                chooser_id=interaction.user.id,
                guild_id=interaction.guild.id,
                user_id=interaction.user.id,
                timeout=25,
            )
            await interaction.followup.send("🏟️ Choose a venue for this match:", view=vview, ephemeral=False)
            await vview.wait()
            chosen_venue_id = vview.venue_id
        except Exception:
            chosen_venue_id = None

        state.conditions = _roll_conditions_for_venue(interaction.guild.id, chosen_venue_id)

        # ----- Loadout pick — both players get simultaneous ephemeral prompts -----
        # P1: use challenger's interaction followup (always works)
        # P2: use opponent's interaction from the ChallengeView accept button
        opp_ix = view.opponent_interaction   # discord.Interaction from opponent's Accept click

        lview1 = LoadoutSelectView(
            chooser_id=interaction.user.id,
            guild_id=interaction.guild.id,
            user_id=interaction.user.id,
            timeout=30,
        )
        lview2 = LoadoutSelectView(
            chooser_id=opponent.id,
            guild_id=interaction.guild.id,
            user_id=opponent.id,
            timeout=30,
        )

        async def _send_p1_loadout():
            try:
                await interaction.followup.send(
                    "🎚 **Loadout Selection** — choose your match loadout (only you can see this):",
                    view=lview1, ephemeral=True,
                )
                await lview1.wait()
            except Exception:
                pass

        async def _send_p2_loadout():
            """DM the opponent so neither player sees the other's pick."""
            try:
                dm = await opponent.create_dm()
                dm_msg = await dm.send(
                    f"🎾 **{interaction.user.display_name}** challenged you to a match!\n"
                    f"🎚 **Choose your loadout** before the match starts (30s):",
                    view=lview2,
                )
                await lview2.wait()
                try:
                    await dm_msg.edit(view=None)
                except Exception:
                    pass
            except discord.Forbidden:
                # Opponent has DMs disabled — fall back to ephemeral via their stored interaction
                try:
                    if opp_ix is not None:
                        await opp_ix.followup.send(
                            "🎚 **Choose your match loadout** (only you can see this):",
                            view=lview2, ephemeral=True,
                        )
                    else:
                        await interaction.followup.send(
                            f"🎚 {opponent.mention} — pick your loadout! *(DMs disabled — only you can use this menu)*",
                            view=lview2, ephemeral=False,
                        )
                    await lview2.wait()
                except Exception:
                    pass
            except Exception:
                pass

        await asyncio.gather(_send_p1_loadout(), _send_p2_loadout())

        p1_choice = lview1.choice
        p2_choice = lview2.choice

        p1_sl = resolve_loadout_sliders(p1_choice, interaction.guild.id, interaction.user.id)
        p2_sl = resolve_loadout_sliders(p2_choice, interaction.guild.id, opponent.id)

        apply_loadout_to_profile(state.p1, p1_sl)
        apply_loadout_to_profile(state.p2, p2_sl)

        # Store sliders on profiles for rally engine access
        state.p1._match_sliders = p1_sl  # type: ignore[attr-defined]
        state.p2._match_sliders = p2_sl  # type: ignore[attr-defined]

        # Build pre-match multipliers (sharpness + venue experience)
        venue_capacity = getattr(state.conditions, "venue_capacity", 5000)
        build_pre_match_multipliers(state.p1, interaction.guild.id, chosen_venue_id, venue_capacity)
        build_pre_match_multipliers(state.p2, interaction.guild.id, chosen_venue_id, venue_capacity)

        # Coin toss
        toss_winner_idx = 0 if random.random() < 0.5 else 1
        toss_winner = interaction.user if toss_winner_idx == 0 else opponent

        view2 = CoinTossChoiceView(chooser_id=toss_winner.id, timeout=20)
        await interaction.followup.send(
            f"🪙 **Coin Toss** — Winner: {toss_winner.mention}\nChoose **Serve** or **Receive** (20s).",
            view=view2, ephemeral=False,
        )
        await view2.wait()

        choice = view2.choice or "serve"
        state.server_idx = toss_winner_idx if choice == "serve" else 1 - toss_winner_idx

        self._active_users.add(interaction.user.id)
        self._active_users.add(opponent.id)
        self.bot.active_match_users.add(interaction.user.id)
        self.bot.active_match_users.add(opponent.id)
        msg = await interaction.followup.send(build_score_text(state))
        await self._run_match_loop(msg, state, guild=interaction.guild)

    @app_commands.command(name="match-sim-bot", description="Play a tennis match simulation vs an admin-created bot.")
    @app_commands.guild_only()
    @app_commands.autocomplete(bot_name=_bot_autocomplete)
    async def match_sim_bot(self, interaction: discord.Interaction, bot_name: str, best_of: int = 3):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=False)
        except Exception:
            pass

        if best_of not in (1, 3, 5):
            return await interaction.followup.send("❌ best_of must be 1, 3, or 5.", ephemeral=True)
        if interaction.user.id in self._active_users:
            return await interaction.followup.send("❌ You already have a match running.", ephemeral=True)
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.followup.send("❌ Guild only.", ephemeral=True)

        ok, have, reason = self._reserve_strung_rackets_or_reason(interaction.guild.id, interaction.user.id, best_of)
        if not ok:
            return await interaction.followup.send(reason, ephemeral=True)

        p1 = _to_profile_user(interaction.guild, interaction.user)
        p2 = _to_profile_bot(bot_name)
        if p2 is None:
            return await interaction.followup.send("❌ Bot not found or disabled.", ephemeral=True)

        # ----- Handedness gate -----
        if not await _gate_handedness(interaction, interaction.guild, interaction.user):
            self._active_users.discard(interaction.user.id)
            self.bot.active_match_users.discard(interaction.user.id)
            return

        state = MatchState(p1=p1, p2=p2, best_of=best_of)
        state.bot_name = bot_name  # type: ignore[attr-defined]

        # ----- Venue pick -----
        chosen_venue_id = None
        try:
            owned = _inv_only_venues(interaction.guild.id, interaction.user.id)
            if owned:
                vview = VenueSelectView(
                    chooser_id=interaction.user.id,
                    guild_id=interaction.guild.id,
                    user_id=interaction.user.id,
                    timeout=25,
                )
                await interaction.followup.send("🏟️ Choose a venue for this match:", view=vview, ephemeral=False)
                await vview.wait()
                chosen_venue_id = vview.venue_id
            else:
                await interaction.followup.send("🏟️ No venues owned — using **default conditions**.", ephemeral=False)
        except Exception:
            chosen_venue_id = None

        state.conditions = _roll_conditions_for_venue(interaction.guild.id, chosen_venue_id)

        # ----- Loadout pick (player only — ephemeral) -----
        try:
            lview = LoadoutSelectView(
                chooser_id=interaction.user.id,
                guild_id=interaction.guild.id,
                user_id=interaction.user.id,
                timeout=30,
            )
            await interaction.followup.send("🎚 Choose your loadout:", view=lview, ephemeral=True)
            await lview.wait()
            p1_choice = lview.choice
        except Exception:
            p1_choice = None

        p1_sl = resolve_loadout_sliders(p1_choice, interaction.guild.id, interaction.user.id)
        apply_loadout_to_profile(state.p1, p1_sl)
        state.p1._match_sliders = p1_sl  # type: ignore[attr-defined]

        bot_row = _bot_get(bot_name) or {}
        bot_pid = bot_row.get("main_loadout_preset_id")
        presets = _loadout_presets_db().get("presets", {}) or {}
        p2_sl = resolve_preset_sliders(bot_pid, presets)
        apply_loadout_to_profile(state.p2, p2_sl)
        state.p2._match_sliders = p2_sl  # type: ignore[attr-defined]

        # Build pre-match multipliers
        venue_capacity = getattr(state.conditions, "venue_capacity", 5000)
        build_pre_match_multipliers(state.p1, interaction.guild.id, chosen_venue_id, venue_capacity)
        # bots don't get experience bonuses; p2.sharpness_mult stays 1.0

        # Coin toss
        toss_winner_idx = 0 if random.random() < 0.5 else 1
        if toss_winner_idx == 0:
            view2 = CoinTossChoiceView(chooser_id=interaction.user.id, timeout=20)
            await interaction.followup.send(
                f"🪙 **Coin Toss** — Winner: {interaction.user.mention}\nChoose **Serve** or **Receive** (20s).",
                view=view2, ephemeral=False,
            )
            await view2.wait()
            choice = view2.choice or "serve"
        else:
            choice = bot_toss_choice(bot_row)

        state.server_idx = toss_winner_idx if choice == "serve" else (1 - toss_winner_idx)

        self._active_users.add(interaction.user.id)
        self.bot.active_match_users.add(interaction.user.id)

        try:
            msg = await interaction.followup.send(build_score_text(state), wait=True)
            await self._run_match_loop(msg, state, guild=interaction.guild)
        finally:
            self._active_users.discard(interaction.user.id)
            self.bot.active_match_users.discard(interaction.user.id)


    @app_commands.command(
        name="bots-sim",
        description="(Admin) Simulate a match between two bots. Fully automatic.",
    )
    @app_commands.guild_only()
    @app_commands.autocomplete(bot1=_bot_autocomplete, bot2=_bot_autocomplete, venue_id=_venue_autocomplete_admin)
    async def bots_sim(
        self,
        interaction: discord.Interaction,
        bot1: str,
        bot2: str,
        best_of: int = 3,
        venue_id: Optional[str] = None,
    ):
        """Admin-only: pit two bots against each other with no human interaction."""
        if not isinstance(interaction.user, discord.Member) or not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=False)
        except Exception:
            pass

        if best_of not in (1, 3, 5):
            return await interaction.followup.send("❌ best_of must be 1, 3, or 5.", ephemeral=True)

        if bot1 == bot2:
            return await interaction.followup.send("❌ Bot1 and Bot2 must be different.", ephemeral=True)

        p1 = _to_profile_bot(bot1)
        p2 = _to_profile_bot(bot2)
        if p1 is None:
            return await interaction.followup.send(f"❌ Bot **{bot1}** not found or disabled.", ephemeral=True)
        if p2 is None:
            return await interaction.followup.send(f"❌ Bot **{bot2}** not found or disabled.", ephemeral=True)

        state = MatchState(p1=p1, p2=p2, best_of=best_of)
        state.bots_sim = True  # type: ignore[attr-defined]

        # ---------- venue ----------
        chosen_venue_id = venue_id if venue_id else None
        if chosen_venue_id:
            from modules.venues import _get_venue
            if not _get_venue(chosen_venue_id):
                await interaction.followup.send(
                    f"⚠️ Venue `{chosen_venue_id}` not found — using default conditions.", ephemeral=False
                )
                chosen_venue_id = None

        state.conditions = _roll_conditions_for_venue(interaction.guild.id, chosen_venue_id)

        # ---------- loadouts (both from their preset) ----------
        presets = _loadout_presets_db().get("presets", {}) or {}

        bot1_row = _bot_get(bot1) or {}
        bot1_pid = bot1_row.get("main_loadout_preset_id")
        p1_sl = resolve_preset_sliders(bot1_pid, presets)
        apply_loadout_to_profile(state.p1, p1_sl)
        state.p1._match_sliders = p1_sl  # type: ignore[attr-defined]

        bot2_row = _bot_get(bot2) or {}
        bot2_pid = bot2_row.get("main_loadout_preset_id")
        p2_sl = resolve_preset_sliders(bot2_pid, presets)
        apply_loadout_to_profile(state.p2, p2_sl)
        state.p2._match_sliders = p2_sl  # type: ignore[attr-defined]

        # Bots don't get sharpness/venue bonuses
        state.p1.sharpness_mult = 1.0
        state.p1.venue_exp_mult = 1.0
        state.p2.sharpness_mult = 1.0
        state.p2.venue_exp_mult = 1.0

        # ---------- auto coin toss ----------
        state.server_idx = 0 if random.random() < 0.5 else 1
        toss_name = state.p1.name if state.server_idx == 0 else state.p2.name

        # ---------- announce ----------
        venue_name = "Default"
        if chosen_venue_id:
            try:
                from modules.venues import _get_venue
                vrow = _get_venue(chosen_venue_id)
                venue_name = str(vrow.get("name", chosen_venue_id)) if vrow else chosen_venue_id
            except Exception:
                pass

        cond_note = ""
        if state.conditions:
            cond = state.conditions
            cond_note = (
                f"🌤️ {getattr(cond,'surface','?').title()} | "
                f"CPI {getattr(cond,'cpi_effective',35):.0f} | "
                f"{getattr(cond,'temp_c',20):.0f}°C"
            )

        await interaction.followup.send(
            f"🤖 **Bot Sim** — **{p1.name}** vs **{p2.name}**\n"
            f"🏟️ {venue_name}  {cond_note}\n"
            f"🪙 Coin toss: **{toss_name}** serves first\n"
            f"Bo{best_of} | Admin: {interaction.user.mention}",
            ephemeral=False,
        )

        msg = await interaction.followup.send(build_score_text(state), wait=True)
        await self._run_match_loop(msg, state, guild=interaction.guild)


    async def _toilet_break(self, msg: discord.Message, state: MatchState) -> None:
        """Possibly pause for a comfort break after a set. ~15% chance for tournament matches,
        ~8% for regular matches. One player takes the break."""
        is_tourn = getattr(state, "is_tournament_match", False)
        chance = 0.15 if is_tourn else 0.08
        if random.random() > chance:
            return
        breaker_idx = random.randint(0, 1)
        breaker = state.p1 if breaker_idx == 0 else state.p2
        name = (breaker.name or f"Player {breaker_idx + 1}").split(" (")[0]  # strip seed suffix if any
        flavour = random.choice([
            f"🚽 **{name}** has requested a comfort break.",
            f"🚽 **{name}** is taking a bathroom break.",
            f"🚽 **{name}** heads off court for a comfort break.",
            f"🚽 **{name}** has left the court for a toilet break.",
        ])
        try:
            await msg.edit(content=build_score_text(state) + "\n\n" + flavour)
        except Exception:
            pass

        # Scale break duration by point_delay_range so instant/fast sims aren't killed
        dr = getattr(state, "point_delay_range", None)
        if dr is not None:
            lo, hi = dr
            if lo == 0 and hi == 0:
                delay = 0.0
            else:
                # toilet break ≈ 5–10× a normal point delay
                delay = random.uniform(lo * 5, hi * 10)
        else:
            delay = random.uniform(60.0, 120.0)
        if delay > 0:
            await asyncio.sleep(delay)
        try:
            await msg.edit(content=build_score_text(state))
        except Exception:
            pass

    async def _run_match_loop(self, msg: discord.Message, state: MatchState, guild: Optional[discord.Guild]):
        sets_needed = match_sets_needed(state.best_of)
        current_set_index = 0
        initial_tb_server_idx: Optional[int] = None
        tb_point_number = 0

        if not hasattr(state, "_new_balls_consumed"):
            state._new_balls_consumed = set()  # type: ignore[attr-defined]
        if not hasattr(state, "stats") or getattr(state, "stats", None) is None:
            state.stats = MatchStats()  # type: ignore

        max_games = _max_games_for_best_of(state.best_of)
        balls_schedule = set(_balls_change_times_upto(max_games))

        # Compute serving context for the first game
        state.serving_context = _compute_serving_context(state)

        while True:
            won1 = sum(1 for a, b in state.sets if a > b)
            won2 = sum(1 for a, b in state.sets if b > a)
            if won1 >= sets_needed or won2 >= sets_needed:
                break

            g1, g2 = state.current_games

            if (not state.in_tiebreak) and g1 == 6 and g2 == 6:
                state.in_tiebreak = True
                state.tiebreak_points = (0, 0)
                prev_game_server = getattr(state, "last_game_server_idx", None)
                if prev_game_server in (0, 1):
                    initial_tb_server_idx = 1 - prev_game_server
                else:
                    initial_tb_server_idx = state.server_idx
                tb_point_number = 0

            if state.in_tiebreak and initial_tb_server_idx is not None:
                state.server_idx = tiebreak_server_index(tb_point_number, initial_tb_server_idx)

            # Crucial point detection (BEFORE point is played)
            _cur_big   = _set_or_match_point_label(state)
            _cur_bp    = _break_point_label(state)
            was_break_point = (_cur_bp is not None)
            was_set_point   = (_cur_big is not None and "Set Point" in _cur_big)
            was_match_point = (_cur_big is not None and
                               ("Match Point" in _cur_big or "Championship Point" in _cur_big))
            server_idx_pre = state.server_idx
            st: MatchStats = state.stats  # type: ignore

            st.ensure_set(current_set_index)
            sb = st.sets[current_set_index]

            if was_break_point:
                st.break_pts_faced[server_idx_pre]          += 1
                st.break_pts_chances[1 - server_idx_pre]    += 1
                sb["break_pts_faced"][server_idx_pre]        += 1
                sb["break_pts_chances"][1 - server_idx_pre] += 1

            winner_idx, desc, shots, meta = simulate_point(state)

            # Update crucial-point tallies BEFORE the point changes game state
            if was_break_point:
                returner_idx = 1 - server_idx_pre
                state.bp_tally[returner_idx] += 1
            if was_set_point:
                # Determine which player has set point
                _is_tb = state.in_tiebreak
                if _is_tb:
                    t1, t2 = state.tiebreak_points
                    tgt = tb_target_for_set(state.best_of, len(state.sets))
                    if t1 >= tgt - 1 and t1 > t2: state.sp_tally[0] += 1
                    elif t2 >= tgt - 1 and t2 > t1: state.sp_tally[1] += 1
                else:
                    bp_l = _break_point_label(state)
                    if bp_l and server_idx_pre == 0: state.sp_tally[1] += 1
                    elif bp_l: state.sp_tally[0] += 1
            if was_match_point:
                _is_tb = state.in_tiebreak
                if _is_tb:
                    t1, t2 = state.tiebreak_points
                    tgt = tb_target_for_set(state.best_of, len(state.sets))
                    if t1 >= tgt - 1 and t1 > t2: state.mp_tally[0] += 1
                    elif t2 >= tgt - 1 and t2 > t1: state.mp_tally[1] += 1
                else:
                    bp_l = _break_point_label(state)
                    if bp_l and server_idx_pre == 0: state.mp_tally[1] += 1
                    elif bp_l: state.mp_tally[0] += 1

            ev = str(meta.get("event", "rally"))
            if ev in ("ace", "double_fault"):
                state.last_rally_shots = None
            else:
                state.last_rally_shots = int(shots) if shots is not None else None

            # Update momentum after each point
            _update_momentum(winner_idx)

            # Rally stats: only genuine rallies (not aces, DFs, or unreturned first shots)
            if ev not in ("ace", "double_fault", "service_winner"):
                st.rally_points      += 1
                st.rally_shots_total += int(shots or 1)
                sb["rally_points"]      = sb.get("rally_points", 0) + 1
                sb["rally_shots_total"] = sb.get("rally_shots_total", 0) + int(shots or 1)

            state.last_point_desc = desc
            state.last_serve_kmh  = meta.get("serve_kmh")

            if was_break_point:
                if winner_idx == server_idx_pre:
                    st.break_pts_saved[server_idx_pre]   += 1
                    sb["break_pts_saved"][server_idx_pre] += 1
                else:
                    st.break_pts_converted[1 - server_idx_pre]   += 1
                    sb["break_pts_converted"][1 - server_idx_pre] += 1

            server_idx   = int(meta.get("server_idx", state.server_idx))
            second_serve = bool(meta.get("second_serve", False))

            st.total_points_played   += 1
            sb["points_played"]      += 1
            st.total_points_won[winner_idx]  += 1
            sb["points_won"][winner_idx]     += 1

            st.first_serves_total[server_idx]    += 1
            sb["first_serves_total"][server_idx] += 1

            if ev == "double_fault":
                st.double_faults[server_idx]               += 1
                sb["double_faults"][server_idx]            += 1
                st.unforced_errors["serve"][server_idx]    += 1
                sb["unforced_errors"]["serve"][server_idx] += 1
                st.second_serve_pts_total[server_idx]      += 1
                sb["second_serve_pts_total"][server_idx]   += 1
            else:
                if not second_serve:
                    st.first_serves_in[server_idx]    += 1
                    sb["first_serves_in"][server_idx] += 1
                if second_serve:
                    st.second_serve_pts_total[server_idx]    += 1
                    sb["second_serve_pts_total"][server_idx] += 1
                    if winner_idx == server_idx:
                        st.second_serve_pts_won[server_idx]    += 1
                        sb["second_serve_pts_won"][server_idx] += 1
                else:
                    st.first_serve_pts_total[server_idx]    += 1
                    sb["first_serve_pts_total"][server_idx] += 1
                    if winner_idx == server_idx:
                        st.first_serve_pts_won[server_idx]    += 1
                        sb["first_serve_pts_won"][server_idx] += 1
                if ev == "ace":
                    st.aces[server_idx]    += 1
                    sb["aces"][server_idx] += 1

            if winner_idx != server_idx:
                st.return_pts_won[winner_idx]    += 1
                sb["return_pts_won"][winner_idx] += 1

            if ev in ("winner", "service_winner"):
                # service_winner = unreturned serve in play (not an ace) → counts as serve winner
                wk = "serve" if ev == "service_winner" else _SHOT_TO_WINNER_KEY.get(
                    str(meta.get("winner_type") or ""), "forehand"
                )
                if wk in st.winners:
                    st.winners[wk][winner_idx] += 1
                if wk in sb["winners"]:
                    sb["winners"][wk][winner_idx] += 1

            if ev == "unforced_error":
                # Attribute the UE to the LOSER (1 - winner_idx)
                us = str(meta.get("unforced_side") or "other")
                us = us if us in st.unforced_errors else "other"
                st.unforced_errors[us][1 - winner_idx]    += 1
                _us2 = us if us in sb["unforced_errors"] else "other"
                sb["unforced_errors"][_us2][1 - winner_idx] += 1

            if ev == "forced_error":
                # Attribute forced error to the LOSER
                fs = str(meta.get("forced_side") or "other")
                fs = fs if fs in st.forced_errors else "other"
                st.forced_errors[fs][1 - winner_idx]    += 1
                _fs2 = fs if fs in sb["forced_errors"] else "other"
                sb["forced_errors"][_fs2][1 - winner_idx] += 1

            ended_on_serve = (ev in ("ace", "double_fault"))
            cost = stamina_cost(int(shots or 1), ended_on_serve=ended_on_serve)
            state.p1.stamina = clamp(state.p1.stamina - cost, STAMINA_MIN, STAMINA_START)
            state.p2.stamina = clamp(state.p2.stamina - cost, STAMINA_MIN, STAMINA_START)

            # ---------- scoring ----------
            if state.in_tiebreak:
                t1, t2 = state.tiebreak_points
                if winner_idx == 0: t1 += 1
                else:               t2 += 1
                state.tiebreak_points = (t1, t2)

                target   = tb_target_for_set(state.best_of, current_set_index)
                tb_winner = tiebreak_won(t1, t2, target)
                tb_point_number += 1

                if tb_winner is not None:
                    # Commit the completed set score, then reset for next set
                    finished_games = (7, 6) if tb_winner == 0 else (6, 7)
                    state.sets.append(finished_games)
                    state.current_games = (0, 0)

                    try:
                        total_games = self._total_games_played(state)
                        consumed = getattr(state, "_new_balls_consumed", set())
                        if total_games in balls_schedule and total_games not in consumed:
                            consumed.add(total_games)
                            state._new_balls_consumed = consumed  # type: ignore[attr-defined]
                            self._consume_racket_on_new_balls(guild.id, state)
                    except Exception:
                        pass

                    loser_tb = min(t1, t2)
                    state.set_tb_loser_points.append(int(loser_tb))
                    won1 = sum(1 for a, b in state.sets if a > b)
                    won2 = sum(1 for a, b in state.sets if b > a)
                    if won1 >= sets_needed or won2 >= sets_needed:
                        break

                    try:
                        set_txt = render_match_stats_text(state, set_idx=current_set_index)
                        view = StatsFilterView(state)
                        await msg.edit(content=set_txt, view=view)
                        await asyncio.sleep(10)
                        await msg.edit(content=build_score_text(state), view=None)
                    except Exception:
                        pass

                    await self._toilet_break(msg, state)

                    state.game_points      = (0, 0)
                    state.in_tiebreak      = False
                    state.tiebreak_points  = (0, 0)
                    initial_tb_server_idx  = None
                    tb_point_number        = 0
                    # Reset set-point tally and break-point tally each new set
                    state.sp_tally = [0, 0]
                    state.bp_tally = [0, 0]
                    current_set_index     += 1
                    state.server_idx       = 1 - state.server_idx
                    state.serving_context  = _compute_serving_context(state)

            else:
                pa, pb = state.game_points
                if winner_idx == 0: pa += 1
                else:               pb += 1
                state.game_points = (pa, pb)

                gw = game_won(pa, pb)
                if gw is not None:
                    g1, g2 = state.current_games
                    if gw == 0: g1 += 1
                    else:       g2 += 1
                    state.current_games = (g1, g2)
                    state.game_points   = (0, 0)

                    try:
                        total_games = self._total_games_played(state)
                        consumed = getattr(state, "_new_balls_consumed", set())
                        if total_games in balls_schedule and total_games not in consumed:
                            consumed.add(total_games)
                            state._new_balls_consumed = consumed  # type: ignore[attr-defined]
                            self._consume_racket_on_new_balls(guild.id, state)
                    except Exception:
                        pass

                    state.last_game_server_idx = state.server_idx  # type: ignore[attr-defined]
                    state.server_idx = 1 - state.server_idx
                    # Reset break-point tally every game
                    state.bp_tally = [0, 0]
                    # Recompute serving context for the new game
                    state.serving_context = _compute_serving_context(state)

                    if (g1 >= 6 or g2 >= 6) and abs(g1 - g2) >= 2:
                        # Commit the completed set, then reset for next set
                        finished_games = (g1, g2)
                        state.sets.append(finished_games)
                        state.current_games = (0, 0)
                        state.set_tb_loser_points.append(None)
                        won1 = sum(1 for a, b in state.sets if a > b)
                        won2 = sum(1 for a, b in state.sets if b > a)
                        if won1 >= sets_needed or won2 >= sets_needed:
                            break

                        try:
                            set_txt = render_match_stats_text(state, set_idx=current_set_index)
                            view = StatsFilterView(state)
                            await msg.edit(content=set_txt, view=view)
                            await asyncio.sleep(10)
                            await msg.edit(content=build_score_text(state), view=None)
                        except Exception:
                            pass

                        await self._toilet_break(msg, state)

                        # Reset set-point tally each new set
                        state.sp_tally = [0, 0]
                        state.bp_tally = [0, 0]
                        state.serving_context = _compute_serving_context(state)
                        current_set_index  += 1
                        # server_idx already flipped after game_won above; no second flip needed

            try:
                await msg.edit(content=build_score_text(state))
            except Exception:
                break

            base_delay_sec = point_delay_seconds(int(shots or 1), pressure=state.in_tiebreak)
            if state.point_delay_range is not None:
                lo, hi = state.point_delay_range
                if lo == 0 and hi == 0:
                    pass  # instant — no sleep
                else:
                    await asyncio.sleep(random.uniform(lo, hi))
            else:
                await asyncio.sleep(max(0.0, base_delay_sec * MATCH_SPEED_MULT))
            try:
                server_prof = state.p1 if state.server_idx == 0 else state.p2
                rest_slider = int(getattr(server_prof, "lo_time_btwn_points", 50))
                rest_slider = max(0, min(100, rest_slider))
                rest_sec    = 6.0 + (20.0 * (rest_slider / 100.0))
                recover     = rest_sec * 0.03
                state.p1.stamina = clamp(state.p1.stamina + recover, STAMINA_MIN, STAMINA_START)
                state.p2.stamina = clamp(state.p2.stamina + recover, STAMINA_MIN, STAMINA_START)
                if not state.is_tournament_match:
                    await asyncio.sleep(rest_sec * 0.00001)
            except Exception:
                pass

        # ---------- persist fatigue ----------
        try:
            if state.p1.user_id is not None:
                set_fatigue_for_user_id(guild, state.p1.user_id, int(clamp(100.0 - state.p1.stamina, 0, 100)))
            if (not state.p2.is_bot) and state.p2.user_id is not None:
                set_fatigue_for_user_id(guild, state.p2.user_id, int(clamp(100.0 - state.p2.stamina, 0, 100)))
        except Exception:
            pass

        # Stat decay is purely time-based — not reset by matches.
        # Training sessions update timestamps via apply_stat_decay calls in training.py.

        # ---------- record venue experience ----------
        try:
            cond = getattr(state, "conditions", None)
            vid  = getattr(cond, "venue_id", None) if cond else None
            if vid:
                if state.p1.user_id is not None:
                    record_venue_experience(guild.id, state.p1.user_id, vid)
                if (not state.p2.is_bot) and state.p2.user_id is not None:
                    record_venue_experience(guild.id, state.p2.user_id, vid)
        except Exception:
            pass

        # ---------- final winner ----------
        won1 = sum(1 for a, b in state.sets if a > b)
        won2 = sum(1 for a, b in state.sets if b > a)
        winner_name = state.p1.name if won1 > won2 else state.p2.name
        winner_idx  = 0 if won1 > won2 else 1

        payout_line = ""
        try:
            if not state.is_tournament_match:
                is_bot_match = bool(getattr(state.p2, "is_bot", False))
                wager_amt    = int(getattr(state, "wager", 0))
                if is_bot_match:
                    bot_name   = getattr(state, "bot_name", None)
                    bot_row    = _bot_get(bot_name) if bot_name else None
                    bot_reward = int((bot_row or {}).get("reward", 250))
                    payout_line = f"🏅 **Bot Win Reward:** +**{bot_reward if winner_idx == 0 else 0}** coins"
                else:
                    if wager_amt > 0:
                        payout_line = f"💰 **Payout:** Winner +**{wager_amt*2}** coins (wager **{wager_amt}** each)"
                    else:
                        payout_line = f"💰 **Payout:** Winner +**250** coins"
        except Exception:
            payout_line = ""

        try:
            view = StatsFilterView(state)
            venue_line = ""
            try:
                c = getattr(state, "conditions", None)
                if c:
                    venue_line = f"🏟️ **{getattr(c, 'venue_name', None) or 'Venue'}**\n"
            except Exception:
                pass
            # Tournament context in result
            tourn_name  = state.tournament_name
            tourn_round = state.tournament_round
            _RD = {"R128":"Round of 128","R64":"Round of 64","R32":"Round of 32",
                   "R16":"Round of 16","QF":"Quarterfinal","SF":"Semifinal","F":"Final"}
            tourn_line = f"🏆 **{tourn_name}** — {_RD.get(tourn_round, tourn_round or '')}\n" if tourn_name else ""
            result  = format_completed_sets_winner_labeled(state)
            content = (
                f"{tourn_line}{venue_line}"
                f"🏁 **Result:** {result}\n"
                f"✅ Winner: **{winner_name}**\n\n"
                + (f"{payout_line}\n\n" if payout_line else "\n")
                + f"{render_match_stats_text(state, set_idx=None)}"
            )
            await msg.edit(content=content, view=view)
        except Exception:
            pass

        # ----- Payout (non-tournament only) -----
        try:
            if not state.is_tournament_match:
                is_bot_match = bool(getattr(state.p2, "is_bot", False))
                wager_amt    = int(getattr(state, "wager", 0))
                if is_bot_match:
                    if winner_idx == 0 and state.p1.user_id is not None:
                        bot_name   = getattr(state, "bot_name", None)
                        bot_row    = _bot_get(bot_name) if bot_name else None
                        bot_reward = max(0, int((bot_row or {}).get("reward", 250)))
                        add_balance(state.p1.user_id, bot_reward)
                else:
                    winner_user_id = state.p1.user_id if winner_idx == 0 else state.p2.user_id
                    if winner_user_id is not None:
                        add_balance(winner_user_id, wager_amt * 2 if wager_amt > 0 else 250)
        except Exception:
            pass

        # ----- Win reward: unspent stat points (non-tournament only) -----
        try:
            is_bot_sim = bool(getattr(state, "bots_sim", False))
            if not is_bot_sim and not state.is_tournament_match:
                winner_prof = state.p1 if winner_idx == 0 else state.p2
                loser_prof  = state.p2 if winner_idx == 0 else state.p1
                reward_pts  = calculate_match_win_reward(winner_prof, loser_prof)
                winner_uid  = winner_prof.user_id
                if winner_uid is not None:
                    from modules.players import get_player_row_by_id, set_player_row_by_id as _srbi2
                    prow = get_player_row_by_id(winner_uid)
                    if prow is not None:
                        prow["unspent_points"] = int(prow.get("unspent_points", 0)) + reward_pts
                        _srbi2(guild, winner_uid, prow)
                    # Post reward to channel
                    try:
                        await msg.channel.send(
                            f"🏆 **{winner_prof.name}** wins and earns **+{reward_pts} unspent stat points**! "
                            f"Use `/player-allocate` to spend them.",
                            delete_after=30,
                        )
                    except Exception:
                        pass
        except Exception:
            pass

        # Clean up active users
        try:
            if state.p1.user_id is not None:
                self._active_users.discard(state.p1.user_id)
                self.bot.active_match_users.discard(state.p1.user_id)
            if not state.p2.is_bot and state.p2.user_id is not None:
                self._active_users.discard(state.p2.user_id)
                self.bot.active_match_users.discard(state.p2.user_id)
        except Exception:
            pass


async def setup(bot: commands.Bot):
    await bot.add_cog(MatchSimCog(bot))