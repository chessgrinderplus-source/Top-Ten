# modules/training.py
from __future__ import annotations

import asyncio
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from modules.players import (
    BASE_STAT,
    CATEGORY_COMPONENTS,
    SUBSTAT_KEYS,
    _pretty_stat_name,
    compute_categories,
    ensure_player_for_member,
    get_player_row_by_id,
    set_player_row_by_id,
)
from modules.matchsim import apply_passive_fatigue_decay

# ═══════════════════════════════════════════════════════════
#  TUNABLE CONSTANTS
# ═══════════════════════════════════════════════════════════

STAT_MAX = 99  # hard cap everywhere

# Cooldown ───────────────────────────────────────────────────
TRAINING_COOLDOWN_MINUTES = 0  # ← minutes between sessions on the same stat

# Fatigue ────────────────────────────────────────────────────
FATIGUE_BASE_COST       = 20
FATIGUE_FITNESS_DIVISOR = 10   # every 10 pts fitness → −1 cost
FATIGUE_MIN_COST        = 5    # floor

# XP ─────────────────────────────────────────────────────────
XP_PER_SESSION_MIN = 10
XP_PER_SESSION_MAX = 100
XP_BASE  = 100
XP_CURVE = 1.3   # ← edit to change levelling speed

# Stat decay ──────────────────────────────────────────────────
#   Anchor: stat 50 → 24 hrs to lose 1 point (1 session/day = break-even)
#   stat 30 → ~6 days/pt  |  stat 60 → ~27 hrs/pt  |  stat 80 → ~8 hrs/pt
DECAY_BASE_HOURS = 48     # ← hours to lose 1 pt at DECAY_ANCHOR stat
DECAY_ANCHOR     = 50     # ← stat value where DECAY_BASE_HOURS applies exactly
DECAY_CURVE      = 1.06   # ← steepness (higher = faster decay at high stats)
DECAY_FREE_BELOW = 15     # ← stats at or below this never decay

# Reflex drill ───────────────────────────────────────────────
REFLEX_PERFECT_MS = 300
REFLEX_ZERO_MS    = 3_000
REFLEX_WAIT_MIN   = 2.0
REFLEX_WAIT_MAX   = 5.0
REFLEX_TIMEOUT    = 8.0

REFLEX_STATS = {
    "return_accuracy", "return_speed",
    "footwork", "speed", "stamina",
    "volley", "half_volley",
}

# Memory drill ───────────────────────────────────────────────
MEMORY_BASE_LENGTH  = 10
MEMORY_MAX_LENGTH   = 30
MEMORY_SHOW_SECONDS = 2.0
MEMORY_TIMEOUT      = 30.0
MEMORY_SYMBOLS      = list("ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()?><.,;:[]{}|-_=+")

MEMORY_STATS = {
    "focus", "tennis_iq", "mental_stamina",
}

# Rhythm drill ───────────────────────────────────────────────
RHYTHM_PERFECT_MS      = 200
RHYTHM_ZERO_MS         = 2_000
RHYTHM_LEAD_TIME       = 3.0
RHYTHM_RESPOND_TIMEOUT = 4.0

RHYTHM_STATS = {
    "fh_power", "fh_timing",
    "bh_power", "bh_timing",
    "fs_speed", "fs_spin",
    "ss_speed", "ss_spin",
    "lob",
}

# Focus drill ────────────────────────────────────────────────
#   Each round: delete previous message, send fresh one with button at bottom.
#   Only target hits score. Speed-weighted: instant = 1.0, end of window = 0.0.
#   Final score = sum(hit_speed_scores) / FOCUS_ROUNDS × 100
FOCUS_ROUNDS       = 25
FOCUS_INTERVAL_SEC = 0.8
FOCUS_TARGET_WORD  = "TENNIS"
FOCUS_WORD_POOL: List[str] = [
    "TEN", "TENSE", "TENTS", "TENNEL",
    "TEENS", "TINE", "TINES", "TINSEL",
    "TENSES", "TANNIN", "TONNE", "TUNES",
    "TEASE", "TEENSY", "TENSELY", "TENTHS",
    "TINNY", "TANNY", "TITAN", "TITANS",
    "TINSELLED", "TENSILE", "TENSOR", "TENURE",
    "TENTACLE",
]
FOCUS_TARGET_RATIO = 0.3

FOCUS_STATS = {
    "fh_accuracy",
    "bh_accuracy",
    "fs_accuracy",
    "ss_accuracy",
    "drop_shot_effectivity",
    "slice",
}

RHYTHM_OR_FOCUS_STATS = {
    "fh_power", "bh_power",
}


# ═══════════════════════════════════════════════════════════
#  Stat → Drill routing
# ═══════════════════════════════════════════════════════════

def _drill_for_stat(stat: str) -> str:
    if stat in REFLEX_STATS:   return "reflex"
    if stat in MEMORY_STATS:   return "memory"
    if stat in RHYTHM_STATS:   return "rhythm"
    if stat in FOCUS_STATS:    return "focus"
    if stat in RHYTHM_OR_FOCUS_STATS:
        return random.choice(["rhythm", "focus"])
    return "reflex"


_DRILL_EMOJI = {"reflex": "⚡", "memory": "🧠", "rhythm": "🎵", "focus": "🎯"}
_DRILL_NAME  = {"reflex": "Reflex Drill", "memory": "Memory Drill",
                "rhythm": "Rhythm Drill", "focus": "Focus Drill"}


# ═══════════════════════════════════════════════════════════
#  Fatigue
# ═══════════════════════════════════════════════════════════

def calculate_fatigue_cost(row: Dict[str, Any]) -> int:
    fitness       = int(row.get("fitness", BASE_STAT))
    trainer_boost = int(row.get("trainer_boost", 0))
    reduction     = fitness // FATIGUE_FITNESS_DIVISOR
    return max(FATIGUE_MIN_COST, FATIGUE_BASE_COST - reduction - trainer_boost)


# ═══════════════════════════════════════════════════════════
#  XP helpers
# ═══════════════════════════════════════════════════════════

def xp_needed_for_level(stat: int) -> float:
    return XP_BASE * (XP_CURVE ** stat)

def _xp_key(stat: str) -> str:
    return f"xp_{stat}"

def performance_to_xp(pct: float) -> float:
    t = max(0.0, min(1.0, pct / 100.0))
    return XP_PER_SESSION_MIN + (XP_PER_SESSION_MAX - XP_PER_SESSION_MIN) * t

def apply_xp_to_row(
    row: Dict[str, Any], stat: str, xp_gained: float,
) -> Tuple[Dict[str, Any], int, int, bool]:
    xp_key     = _xp_key(stat)
    old_stat   = int(row.get(stat, BASE_STAT))
    current    = old_stat
    current_xp = float(row.get(xp_key, 0.0))
    new_xp     = current_xp + xp_gained
    needed     = xp_needed_for_level(current)
    leveled_up = False
    while new_xp >= needed and current < STAT_MAX:
        new_xp    -= needed
        current    = min(STAT_MAX, current + 1)
        row[_float_stat_key(stat)] = float(current)
        needed     = xp_needed_for_level(current)
        leveled_up = True
    row[stat]   = current
    row[xp_key] = round(new_xp, 4)
    return row, old_stat, current, leveled_up

def _stat_xp_line(row: Dict[str, Any], stat: str) -> str:
    """'Stat: 45  ·  XP: 230 / 338  (68%)'"""
    val    = int(row.get(stat, BASE_STAT))
    xp_now = float(row.get(_xp_key(stat), 0.0))
    needed = xp_needed_for_level(val)
    pct    = int(xp_now / needed * 100) if needed else 0
    return f"**{_pretty_stat_name(stat)}**: {val}  ·  XP: {xp_now:.0f} / {needed:.0f}  ({pct}%)"


def _stat_category(stat: str) -> Optional[str]:
    for cat, parts in CATEGORY_COMPONENTS.items():
        if stat in parts:
            return cat
    return None

def _compute_cat_with_old(row: Dict[str, Any], changed_stat: str, old_val: int, cat: str) -> int:
    parts = CATEGORY_COMPONENTS.get(cat, ())
    if not parts:
        return old_val
    vals = [old_val if k == changed_stat else int(row.get(k, BASE_STAT)) for k in parts]
    return round(sum(vals) / len(vals))


# ═══════════════════════════════════════════════════════════
#  Stat decay
# ═══════════════════════════════════════════════════════════

def _float_stat_key(stat: str) -> str:
    return f"stat_float_{stat}"

def _last_maintained_key(stat: str) -> str:
    return f"last_maintained_{stat}"

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def hours_to_lose_one_point(stat: int) -> float:
    if stat <= DECAY_FREE_BELOW:
        return float("inf")
    return DECAY_BASE_HOURS * (DECAY_CURVE ** (DECAY_ANCHOR - stat))


def apply_stat_decay(row: Dict[str, Any], stat: str) -> Dict[str, Any]:
    last_key  = _last_maintained_key(stat)
    float_key = _float_stat_key(stat)
    now       = _utc_now()
    last_str  = row.get(last_key)

    if float_key not in row:
        row[float_key] = float(int(row.get(stat, BASE_STAT)))

    if last_str:
        try:
            last_dt = datetime.fromisoformat(str(last_str))
            hours   = max(0.0, (now - last_dt).total_seconds() / 3600.0)
        except Exception:
            hours = 0.0

        if hours > 0:
            current_val = int(row.get(stat, BASE_STAT))
            hrs_per_pt  = hours_to_lose_one_point(current_val)

            if hrs_per_pt != float("inf") and hours >= hrs_per_pt:
                float_val = float(row.get(float_key, current_val))
                remaining = hours
                while remaining > 0 and current_val > 1:
                    h = hours_to_lose_one_point(current_val)
                    if h == float("inf") or remaining < h:
                        row[float_key] = max(float(current_val - 1),
                                             float_val - remaining / max(h, 0.001))
                        break
                    remaining  -= h
                    current_val = max(1, current_val - 1)
                    float_val   = float(current_val)
                    row[float_key] = float_val
                else:
                    row[float_key] = max(1.0, float(current_val))
                row[stat] = max(1, int(row[float_key]))

    row[last_key] = now.isoformat()
    return row


def apply_all_stat_decay(row: Dict[str, Any]) -> Dict[str, Any]:
    """Apply decay to every sub-stat. Call when loading player view so
    the displayed values are always current without requiring a training session."""
    for s in SUBSTAT_KEYS:
        row = apply_stat_decay(row, s)
    return row


def record_match_activity(guild: Any, user_id: int) -> None:
    """Reset decay timer on ALL stats after a match. Call from matchsim."""
    try:
        row = get_player_row_by_id(user_id)
        if row is None:
            return
        now_iso = _utc_now().isoformat()
        for s in SUBSTAT_KEYS:
            row[_last_maintained_key(s)] = now_iso
        set_player_row_by_id(guild, user_id, row)
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════
#  Cooldown helpers
# ═══════════════════════════════════════════════════════════

def _cooldown_remaining_seconds(row: Dict[str, Any], stat: str) -> float:
    """Returns seconds left on cooldown (0 if ready)."""
    last_str = row.get(_last_maintained_key(stat))
    if not last_str:
        return 0.0
    try:
        last_dt = datetime.fromisoformat(str(last_str))
        elapsed = (_utc_now() - last_dt).total_seconds()
        remaining = TRAINING_COOLDOWN_MINUTES * 60 - elapsed
        return max(0.0, remaining)
    except Exception:
        return 0.0


# ═══════════════════════════════════════════════════════════
#  Active session guard
# ═══════════════════════════════════════════════════════════

_ACTIVE_TRAINING: set[int] = set()


# ═══════════════════════════════════════════════════════════
#  Shared embed builders
# ═══════════════════════════════════════════════════════════

EMBED_COLOR = discord.Color.from_rgb(88, 101, 242)  # blurple-ish

def _drill_header_embed(
    stat: str,
    drill: str,
    row: Dict[str, Any],
    member: discord.Member,
    extra_desc: str = "",
) -> discord.Embed:
    """Opening embed shown at the start of every drill."""
    val    = int(row.get(stat, BASE_STAT))
    xp_now = float(row.get(_xp_key(stat), 0.0))
    needed = xp_needed_for_level(val)
    pct    = int(xp_now / needed * 100) if needed else 0
    h      = hours_to_lose_one_point(val)
    decay  = f"{h:.0f}h/pt" if h != float("inf") else "stable"

    embed = discord.Embed(
        title=f"{_DRILL_EMOJI[drill]}  {_DRILL_NAME[drill]}",
        description=extra_desc or discord.utils.MISSING,
        color=EMBED_COLOR,
    )
    embed.set_author(name=member.display_name, icon_url=member.display_avatar.url)
    embed.add_field(
        name="Training",
        value=f"**{_pretty_stat_name(stat)}**",
        inline=True,
    )
    embed.add_field(
        name="Current",
        value=f"**{val}** / {STAT_MAX}",
        inline=True,
    )
    embed.add_field(
        name="XP to next",
        value=f"{xp_now:.0f} / {needed:.0f}  ({pct}%)",
        inline=True,
    )
    embed.set_footer(text=f"Decay: {decay}  ·  Fatigue cost: {calculate_fatigue_cost(row)}")
    return embed


def _result_embed(
    member: discord.Member,
    stat: str,
    drill: str,
    row_before_xp: Dict[str, Any],
    row_after_xp: Dict[str, Any],
    score: float,
    xp_gained: float,
    old_stat_val: int,
    new_stat_val: int,
    leveled_up: bool,
) -> discord.Embed:
    new_xp   = float(row_after_xp.get(_xp_key(stat), 0.0))
    needed   = xp_needed_for_level(new_stat_val)
    pct      = int(new_xp / needed * 100) if needed else 0
    fatigue  = int(float(row_after_xp.get("fatigue", 0)))

    color = discord.Color.green() if leveled_up else discord.Color.from_rgb(88, 101, 242)
    title = (
        f"🎉  Level Up!  {_pretty_stat_name(stat)}: {old_stat_val} → {new_stat_val}"
        if leveled_up
        else f"✅  Training Complete — {_pretty_stat_name(stat)}"
    )

    embed = discord.Embed(title=title, color=color)
    embed.set_author(name=member.display_name, icon_url=member.display_avatar.url)

    embed.add_field(
        name="Performance",
        value=f"**{score:.1f}%**",
        inline=False,
    )
    embed.add_field(
        name="XP Gained",
        value=f"+**{xp_gained:.0f}** XP",
        inline=True,
    )
    embed.add_field(
        name="Progress",
        value=f"{new_xp:.0f} / {needed:.0f}  ({pct}%)",
        inline=True,
    )
    embed.add_field(
        name="Stat",
        value=f"**{new_stat_val}** / {STAT_MAX}",
        inline=True,
    )
    embed.add_field(
        name="Fatigue",
        value=f"**{fatigue}** / 100",
        inline=True,
    )

    if leveled_up:
        cat     = _stat_category(stat)
        if cat:
            old_cat = _compute_cat_with_old(row_after_xp, stat, old_stat_val, cat)
            new_cat = int(row_after_xp.get(cat, BASE_STAT))
            if new_cat != old_cat:
                embed.add_field(
                    name=f"↑ {cat.title()} (overall)",
                    value=f"{old_cat} → **{new_cat}**",
                    inline=True,
                )

    return embed


# ═══════════════════════════════════════════════════════════
#  DRILL 1 — Reflex
# ═══════════════════════════════════════════════════════════

class _ReflexHitView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=REFLEX_TIMEOUT)
        self.user_id    = user_id
        self.pressed_at: Optional[float] = None
        self.timed_out  = False

    @discord.ui.button(label="⚡  HIT!", style=discord.ButtonStyle.success, custom_id="reflex_hit")
    async def hit_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ Not your drill.", ephemeral=True)
        self.pressed_at = time.monotonic()
        button.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()

    async def on_timeout(self):
        self.timed_out = True


async def run_reflex_drill(
    interaction: discord.Interaction,
    stat: str,
    row: Dict[str, Any],
    channel: discord.abc.Messageable,
) -> float:
    member = interaction.user
    embed  = _drill_header_embed(stat, "reflex", row, member,
                                  "Press **⚡ HIT!** the instant the button turns green!")
    embed.add_field(name="Status", value="*Get ready…*", inline=False)
    msg = await channel.send(embed=embed)

    await asyncio.sleep(random.uniform(REFLEX_WAIT_MIN, REFLEX_WAIT_MAX))

    view        = _ReflexHitView(interaction.user.id)
    appeared_at = time.monotonic()

    embed2 = _drill_header_embed(stat, "reflex", row, member,
                                  "🟢  **PRESS NOW!**")
    await msg.edit(embed=embed2, view=view)
    await view.wait()

    if view.timed_out or view.pressed_at is None:
        embed3 = _drill_header_embed(stat, "reflex", row, member, "⏱️ Too slow!")
        embed3.add_field(name="Score", value="**0%**", inline=False)
        await msg.edit(embed=embed3, view=None)
        return 0.0

    reaction_ms = (view.pressed_at - appeared_at) * 1000
    t     = max(0.0, min(1.0, (REFLEX_ZERO_MS - reaction_ms) / (REFLEX_ZERO_MS - REFLEX_PERFECT_MS)))
    score = round(t * 100, 1)

    embed4 = _drill_header_embed(stat, "reflex", row, member)
    embed4.add_field(name="Reaction Time", value=f"**{reaction_ms:.0f} ms**", inline=True)
    embed4.add_field(name="Score", value=f"**{score:.1f}%**", inline=True)
    await msg.edit(embed=embed4, view=None)
    return score


# ═══════════════════════════════════════════════════════════
#  DRILL 2 — Memory
# ═══════════════════════════════════════════════════════════

class _MemoryRevealView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=60)
        self.user_id  = user_id
        self.revealed = False

    @discord.ui.button(label="👁  Reveal Sequence", style=discord.ButtonStyle.primary, custom_id="memory_reveal")
    async def reveal_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ Not your drill.", ephemeral=True)
        self.revealed = True
        button.disabled = True
        await interaction.response.defer()
        self.stop()

    async def on_timeout(self):
        pass


async def run_memory_drill(
    interaction: discord.Interaction,
    stat: str,
    row: Dict[str, Any],
    channel: discord.abc.Messageable,
) -> float:
    member   = interaction.user
    stat_val = int(row.get(stat, BASE_STAT))
    t        = max(0.0, min(1.0, (stat_val - 1) / 98.0))
    length   = int(MEMORY_BASE_LENGTH + round((MEMORY_MAX_LENGTH - MEMORY_BASE_LENGTH) * t))
    sequence = " ".join(random.choices(MEMORY_SYMBOLS, k=length))

    reveal_view = _MemoryRevealView(interaction.user.id)
    embed = _drill_header_embed(stat, "memory", row, member,
                                 f"Press **Reveal** when ready — you'll have **{MEMORY_SHOW_SECONDS:.0f}s** to memorise.")
    embed.add_field(name="Sequence length", value=f"**{length}** symbols", inline=True)
    msg = await channel.send(embed=embed, view=reveal_view)
    await reveal_view.wait()

    if not reveal_view.revealed:
        embed2 = _drill_header_embed(stat, "memory", row, member, "⏱️ Expired.")
        embed2.add_field(name="Score", value="**0%**", inline=False)
        await msg.edit(embed=embed2, view=None)
        return 0.0

    embed3 = _drill_header_embed(stat, "memory", row, member,
                                  f"Memorise — disappears in **{MEMORY_SHOW_SECONDS:.0f}s!**")
    embed3.add_field(name="Sequence", value=f"```{sequence}```", inline=False)
    await msg.edit(embed=embed3, view=None)
    await asyncio.sleep(MEMORY_SHOW_SECONDS)

    embed4 = _drill_header_embed(stat, "memory", row, member,
                                  f"*Hidden!* Type it now (space-separated). **{MEMORY_TIMEOUT:.0f}s**.")
    await msg.edit(embed=embed4)
    hidden_at = time.monotonic()

    def check(m: discord.Message) -> bool:
        return m.author.id == interaction.user.id and m.channel.id == channel.id  # type: ignore

    try:
        resp = await interaction.client.wait_for("message", check=check, timeout=MEMORY_TIMEOUT)
    except asyncio.TimeoutError:
        embed5 = _drill_header_embed(stat, "memory", row, member, "⏱️ Time's up!")
        embed5.add_field(name="Score", value="**0%**", inline=False)
        await msg.edit(embed=embed5)
        return 0.0

    # Copy-paste detection: a human can't type more than ~5 chars/sec from memory.
    # If they respond in under 2s with a long sequence, it's almost certainly pasted.
    elapsed = time.monotonic() - hidden_at
    min_honest_time = max(2.0, length * 0.18)  # ~180ms per char is generous typing speed
    if elapsed < min_honest_time:
        embed_cheat = _drill_header_embed(stat, "memory", row, member, "❌ Response too fast — looks like a paste.")
        embed_cheat.add_field(name="Score", value="**0%**", inline=False)
        await msg.edit(embed=embed_cheat)
        return 0.0

    real_tokens = sequence.split()
    # Truncate input to the pattern length — extra tokens are ignored
    all_tokens  = resp.content.strip().upper().split()
    user_tokens = all_tokens[:len(real_tokens)]

    correct = sum(1 for a, b in zip(user_tokens, real_tokens) if a == b)
    score   = round((correct / len(real_tokens)) * 100, 1)

    embed6 = _drill_header_embed(stat, "memory", row, member)
    embed6.add_field(name="Expected", value=f"```{sequence}```", inline=False)
    embed6.add_field(name="You typed", value=f"```{' '.join(user_tokens) or '(nothing)'}```", inline=False)
    embed6.add_field(name="Correct", value=f"**{correct} / {len(real_tokens)}**", inline=True)
    embed6.add_field(name="Score", value=f"**{score:.1f}%**", inline=True)
    await msg.edit(embed=embed6)
    return score


# ═══════════════════════════════════════════════════════════
#  DRILL 3 — Rhythm
# ═══════════════════════════════════════════════════════════

class _RhythmSwingView(discord.ui.View):
    def __init__(self, user_id: int):
        super().__init__(timeout=RHYTHM_LEAD_TIME + RHYTHM_RESPOND_TIMEOUT + 1.0)
        self.user_id    = user_id
        self.pressed_at: Optional[float] = None
        self.timed_out  = False

    @discord.ui.button(label="🎾  SWING", style=discord.ButtonStyle.primary, custom_id="rhythm_swing")
    async def swing_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ Not your drill.", ephemeral=True)
        self.pressed_at = time.monotonic()
        button.disabled = True
        await interaction.response.edit_message(view=self)
        self.stop()

    async def on_timeout(self):
        self.timed_out = True


async def run_rhythm_drill(
    interaction: discord.Interaction,
    stat: str,
    row: Dict[str, Any],
    channel: discord.abc.Messageable,
) -> float:
    member = interaction.user
    view   = _RhythmSwingView(interaction.user.id)
    blocks = 10
    step   = RHYTHM_LEAD_TIME / blocks

    embed = _drill_header_embed(stat, "rhythm", row, member,
                                 "Press **🎾 SWING** exactly when the bar fills up!")
    embed.add_field(name="Charge", value="`▱▱▱▱▱▱▱▱▱▱`  *charging…*", inline=False)
    msg = await channel.send(embed=embed, view=view)

    peak_time: float = 0.0

    for i in range(1, blocks + 1):
        if view.pressed_at is not None:
            break
        bar   = "█" * i + "░" * (blocks - i)
        label = f"*{i * 10}%*" if i < blocks else "⚡ **PEAK — SWING NOW!**"
        try:
            e = _drill_header_embed(stat, "rhythm", row, member,
                                    "Press **🎾 SWING** exactly when the bar fills up!")
            e.add_field(name="Charge", value=f"`{bar}`  {label}", inline=False)
            await msg.edit(embed=e, view=view)
            if i == blocks:
                # Record peak time AFTER Discord confirms the edit so it reflects
                # when the user actually sees the peak bar — not before the sleep.
                peak_time = time.monotonic()
        except Exception:
            pass
        await asyncio.sleep(step)

    if view.pressed_at is None and not view.timed_out:
        await asyncio.sleep(RHYTHM_RESPOND_TIMEOUT)
    view.stop()

    if view.timed_out or view.pressed_at is None:
        e = _drill_header_embed(stat, "rhythm", row, member, "⏱️ You didn't swing!")
        e.add_field(name="Score", value="**0%**", inline=False)
        await msg.edit(embed=e, view=None)
        return 0.0

    offset_ms = abs(view.pressed_at - peak_time) * 1000
    t     = max(0.0, min(1.0, (RHYTHM_ZERO_MS - offset_ms) / (RHYTHM_ZERO_MS - RHYTHM_PERFECT_MS)))
    score = round(t * 100, 1)

    e = _drill_header_embed(stat, "rhythm", row, member)
    e.add_field(name="Timing offset", value=f"**{offset_ms:.0f} ms** from peak", inline=True)
    e.add_field(name="Score", value=f"**{score:.1f}%**", inline=True)
    await msg.edit(embed=e, view=None)
    return score


# ═══════════════════════════════════════════════════════════
#  DRILL 4 — Focus
#  Delete + resend each round so the button is always at the
#  bottom of chat. Button position stays consistent.
# ═══════════════════════════════════════════════════════════

class _FocusTapView(discord.ui.View):
    def __init__(self, user_id: int, round_id: str):
        super().__init__(timeout=FOCUS_INTERVAL_SEC)
        self.user_id     = user_id
        self.round_id    = round_id
        self.appeared_at: float = 0.0   # set after channel.send() so timing is accurate
        self.pressed_at: Optional[float] = None
        self.pressed     = False

        # Unique custom_id per round prevents a late interaction from a deleted
        # message routing into the next round's view.
        btn = discord.ui.Button(
            label="✋  TAP",
            style=discord.ButtonStyle.primary,
            custom_id=f"focus_tap_{round_id}",
        )
        btn.callback = self._tap_callback
        self.add_item(btn)

    async def _tap_callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ Not your drill.", ephemeral=True)
        self.pressed_at = time.monotonic()
        self.pressed    = True
        for item in self.children:
            item.disabled = True  # type: ignore
        await interaction.response.defer()
        self.stop()

    async def on_timeout(self):
        pass


async def run_focus_drill(
    interaction: discord.Interaction,
    stat: str,
    row: Dict[str, Any],
    channel: discord.abc.Messageable,
) -> float:
    member = interaction.user

    words: List[str] = []
    for _ in range(FOCUS_ROUNDS):
        if random.random() < FOCUS_TARGET_RATIO:
            words.append(FOCUS_TARGET_WORD)
        else:
            words.append(random.choice(FOCUS_WORD_POOL))

    # Intro message — shown briefly then deleted
    intro = await channel.send(
        f"🎯 **Focus Drill** — **{_pretty_stat_name(stat)}**\n"
        f"Target word: `{FOCUS_TARGET_WORD}` — press **✋ TAP** the instant you see it.\n"
        f"{_stat_xp_line(row, stat)}\n\n"
        f"Starting in **3s…**"
    )
    await asyncio.sleep(3.0)
    try:
        await intro.delete()
    except Exception:
        pass

    round_results: List[Tuple[bool, Optional[float]]] = []
    current_msg: Optional[discord.Message] = None

    for i, word in enumerate(words):
        # Delete previous round message
        if current_msg is not None:
            try:
                await current_msg.delete()
            except Exception:
                pass
            current_msg = None
            # Brief pause after delete before sending next — avoids rate limits
            await asyncio.sleep(0.05)

        round_id = f"{interaction.user.id}_{i}"
        view = _FocusTapView(interaction.user.id, round_id)

        is_target = word == FOCUS_TARGET_WORD

        current_msg = await channel.send(
            f"🎯 **Focus** {i + 1}/{FOCUS_ROUNDS}  ·  "
            f"target: `{FOCUS_TARGET_WORD}`\n\n"
            f"# `{word}`",
            view=view,
        )
        # Set appeared_at AFTER send so timing is relative to when the message
        # actually became visible, not when the view object was constructed.
        view.appeared_at = time.monotonic()

        await view.wait()

        reaction_ms: Optional[float] = None
        if view.pressed and view.pressed_at is not None:
            reaction_ms = (view.pressed_at - view.appeared_at) * 1000
        round_results.append((is_target, reaction_ms))

    # Delete last round message
    if current_msg is not None:
        try:
            await current_msg.delete()
        except Exception:
            pass

    # Scoring
    interval_ms  = FOCUS_INTERVAL_SEC * 1000
    total_score  = 0.0
    target_count = 0
    hits         = 0
    false_pos    = 0

    for is_target, reaction_ms in round_results:
        if is_target:
            target_count += 1
            if reaction_ms is not None:
                hits += 1
                speed = max(0.0, min(1.0, 1.0 - (reaction_ms / interval_ms)))
                total_score += speed
        else:
            if reaction_ms is not None:
                false_pos += 1

    # Divide by target_count so 7/9 perfect hits scores ~78%, not ~28%
    score = round((total_score / max(1, target_count)) * 100, 1)

    # Results embed
    missed = target_count - hits
    embed  = _drill_header_embed(stat, "focus", row, member)
    embed.add_field(name="Target hits",    value=f"**{hits} / {target_count}**", inline=True)
    embed.add_field(name="Missed targets", value=f"**{missed}**",               inline=True)
    embed.add_field(name="False taps",     value=f"**{false_pos}**",            inline=True)
    embed.add_field(name="Score",          value=f"**{score:.1f}%**",           inline=False)
    await channel.send(embed=embed)
    return score


# ═══════════════════════════════════════════════════════════
#  Autocomplete
# ═══════════════════════════════════════════════════════════

async def _stat_autocomplete(
    interaction: discord.Interaction,
    current: str,
) -> List[app_commands.Choice[str]]:
    try:
        cur  = (current or "").lower()
        hits: List[app_commands.Choice[str]] = []
        for k in SUBSTAT_KEYS:
            label = _pretty_stat_name(k)
            if cur in k.lower() or cur in label.lower():
                hits.append(app_commands.Choice(name=label, value=k))
            if len(hits) >= 25:
                break
        return hits
    except Exception:
        return []


# ═══════════════════════════════════════════════════════════
#  XP View  (paginated embed)
# ═══════════════════════════════════════════════════════════

_XP_PAGE_ORDER = ["forehand", "backhand", "serve", "return", "touch", "fitness", "mental"]


class TrainingXPView(discord.ui.View):
    def __init__(self, member: discord.Member, row: Dict[str, Any], timeout: int = 180):
        super().__init__(timeout=timeout)
        self.member       = member
        self.row          = row
        self.current_page = 0
        self._update_buttons()

    def _build_embed(self) -> discord.Embed:
        cat   = _XP_PAGE_ORDER[self.current_page]
        stats = CATEGORY_COMPONENTS.get(cat, ())

        embed = discord.Embed(
            title=f"🏋️  Training XP — {self.member.display_name}",
            description=f"**{cat.title()}**  ·  page {self.current_page + 1} / {len(_XP_PAGE_ORDER)}",
            color=EMBED_COLOR,
        )
        embed.set_thumbnail(url=self.member.display_avatar.url)

        cat_val = int(self.row.get(cat, BASE_STAT))
        embed.add_field(
            name=f"{cat.title()} overall",
            value=f"**{cat_val}** / {STAT_MAX}",
            inline=False,
        )

        lines: List[str] = []
        for stat in stats:
            val     = int(self.row.get(stat, BASE_STAT))
            xp_now  = float(self.row.get(_xp_key(stat), 0.0))
            xp_need = xp_needed_for_level(val)
            pct     = int(xp_now / xp_need * 100) if xp_need else 0
            h       = hours_to_lose_one_point(val)
            decay   = f"{h:.0f}h/pt" if h != float("inf") else "stable"
            lines.append(
                f"**{_pretty_stat_name(stat)}** `{val:>2}`  "
                f"{xp_now:.0f} / {xp_need:.0f} XP  ({pct}%)  —  decay: {decay}"
            )

        embed.add_field(name="Sub-stats", value="\n".join(lines) or "—", inline=False)
        embed.set_footer(text="decay = hrs of inactivity to lose 1 stat point")
        return embed

    def _update_buttons(self):
        self.prev_btn.disabled = self.current_page == 0
        self.next_btn.disabled = self.current_page == len(_XP_PAGE_ORDER) - 1

    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page -= 1
        self._update_buttons()
        await interaction.response.edit_message(embed=self._build_embed(), view=self)

    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current_page += 1
        self._update_buttons()
        await interaction.response.edit_message(embed=self._build_embed(), view=self)


# ═══════════════════════════════════════════════════════════
#  Cog
# ═══════════════════════════════════════════════════════════

class TrainingCog(commands.Cog):

    training = app_commands.Group(
        name="training",
        description="Training drills and XP progress",
        guild_only=True,
    )

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # ── /training stat ────────────────────────────────────────────────────

    @training.command(
        name="stat",
        description="Run a training drill for a specific sub-stat and earn XP.",
    )
    @app_commands.autocomplete(stat=_stat_autocomplete)
    async def training_stat(
        self,
        interaction: discord.Interaction,
        stat: str,
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        if stat not in SUBSTAT_KEYS:
            return await interaction.response.send_message(
                "❌ Invalid sub-stat — use autocomplete.", ephemeral=True,
            )

        uid = interaction.user.id
        if uid in _ACTIVE_TRAINING:
            return await interaction.response.send_message(
                "⚠️ You already have an active training session!", ephemeral=True,
            )

        row = ensure_player_for_member(interaction.guild, interaction.user)
        row = apply_passive_fatigue_decay(row)

        # Cooldown check (before apply_stat_decay updates the timestamp)
        cooldown_secs = _cooldown_remaining_seconds(row, stat)
        if cooldown_secs > 0:
            mins = int(cooldown_secs // 60)
            secs = int(cooldown_secs % 60)
            return await interaction.response.send_message(
                f"⏳ **{_pretty_stat_name(stat)}** is on cooldown — "
                f"try again in **{mins}m {secs}s**.",
                ephemeral=True,
            )

        row = apply_stat_decay(row, stat)

        fatigue_cost    = calculate_fatigue_cost(row)
        current_fatigue = float(row.get("fatigue", 0))

        if current_fatigue + fatigue_cost > 100:
            remaining = int(100 - current_fatigue)
            return await interaction.response.send_message(
                f"😓 Costs **{fatigue_cost}** fatigue but you only have **{remaining}** left. "
                f"Use `/recovery` to recover some.",
                ephemeral=True,
            )

        # Defer silently — drill messages come from channel.send
        await interaction.response.defer(ephemeral=True)

        _ACTIVE_TRAINING.add(uid)
        channel    = interaction.channel
        drill_name = _drill_for_stat(stat)

        # Snapshot stat/XP before training for result embed
        old_stat_val  = int(row.get(stat, BASE_STAT))
        old_xp        = float(row.get(_xp_key(stat), 0.0))
        row_snapshot  = dict(row)

        try:
            if drill_name == "reflex":
                score = await run_reflex_drill(interaction, stat, row, channel)
            elif drill_name == "memory":
                score = await run_memory_drill(interaction, stat, row, channel)
            elif drill_name == "rhythm":
                score = await run_rhythm_drill(interaction, stat, row, channel)
            else:
                score = await run_focus_drill(interaction, stat, row, channel)

            xp_gained = performance_to_xp(score)
            row, old_val, new_val, leveled_up = apply_xp_to_row(row, stat, xp_gained)

            row["fatigue"] = min(100.0, float(row.get("fatigue", 0)) + fatigue_cost)

            cats = compute_categories(row)
            row.update(cats)
            set_player_row_by_id(interaction.guild, uid, row)

            result_embed = _result_embed(
                member        = interaction.user,
                stat          = stat,
                drill         = drill_name,
                row_before_xp = row_snapshot,
                row_after_xp  = row,
                score         = score,
                xp_gained     = xp_gained,
                old_stat_val  = old_val,
                new_stat_val  = new_val,
                leveled_up    = leveled_up,
            )
            await channel.send(embed=result_embed)

        finally:
            _ACTIVE_TRAINING.discard(uid)

    # ── /training xp-view ─────────────────────────────────────────────────

    @training.command(
        name="xp-view",
        description="See training XP and decay rates for all stats (paginated).",
    )
    async def training_xp_view(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
    ):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        target = user or interaction.user
        if not isinstance(target, discord.Member):
            return await interaction.response.send_message("❌ Guild members only.", ephemeral=True)

        row   = ensure_player_for_member(interaction.guild, target)
        row   = apply_passive_fatigue_decay(row)
        view  = TrainingXPView(target, row)
        embed = view._build_embed()
        await interaction.response.send_message(embed=embed, view=view)


async def setup(bot: commands.Bot):
    await bot.add_cog(TrainingCog(bot))