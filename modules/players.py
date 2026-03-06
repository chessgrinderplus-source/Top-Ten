# modules/players.py
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

import discord
from discord import app_commands
from discord.ext import commands


def _data_dir() -> str:
    try:
        import config  # type: ignore
        d = str(getattr(config, "DATA_DIR", "data"))
    except Exception:
        d = "data"
    if not os.path.isabs(d):
        d = os.path.abspath(d)
    return d


PLAYERS_PATH = os.path.join(_data_dir(), "players.json")

PLAYERS: Dict[str, Dict[str, Any]] = {}

BASE_STAT       = 1
STARTING_POINTS = 7

CATEGORY_KEYS = ("forehand", "backhand", "serve", "touch", "fitness")

SUBSTAT_KEYS = (
    "fh_power", "fh_accuracy", "fh_timing",
    "bh_power", "bh_accuracy", "bh_timing",
    "fs_speed", "fs_accuracy", "fs_spin",
    "ss_speed", "ss_accuracy", "ss_spin",
    "return_accuracy", "return_speed",
    "volley", "half_volley", "drop_shot_effectivity", "slice", "lob",
    "footwork", "speed", "stamina",
    "focus", "tennis_iq", "mental_stamina",
)

STAT_KEYS = CATEGORY_KEYS

CATEGORY_COMPONENTS = {
    "forehand": ("fh_power", "fh_accuracy", "fh_timing"),
    "backhand": ("bh_power", "bh_accuracy", "bh_timing"),
    "serve":    ("fs_speed", "fs_accuracy", "fs_spin", "ss_speed", "ss_accuracy", "ss_spin"),
    "return":   ("return_accuracy", "return_speed"),
    "touch":    ("volley", "half_volley", "drop_shot_effectivity", "slice", "lob"),
    "fitness":  ("footwork", "speed", "stamina"),
    "mental":   ("focus", "tennis_iq", "mental_stamina"),
}

ALLOCATABLE_BY_CATEGORY: Dict[str, list] = {
    "forehand": ["fh_power", "fh_accuracy", "fh_timing"],
    "backhand": ["bh_power", "bh_accuracy", "bh_timing"],
    "serve":    ["fs_speed", "fs_accuracy", "fs_spin", "ss_speed", "ss_accuracy", "ss_spin"],
    "return":   ["return_accuracy", "return_speed"],
    "touch":    ["volley", "half_volley", "drop_shot_effectivity", "slice", "lob"],
    "fitness":  ["footwork", "speed", "stamina"],
    "mental":   ["focus", "tennis_iq", "mental_stamina"],
}


# ═══════════════════════════════════════════════════════════
#  Persistence helpers
# ═══════════════════════════════════════════════════════════

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path: str, data) -> None:
    _ensure_dir(os.path.dirname(path))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _db() -> Dict[str, Any]:
    return _load_json(PLAYERS_PATH, {"players_by_id": {}})


def _save_db(db: Dict[str, Any]) -> None:
    _save_json(PLAYERS_PATH, db)


def _clamp_stat(v: int) -> int:
    return max(1, min(99, int(v)))


def _clamp_fatigue(v: float) -> float:
    return max(0.0, min(100.0, float(v)))


def _clamp_points(v: int) -> int:
    return max(0, int(v))


def _refresh_cache_for_guild(guild: Optional[discord.Guild]) -> None:
    PLAYERS.clear()
    by_id = (_db().get("players_by_id", {}) or {})
    for uid_str, row in by_id.items():
        if not isinstance(row, dict):
            continue
        PLAYERS[str(uid_str)] = dict(row)
        if guild:
            try:
                m = guild.get_member(int(uid_str))
                if m:
                    PLAYERS[m.display_name] = dict(row)
            except Exception:
                pass


def get_player_row_by_id(user_id: int) -> Optional[Dict[str, Any]]:
    row = (_db().get("players_by_id", {}) or {}).get(str(user_id))
    return row if isinstance(row, dict) else None


def set_player_row_by_id(guild: Optional[discord.Guild], user_id: int, row: Dict[str, Any]) -> None:
    try:
        db = _db()
        db.setdefault("players_by_id", {})[str(user_id)] = row
        _save_db(db)
        _refresh_cache_for_guild(guild)
    except Exception as e:
        print(f"[PLAYERS][ERROR] Failed to save uid={user_id}: {e}")


def compute_categories(row: Dict[str, Any]) -> Dict[str, int]:
    out = {}
    for cat, parts in CATEGORY_COMPONENTS.items():
        vals = []
        for k in parts:
            try:
                vals.append(int(row.get(k, BASE_STAT)))
            except Exception:
                vals.append(BASE_STAT)
        avg = round(sum(vals) / max(1, len(vals)))
        out[cat] = _clamp_stat(avg)
    return out


def ensure_player_for_member(guild: discord.Guild, member: discord.Member) -> Dict[str, Any]:
    row = get_player_row_by_id(member.id)

    if row is None:
        row = {
            "user_id":        member.id,
            "created_at":     _utc_now_iso(),
            "forehand":       BASE_STAT,
            "backhand":       BASE_STAT,
            "serve":          BASE_STAT,
            "return":         BASE_STAT,
            "touch":          BASE_STAT,
            "fitness":        BASE_STAT,
            "mental":         BASE_STAT,
            **{k: BASE_STAT for k in SUBSTAT_KEYS},
            "fatigue":        0,
            "unspent_points": STARTING_POINTS,
            "spent_points":   0,
        }
        row.update(compute_categories(row))
        set_player_row_by_id(guild, member.id, row)
        return row

    changed = False

    for k in SUBSTAT_KEYS:
        if k not in row:
            row[k] = BASE_STAT
            changed = True

    for k in CATEGORY_KEYS:
        if k not in row:
            row[k] = BASE_STAT
            changed = True

    for k in SUBSTAT_KEYS:
        try:
            clamped = _clamp_stat(int(row.get(k, BASE_STAT)))
        except Exception:
            clamped = BASE_STAT
        if row.get(k) != clamped:
            row[k] = clamped
            changed = True

    fresh_cats = compute_categories(row)
    for k, v in fresh_cats.items():
        if row.get(k) != v:
            row[k] = v
            changed = True

    if changed:
        set_player_row_by_id(guild, member.id, row)

    return row


def set_fatigue_for_user_id(guild: Optional[discord.Guild], user_id: int, fatigue: float) -> None:
    row = get_player_row_by_id(user_id)
    if row is None:
        return
    row["fatigue"] = _clamp_fatigue(fatigue)
    set_player_row_by_id(guild, user_id, row)


# ═══════════════════════════════════════════════════════════
#  Handedness helpers
# ═══════════════════════════════════════════════════════════

def format_handedness(row: Dict[str, Any]) -> Optional[str]:
    hand = row.get("handedness")
    bh   = row.get("backhand_style")
    if not hand or not bh:
        return None
    hand_label = "Right-handed" if hand == "right" else "Left-handed"
    bh_label   = "2HBH" if bh == "two_handed" else "1HBH"
    return f"{hand_label}, {bh_label}"


# ═══════════════════════════════════════════════════════════
#  Formatting helpers
# ═══════════════════════════════════════════════════════════

def _pretty_stat_name(key: str) -> str:
    nice = {
        "fh_power":              "FH Power",
        "fh_accuracy":           "FH Accuracy",
        "fh_timing":             "FH Timing",
        "bh_power":              "BH Power",
        "bh_accuracy":           "BH Accuracy",
        "bh_timing":             "BH Timing",
        "fs_speed":              "1st Serve Speed",
        "fs_accuracy":           "1st Serve Accuracy",
        "fs_spin":               "1st Serve Spin",
        "ss_speed":              "2nd Serve Speed",
        "ss_accuracy":           "2nd Serve Accuracy",
        "ss_spin":               "2nd Serve Spin",
        "return_accuracy":       "Return Accuracy",
        "return_speed":          "Return Speed",
        "touch":                 "Touch",
        "volley":                "Volley",
        "half_volley":           "Half-Volley",
        "drop_shot_effectivity": "Drop Shot Effectivity",
        "slice":                 "Slice",
        "lob":                   "Lob",
        "fitness":               "Fitness",
        "footwork":              "Footwork",
        "speed":                 "Speed",
        "stamina":               "Stamina",
        "focus":                 "Focus",
        "tennis_iq":             "Tennis IQ",
        "mental_stamina":        "Mental Stamina",
    }
    return nice.get(key, key.replace("_", " ").title())


# ═══════════════════════════════════════════════════════════
#  Interactive stat editor — PlayerAllocateView
# ═══════════════════════════════════════════════════════════

class _SetStatModal(discord.ui.Modal, title="Set Stat Value"):
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


class PlayerAllocateView(discord.ui.View):
    """
    Interactive stat-allocation UI.

    is_admin=True  → no point cost; shows −1 / −5 / Reset buttons; can target any member.
    is_admin=False → deducts unspent_points; +buttons only; invoker must be the member.
    """

    def __init__(
        self,
        guild: discord.Guild,
        member: discord.Member,
        row: dict,
        *,
        is_admin: bool = False,
        invoker_id: Optional[int] = None,
    ):
        super().__init__(timeout=300)
        self.guild      = guild
        self.member     = member
        self.row        = dict(row)
        self.is_admin   = is_admin
        self.invoker_id = invoker_id or member.id

        self.selected_category = list(ALLOCATABLE_BY_CATEGORY.keys())[0]
        self.selected_stat     = ALLOCATABLE_BY_CATEGORY[self.selected_category][0]

        self._cat_select:  Optional[discord.ui.Select] = None
        self._stat_select: Optional[discord.ui.Select] = None
        self._build_selects()

    def _build_selects(self):
        self._cat_select = discord.ui.Select(
            placeholder="Category…",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(
                    label=cat.title(),
                    value=cat,
                    default=(cat == self.selected_category),
                )
                for cat in ALLOCATABLE_BY_CATEGORY.keys()
            ],
            row=0,
        )
        self._cat_select.callback = self._on_cat
        self.add_item(self._cat_select)
        self._rebuild_stat_select()

    def _rebuild_stat_select(self):
        if self._stat_select is not None:
            self.remove_item(self._stat_select)

        stats = ALLOCATABLE_BY_CATEGORY[self.selected_category]
        self._stat_select = discord.ui.Select(
            placeholder="Stat…",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(
                    label=_pretty_stat_name(k),
                    value=k,
                    description=f"Current: {self.row.get(k, BASE_STAT)}",
                    default=(k == self.selected_stat),
                )
                for k in stats
            ],
            row=1,
        )
        self._stat_select.callback = self._on_stat
        self.add_item(self._stat_select)

    def _embed(self) -> discord.Embed:
        unspent = int(self.row.get("unspent_points", 0))
        cur_val = int(self.row.get(self.selected_stat, BASE_STAT))
        cats    = compute_categories(self.row)

        if self.is_admin:
            colour = discord.Color.gold()
        elif unspent > 0:
            colour = discord.Color.green()
        else:
            colour = discord.Color.greyple()

        hand_str  = format_handedness(self.row)
        hand_line = f"🎾 Plays **{hand_str}**\n" if hand_str else "🎾 *Playing style not set yet*\n"

        e = discord.Embed(
            title=f"{'🔧 Admin — ' if self.is_admin else ''}📊 {self.member.display_name}",
            description=hand_line,
            color=colour,
        )

        cat_lines = []
        for cat in ALLOCATABLE_BY_CATEGORY.keys():
            marker = "▶" if cat == self.selected_category else "◦"
            cat_lines.append(f"{marker} **{cat.title()}** — {cats.get(cat, BASE_STAT)}")
        e.add_field(name="Categories", value="\n".join(cat_lines), inline=True)

        stat_lines = []
        for k in ALLOCATABLE_BY_CATEGORY[self.selected_category]:
            marker = "▶ " if k == self.selected_stat else "    "
            stat_lines.append(f"{marker}**{_pretty_stat_name(k)}** — {self.row.get(k, BASE_STAT)}")
        e.add_field(name=f"{self.selected_category.title()} Sub-stats", value="\n".join(stat_lines), inline=True)

        pts_line = (
            "*(Admin — no point cost)*"
            if self.is_admin
            else f"Unspent points: **{unspent}**"
        )
        fatigue = float(self.row.get("fatigue", 0))
        e.add_field(name="Fatigue", value=f"**{fatigue:.0f}** / 100", inline=False)

        try:
            from modules.training import _xp_key, xp_needed_for_level
            stat_val = int(self.row.get(self.selected_stat, BASE_STAT))
            if stat_val >= 99:
                xp_display = "⭐ **MAX** (99/99)"
            else:
                xp_now  = float(self.row.get(_xp_key(self.selected_stat), 0.0))
                xp_need = xp_needed_for_level(stat_val)
                pct     = int(xp_now / xp_need * 100) if xp_need else 0
                xp_display = f"XP: {xp_now:.0f} / {xp_need:.0f} ({pct}%)"
        except Exception:
            xp_display = ""

        e.add_field(
            name="Editing",
            value=(
                f"**{_pretty_stat_name(self.selected_stat)}**: `{cur_val}` / 99\n"
                f"{xp_display}\n"
                f"{pts_line}"
            ),
            inline=False,
        )
        return e

    async def _check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id == self.invoker_id:
            return True
        await interaction.response.send_message("❌ This isn't your panel.", ephemeral=True)
        return False

    async def _on_cat(self, interaction: discord.Interaction):
        if not await self._check(interaction):
            return
        self.selected_category = self._cat_select.values[0]
        self.selected_stat     = ALLOCATABLE_BY_CATEGORY[self.selected_category][0]
        for opt in self._cat_select.options:
            opt.default = (opt.value == self.selected_category)
        self._rebuild_stat_select()
        await interaction.response.edit_message(embed=self._embed(), view=self)

    async def _on_stat(self, interaction: discord.Interaction):
        if not await self._check(interaction):
            return
        self.selected_stat = self._stat_select.values[0]
        for opt in self._stat_select.options:
            opt.default = (opt.value == self.selected_stat)
        await interaction.response.edit_message(embed=self._embed(), view=self)

    async def _apply(self, interaction: discord.Interaction, delta: int):
        if not await self._check(interaction):
            return

        fresh = get_player_row_by_id(self.member.id)
        if fresh:
            self.row = fresh

        unspent = _clamp_points(int(self.row.get("unspent_points", 0)))
        old_val = int(self.row.get(self.selected_stat, BASE_STAT))

        if not self.is_admin and delta > 0:
            cost = min(delta, unspent)
            if cost == 0:
                return await interaction.response.send_message(
                    "❌ No unspent points left.", ephemeral=True
                )
            delta = cost

        new_val = _clamp_stat(old_val + delta)
        actual  = new_val - old_val

        if actual == 0:
            msg = "❌ Already at max (99)." if delta > 0 else "❌ Already at min (1)."
            return await interaction.response.send_message(msg, ephemeral=True)

        self.row[self.selected_stat] = new_val

        if not self.is_admin:
            self.row["unspent_points"] = _clamp_points(unspent - actual)
            self.row["spent_points"]   = _clamp_points(int(self.row.get("spent_points", 0)) + actual)

        self.row.update(compute_categories(self.row))
        set_player_row_by_id(self.guild, self.member.id, self.row)

        for opt in self._stat_select.options:
            if opt.value == self.selected_stat:
                opt.description = f"Current: {new_val}"

        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.button(label="+1",  style=discord.ButtonStyle.success,   row=2)
    async def add1(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self._apply(interaction, +1)

    @discord.ui.button(label="+5",  style=discord.ButtonStyle.success,   row=2)
    async def add5(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self._apply(interaction, +5)

    @discord.ui.button(label="+10", style=discord.ButtonStyle.primary,   row=2)
    async def add10(self, interaction: discord.Interaction, _: discord.ui.Button):
        await self._apply(interaction, +10)

    @discord.ui.button(label="Set…", style=discord.ButtonStyle.secondary, row=2)
    async def set_exact(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not await self._check(interaction):
            return

        async def _on_set(ix: discord.Interaction, v: int):
            cur = int(self.row.get(self.selected_stat, BASE_STAT))
            await self._apply(ix, v - cur)

        await interaction.response.send_modal(_SetStatModal(_on_set))

    @discord.ui.button(label="Done", style=discord.ButtonStyle.danger, row=2)
    async def done(self, interaction: discord.Interaction, _: discord.ui.Button):
        self.stop()
        await interaction.response.edit_message(
            content=f"✅ Done editing **{self.member.display_name}**.",
            embed=None,
            view=None,
        )

    @discord.ui.button(label="-1",      style=discord.ButtonStyle.secondary, row=3)
    async def sub1(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not self.is_admin:
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        await self._apply(interaction, -1)

    @discord.ui.button(label="-5",      style=discord.ButtonStyle.secondary, row=3)
    async def sub5(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not self.is_admin:
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        await self._apply(interaction, -5)

    @discord.ui.button(label="Reset→1", style=discord.ButtonStyle.danger,    row=3)
    async def reset_stat(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not self.is_admin:
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        cur = int(self.row.get(self.selected_stat, BASE_STAT))
        await self._apply(interaction, 1 - cur)


# ═══════════════════════════════════════════════════════════
#  Cog
# ═══════════════════════════════════════════════════════════

class PlayersCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    def _is_admin(self, member: discord.Member) -> bool:
        return bool(getattr(member.guild_permissions, "administrator", False))

    @app_commands.command(name="player", description="View and allocate a player's stats.")
    @app_commands.guild_only()
    async def player_cmd(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
    ):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        target = user if user else interaction.user
        if not isinstance(target, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        row = ensure_player_for_member(interaction.guild, target)

        from modules.matchsim import apply_passive_fatigue_decay
        row = apply_passive_fatigue_decay(row)

        # Apply stat decay to all sub-stats so displayed values are always current
        try:
            from modules.training import apply_all_stat_decay
            row = apply_all_stat_decay(row)
        except Exception:
            pass

        set_player_row_by_id(interaction.guild, target.id, row)

        invoker    = interaction.user
        is_admin   = self._is_admin(invoker) if isinstance(invoker, discord.Member) else False
        is_self    = (target.id == invoker.id)
        view_admin = is_admin and not is_self

        view = PlayerAllocateView(
            interaction.guild,
            target,
            row,
            is_admin=view_admin,
            invoker_id=invoker.id,
        )
        await interaction.response.send_message(
            embed=view._embed(),
            view=view,
            ephemeral=(not is_self and not is_admin),
        )

    @app_commands.command(
        name="player-allocate",
        description="Spend your unspent stat points (interactive).",
    )
    @app_commands.guild_only()
    async def player_allocate_cmd(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        row  = ensure_player_for_member(interaction.guild, interaction.user)
        view = PlayerAllocateView(
            interaction.guild,
            interaction.user,
            row,
            is_admin=False,
            invoker_id=interaction.user.id,
        )
        await interaction.response.send_message(embed=view._embed(), view=view, ephemeral=False)

    @app_commands.command(
        name="player-admin",
        description="(Admin) Interactively edit any player's stats, fatigue, and points.",
    )
    @app_commands.guild_only()
    async def player_admin_cmd(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        unspent_points_set: Optional[int] = None,
        unspent_points_add: Optional[int] = None,
        fatigue: Optional[app_commands.Range[int, 0, 100]] = None,
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        if not self._is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        row     = ensure_player_for_member(interaction.guild, user)
        changes: List[str] = []

        if unspent_points_set is not None:
            old = int(row.get("unspent_points", 0))
            row["unspent_points"] = _clamp_points(int(unspent_points_set))
            changes.append(f"Unspent points: {old} → **{row['unspent_points']}**")

        if unspent_points_add is not None:
            old = int(row.get("unspent_points", 0))
            row["unspent_points"] = _clamp_points(old + int(unspent_points_add))
            changes.append(f"Unspent points: {old} → **{row['unspent_points']}**")

        if fatigue is not None:
            old = float(row.get("fatigue", 0))
            row["fatigue"] = float(fatigue)
            changes.append(f"Fatigue: {old:.0f} → **{fatigue}**")

        if changes:
            row.update(compute_categories(row))
            set_player_row_by_id(interaction.guild, user.id, row)

        view = PlayerAllocateView(
            interaction.guild,
            user,
            row,
            is_admin=True,
            invoker_id=interaction.user.id,
        )
        header = (
            "✅ Applied:\n" + "\n".join(f"  • {c}" for c in changes) + "\n\n"
            if changes else ""
        )
        await interaction.response.send_message(
            content=f"{header}Editing **{user.display_name}**:",
            embed=view._embed(),
            view=view,
            ephemeral=False,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(PlayersCog(bot))
    guild = bot.guilds[0] if bot.guilds else None
    _refresh_cache_for_guild(guild)