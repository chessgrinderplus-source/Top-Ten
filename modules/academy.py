# modules/academy.py
from __future__ import annotations

import json
import os
import random
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands, tasks

from modules.economy import get_balance, remove_balance
from modules.players import (
    ensure_player_for_member,
    set_player_row_by_id,  
)

# =========================
# Storage
# =========================
def _data_dir() -> str:
    try:
        import config  # type: ignore
        return str(getattr(config, "DATA_DIR", "data"))
    except Exception:
        return "data"


ACADEMY_PATH = os.path.join(_data_dir(), "academy.json")


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


def _db() -> Dict[str, Any]:
    return _load_json(
        ACADEMY_PATH,
        {
            "academies": {},          # academy_id -> academy row
            "memberships": {},        # guild_id -> user_id -> membership row
            "last_daily_tick": {},    # guild_id -> iso date string (YYYY-MM-DD)
        },
    )


def _save_db(db: Dict[str, Any]) -> None:
    _save_json(ACADEMY_PATH, db)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _today_utc_str() -> str:
    return _utc_now().date().isoformat()


def _is_admin(member: discord.Member) -> bool:
    return bool(getattr(member.guild_permissions, "administrator", False))


# =========================
# Academy helpers
# =========================
def _make_academy_id(title: str) -> str:
    base = "".join(ch.lower() if ch.isalnum() else "-" for ch in title).strip("-")
    base = "-".join([p for p in base.split("-") if p])
    if not base:
        base = "academy"
    return f"academy-{base}-{random.randint(1000, 9999)}"


def _academy_cost_total(cost_per_day: int, days: int) -> int:
    # Spec: “rate of currency cost per day (it costs more to stay for more days)”
    # Simple scaling: linear rate * days (clean + predictable).
    return int(max(0, cost_per_day * days))


def academy_get_membership(guild_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    db = _db()
    g = (db.get("memberships", {}) or {}).get(str(guild_id), {}) or {}
    row = g.get(str(user_id))
    return row if isinstance(row, dict) else None


def academy_get_user_academy_id(guild_id: int, user_id: int) -> Optional[str]:
    row = academy_get_membership(guild_id, user_id)
    if not row:
        return None
    return str(row.get("academy_id")) if row.get("academy_id") else None


def academy_can_challenge(guild_id: int, challenger_id: int, opponent_id: int) -> Tuple[bool, str]:
    """
    Spec: if a player is in an academy, he can only play bots and players in SAME academy.
    This helper is meant to be called by matchsim.
    """
    a1 = academy_get_user_academy_id(guild_id, challenger_id)
    a2 = academy_get_user_academy_id(guild_id, opponent_id)

    if not a1 and not a2:
        return True, "OK"
    if a1 and a2 and a1 == a2:
        return True, "OK"

    if a1 and not a2:
        return False, "❌ You are currently in an academy. You can only play bots or players in the **same academy** until you leave."
    if a2 and not a1:
        return False, "❌ That player is currently in an academy. You can only play them if you join the **same academy**."
    return False, "❌ You can only play players in the **same academy**."


def academy_count_members(guild_id: int, academy_id: str) -> int:
    db = _db()
    g = (db.get("memberships", {}) or {}).get(str(guild_id), {}) or {}
    n = 0
    for _, row in g.items():
        if isinstance(row, dict) and str(row.get("academy_id")) == academy_id:
            n += 1
    return n


# =========================
# Daily tick logic
# =========================
def _grant_daily_points(guild: discord.Guild, user_id: int, pts: int) -> None:
    # unspent points are stored in players.json row
    member = guild.get_member(user_id)
    if not member:
        return
    prow = ensure_player_for_member(guild, member)
    cur = int(prow.get("unspent_points", 0))
    prow["unspent_points"] = max(0, cur + int(pts))
    set_player_row_by_id(guild, user_id, prow)  # type: ignore


def academy_daily_tick(guild: discord.Guild) -> None:
    """
    Called once per UTC day:
    - For each membership: increment days_spent, grant random points in range.
    - If finished: end membership.
    """
    db = _db()
    gid = str(guild.id)

    memberships = (db.get("memberships", {}) or {}).get(gid, {}) or {}
    academies = db.get("academies", {}) or {}

    to_delete: List[str] = []
    for uid_str, mrow in memberships.items():
        if not isinstance(mrow, dict):
            continue
        academy_id = str(mrow.get("academy_id", ""))
        arow = academies.get(academy_id)
        if not isinstance(arow, dict):
            # academy deleted; remove membership
            to_delete.append(uid_str)
            continue

        total_days = int(mrow.get("days_total", 0))
        spent = int(mrow.get("days_spent", 0))

        if spent >= total_days:
            to_delete.append(uid_str)
            continue

        # grant daily stat points
        pmin = int(arow.get("points_min", 1))
        pmax = int(arow.get("points_max", 1))
        if pmax < pmin:
            pmax = pmin
        pts = random.randint(pmin, pmax)
        _grant_daily_points(guild, int(uid_str), pts)

        mrow["days_spent"] = spent + 1

        # done?
        if (spent + 1) >= total_days:
            to_delete.append(uid_str)

    for uid_str in to_delete:
        memberships.pop(uid_str, None)

    db.setdefault("memberships", {})[gid] = memberships
    db.setdefault("last_daily_tick", {})[gid] = _today_utc_str()
    _save_db(db)


# =========================
# UI helpers (pagination)
# =========================
class SimplePager(discord.ui.View):
    def __init__(self, pages: List[discord.Embed], author_id: int):
        super().__init__(timeout=180)
        self.pages = pages
        self.author_id = author_id
        self.i = 0

        self.prev_btn.disabled = True
        if len(pages) <= 1:
            self.next_btn.disabled = True

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("❌ Only the command user can use these buttons.", ephemeral=True)
            return False
        return True

    def _sync(self):
        self.prev_btn.disabled = self.i <= 0
        self.next_btn.disabled = self.i >= (len(self.pages) - 1)

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.i = max(0, self.i - 1)
        self._sync()
        await interaction.response.edit_message(embed=self.pages[self.i], view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.i = min(len(self.pages) - 1, self.i + 1)
        self._sync()
        await interaction.response.edit_message(embed=self.pages[self.i], view=self)


# =========================
# Cog
# =========================
class AcademyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._daily_loop.start()

    def cog_unload(self):
        self._daily_loop.cancel()

    @tasks.loop(minutes=10)
    async def _daily_loop(self):
        # run daily tick once per UTC day per guild
        db = _db()
        last = db.get("last_daily_tick", {}) or {}
        today = _today_utc_str()

        for guild in list(self.bot.guilds):
            gid = str(guild.id)
            if last.get(gid) == today:
                continue
            academy_daily_tick(guild)

    @_daily_loop.before_loop
    async def _before(self):
        await self.bot.wait_until_ready()

    # ---------- Admin commands ----------
    @app_commands.command(name="academy-create", description="(Admin) Create an academy.")
    @app_commands.guild_only()
    async def academy_create(
        self,
        interaction: discord.Interaction,
        academy_name: str,
        min_days: app_commands.Range[int, 1, 365],
        max_days: app_commands.Range[int, 1, 365],
        cost_per_day: app_commands.Range[int, 0, 10_000_000],
        leave_early_cost: app_commands.Range[int, 0, 10_000_000],
        points_min: app_commands.Range[int, 0, 100],
        points_max: app_commands.Range[int, 0, 100],
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        if max_days < min_days:
            return await interaction.response.send_message("❌ max_days must be >= min_days.", ephemeral=True)
        if points_max < points_min:
            return await interaction.response.send_message("❌ points_max must be >= points_min.", ephemeral=True)

        db = _db()
        academy_id = _make_academy_id(academy_name)
        row = {
            "id": academy_id,
            "name": academy_name,
            "min_days": int(min_days),
            "max_days": int(max_days),
            "cost_per_day": int(cost_per_day),
            "leave_early_cost": int(leave_early_cost),
            "points_min": int(points_min),
            "points_max": int(points_max),
            "created_at": _utc_now_iso(),
            "created_by": interaction.user.id,
        }
        db.setdefault("academies", {})[academy_id] = row
        _save_db(db)

        e = discord.Embed(title="✅ Academy Created", description=f"**{academy_name}**", color=discord.Color.green())
        e.add_field(name="ID", value=academy_id, inline=False)
        e.add_field(name="Duration Range (days)", value=f"{min_days} – {max_days}", inline=True)
        e.add_field(name="Cost / Day", value=str(cost_per_day), inline=True)
        e.add_field(name="Leave Early Cost", value=str(leave_early_cost), inline=True)
        e.add_field(name="Stat Points / Day", value=f"{points_min} – {points_max}", inline=True)
        await interaction.response.send_message(embed=e, ephemeral=False)

    @app_commands.command(name="academy-delete", description="(Admin) Delete an academy.")
    @app_commands.guild_only()
    async def academy_delete(self, interaction: discord.Interaction, academy_id: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        db = _db()
        academies = db.get("academies", {}) or {}
        if academy_id not in academies:
            return await interaction.response.send_message("❌ Academy not found.", ephemeral=True)

        academies.pop(academy_id, None)
        db["academies"] = academies
        _save_db(db)
        await interaction.response.send_message(f"🗑️ Deleted academy **{academy_id}**.", ephemeral=False)

    @app_commands.command(name="academy-edit", description="(Admin) Edit an academy.")
    @app_commands.guild_only()
    async def academy_edit(
        self,
        interaction: discord.Interaction,
        academy_id: str,
        academy_name: Optional[str] = None,
        min_days: Optional[app_commands.Range[int, 1, 365]] = None,
        max_days: Optional[app_commands.Range[int, 1, 365]] = None,
        cost_per_day: Optional[app_commands.Range[int, 0, 10_000_000]] = None,
        leave_early_cost: Optional[app_commands.Range[int, 0, 10_000_000]] = None,
        points_min: Optional[app_commands.Range[int, 0, 100]] = None,
        points_max: Optional[app_commands.Range[int, 0, 100]] = None,
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        db = _db()
        academies = db.get("academies", {}) or {}
        row = academies.get(academy_id)
        if not isinstance(row, dict):
            return await interaction.response.send_message("❌ Academy not found.", ephemeral=True)

        if academy_name is not None:
            row["name"] = str(academy_name)
        if min_days is not None:
            row["min_days"] = int(min_days)
        if max_days is not None:
            row["max_days"] = int(max_days)
        if int(row.get("max_days", 1)) < int(row.get("min_days", 1)):
            return await interaction.response.send_message("❌ max_days must be >= min_days.", ephemeral=True)

        if cost_per_day is not None:
            row["cost_per_day"] = int(cost_per_day)
        if leave_early_cost is not None:
            row["leave_early_cost"] = int(leave_early_cost)
        if points_min is not None:
            row["points_min"] = int(points_min)
        if points_max is not None:
            row["points_max"] = int(points_max)
        if int(row.get("points_max", 0)) < int(row.get("points_min", 0)):
            return await interaction.response.send_message("❌ points_max must be >= points_min.", ephemeral=True)

        academies[academy_id] = row
        db["academies"] = academies
        _save_db(db)

        await interaction.response.send_message(f"✅ Updated academy **{academy_id}**.", ephemeral=False)

    @app_commands.command(name="academy-list", description="List all academies.")
    @app_commands.guild_only()
    async def academy_list(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        db = _db()
        academies = list((db.get("academies", {}) or {}).values())
        academies = [a for a in academies if isinstance(a, dict)]
        if not academies:
            return await interaction.response.send_message("ℹ️ No academies created yet.", ephemeral=True)

        academies.sort(key=lambda r: str(r.get("name", "")).lower())
        pages: List[discord.Embed] = []
        chunk = 6
        for i in range(0, len(academies), chunk):
            sub = academies[i:i + chunk]
            e = discord.Embed(title="🏫 Academies", color=discord.Color.blurple())
            for row in sub:
                aid = str(row.get("id"))
                name = str(row.get("name"))
                members = academy_count_members(interaction.guild.id, aid)
                e.add_field(
                    name=f"{name} ({members} players)",
                    value=(
                        f"ID: `{aid}`\n"
                        f"Days: {row.get('min_days')}–{row.get('max_days')}\n"
                        f"Cost/day: {row.get('cost_per_day')} | Leave early: {row.get('leave_early_cost')}\n"
                        f"Points/day: {row.get('points_min')}–{row.get('points_max')}"
                    ),
                    inline=False,
                )
            e.set_footer(text=f"Page {len(pages) + 1}/{(len(academies) + chunk - 1)//chunk}")
            pages.append(e)

        view = SimplePager(pages, author_id=interaction.user.id)
        await interaction.response.send_message(embed=pages[0], view=view, ephemeral=False)

    # ---------- Player commands ----------
    @app_commands.command(name="academy-join", description="Join an academy for a number of days (pay upfront).")
    @app_commands.guild_only()
    async def academy_join(self, interaction: discord.Interaction, academy_id: str, days: app_commands.Range[int, 1, 365]):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        db = _db()
        academies = db.get("academies", {}) or {}
        arow = academies.get(academy_id)
        if not isinstance(arow, dict):
            return await interaction.response.send_message("❌ Academy not found.", ephemeral=True)

        # already in academy?
        cur = academy_get_membership(interaction.guild.id, interaction.user.id)
        if cur:
            return await interaction.response.send_message("❌ You are already in an academy. Use `/academy-leave` to leave early.", ephemeral=True)

        min_days = int(arow.get("min_days", 1))
        max_days = int(arow.get("max_days", 1))
        if days < min_days or days > max_days:
            return await interaction.response.send_message(f"❌ Days must be within **{min_days}–{max_days}**.", ephemeral=True)

        cost_per_day = int(arow.get("cost_per_day", 0))
        total_cost = _academy_cost_total(cost_per_day, int(days))
        if get_balance(interaction.user.id) < total_cost:
            return await interaction.response.send_message(f"❌ You need **{total_cost}** currency to join for {days} days.", ephemeral=True)

        # charge now
        ok = remove_balance(interaction.user.id, total_cost)
        if not ok:
            return await interaction.response.send_message("❌ You don’t have enough currency.", ephemeral=True)

        membership = {
            "academy_id": academy_id,
            "academy_name": str(arow.get("name", academy_id)),
            "joined_at": _utc_now_iso(),
            "days_total": int(days),
            "days_spent": 0,
            "cost_paid": total_cost,
            "leave_early_cost": int(arow.get("leave_early_cost", 0)),
        }

        db = _db()
        db.setdefault("memberships", {}).setdefault(str(interaction.guild.id), {})[str(interaction.user.id)] = membership
        _save_db(db)

        e = discord.Embed(title="✅ Academy Joined", description=f"**{membership['academy_name']}**", color=discord.Color.green())
        e.add_field(name="Days", value=str(days), inline=True)
        e.add_field(name="Cost Paid", value=str(total_cost), inline=True)
        e.add_field(name="Leave Early Cost", value=str(membership["leave_early_cost"]), inline=False)
        e.add_field(name="Important", value="While in an academy, you can only play bots or players in the **same academy**.", inline=False)
        await interaction.response.send_message(embed=e, ephemeral=False)

    @app_commands.command(name="academy-leave", description="Leave your academy early (pay leave-early cost).")
    @app_commands.guild_only()
    async def academy_leave(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        db = _db()
        g = db.setdefault("memberships", {}).setdefault(str(interaction.guild.id), {})
        mrow = g.get(str(interaction.user.id))
        if not isinstance(mrow, dict):
            return await interaction.response.send_message("❌ You are not currently in an academy.", ephemeral=True)

        fee = int(mrow.get("leave_early_cost", 0))
        if get_balance(interaction.user.id) < fee:
            return await interaction.response.send_message(f"❌ You need **{fee}** currency to leave early.", ephemeral=True)

        ok = remove_balance(interaction.user.id, fee)
        if not ok:
            return await interaction.response.send_message("❌ You don’t have enough currency.", ephemeral=True)

        g.pop(str(interaction.user.id), None)
        db["memberships"][str(interaction.guild.id)] = g
        _save_db(db)

        await interaction.response.send_message(f"✅ You left the academy early. Paid **{fee}**.", ephemeral=False)

    @app_commands.command(name="academy-status", description="View your current academy status.")
    @app_commands.guild_only()
    async def academy_status(self, interaction: discord.Interaction, user: Optional[discord.Member] = None):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        target = user if user else interaction.user
        if not isinstance(target, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        mrow = academy_get_membership(interaction.guild.id, target.id)
        if not mrow:
            return await interaction.response.send_message("ℹ️ Not currently in an academy.", ephemeral=True)

        days_total = int(mrow.get("days_total", 0))
        days_spent = int(mrow.get("days_spent", 0))
        left = max(0, days_total - days_spent)

        e = discord.Embed(title="🏫 Academy Status", description=f"**{target.display_name}**", color=discord.Color.blurple())
        e.add_field(name="Academy", value=str(mrow.get("academy_name", mrow.get("academy_id"))), inline=False)
        e.add_field(name="Progress", value=f"{days_spent}/{days_total} days (left: {left})", inline=True)
        e.add_field(name="Leave Early Cost", value=str(mrow.get("leave_early_cost", 0)), inline=True)
        await interaction.response.send_message(embed=e, ephemeral=False)


async def setup(bot: commands.Bot):
    await bot.add_cog(AcademyCog(bot))