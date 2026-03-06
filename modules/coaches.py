# modules/coaches.py
from __future__ import annotations

import json
import os
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import discord
from discord import app_commands
from discord.ext import commands, tasks

from modules.economy import get_balance, remove_balance
from modules.players import (
    ensure_player_for_member,
    set_player_row_by_id,  # type: ignore
    set_fatigue_for_user_id,
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


COACHES_PATH = os.path.join(_data_dir(), "coaches.json")


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
        COACHES_PATH,
        {
            "coaches": {},          # id -> row
            "trainers": {},         # id -> row
            "contracts": {},        # guild_id -> user_id -> {"type": "coach"/"trainer", "id":..., "weeks_left":..., "next_ts":...}
        },
    )


def _save_db(db: Dict[str, Any]) -> None:
    _save_json(COACHES_PATH, db)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return _utc_now().isoformat()


def _ts_now() -> int:
    return int(_utc_now().timestamp())


def _is_admin(member: discord.Member) -> bool:
    return bool(getattr(member.guild_permissions, "administrator", False))


# =========================
# Contract helpers
# =========================
def _make_id(prefix: str) -> str:
    import random
    return f"{prefix}-{random.randint(1000, 9999)}"


def _get_contract(guild_id: int, user_id: int) -> Optional[Dict[str, Any]]:
    db = _db()
    g = (db.get("contracts", {}) or {}).get(str(guild_id), {}) or {}
    row = g.get(str(user_id))
    return row if isinstance(row, dict) else None


def _set_contract(guild_id: int, user_id: int, row: Optional[Dict[str, Any]]) -> None:
    db = _db()
    g = db.setdefault("contracts", {}).setdefault(str(guild_id), {})
    if row is None:
        g.pop(str(user_id), None)
    else:
        g[str(user_id)] = row
    db["contracts"][str(guild_id)] = g
    _save_db(db)


def _add_unspent_points(guild: discord.Guild, member: discord.Member, pts: int) -> None:
    prow = ensure_player_for_member(guild, member)
    cur = int(prow.get("unspent_points", 0))
    prow["unspent_points"] = max(0, cur + int(pts))
    set_player_row_by_id(guild, member.id, prow)  # type: ignore


def _apply_stamina_boost_via_fatigue(guild: discord.Guild, member: discord.Member, boost: int) -> None:
    prow = ensure_player_for_member(guild, member)
    fatigue = int(prow.get("fatigue", 0))
    # stamina boost = reduce fatigue
    new_fatigue = max(0, fatigue - int(boost))
    set_fatigue_for_user_id(guild, member.id, new_fatigue)


# =========================
# Pagination embed
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
class CoachesCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._weekly_loop.start()

    def cog_unload(self):
        self._weekly_loop.cancel()

    @tasks.loop(minutes=5)
    async def _weekly_loop(self):
        # check all guild contracts for payouts
        db = _db()
        contracts_all = db.get("contracts", {}) or {}
        now = _ts_now()

        for guild in list(self.bot.guilds):
            gcontracts = (contracts_all.get(str(guild.id), {}) or {})
            changed = False

            for uid_str, crow in list(gcontracts.items()):
                if not isinstance(crow, dict):
                    continue
                next_ts = int(crow.get("next_ts", 0))
                if next_ts <= 0 or now < next_ts:
                    continue

                user_id = int(uid_str)
                member = guild.get_member(user_id)
                if not member:
                    # keep contract but can’t apply; push next week
                    crow["next_ts"] = int((datetime.fromtimestamp(next_ts, tz=timezone.utc) + timedelta(days=7)).timestamp())
                    gcontracts[uid_str] = crow
                    changed = True
                    continue

                ctype = str(crow.get("type"))
                item_id = str(crow.get("id"))
                weeks_left = int(crow.get("weeks_left", 0))

                # fetch item
                if ctype == "coach":
                    item = (db.get("coaches", {}) or {}).get(item_id)
                    if not isinstance(item, dict):
                        # item deleted => cancel
                        gcontracts.pop(uid_str, None)
                        changed = True
                        continue
                    wage = int(item.get("weekly_wage", 0))
                    pts = int(item.get("weekly_points", 0))
                    # pay wage now
                    if get_balance(user_id) < wage or not remove_balance(user_id, wage):
                        # can’t pay => cancel
                        gcontracts.pop(uid_str, None)
                        changed = True
                        try:
                            await member.send("⚠️ Your coach contract ended because you didn’t have enough currency to pay weekly wages.")
                        except Exception:
                            pass
                        continue
                    _add_unspent_points(guild, member, pts)

                elif ctype == "trainer":
                    item = (db.get("trainers", {}) or {}).get(item_id)
                    if not isinstance(item, dict):
                        gcontracts.pop(uid_str, None)
                        changed = True
                        continue
                    wage = int(item.get("weekly_wage", 0))
                    boost = int(item.get("weekly_stamina_boost", 0))
                    if get_balance(user_id) < wage or not remove_balance(user_id, wage):
                        gcontracts.pop(uid_str, None)
                        changed = True
                        try:
                            await member.send("⚠️ Your fitness trainer contract ended because you didn’t have enough currency to pay weekly wages.")
                        except Exception:
                            pass
                        continue
                    _apply_stamina_boost_via_fatigue(guild, member, boost)

                else:
                    continue

                # decrement weeks + schedule next
                weeks_left -= 1
                if weeks_left <= 0:
                    gcontracts.pop(uid_str, None)
                    changed = True
                    try:
                        await member.send("✅ Your contract has expired.")
                    except Exception:
                        pass
                else:
                    crow["weeks_left"] = weeks_left
                    crow["next_ts"] = int((_utc_now() + timedelta(days=7)).timestamp())
                    gcontracts[uid_str] = crow
                    changed = True

            if changed:
                contracts_all[str(guild.id)] = gcontracts
                db["contracts"] = contracts_all
                _save_db(db)

    @_weekly_loop.before_loop
    async def _before(self):
        await self.bot.wait_until_ready()

    # ---------- Admin: Coaches ----------
    @app_commands.command(name="coach-create", description="(Admin) Create a coach.")
    @app_commands.guild_only()
    async def coach_create(
        self,
        interaction: discord.Interaction,
        coach_name: str,
        weekly_wage: app_commands.Range[int, 0, 10_000_000],
        weekly_stat_points: app_commands.Range[int, 0, 10_000],
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        db = _db()
        cid = _make_id("coach")
        db.setdefault("coaches", {})[cid] = {
            "id": cid,
            "name": coach_name,
            "weekly_wage": int(weekly_wage),
            "weekly_points": int(weekly_stat_points),
            "created_at": _utc_now_iso(),
        }
        _save_db(db)
        await interaction.response.send_message(f"✅ Coach created: **{coach_name}** (ID `{cid}`).", ephemeral=False)

    @app_commands.command(name="coach-edit", description="(Admin) Edit a coach.")
    @app_commands.guild_only()
    async def coach_edit(
        self,
        interaction: discord.Interaction,
        coach_id: str,
        coach_name: Optional[str] = None,
        weekly_wage: Optional[app_commands.Range[int, 0, 10_000_000]] = None,
        weekly_stat_points: Optional[app_commands.Range[int, 0, 10_000]] = None,
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        db = _db()
        row = (db.get("coaches", {}) or {}).get(coach_id)
        if not isinstance(row, dict):
            return await interaction.response.send_message("❌ Coach not found.", ephemeral=True)

        if coach_name is not None:
            row["name"] = str(coach_name)
        if weekly_wage is not None:
            row["weekly_wage"] = int(weekly_wage)
        if weekly_stat_points is not None:
            row["weekly_points"] = int(weekly_stat_points)

        db["coaches"][coach_id] = row
        _save_db(db)
        await interaction.response.send_message(f"✅ Coach updated: `{coach_id}`.", ephemeral=False)

    @app_commands.command(name="coach-delete", description="(Admin) Delete a coach.")
    @app_commands.guild_only()
    async def coach_delete(self, interaction: discord.Interaction, coach_id: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        db = _db()
        coaches = db.get("coaches", {}) or {}
        if coach_id not in coaches:
            return await interaction.response.send_message("❌ Coach not found.", ephemeral=True)
        coaches.pop(coach_id, None)
        db["coaches"] = coaches
        _save_db(db)
        await interaction.response.send_message(f"🗑️ Deleted coach `{coach_id}`.", ephemeral=False)

    # ---------- Admin: Fitness trainers ----------
    @app_commands.command(name="fitness-trainer-create", description="(Admin) Create a fitness trainer.")
    @app_commands.guild_only()
    async def trainer_create(
        self,
        interaction: discord.Interaction,
        trainer_name: str,
        weekly_wage: app_commands.Range[int, 0, 10_000_000],
        weekly_stamina_boost: app_commands.Range[int, 0, 100],
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        db = _db()
        tid = _make_id("trainer")
        db.setdefault("trainers", {})[tid] = {
            "id": tid,
            "name": trainer_name,
            "weekly_wage": int(weekly_wage),
            "weekly_stamina_boost": int(weekly_stamina_boost),
            "created_at": _utc_now_iso(),
        }
        _save_db(db)
        await interaction.response.send_message(f"✅ Fitness trainer created: **{trainer_name}** (ID `{tid}`).", ephemeral=False)

    @app_commands.command(name="fitness-trainer-edit", description="(Admin) Edit a fitness trainer.")
    @app_commands.guild_only()
    async def trainer_edit(
        self,
        interaction: discord.Interaction,
        trainer_id: str,
        trainer_name: Optional[str] = None,
        weekly_wage: Optional[app_commands.Range[int, 0, 10_000_000]] = None,
        weekly_stamina_boost: Optional[app_commands.Range[int, 0, 100]] = None,
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        db = _db()
        row = (db.get("trainers", {}) or {}).get(trainer_id)
        if not isinstance(row, dict):
            return await interaction.response.send_message("❌ Fitness trainer not found.", ephemeral=True)

        if trainer_name is not None:
            row["name"] = str(trainer_name)
        if weekly_wage is not None:
            row["weekly_wage"] = int(weekly_wage)
        if weekly_stamina_boost is not None:
            row["weekly_stamina_boost"] = int(weekly_stamina_boost)

        db["trainers"][trainer_id] = row
        _save_db(db)
        await interaction.response.send_message(f"✅ Fitness trainer updated: `{trainer_id}`.", ephemeral=False)

    @app_commands.command(name="fitness-trainer-delete", description="(Admin) Delete a fitness trainer.")
    @app_commands.guild_only()
    async def trainer_delete(self, interaction: discord.Interaction, trainer_id: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        db = _db()
        trainers = db.get("trainers", {}) or {}
        if trainer_id not in trainers:
            return await interaction.response.send_message("❌ Fitness trainer not found.", ephemeral=True)
        trainers.pop(trainer_id, None)
        db["trainers"] = trainers
        _save_db(db)
        await interaction.response.send_message(f"🗑️ Deleted fitness trainer `{trainer_id}`.", ephemeral=False)

    # ---------- Player: lists ----------
    @app_commands.command(name="coach-list", description="View all coaches.")
    @app_commands.guild_only()
    async def coach_list(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        db = _db()
        coaches = list((db.get("coaches", {}) or {}).values())
        coaches = [c for c in coaches if isinstance(c, dict)]
        if not coaches:
            return await interaction.response.send_message("ℹ️ No coaches available yet.", ephemeral=True)

        coaches.sort(key=lambda r: str(r.get("name", "")).lower())
        pages: List[discord.Embed] = []
        chunk = 8
        for i in range(0, len(coaches), chunk):
            sub = coaches[i:i + chunk]
            e = discord.Embed(title="🧑‍🏫 Coaches", color=discord.Color.blurple())
            for row in sub:
                e.add_field(
                    name=f"{row.get('name')} — `{row.get('id')}`",
                    value=f"Wage/week: **{row.get('weekly_wage')}**\nStat points/week: **{row.get('weekly_points')}**",
                    inline=False,
                )
            e.set_footer(text=f"Page {len(pages)+1}/{(len(coaches)+chunk-1)//chunk}")
            pages.append(e)

        view = SimplePager(pages, author_id=interaction.user.id)
        await interaction.response.send_message(embed=pages[0], view=view, ephemeral=False)

    @app_commands.command(name="fitness-trainer-list", description="View all fitness trainers.")
    @app_commands.guild_only()
    async def trainer_list(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        db = _db()
        trainers = list((db.get("trainers", {}) or {}).values())
        trainers = [t for t in trainers if isinstance(t, dict)]
        if not trainers:
            return await interaction.response.send_message("ℹ️ No fitness trainers available yet.", ephemeral=True)

        trainers.sort(key=lambda r: str(r.get("name", "")).lower())
        pages: List[discord.Embed] = []
        chunk = 8
        for i in range(0, len(trainers), chunk):
            sub = trainers[i:i + chunk]
            e = discord.Embed(title="🏃 Fitness Trainers", color=discord.Color.blurple())
            for row in sub:
                e.add_field(
                    name=f"{row.get('name')} — `{row.get('id')}`",
                    value=f"Wage/week: **{row.get('weekly_wage')}**\nStamina boost/week: **{row.get('weekly_stamina_boost')}**",
                    inline=False,
                )
            e.set_footer(text=f"Page {len(pages)+1}/{(len(trainers)+chunk-1)//chunk}")
            pages.append(e)

        view = SimplePager(pages, author_id=interaction.user.id)
        await interaction.response.send_message(embed=pages[0], view=view, ephemeral=False)

    # ---------- Player: buy ----------
    @app_commands.command(name="coach-buy", description="Hire a coach for X weeks (pays wages weekly).")
    @app_commands.guild_only()
    async def coach_buy(self, interaction: discord.Interaction, coach_id: str, contract_weeks: app_commands.Range[int, 1, 520]):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        # only one active coach contract at a time
        existing = _get_contract(interaction.guild.id, interaction.user.id)
        if existing and str(existing.get("type")) == "coach":
            return await interaction.response.send_message("❌ You already have a coach contract.", ephemeral=True)

        db = _db()
        coach = (db.get("coaches", {}) or {}).get(coach_id)
        if not isinstance(coach, dict):
            return await interaction.response.send_message("❌ Coach not found.", ephemeral=True)

        wage = int(coach.get("weekly_wage", 0))
        total_needed = wage * int(contract_weeks)
        if get_balance(interaction.user.id) < total_needed:
            max_weeks = 0 if wage <= 0 else (get_balance(interaction.user.id) // wage)
            return await interaction.response.send_message(
                f"❌ You need **{total_needed}** currency for {contract_weeks} weeks.\n"
                f"Max you can afford with this coach: **{max_weeks}** weeks.",
                ephemeral=True,
            )

        # contract starts now, first wage paid after 7 days (per spec)
        row = {
            "type": "coach",
            "id": coach_id,
            "weeks_left": int(contract_weeks),
            "next_ts": int((_utc_now() + timedelta(days=7)).timestamp()),
            "started_at": _utc_now_iso(),
        }
        _set_contract(interaction.guild.id, interaction.user.id, row)

        await interaction.response.send_message(
            f"✅ You hired **{coach.get('name')}** for **{contract_weeks}** weeks.\n"
            f"First payout + wage occurs in **7 days**.",
            ephemeral=False,
        )

    @app_commands.command(name="fitness-trainer-buy", description="Hire a fitness trainer for X weeks (pays wages weekly).")
    @app_commands.guild_only()
    async def trainer_buy(self, interaction: discord.Interaction, trainer_id: str, contract_weeks: app_commands.Range[int, 1, 520]):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        existing = _get_contract(interaction.guild.id, interaction.user.id)
        if existing and str(existing.get("type")) == "trainer":
            return await interaction.response.send_message("❌ You already have a fitness trainer contract.", ephemeral=True)

        db = _db()
        trainer = (db.get("trainers", {}) or {}).get(trainer_id)
        if not isinstance(trainer, dict):
            return await interaction.response.send_message("❌ Fitness trainer not found.", ephemeral=True)

        wage = int(trainer.get("weekly_wage", 0))
        total_needed = wage * int(contract_weeks)
        if get_balance(interaction.user.id) < total_needed:
            max_weeks = 0 if wage <= 0 else (get_balance(interaction.user.id) // wage)
            return await interaction.response.send_message(
                f"❌ You need **{total_needed}** currency for {contract_weeks} weeks.\n"
                f"Max you can afford with this trainer: **{max_weeks}** weeks.",
                ephemeral=True,
            )

        row = {
            "type": "trainer",
            "id": trainer_id,
            "weeks_left": int(contract_weeks),
            "next_ts": int((_utc_now() + timedelta(days=7)).timestamp()),
            "started_at": _utc_now_iso(),
        }
        _set_contract(interaction.guild.id, interaction.user.id, row)

        await interaction.response.send_message(
            f"✅ You hired **{trainer.get('name')}** for **{contract_weeks}** weeks.\n"
            f"First stamina boost + wage occurs in **7 days**.",
            ephemeral=False,
        )

    # ---------- Player: my contracts ----------
    @app_commands.command(name="my-coach", description="View your current coach contract.")
    @app_commands.guild_only()
    async def my_coach(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        row = _get_contract(interaction.guild.id, interaction.user.id)
        if not row or str(row.get("type")) != "coach":
            return await interaction.response.send_message("ℹ️ No active coach contract.", ephemeral=True)

        db = _db()
        coach = (db.get("coaches", {}) or {}).get(str(row.get("id")))
        name = coach.get("name") if isinstance(coach, dict) else str(row.get("id"))
        await interaction.response.send_message(
            f"🧑‍🏫 Coach: **{name}**\nWeeks left: **{row.get('weeks_left')}**\nNext payout: <t:{int(row.get('next_ts'))}:R>",
            ephemeral=False,
        )

    @app_commands.command(name="my-fitness-trainer", description="View your current fitness trainer contract.")
    @app_commands.guild_only()
    async def my_trainer(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        row = _get_contract(interaction.guild.id, interaction.user.id)
        if not row or str(row.get("type")) != "trainer":
            return await interaction.response.send_message("ℹ️ No active fitness trainer contract.", ephemeral=True)

        db = _db()
        trainer = (db.get("trainers", {}) or {}).get(str(row.get("id")))
        name = trainer.get("name") if isinstance(trainer, dict) else str(row.get("id"))
        await interaction.response.send_message(
            f"🏃 Trainer: **{name}**\nWeeks left: **{row.get('weeks_left')}**\nNext payout: <t:{int(row.get('next_ts'))}:R>",
            ephemeral=False,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(CoachesCog(bot))