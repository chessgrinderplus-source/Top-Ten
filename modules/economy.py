import discord
from discord.ext import commands, tasks
from discord import app_commands
from datetime import datetime, timedelta
import zoneinfo

import config
from utils import load_json, save_json, ensure_dir

TORONTO = zoneinfo.ZoneInfo("America/Toronto")


def _is_admin(member: discord.Member) -> bool:
    return member.guild_permissions.administrator


def _econ_path():
    return config.ECONOMY_FILE


def _load():
    ensure_dir(config.DATA_DIR)
    data = load_json(_econ_path(), {})
    data.setdefault("balances", {})        # {"user_id": int}
    data.setdefault("role_rewards", {})    # {"role_id": coins_per_week}
    data.setdefault("last_weekly_ts", 0)   # unix ts
    return data


def _save(data):
    save_json(_econ_path(), data)


def get_balance(user_id: int) -> int:
    data = _load()
    return int(data["balances"].get(str(user_id), 0))


def add_balance(user_id: int, amount: int):
    data = _load()
    cur = int(data["balances"].get(str(user_id), 0))
    data["balances"][str(user_id)] = int(max(0, cur + int(amount)))
    _save(data)


def remove_balance(user_id: int, amount: int) -> bool:
    data = _load()
    cur = int(data["balances"].get(str(user_id), 0))
    amount = int(amount)
    if cur < amount:
        return False
    data["balances"][str(user_id)] = int(cur - amount)
    _save(data)
    return True


def _chunk_pages(lines: list[str], max_chars: int = 3500) -> list[str]:
    pages = []
    cur = ""
    for ln in lines:
        add = ln + "\n"
        if len(cur) + len(add) > max_chars:
            pages.append(cur.rstrip())
            cur = ""
        cur += add
    if cur.strip():
        pages.append(cur.rstrip())
    return pages or ["(empty)"]


class PagerView(discord.ui.View):
    def __init__(self, pages: list[str], user_id: int, title: str):
        super().__init__(timeout=180)
        self.pages = pages
        self.user_id = user_id
        self.i = 0
        self.title = title
        self._locked = False

    def _embed(self) -> discord.Embed:
        e = discord.Embed(title=self.title, description=self.pages[self.i])
        e.set_footer(text=f"Page {self.i+1}/{len(self.pages)}")
        return e

    async def _edit(self, interaction: discord.Interaction):
        try:
            if not interaction.response.is_done():
                await interaction.response.edit_message(embed=self._embed(), view=self)
            else:
                await interaction.edit_original_response(embed=self._embed(), view=self)
        except Exception:
            pass

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
    async def prev(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ This menu isn’t for you.", ephemeral=True)
        if self._locked:
            return
        self._locked = True
        try:
            self.i = (self.i - 1) % len(self.pages)
            await self._edit(interaction)
        finally:
            self._locked = False

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ This menu isn’t for you.", ephemeral=True)
        if self._locked:
            return
        self._locked = True
        try:
            self.i = (self.i + 1) % len(self.pages)
            await self._edit(interaction)
        finally:
            self._locked = False


def _next_weekly_run_dt(now: datetime | None = None) -> datetime:
    now = now or datetime.now(TORONTO)
    days_ahead = (0 - now.weekday()) % 7  # Monday=0
    candidate = now.replace(hour=9, minute=0, second=0, microsecond=0) + timedelta(days=days_ahead)
    if candidate <= now:
        candidate += timedelta(days=7)
    return candidate


async def _ensure_members_loaded(guild: discord.Guild):
    try:
        await guild.chunk(cache=True)
    except Exception:
        pass


async def _iter_members(guild: discord.Guild):
    if guild.members:
        for m in guild.members:
            yield m
        return

    await _ensure_members_loaded(guild)
    if guild.members:
        for m in guild.members:
            yield m
        return

    try:
        async for m in guild.fetch_members(limit=None):
            yield m
    except Exception:
        return


async def _run_weekly_payout(bot: commands.Bot, guild: discord.Guild, only_role_id: int | None = None) -> dict:
    data = _load()
    rr = data.get("role_rewards", {})
    if not rr:
        return {"roles": 0, "users_paid": 0, "total_paid": 0, "warning": None}

    role_rewards: dict[int, int] = {}
    for role_id_str, weekly in rr.items():
        try:
            rid = int(role_id_str)
            weekly = int(weekly)
        except Exception:
            continue
        if weekly > 0:
            role_rewards[rid] = weekly

    if not role_rewards:
        return {"roles": 0, "users_paid": 0, "total_paid": 0, "warning": None}

    if only_role_id is not None:
        only_role_id = int(only_role_id)
        if only_role_id not in role_rewards:
            return {"roles": 0, "users_paid": 0, "total_paid": 0, "warning": None, "missing_role_reward": True}
        role_rewards = {only_role_id: role_rewards[only_role_id]}

    users_paid = 0
    total_paid = 0

    async for member in _iter_members(guild):
        if member.bot:
            continue

        total_for_member = 0
        for r in member.roles:
            weekly = role_rewards.get(r.id)
            if weekly:
                total_for_member += weekly

        if total_for_member > 0:
            add_balance(member.id, total_for_member)
            users_paid += 1
            total_paid += total_for_member

    warning = None
    if not bot.intents.members:
        warning = "⚠️ This bot is running with `intents.members=False` in code."

    return {"roles": len(role_rewards), "users_paid": users_paid, "total_paid": total_paid, "warning": warning}


class EconomyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.weekly_payout.start()

    # PUBLIC
    @app_commands.command(name="balance", description="View a user's coin balance.")
    @app_commands.describe(target="Optional: user to view (default: you)")
    async def balance(self, interaction: discord.Interaction, target: discord.User | None = None):
        who = target or interaction.user
        bal = get_balance(who.id)
        await interaction.response.send_message(f"💰 **{who.display_name}** has **{bal}** coins.")

    # ADMIN (PUBLIC OUTPUT)
    @app_commands.command(name="admin-add", description="Admin: add coins to a user.")
    async def admin_add(self, interaction: discord.Interaction, user: discord.Member, amount: int):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        amount = int(amount)
        if amount <= 0:
            return await interaction.response.send_message("❌ Amount must be > 0.", ephemeral=True)

        add_balance(user.id, amount)
        await interaction.response.send_message(f"✅ Added **{amount}** coins to {user.mention}.")

    @app_commands.command(name="admin-remove", description="Admin: remove coins from a user.")
    async def admin_remove(self, interaction: discord.Interaction, user: discord.Member, amount: int):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        amount = int(amount)
        if amount <= 0:
            return await interaction.response.send_message("❌ Amount must be > 0.", ephemeral=True)

        ok = remove_balance(user.id, amount)
        if not ok:
            return await interaction.response.send_message("❌ That user doesn’t have enough coins.", ephemeral=True)

        await interaction.response.send_message(f"✅ Removed **{amount}** coins from {user.mention}.")

    # REWARDS (PUBLIC OUTPUT)
    @app_commands.command(name="reward-create", description="Admin: create a weekly reward payout for a role.")
    async def reward_create(self, interaction: discord.Interaction, role: discord.Role, weekly: int):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        weekly = int(weekly)
        if weekly <= 0:
            return await interaction.response.send_message("❌ weekly must be > 0.", ephemeral=True)

        data = _load()
        data["role_rewards"][str(role.id)] = weekly
        _save(data)

        await interaction.response.send_message(f"✅ Reward created: **{role.name}** → **{weekly} coins/week**")

    @app_commands.command(name="reward-edit", description="Admin: edit a weekly reward payout for a role.")
    async def reward_edit(self, interaction: discord.Interaction, role: discord.Role, weekly: int):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        weekly = int(weekly)
        if weekly <= 0:
            return await interaction.response.send_message("❌ weekly must be > 0.", ephemeral=True)

        data = _load()
        if str(role.id) not in data["role_rewards"]:
            return await interaction.response.send_message("❌ No reward exists for that role. Use /reward-create.", ephemeral=True)

        data["role_rewards"][str(role.id)] = weekly
        _save(data)

        await interaction.response.send_message(f"✅ Reward updated: **{role.name}** → **{weekly} coins/week**")

    @app_commands.command(name="reward-delete", description="Admin: delete a weekly reward payout for a role.")
    async def reward_delete(self, interaction: discord.Interaction, role: discord.Role):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        data = _load()
        if str(role.id) not in data["role_rewards"]:
            return await interaction.response.send_message("❌ No reward existed for that role.", ephemeral=True)

        del data["role_rewards"][str(role.id)]
        _save(data)

        await interaction.response.send_message(f"✅ Reward deleted for role **{role.name}**.")

    @app_commands.command(name="reward-view", description="View all weekly rewards (paginated).")
    async def reward_view(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Use this in the server.", ephemeral=True)

        data = _load()
        rr = data.get("role_rewards", {})
        if not rr:
            return await interaction.response.send_message("ℹ️ No weekly rewards set.")

        lines = []
        for role_id_str, weekly in rr.items():
            role = interaction.guild.get_role(int(role_id_str)) if role_id_str.isdigit() else None
            role_name = role.name if role else f"(missing role {role_id_str})"
            lines.append(f"- **{role_name}** → **{int(weekly)}** / week")

        lines.sort(key=lambda s: s.lower())
        pages = _chunk_pages(lines)

        view = PagerView(pages, interaction.user.id, "Weekly Role Rewards")
        await interaction.response.send_message(embed=view._embed(), view=view)

    @app_commands.command(name="reward-run-now", description="Admin: run weekly rewards immediately (test).")
    @app_commands.describe(role="Optional: run payout for ONE role reward only")
    async def reward_run_now(self, interaction: discord.Interaction, role: discord.Role | None = None):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        if not interaction.guild:
            return await interaction.response.send_message("❌ Use this in the server.", ephemeral=True)

        next_dt = _next_weekly_run_dt()
        next_ts = int(next_dt.timestamp())
        next_when = f"Next scheduled run: **{next_dt.strftime('%A')}** <t:{next_ts}:F> (<t:{next_ts}:R>)"

        # ACK FAST (PUBLIC)
        await interaction.response.send_message(
            (f"⏳ Running payout for **{role.name}**...\n" if role else "⏳ Running payout for **ALL** reward roles...\n")
            + next_when
        )

        summary = await _run_weekly_payout(self.bot, interaction.guild, only_role_id=(role.id if role else None))
        if role and summary.get("missing_role_reward"):
            return await interaction.edit_original_response(content=f"❌ No weekly reward configured for **{role.name}**.\n{next_when}")

        data = _load()
        data["last_weekly_ts"] = int(datetime.now(TORONTO).timestamp())
        _save(data)

        msg = (
            f"✅ Rewards paid.\n"
            f"Rewards processed: **{summary['roles']}**\n"
            f"Users paid: **{summary['users_paid']}**\n"
            f"Total paid: **{summary['total_paid']}**\n\n"
            f"{next_when}"
        )
        if summary.get("warning"):
            msg += f"\n\n{summary['warning']}"

        await interaction.edit_original_response(content=msg)

    # Weekly payout task (silent)
    @tasks.loop(minutes=5)
    async def weekly_payout(self):
        now = datetime.now(TORONTO)
        data = _load()

        last_ts = int(data.get("last_weekly_ts", 0))
        last = datetime.fromtimestamp(last_ts, TORONTO) if last_ts else None

        is_monday = now.weekday() == 0
        in_window = (now.hour == 9 and 0 <= now.minute <= 4)
        if not (is_monday and in_window):
            return

        if last and (now - last) < timedelta(days=6):
            return

        guild = self.bot.get_guild(int(config.HOME_GUILD_ID))
        if not guild:
            return

        await _run_weekly_payout(self.bot, guild)

        data["last_weekly_ts"] = int(now.timestamp())
        _save(data)

    @weekly_payout.before_loop
    async def before_weekly_payout(self):
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    await bot.add_cog(EconomyCog(bot))
