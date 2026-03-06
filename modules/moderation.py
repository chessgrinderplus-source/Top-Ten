import discord
from discord.ext import commands
from discord import app_commands
from datetime import timedelta

import config
from utils import load_json, save_json, now_ts


def _is_admin(member: discord.Member) -> bool:
    return member.guild_permissions.administrator


def _settings_path():
    return f"{config.DATA_DIR}/settings.json"


def get_settings():
    return load_json(_settings_path(), {"modlog_channel_id": None})


def set_modlog_channel_id(channel_id: int | None):
    s = get_settings()
    s["modlog_channel_id"] = channel_id
    save_json(_settings_path(), s)


async def log_mod_action(guild: discord.Guild, text: str):
    s = get_settings()
    cid = s.get("modlog_channel_id")
    if not cid:
        return
    ch = guild.get_channel(int(cid))
    if not ch:
        return
    try:
        await ch.send(text)
    except Exception:
        pass


class ModerationCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="set-modlog-channel", description="Set the mod-logs channel by channel ID.")
    async def set_modlog_channel(self, interaction: discord.Interaction, channel_id: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        if not channel_id.isdigit():
            return await interaction.response.send_message("❌ Invalid channel ID.", ephemeral=True)

        cid = int(channel_id)
        channel = interaction.guild.get_channel(cid) if interaction.guild else None
        if channel is None:
            return await interaction.response.send_message("❌ That channel ID is not in this server.", ephemeral=True)

        set_modlog_channel_id(cid)
        await interaction.response.send_message(f"✅ Mod log channel set to <#{cid}>.")
        await log_mod_action(interaction.guild, f"🛠️ {interaction.user.mention} set mod log channel to <#{cid}>.")

    @app_commands.command(name="com-timeout", description="Disable a user from using bot commands.")
    async def com_timeout(self, interaction: discord.Interaction, user: discord.Member, minutes: int | None = None):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        data = load_json(config.COMMAND_TIMEOUT_FILE, {})
        g = data.setdefault(str(interaction.guild.id), {})

        if minutes is None:
            g[str(user.id)] = {"until": 0}
            save_json(config.COMMAND_TIMEOUT_FILE, data)
            await interaction.response.send_message(f"✅ {user.mention} can no longer use bot commands.")
            await log_mod_action(interaction.guild, f"🔒 {interaction.user.mention} used /com-timeout on {user.mention} (indefinite)")
            return

        minutes = max(1, min(40320, int(minutes)))
        until_ts = now_ts() + minutes * 60
        g[str(user.id)] = {"until": until_ts}
        save_json(config.COMMAND_TIMEOUT_FILE, data)

        await interaction.response.send_message(f"✅ {user.mention} blocked from bot commands for {minutes} minute(s).")
        await log_mod_action(interaction.guild, f"🔒 {interaction.user.mention} used /com-timeout on {user.mention} ({minutes}m)")

    @app_commands.command(name="com-timeout-remove", description="Re-enable a user to use bot commands.")
    async def com_timeout_remove(self, interaction: discord.Interaction, user: discord.Member):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        data = load_json(config.COMMAND_TIMEOUT_FILE, {})
        g = data.get(str(interaction.guild.id), {})
        if str(user.id) in g:
            del g[str(user.id)]
            save_json(config.COMMAND_TIMEOUT_FILE, data)

        await interaction.response.send_message(f"✅ {user.mention} can use bot commands again.")
        await log_mod_action(interaction.guild, f"🔓 {interaction.user.mention} used /com-timeout-remove on {user.mention}")

    @app_commands.command(name="kick", description="Kick a user from the server.")
    async def kick(self, interaction: discord.Interaction, user: discord.Member, reason: str = "No reason provided"):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        try:
            await user.kick(reason=reason)
            await interaction.response.send_message(f"✅ Kicked {user.mention}. Reason: {reason}")
            await log_mod_action(interaction.guild, f"👢 {interaction.user.mention} kicked {user.mention}. Reason: {reason}")
        except discord.Forbidden:
            await interaction.response.send_message("❌ I can’t kick that user (permissions/role hierarchy).")
        except Exception as e:
            await interaction.response.send_message(f"❌ Failed: {repr(e)}")

    @app_commands.command(name="ban", description="Ban a user from the server.")
    async def ban(self, interaction: discord.Interaction, user: discord.Member, reason: str = "No reason provided", delete_message_days: int = 0):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        delete_message_days = max(0, min(7, int(delete_message_days)))
        try:
            await user.ban(reason=reason, delete_message_days=delete_message_days)
            await interaction.response.send_message(f"✅ Banned {user.mention}. Reason: {reason}")
            await log_mod_action(interaction.guild, f"⛔ {interaction.user.mention} banned {user.mention}. Reason: {reason}")
        except discord.Forbidden:
            await interaction.response.send_message("❌ I can’t ban that user (permissions/role hierarchy).")
        except Exception as e:
            await interaction.response.send_message(f"❌ Failed: {repr(e)}")

    @app_commands.command(name="timeout", description="Timeout a user (mute) for N minutes.")
    async def timeout(self, interaction: discord.Interaction, user: discord.Member, minutes: int, reason: str = "No reason provided"):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        minutes = max(1, min(40320, int(minutes)))
        until = discord.utils.utcnow() + timedelta(minutes=minutes)
        try:
            await user.timeout(until, reason=reason)
            await interaction.response.send_message(f"✅ Timed out {user.mention} for {minutes} minute(s). Reason: {reason}")
            await log_mod_action(interaction.guild, f"🔇 {interaction.user.mention} timed out {user.mention} for {minutes}m. Reason: {reason}")
        except discord.Forbidden:
            await interaction.response.send_message("❌ I can’t timeout that user (permissions/role hierarchy).")
        except Exception as e:
            await interaction.response.send_message(f"❌ Failed: {repr(e)}")

    @app_commands.command(name="untimeout", description="Remove a user's timeout.")
    async def untimeout(self, interaction: discord.Interaction, user: discord.Member, reason: str = "No reason provided"):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        try:
            await user.timeout(None, reason=reason)
            await interaction.response.send_message(f"✅ Timeout removed for {user.mention}. Reason: {reason}")
            await log_mod_action(interaction.guild, f"🔊 {interaction.user.mention} removed timeout for {user.mention}. Reason: {reason}")
        except discord.Forbidden:
            await interaction.response.send_message("❌ I can’t remove timeout (permissions/role hierarchy).")
        except Exception as e:
            await interaction.response.send_message(f"❌ Failed: {repr(e)}")

    @app_commands.command(name="purge", description="Delete N recent messages in this channel.")
    async def purge(self, interaction: discord.Interaction, amount: int):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        amount = max(1, min(100, int(amount)))
        channel = interaction.channel
        if not isinstance(channel, discord.TextChannel):
            return await interaction.response.send_message("❌ Purge can only be used in text channels.")

        await interaction.response.defer()  # PUBLIC, but gives us time
        try:
            deleted = await channel.purge(limit=amount)
            await interaction.followup.send(f"✅ Deleted {len(deleted)} message(s).")
            await log_mod_action(interaction.guild, f"🧹 {interaction.user.mention} purged {len(deleted)} messages in {channel.mention}.")
        except discord.Forbidden:
            await interaction.followup.send("❌ I can’t purge here (need Manage Messages).")
        except Exception as e:
            await interaction.followup.send(f"❌ Failed: {repr(e)}")


async def setup(bot: commands.Bot):
    await bot.add_cog(ModerationCog(bot))

