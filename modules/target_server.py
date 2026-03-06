import discord
from discord.ext import commands, tasks
from discord import app_commands

import config
from utils import load_json, save_json


def _is_admin(member: discord.Member) -> bool:
    return member.guild_permissions.administrator


class TargetServerCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.membership_task.start()

    def _path(self):
        return config.TARGET_SERVER_FILE

    def _load(self):
        return load_json(self._path(), {})

    def _save(self, data):
        save_json(self._path(), data)

    def _get_settings(self, home_guild_id: int):
        data = self._load()
        return data.get(str(home_guild_id))

    def _set_settings(self, home_guild_id: int, target_guild_id: int, role_id: int):
        data = self._load()
        data[str(home_guild_id)] = {"target_guild_id": target_guild_id, "role_id": role_id}
        self._save(data)

    def _delete_settings(self, home_guild_id: int):
        data = self._load()
        if str(home_guild_id) in data:
            del data[str(home_guild_id)]
            self._save(data)

    @app_commands.command(name="set-target-server", description="Link a target server and role.")
    async def set_target_server(self, interaction: discord.Interaction, target_server: str, role: discord.Role):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        if not target_server.isdigit():
            return await interaction.response.send_message("❌ target_server must be a Server ID.", ephemeral=True)

        target_guild_id = int(target_server)
        target_guild = self.bot.get_guild(target_guild_id)
        if target_guild is None:
            return await interaction.response.send_message("❌ I’m not in that target server.", ephemeral=True)

        self._set_settings(interaction.guild.id, target_guild_id, role.id)
        await interaction.response.send_message("✅ Target server linked.")

    @app_commands.command(name="delete-target-server", description="Delete the target server link and role settings.")
    async def delete_target_server(self, interaction: discord.Interaction, confirm: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        if confirm.lower() not in ("yes", "y"):
            return await interaction.response.send_message("❌ Cancelled.", ephemeral=True)

        self._delete_settings(interaction.guild.id)
        await interaction.response.send_message("✅ Target server link deleted.")

    @app_commands.command(name="check-membership", description="Check if a user is in the target server.")
    async def check_membership(self, interaction: discord.Interaction, user: discord.Member):
        settings = self._get_settings(interaction.guild.id)
        if not settings:
            return await interaction.response.send_message("❌ No target server is set.", ephemeral=True)

        target_guild = self.bot.get_guild(int(settings["target_guild_id"]))
        if not target_guild:
            return await interaction.response.send_message("❌ Target server not found.", ephemeral=True)

        member = target_guild.get_member(user.id)
        if member:
            await interaction.response.send_message(f"✅ {user.mention} is a member of the target server.")
        else:
            await interaction.response.send_message(f"❌ {user.mention} is NOT a member of the target server.")

    @tasks.loop(minutes=getattr(config, "MEMBERSHIP_CHECK_MINUTES", 5))
    async def membership_task(self):
        home_id = config.HOME_GUILD_ID
        if not home_id:
            return

        home_guild = self.bot.get_guild(int(home_id))
        if home_guild is None:
            return

        settings = self._get_settings(home_guild.id)
        if not settings:
            return

        target_guild = self.bot.get_guild(int(settings["target_guild_id"]))
        if target_guild is None:
            return

        role = home_guild.get_role(int(settings["role_id"]))
        if role is None:
            return

        for member in home_guild.members:
            if member.bot:
                continue

            in_target = target_guild.get_member(member.id) is not None
            has_role = role in member.roles

            try:
                if in_target and not has_role:
                    await member.add_roles(role, reason="Member is in target server")
                elif (not in_target) and has_role:
                    await member.remove_roles(role, reason="Member is not in target server")
            except Exception:
                pass

    @membership_task.before_loop
    async def before_membership_task(self):
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    await bot.add_cog(TargetServerCog(bot))
