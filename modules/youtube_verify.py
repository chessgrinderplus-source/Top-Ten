import re
import discord
from discord.ext import commands, tasks
from discord import app_commands

import config
from utils import load_json, save_json

YT_RE = re.compile(r"(?:youtube\.com/(?:channel/|@|c/)|youtu\.be/)([A-Za-z0-9_\-@]+)")


def _is_admin(member: discord.Member) -> bool:
    return member.guild_permissions.administrator


def extract_channel_id(link: str) -> str | None:
    link = link.strip()
    m = YT_RE.search(link)
    if not m:
        return None
    return m.group(1)


async def yt_api_is_subscribed(api_key: str, session, user_channel_id: str, target_channel_id: str) -> bool:
    url = "https://www.googleapis.com/youtube/v3/subscriptions"
    params = {
        "part": "snippet",
        "channelId": user_channel_id,
        "forChannelId": target_channel_id,
        "maxResults": 1,
        "key": api_key
    }
    async with session.get(url, params=params) as resp:
        if resp.status in (403, 429):
            raise RuntimeError("rate_limited")
        if resp.status != 200:
            raise RuntimeError(f"bad_status_{resp.status}")
        data = await resp.json()
        return len(data.get("items", [])) > 0


class ChannelSelect(discord.ui.Select):
    def __init__(self, parent, options):
        super().__init__(placeholder="Select a YouTube channel...", min_values=1, max_values=1, options=options)
        self.parent_view = parent

    async def callback(self, interaction: discord.Interaction):
        await self.parent_view.handle_selection(interaction, self.values[0])


class ChannelSelectView(discord.ui.View):
    def __init__(self, cog, user_id: int, options):
        super().__init__(timeout=120)
        self.cog = cog
        self.user_id = user_id
        self.add_item(ChannelSelect(self, options))

    async def handle_selection(self, interaction: discord.Interaction, title: str):
        await self.cog.verify_selected(interaction, self.user_id, title)


class YouTubeVerifyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.recheck_task.start()

    def _home_only(self, interaction: discord.Interaction) -> bool:
        return interaction.guild is not None and interaction.guild.id == config.HOME_GUILD_ID

    def _load(self):
        return load_json(config.YOUTUBE_FILE, {"channels": [], "users": {}, "verified": []})

    def _save(self, data):
        save_json(config.YOUTUBE_FILE, data)

    @app_commands.command(name="create-channel", description="Admin: add a YouTube channel to verify against.")
    async def create_channel(self, interaction: discord.Interaction, title: str, youtube_link: str, role: discord.Role, api_key: str):
        if not self._home_only(interaction):
            return await interaction.response.send_message("❌ Disabled in this server.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        channel_id = extract_channel_id(youtube_link)
        if not channel_id:
            return await interaction.response.send_message("❌ Invalid YouTube link.", ephemeral=True)

        api_key = api_key.strip()
        if not api_key:
            return await interaction.response.send_message("❌ API key required.", ephemeral=True)

        data = self._load()
        data["channels"] = [c for c in data["channels"] if c["title"] != title]
        data["channels"].append({
            "title": title,
            "channel_id": channel_id,
            "role_id": role.id,
            "api_key": api_key
        })
        self._save(data)

        await interaction.response.send_message("✅ Channel added.")

    @app_commands.command(name="yt-login", description="Link your own YouTube channel.")
    async def yt_login(self, interaction: discord.Interaction, youtube_link: str):
        if not self._home_only(interaction):
            return await interaction.response.send_message("❌ Disabled in this server.", ephemeral=True)

        channel_id = extract_channel_id(youtube_link)
        if not channel_id:
            return await interaction.response.send_message("❌ Invalid YouTube link.", ephemeral=True)

        data = self._load()
        data.setdefault("users", {})[str(interaction.user.id)] = {"channel_id": channel_id}
        self._save(data)

        await interaction.response.send_message("✅ Saved. ⚠️ Your subscriptions must be public for verification to work.")

    @app_commands.command(name="verify-sub", description="Verify your YouTube subscription.")
    async def verify_sub(self, interaction: discord.Interaction):
        if not self._home_only(interaction):
            return await interaction.response.send_message("❌ Disabled in this server.", ephemeral=True)

        data = self._load()
        if not data["channels"]:
            return await interaction.response.send_message("ℹ️ No channels added yet.", ephemeral=True)

        if str(interaction.user.id) not in data.get("users", {}):
            return await interaction.response.send_message("❌ Use /yt-login first.", ephemeral=True)

        options = [discord.SelectOption(label=c["title"], value=c["title"]) for c in data["channels"]]
        view = ChannelSelectView(self, interaction.user.id, options)
        await interaction.response.send_message("Select the channel you’re subscribed to:", view=view, ephemeral=True)

    async def verify_selected(self, interaction: discord.Interaction, user_id: int, title: str):
        if interaction.user.id != user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)

        data = self._load()
        user_channel_id = data["users"][str(user_id)]["channel_id"]
        entry = next(c for c in data["channels"] if c["title"] == title)

        import aiohttp
        ok = False
        async with aiohttp.ClientSession() as session:
            try:
                ok = await yt_api_is_subscribed(entry["api_key"], session, user_channel_id, entry["channel_id"])
            except Exception:
                ok = False

        if not ok:
            return await interaction.response.send_message("❌ Couldn’t confirm subscription (make subs public).", ephemeral=True)

        role = interaction.guild.get_role(entry["role_id"])
        if role:
            await interaction.user.add_roles(role, reason="YouTube verified")

        data.setdefault("verified", []).append({
            "discord_id": user_id,
            "user_channel_id": user_channel_id,
            "target_channel_id": entry["channel_id"],
            "role_id": entry["role_id"],
            "title": title
        })
        self._save(data)

        await interaction.response.send_message(f"✅ Verified! You got the **{role.name if role else 'role'}** role.", ephemeral=True)

    @tasks.loop(minutes=30)
    async def recheck_task(self):
        data = self._load()
        home = self.bot.get_guild(config.HOME_GUILD_ID)
        if not home:
            return

        import aiohttp
        still_verified = []

        async with aiohttp.ClientSession() as session:
            for v in data.get("verified", []):
                entry = next((c for c in data["channels"] if c["title"] == v["title"]), None)
                if not entry:
                    continue

                ok = True
                try:
                    ok = await yt_api_is_subscribed(entry["api_key"], session, v["user_channel_id"], v["target_channel_id"])
                except Exception:
                    ok = True

                member = home.get_member(v["discord_id"])
                role = home.get_role(v["role_id"])

                if ok:
                    still_verified.append(v)
                else:
                    if member and role:
                        try:
                            await member.remove_roles(role, reason="Unsubscribed")
                        except Exception:
                            pass

        data["verified"] = still_verified
        self._save(data)

    @recheck_task.before_loop
    async def before_recheck(self):
        await self.bot.wait_until_ready()


async def setup(bot: commands.Bot):
    await bot.add_cog(YouTubeVerifyCog(bot))
