import os
import discord
from discord.ext import commands
import config
from utils import ensure_dir, load_json, now_ts
from modules.academy import academy_can_challenge
from flask import Flask
from threading import Thread

intents = discord.Intents.default()
intents.members = True
intents.guilds = True
intents.message_content = True   # required for the Memory training drill

app = Flask('')

@app.route('/')
def home(): return "alive"

def keep_alive():
    app.run(host='0.0.0.0', port=8080)

Thread(target=keep_alive).start()



class MyBot(commands.Bot):
    async def setup_hook(self):
        extensions = [
            "modules.moderation",
            "modules.target_server",
            "modules.youtube_verify",
            "modules.economy",
            "modules.cards",
            "modules.fantasy",
            "modules.matchsim",
            "modules.players",
            "modules.academy",
            "modules.coaches",
            "modules.gear",
            "modules.venues",
            "modules.loadouts",
            "modules.training",
            "modules.tournaments",
        ]
        for ext in extensions:
            try:
                await self.load_extension(ext)
                print(f"Loaded extension: {ext}")
            except Exception as e:
                print(f"FAILED extension: {ext} -> {repr(e)}")

        GUILD_ID = 1333962919536492607
        guild = discord.Object(id=GUILD_ID)

        # ---- Command Sync ----
        if os.getenv("SYNC_COMMANDS") == "1":
            # Optional: clear guild commands (ONLY when you explicitly set RESET_SYNC=1)
            if os.getenv("RESET_SYNC") == "1":
                try:
                    self.tree.clear_commands(guild=guild)
                    await self.tree.sync(guild=guild)
                    print("✅ Cleared guild commands (RESET_SYNC=1).")
                except Exception as e:
                    print(f"❌ Failed to clear guild commands: {repr(e)}")

            # 1) Sync GLOBAL (so Discord has the canonical set)
            try:
                synced_global = await self.tree.sync()
                print(f"✅ Synced {len(synced_global)} commands globally")
            except Exception as e:
                print(f"❌ Global sync failed: {repr(e)}")

            # 2) Copy GLOBAL -> GUILD and sync (commands appear instantly in your server)
            try:
                self.tree.copy_global_to(guild=guild)
                synced_guild = await self.tree.sync(guild=guild)
                print(f"✅ Synced {len(synced_guild)} commands to guild {GUILD_ID}")
            except Exception as e:
                print(f"❌ Guild sync failed: {repr(e)}")

            try:
                print("Loaded app commands:", [c.name for c in self.tree.get_commands()])
            except Exception:
                pass
        else:
            print("⏭️ Skipping command sync (set SYNC_COMMANDS=1 to sync)")


bot = MyBot(command_prefix="!", intents=intents)


@bot.tree.interaction_check
async def global_slash_gate(interaction: discord.Interaction) -> bool:
    home_id = int(getattr(config, "HOME_GUILD_ID", 0) or 0)
    if home_id and (interaction.guild is None or interaction.guild.id != home_id):
        return False
    if not getattr(config, "COMMAND_TIMEOUT_FILE", None) or not interaction.guild:
        return True
    data = load_json(getattr(config, "COMMAND_TIMEOUT_FILE", ""), {})
    g = data.get(str(interaction.guild.id), {})
    entry = g.get(str(interaction.user.id))
    if entry:
        until = int(entry.get("until", 0))
        if until == 0 or now_ts() < until:
            return False
    return True

@bot.tree.error
async def on_tree_error(interaction: discord.Interaction, error):
    # Silently ignore expired/unknown interactions - these are normal on Replit
    if isinstance(error, discord.app_commands.errors.CommandInvokeError):
        error = error.original
    if isinstance(error, (discord.errors.NotFound, discord.errors.HTTPException)):
        if getattr(error, 'code', None) in (10062, 40060):
            return  # Unknown interaction / already acknowledged — safe to ignore
    # Re-raise everything else so real errors still show
    raise error

@bot.event
async def on_ready():
    print(f"✅ Logged in as {bot.user} (id={bot.user.id})")
    if bot.guilds:
        print("Bot is in guilds:")
        for g in bot.guilds:
            print(" -", g.id, g.name)


if __name__ == "__main__":
    bot.run(config.DISCORD_TOKEN)