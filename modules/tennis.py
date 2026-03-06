# modules/tennis.py
import time
import discord
from discord.ext import commands
from discord import app_commands

import config

from modules.tennis_providers import get_provider
from modules.tennis_qa import answer_question


# Small in-memory cache (works fine for now)
_CACHE: dict[str, tuple[int, object]] = {}

def cache_get(key: str):
    item = _CACHE.get(key)
    if not item:
        return None
    exp, val = item
    if int(time.time()) >= exp:
        _CACHE.pop(key, None)
        return None
    return val

def cache_set(key: str, val, ttl: int):
    _CACHE[key] = (int(time.time()) + int(ttl), val)


class PagerView(discord.ui.View):
    def __init__(self, pages: list[str], user_id: int, title: str):
        super().__init__(timeout=180)
        self.pages = pages
        self.user_id = user_id
        self.title = title
        self.i = 0
        self._locked = False

    def _embed(self):
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


class TennisCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.provider = get_provider()

    # ---------- Rankings ----------
    @app_commands.command(name="rankings", description="Show ATP/WTA rankings.")
    @app_commands.describe(tour="ATP or WTA", kind="singles or doubles", top="How many (max 200)")
    async def rankings(self, interaction: discord.Interaction, tour: str, kind: str = "singles", top: int = 50):
        tour = (tour or "").strip().upper()
        kind = (kind or "singles").strip().lower()
        top = max(1, min(int(top), 200))

        cache_key = f"rankings:{tour}:{kind}:{top}"
        cached = cache_get(cache_key)
        if cached:
            return await interaction.response.send_message(embed=cached)

        await interaction.response.defer(thinking=True)
        rows = await self.provider.get_rankings(tour=tour, kind=kind, limit=top)

        if not rows:
            return await interaction.edit_original_response(content="ℹ️ No rankings data found.")

        lines = []
        for r in rows:
            mv = r.get("movement")
            mv_txt = ""
            if isinstance(mv, int) and mv != 0:
                mv_txt = f" ({'+' if mv>0 else ''}{mv})"
            lines.append(f"**{r['rank']}.** {r['name']} {mv_txt} — {r.get('country','')} — **{r.get('points','')}**")

        desc = "\n".join(lines[:top])
        e = discord.Embed(title=f"{tour} {kind.title()} Rankings (Top {top})", description=desc)
        e.set_footer(text=f"Source: {self.provider.source_name}")

        cache_set(cache_key, e, ttl=60 * 60)  # 1h
        await interaction.edit_original_response(embed=e)

    @app_commands.command(name="rank", description="Show current rank for a player.")
    async def rank(self, interaction: discord.Interaction, player: str, tour: str = "ATP", kind: str = "singles"):
        await interaction.response.defer(thinking=True)
        p = await self.provider.search_player(player, tour=tour)
        if not p:
            return await interaction.edit_original_response(content="❌ Player not found.")
        info = await self.provider.get_player(p["player_id"], tour=tour, kind=kind)
        if not info:
            return await interaction.edit_original_response(content="❌ No player data returned.")

        e = discord.Embed(
            title=f"{info.get('name','Player')}",
            description=(
                f"**Tour:** {tour.upper()}  |  **Type:** {kind}\n"
                f"**Rank:** {info.get('rank','?')}\n"
                f"**Points:** {info.get('points','?')}\n"
            ),
        )
        e.set_footer(text=f"Source: {self.provider.source_name}")
        await interaction.edit_original_response(embed=e)

    
    @app_commands.command(name="player", description="Quick player info (rank, country, bio fields if available).")
    async def player(self, interaction: discord.Interaction, name: str, tour: str = "ATP"):
        await interaction.response.defer(thinking=True)
        p = await self.provider.search_player(name, tour=tour)
        if not p:
            return await interaction.edit_original_response(content="❌ Player not found.")
        info = await self.provider.get_player(p["player_id"], tour=tour, kind="singles")

        lines = []
        lines.append(f"**Name:** {info.get('name','?')}")
        if info.get("country"):
            lines.append(f"**Country:** {info['country']}")
        if info.get("age"):
            lines.append(f"**Age:** {info['age']}")
        if info.get("hand"):
            lines.append(f"**Hand:** {info['hand']}")
        if info.get("rank"):
            lines.append(f"**Rank:** {info['rank']}")
        if info.get("points"):
            lines.append(f"**Points:** {info['points']}")
        if info.get("wl"):
            lines.append(f"**Season W/L:** {info['wl']}")

        e = discord.Embed(title="Player", description="\n".join(lines))
        e.set_footer(text=f"Source: {self.provider.source_name} • Player ID: {p['player_id']}")
        await interaction.edit_original_response(embed=e)

    @app_commands.command(name="player-stats", description="Quick player stats (season splits if provider supports).")
    async def player_stats(self, interaction: discord.Interaction, name: str, tour: str = "ATP", season: int | None = None):
        await interaction.response.defer(thinking=True)
        p = await self.provider.search_player(name, tour=tour)
        if not p:
            return await interaction.edit_original_response(content="❌ Player not found.")

        stats = await self.provider.get_player_stats(p["player_id"], tour=tour, season=season)
        if not stats:
            return await interaction.edit_original_response(content="ℹ️ No stats available from this provider yet.")

        lines = [f"**{stats.get('name','Player')}**"]
        if season:
            lines.append(f"**Season:** {season}")
        lines.append("")
        for k, v in stats.get("stats", {}).items():
            lines.append(f"- **{k}:** {v}")

        e = discord.Embed(title="Player Stats", description="\n".join(lines))
        e.set_footer(text=f"Source: {self.provider.source_name}")
        await interaction.edit_original_response(embed=e)

    # ---------- H2H (placeholder until you hook a historical DB) ----------
    @app_commands.command(name="h2h", description="Head-to-head (phase 1 placeholder; phase 2 uses historical DB).")
    async def h2h(self, interaction: discord.Interaction, p1: str, p2: str):
        await interaction.response.send_message(
            "ℹ️ H2H will be enabled in Phase 2 when we import the historical dataset into a DB.\n"
            "For now, the provider layer is ready — we just need the data source."
        )

    # ---------- GPT Q&A (grounded) ----------
    @app_commands.command(name="ask", description="Ask a question grounded in rankings/live/player data.")
    async def ask(self, interaction: discord.Interaction, question: str):
        await interaction.response.defer(thinking=True)
        text, source_note = await answer_question(
            question=question,
            provider=self.provider,
        )
        e = discord.Embed(title="Ask Tennis", description=text)
        e.set_footer(text=source_note)
        await interaction.edit_original_response(embed=e)


async def setup(bot: commands.Bot):
    await bot.add_cog(TennisCog(bot))