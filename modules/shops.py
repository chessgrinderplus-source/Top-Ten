# modules/shops.py
from discord.ext import commands

GEAR_SHOP = {
    "power_racket": {"power": 8, "price": 500},
    "control_racket": {"control": 8, "price": 500},
    "spin_strings": {"spin": 6, "price": 400}
}

VENUES = {
    "default": {"surface": "hard", "pace": 1.0},
    "clay_court": {"surface": "clay", "pace": 0.9},
    "grass_court": {"surface": "grass", "pace": 1.1}
}

class ShopCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command()
    async def gear_shop(self, ctx):
        msg = "\n".join([f"{k} → {v}" for k, v in GEAR_SHOP.items()])
        await ctx.send(f"🛒 **Gear Shop**\n{msg}")

    @commands.command()
    async def venue_shop(self, ctx):
        msg = "\n".join([f"{k} → {v}" for k, v in VENUES.items()])
        await ctx.send(f"🏟 **Venues**\n{msg}")
