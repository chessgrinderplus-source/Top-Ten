# modules/match_bot.py
from discord.ext import commands

class MatchBotCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.bots = {}  # Stores all bot players

    @commands.command()
    async def match_sim_bot_create(self, ctx, name: str):
        """Create a bot player."""
        self.bots[name] = {
            'forehand': 50,
            'backhand': 50,
            'serve': 50,
            'touch': 50,
            'fitness': 50,
            'hand': 'right',
            'loadout_main': {},
            'loadout_alt': {},
            'fatigue': 0
        }
        await ctx.send(f'Bot player {name} created!')

    @commands.command()
    async def match_sim_bot_edit(self, ctx, name: str):
        if name not in self.bots:
            await ctx.send("Bot not found.")
        else:
            await ctx.send(f'Bot {name} is ready to edit.')
