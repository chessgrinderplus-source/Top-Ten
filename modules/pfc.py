# modules/pfc.py
# ─────────────────────────────────────────────────────────────────────────────
# Perfect Fit Challenge — Full Discord Module
# ─────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import itertools
import os
import random
import uuid
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

import config
from utils import ensure_dir, load_json, save_json, now_ts


# ─────────────────────────────────────────────────────────────────────────────
# STORAGE
# ─────────────────────────────────────────────────────────────────────────────

def _data_path() -> str:
    return getattr(config, "PFC_FILE", os.path.join(config.DATA_DIR, "pfc_data.json"))


def _challenge_json_path() -> str:
    """
    Resolve the challenge JSON path.
    Priority:
      1. PFC_CHALLENGE_FILE env var / config attribute
      2. data/pfc_challenge.json relative to THIS file's project root
      3. data/pfc_challenge.json relative to cwd
    """
    # Config override
    if hasattr(config, "PFC_CHALLENGE_FILE") and config.PFC_CHALLENGE_FILE:
        return config.PFC_CHALLENGE_FILE

    # Absolute path from this file's location
    # modules/pfc.py  →  project_root/modules/  →  project_root/
    module_dir   = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(module_dir)
    abs_path     = os.path.join(project_root, "data", "pfc_challenge.json")
    if os.path.exists(abs_path):
        return abs_path

    # Fallback: config.DATA_DIR (relative to cwd)
    return os.path.join(config.DATA_DIR, "pfc_challenge.json")


def _load() -> dict:
    ensure_dir(config.DATA_DIR)
    data = load_json(_data_path(), {})
    data.setdefault("active",    None)
    data.setdefault("past",      [])
    data.setdefault("blacklist", [])
    data.setdefault("sessions",  {})
    return data


def _save(data: dict) -> None:
    save_json(_data_path(), data)


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _is_admin(member: discord.Member) -> bool:
    return member.guild_permissions.administrator


def _load_challenge_json() -> tuple[dict | None, str]:
    """
    Load and validate the generator output file.
    Returns (data_or_None, path_that_was_tried).
    """
    path = _challenge_json_path()
    if not os.path.exists(path):
        return None, path
    raw = load_json(path, None)
    if not raw or not raw.get("categories") or not raw.get("players"):
        return None, path
    return raw, path


def _score_submission(submission: dict, challenge: dict) -> float:
    total   = 0.0
    lineup  = submission.get("lineup", {})
    players = {p["name"]: p for p in challenge["players"]}
    for cat_id, player_name in lineup.items():
        player = players.get(player_name)
        if not player:
            continue
        v = player.get("stats", {}).get(cat_id)
        if v is not None:
            total += float(v)
    return total


def _compute_best_lineup(challenge: dict) -> tuple[list[dict], float]:
    categories = challenge["categories"]
    players    = challenge["players"]
    cat_ids    = [c["id"] for c in categories]
    n_cats     = len(cat_ids)
    values     = [[float(p.get("stats", {}).get(cid) or 0) for cid in cat_ids] for p in players]
    n_players  = len(players)
    best_score = -1.0
    best_assign = list(range(n_cats))

    if n_players <= 10:
        for perm in itertools.permutations(range(n_players), n_cats):
            score = sum(values[perm[ci]][ci] for ci in range(n_cats))
            if score > best_score:
                best_score  = score
                best_assign = list(perm)
    else:
        used = set()
        for ci in range(n_cats):
            best_pi, best_val = -1, -1.0
            for pi in range(n_players):
                if pi not in used and values[pi][ci] > best_val:
                    best_val, best_pi = values[pi][ci], pi
            best_assign[ci] = best_pi
            used.add(best_pi)
        best_score = sum(values[best_assign[ci]][ci] for ci in range(n_cats))

    lineup = [
        {
            "category":   categories[ci]["display_name"],
            "player":     players[pi]["name"] if pi >= 0 else "—",
            "stat_value": values[pi][ci] if pi >= 0 else 0.0,
        }
        for ci, pi in enumerate(best_assign)
    ]
    return lineup, best_score


def _leaderboard_embed(challenge: dict, blacklist: list, guild: discord.Guild) -> discord.Embed:
    subs   = challenge.get("submissions", {})
    scored = [
        (uid, _score_submission(sub, challenge))
        for uid, sub in subs.items()
        if uid not in blacklist
    ]
    scored.sort(key=lambda x: x[1], reverse=True)

    status = "🔴 ENDED" if challenge.get("ended") else "🟢 ACTIVE"
    embed  = discord.Embed(
        title=f"🏆 {challenge.get('name', 'PFC')} — Leaderboard",
        description=f"Status: {status}",
        colour=discord.Colour.gold(),
    )
    if not scored:
        embed.add_field(name="No entries yet", value="Be the first to play!", inline=False)
        return embed

    medals = ["🥇", "🥈", "🥉"]
    lines  = []
    for rank, (uid, score) in enumerate(scored[:25], 1):
        member   = guild.get_member(int(uid))
        name_str = member.display_name if member else f"User {uid}"
        badge    = medals[rank - 1] if rank <= 3 else f"**{rank}.**"
        lines.append(f"{badge} {name_str} — **{score:,.1f}**")

    embed.add_field(name="Rankings", value="\n".join(lines), inline=False)
    embed.set_footer(text=f"Week: {challenge.get('week','—')}  •  {len(scored)} entries")
    return embed


# ─────────────────────────────────────────────────────────────────────────────
# SESSION MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

def _start_session(data: dict, user_id: str, challenge: dict) -> dict:
    players = [p["name"] for p in challenge["players"]]
    random.shuffle(players)
    session = {
        "challenge_id": challenge["id"],
        "queue":        players,
        "current_idx":  0,
        "slots":        {},
        "started_at":   now_ts(),
        "completed":    False,
    }
    data["sessions"][user_id] = session
    return session


def _get_session(data: dict, user_id: str, challenge_id: str) -> dict | None:
    session = data["sessions"].get(user_id)
    if not session:
        return None
    if session.get("challenge_id") != challenge_id:
        return None
    if session.get("completed"):
        return None
    return session


# ─────────────────────────────────────────────────────────────────────────────
# EMBED BUILDERS
# ─────────────────────────────────────────────────────────────────────────────

def _game_embed(session: dict, challenge: dict, just_placed: str | None = None) -> discord.Embed:
    cats    = challenge["categories"]
    players = {p["name"]: p for p in challenge["players"]}
    n_cats  = len(cats)
    slots   = session["slots"]
    idx     = session["current_idx"]
    queue   = session["queue"]

    placed_count = len(slots)
    current_name = queue[idx] if idx < len(queue) else None
    player_data  = players.get(current_name, {}) if current_name else {}
    tour_tag     = f" [{player_data.get('tour','')}]" if player_data.get("tour") else ""

    embed = discord.Embed(
        title=f"🎾 {challenge.get('name','Perfect Fit Challenge')}",
        colour=discord.Colour.blue(),
    )

    if current_name:
        embed.add_field(
            name="🎯 Current Player",
            value=f"## {current_name}{tour_tag}\nAssign this player to one of the empty slots below.",
            inline=False,
        )

    if just_placed:
        embed.add_field(name="✅ Just Placed", value=just_placed, inline=False)

    slot_lines = []
    for cat in cats:
        cid   = cat["id"]
        dname = cat["display_name"]
        if cid in slots:
            pname = slots[cid]
            pdata = players.get(pname, {})
            val   = pdata.get("stats", {}).get(cid)
            vstr  = f"  •  `{val:,.1f}`" if val is not None else ""
            slot_lines.append(f"✅ **{dname}**\n└ {pname}{vstr}")
        else:
            slot_lines.append(f"⬜ **{dname}**\n└ *(empty)*")

    embed.add_field(
        name=f"📋 Slots  ({placed_count}/{n_cats} filled)",
        value="\n".join(slot_lines),
        inline=False,
    )
    embed.set_footer(text=f"Player {min(idx + 1, n_cats)} of {n_cats}")
    return embed


def _result_embed(session: dict, challenge: dict, user: discord.Member) -> discord.Embed:
    cats    = challenge["categories"]
    players = {p["name"]: p for p in challenge["players"]}
    slots   = session["slots"]
    total   = 0.0
    lines   = []

    for cat in cats:
        cid   = cat["id"]
        dname = cat["display_name"]
        pname = slots.get(cid, "—")
        pdata = players.get(pname, {})
        val   = pdata.get("stats", {}).get(cid)
        if val is not None:
            total += float(val)
            lines.append(f"**{dname}**\n└ {pname}  •  `{val:,.1f}`")
        else:
            lines.append(f"**{dname}**\n└ {pname}  •  `N/A`")

    embed = discord.Embed(
        title=f"🏁 {challenge.get('name','PFC')} — Your Results",
        description=f"**{user.display_name}** finished with a score of **{total:,.1f}**!",
        colour=discord.Colour.green(),
    )
    embed.add_field(name="Your Lineup", value="\n".join(lines), inline=False)
    embed.add_field(name="Total Score", value=f"**{total:,.1f}**", inline=True)
    embed.set_footer(text="Use /pfc-leaderboard to see how you stack up.")
    return embed


# ─────────────────────────────────────────────────────────────────────────────
# GAMEPLAY VIEW
# ─────────────────────────────────────────────────────────────────────────────

class SlotSelect(discord.ui.Select):
    def __init__(self, parent: "SlotPickerView"):
        slots   = parent.session["slots"]
        options = [
            discord.SelectOption(label=cat["display_name"][:100], value=cat["id"])
            for cat in parent.challenge["categories"]
            if cat["id"] not in slots
        ]
        super().__init__(
            placeholder="Choose a slot for this player…",
            min_values=1, max_values=1,
            options=options[:25],
        )
        self.parent_view = parent

    async def callback(self, interaction: discord.Interaction):
        await self.parent_view.on_slot_chosen(interaction, self.values[0])


class SlotPickerView(discord.ui.View):
    def __init__(self, cog: "PFCCog", user_id: int, challenge: dict, session: dict):
        super().__init__(timeout=300)
        self.cog       = cog
        self.user_id   = user_id
        self.challenge = challenge
        self.session   = session
        self._locked   = False
        self._rebuild()

    def _rebuild(self):
        self.clear_items()
        self.add_item(SlotSelect(self))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This game isn't yours.", ephemeral=True)
            return False
        return True

    async def on_slot_chosen(self, interaction: discord.Interaction, cat_id: str):
        if self._locked:
            return
        self._locked = True
        try:
            await self._process(interaction, cat_id)
        except Exception as e:
            print(f"[pfc] SlotPickerView error: {e}")
            try:
                await interaction.response.send_message(
                    "❌ Something went wrong. Try `/pfc-play` again.", ephemeral=True
                )
            except Exception:
                pass
        finally:
            self._locked = False

    async def _process(self, interaction: discord.Interaction, cat_id: str):
        data      = _load()
        uid_str   = str(self.user_id)
        challenge = data.get("active")

        if not challenge or challenge["id"] != self.challenge["id"]:
            await interaction.response.edit_message(
                content="❌ The challenge has changed or ended.", embed=None, view=None
            )
            return

        session = data["sessions"].get(uid_str)
        if not session or session.get("completed"):
            await interaction.response.edit_message(
                content="❌ Session expired. Use `/pfc-play` to start again.",
                embed=None, view=None,
            )
            return

        if cat_id in session["slots"]:
            await interaction.response.send_message(
                "❌ That slot is already filled. Pick another.", ephemeral=True
            )
            return

        idx          = session["current_idx"]
        queue        = session["queue"]
        n_cats       = len(challenge["categories"])
        current_name = queue[idx]
        cat_display  = next(c["display_name"] for c in challenge["categories"] if c["id"] == cat_id)

        session["slots"][cat_id] = current_name
        session["current_idx"]   = idx + 1
        placed_msg               = f"{current_name} → **{cat_display}**"
        remaining                = n_cats - len(session["slots"])

        # ── Last player — auto-assign ──────────────────────────────────────
        if remaining == 1:
            next_name    = queue[session["current_idx"]]
            last_cat_id  = next(c["id"] for c in challenge["categories"] if c["id"] not in session["slots"])
            last_display = next(c["display_name"] for c in challenge["categories"] if c["id"] == last_cat_id)
            session["slots"][last_cat_id] = next_name
            session["current_idx"]        += 1
            session["completed"]           = True
            challenge["submissions"][uid_str] = {"lineup": session["slots"], "submitted_at": now_ts()}
            data["sessions"][uid_str] = session
            _save(data)
            self.challenge = challenge
            self.session   = session
            auto_note      = f"*{next_name} was auto-assigned to **{last_display}***"
            res_embed      = _result_embed(session, challenge, interaction.user)
            res_embed.description = auto_note + "\n\n" + (res_embed.description or "")
            await interaction.response.edit_message(embed=res_embed, view=None)
            return

        # ── All done ───────────────────────────────────────────────────────
        if remaining == 0:
            session["completed"] = True
            challenge["submissions"][uid_str] = {"lineup": session["slots"], "submitted_at": now_ts()}
            data["sessions"][uid_str] = session
            _save(data)
            self.challenge = challenge
            self.session   = session
            await interaction.response.edit_message(
                embed=_result_embed(session, challenge, interaction.user), view=None
            )
            return

        # ── Continue ───────────────────────────────────────────────────────
        data["sessions"][uid_str] = session
        _save(data)
        self.challenge = challenge
        self.session   = session
        self._rebuild()
        await interaction.response.edit_message(
            embed=_game_embed(session, challenge, just_placed=placed_msg),
            view=self,
        )


# ─────────────────────────────────────────────────────────────────────────────
# COG
# ─────────────────────────────────────────────────────────────────────────────

class PFCCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def _require_admin(self, interaction: discord.Interaction) -> bool:
        if not _is_admin(interaction.user):
            await interaction.response.send_message(
                "❌ You need administrator permissions.", ephemeral=True
            )
            return False
        return True

    async def _require_active(self, interaction: discord.Interaction) -> dict | None:
        data = _load()
        if not data["active"]:
            await interaction.response.send_message(
                "❌ No active challenge. Use `/pfc-create` to start one.", ephemeral=True
            )
            return None
        return data

    # ── PLAY ─────────────────────────────────────────────────────────────────

    @app_commands.command(name="pfc-play", description="Play the current Perfect Fit Challenge.")
    async def pfc_play(self, interaction: discord.Interaction):
        data      = _load()
        challenge = data.get("active")

        if not challenge:
            await interaction.response.send_message(
                "❌ No active challenge right now. Check back soon!", ephemeral=True
            )
            return

        uid_str = str(interaction.user.id)

        if uid_str in challenge.get("submissions", {}):
            score = _score_submission(challenge["submissions"][uid_str], challenge)
            await interaction.response.send_message(
                f"✅ You've already completed this challenge!\n"
                f"Your score: **{score:,.1f}**\n"
                f"Use `/pfc-leaderboard` to see the standings.",
                ephemeral=True,
            )
            return

        session = _get_session(data, uid_str, challenge["id"])
        if not session:
            session = _start_session(data, uid_str, challenge)
            _save(data)

        embed = _game_embed(session, challenge)
        view  = SlotPickerView(self, interaction.user.id, challenge, session)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    # ── CREATE ────────────────────────────────────────────────────────────────

    @app_commands.command(name="pfc-create", description="[Admin] Load the generated challenge and activate it.")
    @app_commands.describe(
        name="Display name for this week's challenge (e.g. 'Big 3 Showdown')",
        num_categories="How many category slots to use (default: all from the JSON)",
    )
    async def pfc_create(self, interaction: discord.Interaction, name: str, num_categories: Optional[int] = None):
        if not await self._require_admin(interaction):
            return

        raw, path = _load_challenge_json()
        if not raw:
            await interaction.response.send_message(
                f"❌ Could not load challenge data.\n"
                f"**Looked for:** `{path}`\n\n"
                f"Make sure you've run `python tools/pfc_generate.py` from your **project root**, "
                f"and that the `data/` folder exists.",
                ephemeral=True,
            )
            return

        cats    = raw["categories"]
        players = raw["players"]

        if num_categories is not None:
            if not (1 <= num_categories <= len(cats)):
                await interaction.response.send_message(
                    f"❌ `num_categories` must be between 1 and {len(cats)}.", ephemeral=True
                )
                return
            cats = cats[:num_categories]

        data = _load()
        if data["active"]:
            data["active"]["ended"]    = True
            data["active"]["ended_at"] = now_ts()
            data["past"].append(data["active"])

        cid            = f"pfc-{uuid.uuid4().hex[:8]}"
        data["active"] = {
            "id":          cid,
            "name":        name.strip(),
            "week":        raw.get("week", ""),
            "created_at":  now_ts(),
            "ended":       False,
            "ended_at":    None,
            "categories":  cats,
            "players":     players,
            "submissions": {},
        }
        data["sessions"] = {}
        _save(data)

        embed = discord.Embed(title="✅ Perfect Fit Challenge Created!", colour=discord.Colour.green())
        embed.add_field(name="Name",        value=name,                  inline=True)
        embed.add_field(name="Week",        value=raw.get("week","—"),   inline=True)
        embed.add_field(name="Slots",       value=str(len(cats)),        inline=True)
        embed.add_field(name="Player Pool", value=str(len(players)),     inline=True)
        embed.add_field(name="ID",          value=cid,                   inline=True)
        embed.add_field(
            name="Stat Slots",
            value="\n".join(f"• {c['display_name']}" for c in cats) or "—",
            inline=False,
        )
        await interaction.response.send_message(embed=embed)

    # ── EDIT ──────────────────────────────────────────────────────────────────

    @app_commands.command(name="pfc-edit", description="[Admin] Rename the active challenge.")
    @app_commands.describe(name="New display name")
    async def pfc_edit(self, interaction: discord.Interaction, name: str):
        if not await self._require_admin(interaction): return
        data = await self._require_active(interaction)
        if not data: return
        old = data["active"]["name"]
        data["active"]["name"] = name.strip()
        _save(data)
        await interaction.response.send_message(f"✅ Renamed: **{old}** → **{name.strip()}**")

    # ── END ───────────────────────────────────────────────────────────────────

    @app_commands.command(name="pfc-end", description="[Admin] End the challenge and freeze the leaderboard.")
    async def pfc_end(self, interaction: discord.Interaction):
        if not await self._require_admin(interaction): return
        data = await self._require_active(interaction)
        if not data: return
        c = data["active"]
        c["ended"] = True; c["ended_at"] = now_ts()
        data["past"].append(c)
        data["active"]   = None
        data["sessions"] = {}
        _save(data)
        embed = discord.Embed(
            title="🔴 Challenge Ended",
            description=f"**{c['name']}** is now closed. Leaderboard is final.",
            colour=discord.Colour.red(),
        )
        embed.add_field(name="Entries", value=str(len(c.get("submissions",{}))), inline=True)
        await interaction.response.send_message(embed=embed)

    # ── RELOAD ────────────────────────────────────────────────────────────────

    @app_commands.command(name="pfc-reload", description="[Admin] Reload challenge data from disk without wiping submissions.")
    async def pfc_reload(self, interaction: discord.Interaction):
        if not await self._require_admin(interaction): return
        data = await self._require_active(interaction)
        if not data: return
        raw, path = _load_challenge_json()
        if not raw:
            await interaction.response.send_message(
                f"❌ Could not load `pfc_challenge.json`.\nLooked at: `{path}`", ephemeral=True
            )
            return
        n_cats = len(data["active"]["categories"])
        data["active"]["categories"] = raw["categories"][:n_cats]
        data["active"]["players"]    = raw["players"]
        data["active"]["week"]       = raw.get("week", data["active"]["week"])
        _save(data)
        await interaction.response.send_message(
            f"✅ Reloaded — {len(raw['players'])} players, {n_cats} categories. Submissions preserved.",
            ephemeral=True,
        )

    # ── BEST LINEUP ───────────────────────────────────────────────────────────

    @app_commands.command(name="pfc-best-lineup", description="[Admin] Calculate the theoretically perfect lineup and max score.")
    async def pfc_best_lineup(self, interaction: discord.Interaction):
        if not await self._require_admin(interaction): return
        data = await self._require_active(interaction)
        if not data: return
        await interaction.response.defer(ephemeral=True, thinking=True)
        challenge     = data["active"]
        lineup, total = _compute_best_lineup(challenge)
        embed = discord.Embed(
            title=f"🧠 Perfect Lineup — {challenge['name']}",
            description="Theoretically optimal assignment.\n*Admin only.*",
            colour=discord.Colour.purple(),
        )
        lines = [f"**{e['category']}**\n└ {e['player']}  •  `{e['stat_value']:,.1f}`" for e in lineup]
        for i in range(0, len(lines), 4):
            embed.add_field(
                name=f"Slots {i+1}–{min(i+4,len(lines))}",
                value="\n".join(lines[i:i+4]),
                inline=False,
            )
        embed.add_field(name="Max Possible Score", value=f"**{total:,.1f}**", inline=False)
        await interaction.edit_original_response(embed=embed)

    # ── BLACKLIST ─────────────────────────────────────────────────────────────

    @app_commands.command(name="pfc-blacklist", description="[Admin] Add or remove a user from the leaderboard blacklist.")
    @app_commands.describe(action="add or remove", user="The user")
    @app_commands.choices(action=[
        app_commands.Choice(name="add",    value="add"),
        app_commands.Choice(name="remove", value="remove"),
    ])
    async def pfc_blacklist(self, interaction: discord.Interaction, action: str, user: discord.Member):
        if not await self._require_admin(interaction): return
        data = _load(); uid = str(user.id)
        if action == "add":
            if uid not in data["blacklist"]: data["blacklist"].append(uid)
            _save(data)
            await interaction.response.send_message(f"✅ **{user.display_name}** added to blacklist.", ephemeral=True)
        else:
            if uid in data["blacklist"]: data["blacklist"].remove(uid)
            _save(data)
            await interaction.response.send_message(f"✅ **{user.display_name}** removed from blacklist.", ephemeral=True)

    @app_commands.command(name="pfc-blacklist-view", description="[Admin] View the current leaderboard blacklist.")
    async def pfc_blacklist_view(self, interaction: discord.Interaction):
        if not await self._require_admin(interaction): return
        data = _load(); bl = data.get("blacklist", [])
        if not bl:
            await interaction.response.send_message("The blacklist is empty.", ephemeral=True)
            return
        lines = []
        for uid in bl:
            member = interaction.guild.get_member(int(uid))
            lines.append(f"• {member.display_name if member else f'User {uid}'}")
        embed = discord.Embed(title="🚫 PFC Leaderboard Blacklist",
                              description="\n".join(lines), colour=discord.Colour.dark_grey())
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # ── INFO ──────────────────────────────────────────────────────────────────

    @app_commands.command(name="pfc-info", description="Show the current Perfect Fit Challenge details.")
    async def pfc_info(self, interaction: discord.Interaction):
        data      = _load()
        challenge = data.get("active")
        if not challenge:
            await interaction.response.send_message("No active challenge right now. Check back soon!", ephemeral=True)
            return
        embed = discord.Embed(title=f"🎾 {challenge['name']}", colour=discord.Colour.blue())
        embed.add_field(name="Week",        value=challenge.get("week","—"),        inline=True)
        embed.add_field(name="Slots",       value=str(len(challenge["categories"])), inline=True)
        embed.add_field(name="Player Pool", value=str(len(challenge["players"])),   inline=True)
        cat_lines = "\n".join(f"`{i+1}.` {c['display_name']}" for i, c in enumerate(challenge["categories"]))
        embed.add_field(name="🗂️ Stat Slots", value=cat_lines or "—", inline=False)
        atp   = [p["name"] for p in challenge["players"] if p.get("tour","").upper() == "ATP"]
        wta   = [p["name"] for p in challenge["players"] if p.get("tour","").upper() == "WTA"]
        other = [p["name"] for p in challenge["players"] if p.get("tour","").upper() not in ("ATP","WTA")]
        if atp:   embed.add_field(name=f"👨 ATP ({len(atp)})",      value=", ".join(atp),   inline=False)
        if wta:   embed.add_field(name=f"👩 WTA ({len(wta)})",      value=", ".join(wta),   inline=False)
        if other: embed.add_field(name=f"Players ({len(other)})", value=", ".join(other), inline=False)
        uid_str = str(interaction.user.id)
        if uid_str in challenge.get("submissions", {}):
            score = _score_submission(challenge["submissions"][uid_str], challenge)
            embed.add_field(name="✅ Your Score", value=f"**{score:,.1f}** — already submitted", inline=False)
        else:
            embed.add_field(name="▶️ Play", value="Use `/pfc-play` to take part!", inline=False)
        embed.set_footer(text="Use /pfc-commands to see all commands.")
        await interaction.response.send_message(embed=embed)

    # ── LEADERBOARD ───────────────────────────────────────────────────────────

    @app_commands.command(name="pfc-leaderboard", description="Show the Perfect Fit Challenge leaderboard.")
    @app_commands.describe(historical="Show a past challenge by ID or name")
    async def pfc_leaderboard(self, interaction: discord.Interaction, historical: Optional[str] = None):
        data = _load()
        if historical:
            challenge = next(
                (c for c in data.get("past",[])
                 if c.get("id") == historical or c.get("name","").lower() == historical.lower()),
                None,
            )
            if not challenge:
                await interaction.response.send_message(f"❌ No past challenge: `{historical}`.", ephemeral=True)
                return
        else:
            challenge = data.get("active")
            if not challenge:
                past = data.get("past", [])
                challenge = past[-1] if past else None
            if not challenge:
                await interaction.response.send_message("No challenge data yet.", ephemeral=True)
                return
        embed = _leaderboard_embed(challenge, data.get("blacklist",[]), interaction.guild)
        await interaction.response.send_message(embed=embed)

    # ── COMMANDS ──────────────────────────────────────────────────────────────

    @app_commands.command(name="pfc-commands", description="Show all Perfect Fit Challenge commands.")
    async def pfc_commands(self, interaction: discord.Interaction):
        is_admin = _is_admin(interaction.user)
        embed    = discord.Embed(title="🎾 Perfect Fit Challenge — Commands", colour=discord.Colour.blurple())
        embed.add_field(
            name="🎮 Playing",
            value=(
                "`/pfc-play` — Play the current challenge\n"
                "`/pfc-info` — View challenge details & player pool\n"
                "`/pfc-leaderboard` — View the standings\n"
                "`/pfc-commands` — Show this message"
            ),
            inline=False,
        )
        if is_admin:
            embed.add_field(
                name="🔧 Admin",
                value=(
                    "`/pfc-create` — Load & activate a new challenge\n"
                    "`/pfc-edit` — Rename the active challenge\n"
                    "`/pfc-end` — End the challenge and freeze the leaderboard\n"
                    "`/pfc-reload` — Reload data without wiping submissions\n"
                    "`/pfc-best-lineup` — See the perfect lineup (ephemeral)\n"
                    "`/pfc-blacklist` — Add/remove a user from the leaderboard\n"
                    "`/pfc-blacklist-view` — View the current blacklist"
                ),
                inline=False,
            )
        embed.set_footer(text="Each week: assign random players to stat slots. Better stats in the right slots = higher score!")
        await interaction.response.send_message(embed=embed)


# ─────────────────────────────────────────────────────────────────────────────
# SETUP
# ─────────────────────────────────────────────────────────────────────────────

async def setup(bot: commands.Bot):
    await bot.add_cog(PFCCog(bot))