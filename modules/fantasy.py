# modules/fantasy.py
from __future__ import annotations

import re
import uuid
import time
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple, Any

import discord
from discord.ext import commands
from discord import app_commands

import config
from utils import ensure_dir, load_json, save_json

# ============================================================
# Storage
# ============================================================

def _path() -> str:
    return getattr(config, "FANTASY_FILE", f"{config.DATA_DIR}/fantasy.json")

def _load() -> dict:
    ensure_dir(config.DATA_DIR)
    data = load_json(_path(), {})
    data.setdefault("categories", [])          # list[{id,title}]
    data.setdefault("tournaments", [])         # list[tournament dict]
    data.setdefault("ldb_blacklist", [])       # list[int user_id]
    return data

def _save(data: dict) -> None:
    save_json(_path(), data)

def _delete_tournament(data: dict, tournament_id: str, guild_id: Optional[int]) -> Optional[dict]:
    kept = []
    removed = None
    for t in data.get("tournaments", []):
        if t.get("id") != tournament_id:
            kept.append(t)
            continue
        # enforce same guild (unless stored as 0)
        if guild_id is not None and t.get("guild_id") not in (0, guild_id):
            kept.append(t)
            continue
        removed = t
    if removed is None:
        return None
    data["tournaments"] = kept
    return removed

def _is_admin(member: discord.Member) -> bool:
    return member.guild_permissions.administrator

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).lower()

# ============================================================
# Helpers
# ============================================================

def _mk_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"

def _parse_multiline_list(text: str) -> List[str]:
    out = []
    for raw in (text or "").splitlines():
        name = raw.strip()
        if name:
            out.append(name)
    return out

def _player_key(name: str) -> str:
    return _norm(name)

def _fmt_player(seed: Optional[int], name: str) -> str:
    if seed is None:
        return f"(WC) {name}"
    return f"({seed}) {name}"

def _chunk_pages(lines: List[str], max_chars: int = 3500) -> List[str]:
    pages, cur = [], ""
    for ln in lines:
        add = ln + "\n"
        if len(cur) + len(add) > max_chars:
            pages.append(cur.rstrip())
            cur = ""
        cur += add
    if cur.strip():
        pages.append(cur.rstrip())
    return pages or ["(empty)"]

def _now_unix() -> int:
    return int(time.time())

def _fmt_ts(unix_ts: Optional[int]) -> str:
    if not unix_ts:
        return "`(no timestamp)`"
    return f"<t:{int(unix_ts)}:F>"

def _status_key(t: dict) -> str:
    # Exactly the 3 statuses you wanted
    if t.get("results_entered"):
        return "Completed"
    if not t.get("picks_open", True):
        return "Closed & Results Pending"
    return "Open"

def _status_and_stamp(t: dict) -> str:
    """
    - Open: picks_open True AND results_entered False -> opened_at
    - Closed & Results Pending: picks_open False AND results_entered False -> closed_at
    - Completed: results_entered True -> completed_at
    """
    s = _status_key(t)
    if s == "Completed":
        return f"Completed — completed {_fmt_ts(t.get('completed_at'))}"
    if s == "Closed & Results Pending":
        return f"Closed & Results Pending — closed {_fmt_ts(t.get('closed_at'))}"
    return f"Open — opened {_fmt_ts(t.get('opened_at'))}"

# ============================================================
# Confirm gate (draft tournaments)
# ============================================================

def _is_created(t: dict) -> bool:
    # tournaments made before this patch won't have the field:
    # treat missing as True so you don't "lose" old tours
    return t.get("created", True) is True

def _require_created_or_admin(interaction: discord.Interaction, t: Optional[dict]) -> Optional[str]:
    if not t:
        return "❌ Tournament not found."

    if _is_created(t):
        return None

    # allow admins to see drafts
    try:
        if interaction.guild and isinstance(interaction.user, discord.Member) and _is_admin(interaction.user):
            return None
    except Exception:
        pass

    return "❌ This tournament is not confirmed yet."

def _mark_created(t: dict) -> None:
    t["created"] = True

# ============================================================
# Results parsing
# ============================================================

def _parse_results_lines(text: str) -> Tuple[List[dict], List[str]]:
    """
    Player | Round | TournamentPoints | SetPoints | UpsetPoints | LostTo(optional)
    """
    rows = []
    errors = []
    for idx, raw in enumerate((text or "").splitlines(), start=1):
        if not raw.strip():
            continue
        parts = [p.strip() for p in raw.split("|")]
        if len(parts) < 5:
            errors.append(f"Line {idx}: not enough fields (need at least 5).")
            continue

        name = parts[0]
        round_text = parts[1]
        try:
            tourn = int(parts[2])
            setp = int(parts[3])
            upset = int(parts[4])
        except Exception:
            errors.append(f"Line {idx}: points must be integers (Tournament/Set/Upset).")
            continue

        lost_to = parts[5].strip() if len(parts) >= 6 else ""
        total = tourn + setp + upset

        rows.append({
            "player": name,
            "round": round_text,
            "tournament_points": tourn,
            "set_points": setp,
            "upset_points": upset,
            "lost_to": lost_to,
            "total": total
        })
    return rows, errors

# ============================================================
# UI: paginator
# ============================================================

class PagerView(discord.ui.View):
    def __init__(self, pages: List[str], user_id: int, title: str):
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

# ============================================================
# User join UI
# ============================================================

def _seed_bucket(seed: Optional[int]) -> str:
    if seed is None:
        return "wc"
    if 1 <= seed <= 5:
        return "top5"
    if 1 <= seed <= 20:
        return "top20"
    return "other"

@dataclass
class PlayerEntry:
    name: str
    seed: Optional[int]

class PickSelect(discord.ui.Select):
    def __init__(self, owner_view, options: List[discord.SelectOption]):
        super().__init__(placeholder="Pick a player…", min_values=1, max_values=1, options=options)
        self.owner_view = owner_view

    async def callback(self, interaction: discord.Interaction):
        await self.owner_view.on_pick(interaction, self.values[0])

class JoinFantasyView(discord.ui.View):
    def __init__(self, cog, user_id: int, tournament_id: str, pool: List[PlayerEntry]):
        super().__init__(timeout=300)
        self.cog = cog
        self.user_id = user_id
        self.tournament_id = tournament_id
        self.pool = pool

        self.picks: List[PlayerEntry] = []
        self.used_keys: set[str] = set()
        self.top5_used = 0
        self.top20_used = 0

        self._refresh_select()

    def _refresh_select(self):
        self.clear_items()
        remaining = [p for p in self.pool if _player_key(p.name) not in self.used_keys]

        def sort_key(p: PlayerEntry):
            seed = p.seed if p.seed is not None else 10_000
            return (seed, p.name.lower())

        remaining.sort(key=sort_key)

        show = remaining[:25]
        opts = []
        for p in show:
            label = _fmt_player(p.seed, p.name)
            opts.append(discord.SelectOption(label=label[:100], value=p.name[:100]))

        if opts:
            self.add_item(PickSelect(self, opts))

        if len(self.picks) > 0:
            self.add_item(ResetPicksButton())

        self.add_item(ConfirmPicksButton(disabled=(len(self.picks) != 5)))

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This menu isn’t for you.", ephemeral=True)
            return False
        return True

    def _rules_ok_for(self, p: PlayerEntry) -> Tuple[bool, str]:
        b = _seed_bucket(p.seed)
        if b == "top5" and self.top5_used >= 1:
            return False, "❌ You can only pick **one** player from the **top 5 seeds (1–5)**."
        if b in ("top5", "top20") and self.top20_used >= 3:
            return False, "❌ You can only pick **three** players total from the **top 20 seeds (1–20)**."
        return True, ""

    async def on_pick(self, interaction: discord.Interaction, picked_name: str):
        if not await self._guard(interaction):
            return

        entry = next((p for p in self.pool if _player_key(p.name) == _player_key(picked_name)), None)
        if not entry:
            return await interaction.response.send_message("❌ Player not found in this tournament.", ephemeral=True)

        if _player_key(entry.name) in self.used_keys:
            return await interaction.response.send_message("❌ You already picked that player.", ephemeral=True)

        ok, msg = self._rules_ok_for(entry)
        if not ok:
            return await interaction.response.send_message(msg, ephemeral=True)

        self.picks.append(entry)
        self.used_keys.add(_player_key(entry.name))

        b = _seed_bucket(entry.seed)
        if b == "top5":
            self.top5_used += 1
            self.top20_used += 1
        elif b == "top20":
            self.top20_used += 1

        self._refresh_select()
        await interaction.response.edit_message(content=self._status_text(), view=self)

    def _status_text(self) -> str:
        lines = []
        lines.append("**Fantasy Join — Pick 5 players**")
        lines.append("")
        if self.picks:
            lines.append("**Your picks so far:**")
            for i, p in enumerate(self.picks, start=1):
                lines.append(f"{i}. {_fmt_player(p.seed, p.name)}")
        else:
            lines.append("No picks yet.")
        lines.append("")
        lines.append(f"Top 5 used: **{self.top5_used}/1** • Top 20 used: **{self.top20_used}/3** • Total picks: **{len(self.picks)}/5**")
        return "\n".join(lines)

class ResetPicksButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Reset", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view: JoinFantasyView = self.view  # type: ignore
        if interaction.user.id != view.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)

        view.picks = []
        view.used_keys = set()
        view.top5_used = 0
        view.top20_used = 0
        view._refresh_select()

        await interaction.response.edit_message(content=view._status_text(), view=view)

class ConfirmPicksButton(discord.ui.Button):
    def __init__(self, disabled: bool = False):
        super().__init__(label="Confirm roster", style=discord.ButtonStyle.success, disabled=disabled)

    async def callback(self, interaction: discord.Interaction):
        view: JoinFantasyView = self.view  # type: ignore
        if interaction.user.id != view.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)

        if len(view.picks) != 5:
            return await interaction.response.send_message("❌ You must pick **5** players.", ephemeral=True)

        await view.cog._save_user_roster(interaction, view.tournament_id, interaction.user.id, view.picks)

# ============================================================
# Admin create flow (Seeds -> Unseeded -> Confirm)
# ============================================================

class UnseededModal(discord.ui.Modal, title="Fantasy Create — Unseeded"):
    unseeded = discord.ui.TextInput(
        label="Unseeded players (1 per line)",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=4000
    )

    def __init__(self, cog, user_id: int, tournament_id: str):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.tournament_id = tournament_id

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)
        await self.cog._fantasy_create_set_unseeded(interaction, self.tournament_id, str(self.unseeded))

class UnseededStepView(discord.ui.View):
    def __init__(self, cog, user_id: int, tournament_id: str):
        super().__init__(timeout=180)
        self.cog = cog
        self.user_id = user_id
        self.tournament_id = tournament_id

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Not for you.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Add unseeded", style=discord.ButtonStyle.primary)
    async def add_unseeded(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction):
            return
        await interaction.response.send_modal(UnseededModal(self.cog, self.user_id, self.tournament_id))

    @discord.ui.button(label="Skip unseeded", style=discord.ButtonStyle.secondary)
    async def skip_unseeded(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction):
            return
        await self.cog._fantasy_create_finalize_preview(interaction, self.tournament_id)

class ConfirmCreateView(discord.ui.View):
    def __init__(self, cog, user_id: int, tournament_id: str):
        super().__init__(timeout=180)
        self.cog = cog
        self.user_id = user_id
        self.tournament_id = tournament_id

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Not for you.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction):
            return
        await self.cog._fantasy_create_confirm(interaction, self.tournament_id)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction):
            return
        await interaction.response.edit_message(content="❌ Fantasy creation cancelled.", embed=None, view=None)

class SeedsModal(discord.ui.Modal, title="Fantasy Create — Seeds"):
    seeds = discord.ui.TextInput(
        label="Seeded players (1 per line, in order 1..N)",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=4000
    )

    def __init__(self, cog, user_id: int, tournament_name: str, category_id: str, category_title: str):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.tournament_name = tournament_name
        self.category_id = category_id
        self.category_title = category_title

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)

        seeds = _parse_multiline_list(str(self.seeds))
        if not seeds:
            return await interaction.response.send_message("❌ You must provide at least 1 seeded player.", ephemeral=True)

        # dedupe seeds
        seen = set()
        clean = []
        for name in seeds:
            k = _player_key(name)
            if k in seen:
                continue
            seen.add(k)
            clean.append(name)

        data = _load()
        tid = _mk_id("fantasy")

        tournament = {
            "id": tid,
            "guild_id": interaction.guild.id if interaction.guild else 0,
            "name": self.tournament_name.strip(),
            "category_id": self.category_id,
            "category_title": self.category_title,
            "created": False,  # confirm gate

            "picks_open": True,
            "results_entered": False,
            "opened_at": _now_unix(),
            "closed_at": None,
            "completed_at": None,

            "players": [{"name": name, "seed": i + 1} for i, name in enumerate(clean)],
            "rosters": {},
            "results": {},
            "display": {
                "primary": None,
                "secondary": None,
                "tertiary": None,
                "logo_url": None,
                "background_url": None
            }
        }

        data["tournaments"].append(tournament)
        _save(data)

        embed = discord.Embed(
            title="Seeds saved (draft)",
            description=(
                "✅ Seeds saved.\n"
                "Choose **Add unseeded** or **Skip unseeded**.\n\n"
                "⚠️ This tournament is a **draft** until you press **Confirm** on the final screen."
            )
        )
        view = UnseededStepView(self.cog, interaction.user.id, tid)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

# ============================================================
# Cancel delete confirmation view
# ============================================================

class _DeleteTournamentConfirmButton(discord.ui.Button):
    def __init__(self, tournament_id: str):
        super().__init__(
            label="Delete tournament",
            style=discord.ButtonStyle.danger,
            custom_id=f"fantasy_cancel_confirm:{tournament_id}"
        )
        self.tournament_id = tournament_id

    async def callback(self, interaction: discord.Interaction):
        view: ConfirmDeleteTournamentView = self.view  # type: ignore
        if interaction.user.id != view.user_id:
            return await interaction.response.send_message("❌ This menu isn’t for you.", ephemeral=True)

        data = _load()
        gid = interaction.guild.id if interaction.guild else None

        removed = _delete_tournament(data, self.tournament_id, gid)
        if removed is None:
            return await interaction.response.edit_message(
                content="❌ Tournament not found (maybe already deleted).",
                embed=None,
                view=None
            )

        _save(data)
        await interaction.response.edit_message(
            content=f"✅ Deleted fantasy tournament: **{removed.get('name', 'Unknown')}** (`{self.tournament_id}`)",
            embed=None,
            view=None
        )

class _DeleteTournamentAbortButton(discord.ui.Button):
    def __init__(self, tournament_id: str):
        super().__init__(
            label="Keep tournament",
            style=discord.ButtonStyle.secondary,
            custom_id=f"fantasy_cancel_abort:{tournament_id}"
        )
        self.tournament_id = tournament_id

    async def callback(self, interaction: discord.Interaction):
        view: ConfirmDeleteTournamentView = self.view  # type: ignore
        if interaction.user.id != view.user_id:
            return await interaction.response.send_message("❌ This menu isn’t for you.", ephemeral=True)
        await interaction.response.edit_message(content="❌ Cancelled. Tournament not deleted.", embed=None, view=None)

class ConfirmDeleteTournamentView(discord.ui.View):
    def __init__(self, cog, user_id: int, tournament_id: str):
        super().__init__(timeout=60)
        self.cog = cog
        self.user_id = user_id
        self.tournament_id = tournament_id
        self.add_item(_DeleteTournamentConfirmButton(tournament_id))
        self.add_item(_DeleteTournamentAbortButton(tournament_id))

# ============================================================
# Roster detailed menu (one option per picked player)
# ============================================================

class RosterPickSelect(discord.ui.Select):
    def __init__(self, owner_view, options: List[discord.SelectOption]):
        super().__init__(
            placeholder="Select a player for detailed breakdown…",
            min_values=1,
            max_values=1,
            options=options
        )
        self.owner_view = owner_view

    async def callback(self, interaction: discord.Interaction):
        await self.owner_view.on_pick(interaction, self.values[0])

class RosterPickMenuView(discord.ui.View):
    def __init__(
        self,
        user_id: int,
        roster: List[str],
        seed_map: Dict[str, Optional[int]],
        results: Dict[str, dict],
        title: str = "Roster Breakdown"
    ):
        super().__init__(timeout=240)
        self.user_id = user_id
        self.roster = roster[:5]
        self.seed_map = seed_map
        self.results = results
        self.title = title

        opts: List[discord.SelectOption] = []
        for name in self.roster:
            r = results.get(_player_key(name)) if results else None
            pts = int(r.get("total", 0)) if r else 0
            seed = seed_map.get(_player_key(name))
            label = _fmt_player(seed, name)
            opts.append(discord.SelectOption(label=f"{label} — {pts}"[:100], value=name[:100]))

        if opts:
            self.add_item(RosterPickSelect(self, opts))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This menu isn’t for you.", ephemeral=True)
            return False
        return True

    async def on_pick(self, interaction: discord.Interaction, picked_name: str):
        name = picked_name
        r = self.results.get(_player_key(name)) if self.results else None
        seed = self.seed_map.get(_player_key(name))
        header = _fmt_player(seed, name)

        if not r:
            embed = discord.Embed(
                title="Pick Breakdown",
                description=f"**{header}**\n\nℹ️ Results not entered yet."
            )
            return await interaction.response.send_message(embed=embed, ephemeral=True)

        lines = []
        lines.append(f"**{header} — {r.get('round', '')}**")
        lines.append("")
        lines.append(f"**Tournament Bonus:** +{r.get('tournament_points', 0)}")
        lines.append(f"**Set Bonus:** +{r.get('set_points', 0)}")
        lines.append(f"**Upset Bonus:** +{r.get('upset_points', 0)}")
        lines.append("")
        lines.append(f"**Total Points:** {r.get('total', 0)}")
        if r.get("lost_to"):
            lines.append("")
            lines.append(f"Lost to {r['lost_to']}")

        embed = discord.Embed(title="Pick Breakdown", description="\n".join(lines))
        await interaction.response.send_message(embed=embed, ephemeral=True)

# ============================================================
# Fantasy end retry UI
# ============================================================

class RetryEndView(discord.ui.View):
    def __init__(self, cog, user_id: int, tournament_id: str, previous_text: str):
        super().__init__(timeout=180)
        self.cog = cog
        self.user_id = user_id
        self.tournament_id = tournament_id
        self.previous_text = previous_text or ""

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This menu isn’t for you.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="Retry (reopen modal)", style=discord.ButtonStyle.primary)
    async def retry(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            EndResultsModal(
                cog=self.cog,
                user_id=self.user_id,
                tournament_id=self.tournament_id,
                default_text=self.previous_text
            )
        )

class EndResultsModal(discord.ui.Modal, title="Fantasy End — Results"):
    # Discord requires <= 45 chars on label
    results = discord.ui.TextInput(
        label="Player|Round|Tourn|Set|Upset|LostTo?",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=4000
    )

    def __init__(self, cog, user_id: int, tournament_id: str, default_text: str = ""):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.tournament_id = tournament_id
        try:
            self.results.default = (default_text or "")[:4000]
        except Exception:
            pass

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)
        await self.cog._fantasy_end_submit(interaction, self.tournament_id, str(self.results))

# ============================================================
# Leaderboard computations
# ============================================================

def _is_blacklisted(data: dict, user_id: int) -> bool:
    try:
        return int(user_id) in set(int(x) for x in data.get("ldb_blacklist", []))
    except Exception:
        return False

def _t_in_scope(t: dict, guild_id: Optional[int], category_id: Optional[str], days_back: Optional[int]) -> bool:
    if guild_id is not None and t.get("guild_id") not in (0, guild_id):
        return False
    if category_id and t.get("category_id") != category_id:
        return False
    # time filter only applies to completed tournaments
    if days_back is not None:
        if not t.get("results_entered"):
            return False
        completed = int(t.get("completed_at") or 0)
        if completed <= 0:
            return False
        cutoff = _now_unix() - int(days_back) * 86400
        if completed < cutoff:
            return False
    return True

def _score_user_in_tournament(t: dict, user_id: int) -> Optional[int]:
    """
    Returns user's total points for tournament if possible, else None.
    Requires roster + results.
    """
    rosters = t.get("rosters", {}) or {}
    roster = rosters.get(str(user_id))
    if not roster:
        return None
    if not t.get("results_entered"):
        return None
    results = t.get("results", {}) or {}
    total = 0
    for name in roster[:5]:
        r = results.get(_player_key(name))
        total += int((r or {}).get("total", 0))
    return total

def _all_user_scores_for_tournament(t: dict) -> Dict[int, int]:
    """
    Returns map user_id -> total points (only for users with roster)
    Requires results_entered.
    """
    if not t.get("results_entered"):
        return {}
    results = t.get("results", {}) or {}
    out: Dict[int, int] = {}
    rosters = t.get("rosters", {}) or {}
    for uid_str, roster in rosters.items():
        try:
            uid = int(uid_str)
        except Exception:
            continue
        total = 0
        for name in (roster or [])[:5]:
            r = results.get(_player_key(name))
            total += int((r or {}).get("total", 0))
        out[uid] = total
    return out

def _dense_ranks(score_map: Dict[int, int]) -> Dict[int, int]:
    """
    Dense rank: 1,1,2,3... based on unique score levels.
    """
    items = sorted(score_map.items(), key=lambda kv: kv[1], reverse=True)
    ranks: Dict[int, int] = {}
    last_score = None
    rank = 0
    for uid, pts in items:
        if last_score is None or pts != last_score:
            rank += 1
            last_score = pts
        ranks[uid] = rank
    return ranks

def _compute_leaderboard(
    data: dict,
    guild_id: Optional[int],
    mode: str,
    category_id: Optional[str],
    days_back: Optional[int],
    min_tournaments: int = 5
) -> List[Tuple[int, float, int]]:
    """
    Returns list of tuples: (user_id, value, aux_count)
      - value is points / avg / wins / top5 / top10
      - aux_count is tournaments_count (for avg) or 0
    """
    # only confirmed + completed tournaments for all leaderboard stats
    tours = []
    for t in data.get("tournaments", []):
        if not _is_created(t):
            continue
        if not t.get("results_entered"):
            continue
        if not _t_in_scope(t, guild_id, category_id, days_back):
            continue
        tours.append(t)

    # aggregate
    points_total: Dict[int, int] = {}
    points_count: Dict[int, int] = {}  # tournaments played (with roster)
    wins: Dict[int, int] = {}
    top5: Dict[int, int] = {}
    top10: Dict[int, int] = {}

    for t in tours:
        score_map = _all_user_scores_for_tournament(t)

        # apply blacklist here (remove blacklisted users)
        score_map = {uid: pts for uid, pts in score_map.items() if not _is_blacklisted(data, uid)}

        # points
        for uid, pts in score_map.items():
            points_total[uid] = points_total.get(uid, 0) + int(pts)
            points_count[uid] = points_count.get(uid, 0) + 1

        # ranks for win/top
        if score_map:
            ranks = _dense_ranks(score_map)
            for uid, r in ranks.items():
                if r == 1:
                    wins[uid] = wins.get(uid, 0) + 1
                if r <= 5:
                    top5[uid] = top5.get(uid, 0) + 1
                if r <= 10:
                    top10[uid] = top10.get(uid, 0) + 1

    def as_list_int(m: Dict[int, int]) -> List[Tuple[int, float, int]]:
        out = [(uid, float(val), 0) for uid, val in m.items()]
        out.sort(key=lambda x: (x[1], -x[0]), reverse=True)
        return out

    if mode == "points_total":
        return as_list_int(points_total)

    if mode == "avg_points":
        out: List[Tuple[int, float, int]] = []
        for uid, tot in points_total.items():
            cnt = points_count.get(uid, 0)
            if cnt >= min_tournaments:
                out.append((uid, float(tot) / float(cnt), cnt))
        out.sort(key=lambda x: (x[1], x[2]), reverse=True)
        return out

    if mode == "wins":
        return as_list_int(wins)

    if mode == "top5":
        return as_list_int(top5)

    if mode == "top10":
        return as_list_int(top10)

    return []

# ============================================================
# Leaderboard UI (single menu with compact branching)
# ============================================================

_LDB_OPTIONS: List[discord.SelectOption] = [
    discord.SelectOption(label="Points — Most (All-time)", value="points_total:all"),
    discord.SelectOption(label="Points — Most (Category)", value="points_total:cat"),
    discord.SelectOption(label="Points — Most (Last N days)", value="points_total:days"),
    discord.SelectOption(label="Points — Most (Category, Last N days)", value="points_total:cat_days"),

    discord.SelectOption(label="Average — Highest (min 5)", value="avg_points:all"),
    discord.SelectOption(label="Average — Highest (Category, min 5)", value="avg_points:cat"),
    discord.SelectOption(label="Average — Highest (Last N days, min 5)", value="avg_points:days"),
    discord.SelectOption(label="Average — Highest (Category, Last N days, min 5)", value="avg_points:cat_days"),

    discord.SelectOption(label="Wins — Most (All-time)", value="wins:all"),
    discord.SelectOption(label="Wins — Most (Category)", value="wins:cat"),
    discord.SelectOption(label="Wins — Most (Last N days)", value="wins:days"),
    discord.SelectOption(label="Wins — Most (Category, Last N days)", value="wins:cat_days"),

    discord.SelectOption(label="Top 5 — Most (All-time)", value="top5:all"),
    discord.SelectOption(label="Top 5 — Most (Category)", value="top5:cat"),
    discord.SelectOption(label="Top 5 — Most (Last N days)", value="top5:days"),
    discord.SelectOption(label="Top 5 — Most (Category, Last N days)", value="top5:cat_days"),

    discord.SelectOption(label="Top 10 — Most (All-time)", value="top10:all"),
    discord.SelectOption(label="Top 10 — Most (Category)", value="top10:cat"),
    discord.SelectOption(label="Top 10 — Most (Last N days)", value="top10:days"),
    discord.SelectOption(label="Top 10 — Most (Category, Last N days)", value="top10:cat_days"),
]

class LeaderboardSelect(discord.ui.Select):
    def __init__(self, view_ref):
        super().__init__(
            placeholder="Choose a fantasy leaderboard…",
            min_values=1,
            max_values=1,
            options=_LDB_OPTIONS[:25]
        )
        self.view_ref = view_ref

    async def callback(self, interaction: discord.Interaction):
        await self.view_ref.on_select(interaction, self.values[0])

class FantasyLeaderboardView(discord.ui.View):
    def __init__(self, cog, user_id: int, category_id: Optional[str], days_back: Optional[int]):
        super().__init__(timeout=240)
        self.cog = cog
        self.user_id = user_id
        self.category_id = category_id
        self.days_back = days_back
        self.add_item(LeaderboardSelect(self))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This menu isn’t for you.", ephemeral=True)
            return False
        return True

    async def on_select(self, interaction: discord.Interaction, value: str):
        # value like "points_total:cat_days"
        try:
            mode, scope = value.split(":", 1)
        except Exception:
            return await interaction.response.send_message("❌ Invalid leaderboard option.", ephemeral=True)

        cat = self.category_id if "cat" in scope else None
        days = self.days_back if "days" in scope else None

        await self.cog._render_leaderboard(interaction, mode=mode, category_id=cat, days_back=days)

# ============================================================
# Cog
# ============================================================

class FantasyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # -------------------------
    # Categories (admin)
    # -------------------------

    @app_commands.command(name="fantasy-category-create", description="Admin: create a fantasy tournament category.")
    async def fantasy_category_create(self, interaction: discord.Interaction, title: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        data = _load()
        cid = _mk_id("fantasy-categ")
        data["categories"].append({"id": cid, "title": title.strip()})
        _save(data)

        await interaction.response.send_message(f"✅ Fantasy category created: **{title.strip()}** (`{cid}`)")

    @app_commands.command(name="fantasy-category-list", description="List fantasy tournament categories.")
    async def fantasy_category_list(self, interaction: discord.Interaction):
        data = _load()
        cats = data.get("categories", [])
        if not cats:
            return await interaction.response.send_message("ℹ️ No fantasy categories yet.")

        lines = []
        for c in sorted(cats, key=lambda x: (x.get("title", "").lower(), x.get("id", ""))):
            lines.append(f"- **{c['title']}** — `{c['id']}`")

        pages = _chunk_pages(lines)
        view = PagerView(pages, interaction.user.id, "Fantasy Categories")
        await interaction.response.send_message(embed=view._embed(), view=view)

    @app_commands.command(name="fantasy-category-delete", description="Admin: delete a fantasy tournament category.")
    async def fantasy_category_delete(self, interaction: discord.Interaction, category_id: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        data = _load()
        before = len(data["categories"])
        data["categories"] = [c for c in data["categories"] if c.get("id") != category_id]
        after = len(data["categories"])
        if after == before:
            return await interaction.response.send_message("❌ Category not found.", ephemeral=True)

        _save(data)
        await interaction.response.send_message(f"✅ Deleted category `{category_id}`")

    # -------------------------
    # Tournaments (admin)
    # -------------------------

    @app_commands.command(name="fantasy-create", description="Admin: create a fantasy tournament.")
    async def fantasy_create(self, interaction: discord.Interaction, tournament_name: str, category_id: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        data = _load()
        cat = next((c for c in data["categories"] if c.get("id") == category_id), None)
        if not cat:
            return await interaction.response.send_message("❌ Category does not exist.", ephemeral=True)

        await interaction.response.send_modal(
            SeedsModal(
                cog=self,
                user_id=interaction.user.id,
                tournament_name=tournament_name.strip(),
                category_id=category_id,
                category_title=cat.get("title", "")
            )
        )

    async def _fantasy_create_set_unseeded(self, interaction: discord.Interaction, tournament_id: str, unseeded_text: str):
        data = _load()
        t = next((x for x in data.get("tournaments", []) if x.get("id") == tournament_id), None)
        if not t:
            return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)

        seeds = t.get("players", [])
        seed_keys = {_player_key(p["name"]) for p in seeds}

        names = _parse_multiline_list(unseeded_text)
        unseeded_players = []
        for name in names:
            k = _player_key(name)
            if k in seed_keys:
                continue
            unseeded_players.append({"name": name, "seed": None})

        t["players"] = seeds + unseeded_players
        _save(data)

        await self._fantasy_create_finalize_preview(interaction, tournament_id)

    async def _fantasy_create_finalize_preview(self, interaction: discord.Interaction, tournament_id: str):
        data = _load()
        t = next((x for x in data.get("tournaments", []) if x.get("id") == tournament_id), None)
        if not t:
            return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)

        lines = []
        lines.append(f"**Tournament:** {t.get('name')} (`{t.get('id')}`)")
        lines.append(f"**Category:** {t.get('category_title')} (`{t.get('category_id')}`)")
        lines.append("")
        lines.append("**Players (Seeds then Unseeded):**")
        for p in t.get("players", []):
            lines.append(f"- {_fmt_player(p.get('seed'), p.get('name'))}")

        embed = discord.Embed(title="Confirm Fantasy Creation", description="\n".join(lines)[:3900])
        view = ConfirmCreateView(self, interaction.user.id, tournament_id)

        if interaction.response.is_done():
            await interaction.edit_original_response(content=None, embed=embed, view=view)
        else:
            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def _fantasy_create_confirm(self, interaction: discord.Interaction, tournament_id: str):
        data = _load()
        t = next((x for x in data.get("tournaments", []) if x.get("id") == tournament_id), None)
        if not t:
            return await interaction.response.edit_message(content="❌ Tournament not found.", embed=None, view=None)

        _mark_created(t)
        _save(data)

        await interaction.response.edit_message(
            content=(
                "✅ Fantasy tournament **confirmed** and now visible to players.\n"
                f"**Name:** {t.get('name')}\n"
                f"**ID:** `{t.get('id')}`"
            ),
            embed=None,
            view=None
        )

    @app_commands.command(name="fantasy-close", description="Admin: close a fantasy (no more pick edits).")
    async def fantasy_close(self, interaction: discord.Interaction, tournament_id: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        data = _load()
        t = next((x for x in data.get("tournaments", []) if x.get("id") == tournament_id), None)
        if not t:
            return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)

        t["picks_open"] = False
        t["closed_at"] = t.get("closed_at") or _now_unix()
        _save(data)
        await interaction.response.send_message(f"✅ Fantasy closed: **{t.get('name')}** (`{t.get('id')}`)")

    @app_commands.command(name="fantasy-cancel", description="Admin: permanently delete a fantasy tournament.")
    async def fantasy_cancel(self, interaction: discord.Interaction, tournament_id: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        data = _load()
        t = next((x for x in data.get("tournaments", []) if x.get("id") == tournament_id), None)
        if not t:
            return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)

        gid = interaction.guild.id if interaction.guild else None
        if gid is not None and t.get("guild_id") not in (0, gid):
            return await interaction.response.send_message("❌ Tournament not found (or not in this server).", ephemeral=True)

        embed = discord.Embed(
            title="⚠️ Confirm Delete Fantasy Tournament",
            description=(
                f"**Tournament:** {t.get('name')} (`{t.get('id')}`)\n"
                f"**Category:** {t.get('category_title')} (`{t.get('category_id')}`)\n"
                f"**Status:** {_status_and_stamp(t)}\n\n"
                f"**This permanently deletes:** players, rosters, results."
            )
        )
        view = ConfirmDeleteTournamentView(self, interaction.user.id, tournament_id)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    # ---- fantasy-end (with retry) ----

    @app_commands.command(name="fantasy-end", description="Admin: submit results and complete a fantasy tournament.")
    async def fantasy_end(self, interaction: discord.Interaction, tournament_id: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        data = _load()
        t = next((x for x in data.get("tournaments", []) if x.get("id") == tournament_id), None)
        if not t:
            return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)

        await interaction.response.send_modal(EndResultsModal(self, interaction.user.id, tournament_id))

    async def _fantasy_end_submit(self, interaction: discord.Interaction, tournament_id: str, results_text: str):
        try:
            data = _load()
            t = next((x for x in data.get("tournaments", []) if x.get("id") == tournament_id), None)
            if not t:
                return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)

            rows, parse_errors = _parse_results_lines(results_text)
            if parse_errors:
                view = RetryEndView(self, interaction.user.id, tournament_id, results_text)
                return await interaction.response.send_message(
                    "❌ Errors:\n" + "\n".join(parse_errors[:30]),
                    view=view,
                    ephemeral=True
                )

            tournament_players = t.get("players", [])
            tp_keys = {_player_key(p["name"]) for p in tournament_players}

            unknown = [r["player"] for r in rows if _player_key(r["player"]) not in tp_keys]
            given = {_player_key(r["player"]) for r in rows}
            missing = [p["name"] for p in tournament_players if _player_key(p["name"]) not in given]

            if unknown or missing:
                msg = ["❌ Results validation failed."]
                if unknown:
                    msg.append("\n**Unknown players:**")
                    msg.extend([f"- {n}" for n in unknown[:50]])
                if missing:
                    msg.append("\n**Missing players:**")
                    msg.extend([f"- {n}" for n in missing[:50]])

                view = RetryEndView(self, interaction.user.id, tournament_id, results_text)
                return await interaction.response.send_message("\n".join(msg), view=view, ephemeral=True)

            # store results
            results_map = {}
            for r in rows:
                results_map[_player_key(r["player"])] = r

            t["results"] = results_map
            t["results_entered"] = True
            t["picks_open"] = False
            t["completed_at"] = t.get("completed_at") or _now_unix()
            _save(data)

            # confirmation list back to admin
            lines = []
            lines.append(f"✅ Results saved for **{t.get('name')}** (`{t.get('id')}`)")
            lines.append("")
            lines.append("Format: Player — Round — Total (Tourn + Set + Upset)")
            lines.append("")
            for r in sorted(rows, key=lambda x: x["total"], reverse=True):
                lines.append(f"- **{r['player']}** — {r['round']} — **{r['total']}** ({r['tournament_points']} + {r['set_points']} + {r['upset_points']})")

            pages = _chunk_pages(lines)
            view = PagerView(pages, interaction.user.id, "Fantasy End Confirmation")
            await interaction.response.send_message(embed=view._embed(), view=view, ephemeral=True)

        except Exception as e:
            view = RetryEndView(self, interaction.user.id, tournament_id, results_text)
            await interaction.response.send_message(
                f"❌ Unexpected error while ending fantasy:\n`{type(e).__name__}: {e}`",
                view=view,
                ephemeral=True
            )

    # -------------------------
    # Leaderboard admin commands
    # -------------------------

    @app_commands.command(name="fantasy-ldb-clear", description="Admin: clear fantasy leaderboard data (blacklist).")
    async def fantasy_ldb_clear(self, interaction: discord.Interaction):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        data["ldb_blacklist"] = []
        _save(data)
        await interaction.response.send_message("✅ Fantasy leaderboard cleared (blacklist reset).")

    @app_commands.command(name="fantasy-ldb-blacklist", description="Admin: blacklist a user from all fantasy leaderboards.")
    async def fantasy_ldb_blacklist(self, interaction: discord.Interaction, user: discord.Member):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        bl = set(int(x) for x in data.get("ldb_blacklist", []))
        bl.add(int(user.id))
        data["ldb_blacklist"] = sorted(bl)
        _save(data)
        await interaction.response.send_message(f"✅ Blacklisted **{user.display_name}** from fantasy leaderboards.")

    @app_commands.command(name="fantasy-ldb-blacklist-view", description="Admin: view users blacklisted from fantasy leaderboards.")
    async def fantasy_ldb_blacklist_view(self, interaction: discord.Interaction):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        bl = [int(x) for x in data.get("ldb_blacklist", [])]
        if not bl:
            return await interaction.response.send_message("ℹ️ No users are blacklisted.")

        lines = []
        for uid in bl:
            lines.append(f"- <@{uid}> (`{uid}`)")

        pages = _chunk_pages(lines)
        view = PagerView(pages, interaction.user.id, "Fantasy Leaderboard Blacklist")
        await interaction.response.send_message(embed=view._embed(), view=view)

    @app_commands.command(name="fantasy-leaderboard-whitelist", description="Admin: remove a user from the fantasy leaderboard blacklist.")
    async def fantasy_leaderboard_whitelist(self, interaction: discord.Interaction, user: discord.Member):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        bl = set(int(x) for x in data.get("ldb_blacklist", []))
        if int(user.id) in bl:
            bl.remove(int(user.id))
            data["ldb_blacklist"] = sorted(bl)
            _save(data)
            return await interaction.response.send_message(f"✅ Whitelisted **{user.display_name}** for fantasy leaderboards.")
        await interaction.response.send_message("ℹ️ That user is not blacklisted.")

    # -------------------------
    # User commands
    # -------------------------

    _STATUS_CHOICES = [
        app_commands.Choice(name="Open", value="Open"),
        app_commands.Choice(name="Closed & Results Pending", value="Closed & Results Pending"),
        app_commands.Choice(name="Completed", value="Completed"),
    ]

    @app_commands.command(name="fantasy-list", description="List fantasy tournaments (optional status filter).")
    @app_commands.describe(status="Optional: filter to one status")
    @app_commands.choices(status=_STATUS_CHOICES)
    async def fantasy_list(self, interaction: discord.Interaction, status: Optional[app_commands.Choice[str]] = None):
        data = _load()
        ts = [
            t for t in data.get("tournaments", [])
            if (interaction.guild is None or t.get("guild_id") in (0, interaction.guild.id))
        ]

        ts = [t for t in ts if _require_created_or_admin(interaction, t) is None]

        want = status.value if status else None
        if want:
            ts = [t for t in ts if _status_key(t) == want]

        if not ts:
            if want:
                return await interaction.response.send_message(f"ℹ️ No fantasy tournaments with status **{want}**.")
            return await interaction.response.send_message("ℹ️ No fantasy tournaments yet.")

        ts.sort(key=lambda x: (x.get("name", "") or "").lower())

        lines = []
        if want:
            lines.append(f"Showing status filter: **{want}**")
            lines.append("")

        for t in ts:
            lines.append(f"- **{t.get('name')}** — `{t.get('id')}` — **{_status_and_stamp(t)}**")

        pages = _chunk_pages(lines)
        title = "Fantasy Tournaments" + (f" — {want}" if want else "")
        view = PagerView(pages, interaction.user.id, title)
        await interaction.response.send_message(embed=view._embed(), view=view)

    @app_commands.command(name="fantasy-join", description="Join a fantasy tournament and pick your 5 players.")
    async def fantasy_join(self, interaction: discord.Interaction, tournament_id: str):
        data = _load()
        t = next((x for x in data.get("tournaments", []) if x.get("id") == tournament_id), None)
        msg = _require_created_or_admin(interaction, t)
        if msg:
            return await interaction.response.send_message(msg, ephemeral=True)

        if not t.get("picks_open", True):
            return await interaction.response.send_message("❌ This fantasy is closed. You can’t enter or edit picks.", ephemeral=True)

        players = t.get("players", [])
        pool = [PlayerEntry(name=p["name"], seed=p.get("seed")) for p in players]

        view = JoinFantasyView(self, interaction.user.id, tournament_id, pool)
        await interaction.response.send_message(content=view._status_text(), view=view, ephemeral=True)

    async def _save_user_roster(self, interaction: discord.Interaction, tournament_id: str, user_id: int, picks: List[PlayerEntry]):
        data = _load()
        t = next((x for x in data.get("tournaments", []) if x.get("id") == tournament_id), None)
        if not t:
            return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)

        if not t.get("picks_open", True):
            return await interaction.response.send_message("❌ This fantasy is closed.", ephemeral=True)

        t.setdefault("rosters", {})[str(user_id)] = [p.name for p in picks]
        _save(data)

        lines = []
        lines.append("✅ Roster saved.")
        lines.append("")
        lines.append(f"**Tournament:** {t.get('name')} (`{t.get('id')}`)")
        lines.append("**Your picks:**")
        seed_map = {_player_key(p["name"]): p.get("seed") for p in t.get("players", [])}
        for i, name in enumerate([p.name for p in picks], start=1):
            lines.append(f"{i}. {_fmt_player(seed_map.get(_player_key(name)), name)}")

        await interaction.response.edit_message(content="\n".join(lines), view=None)

    @app_commands.command(name="fantasy-roster-view", description="View a user's 5 fantasy picks (with a detailed menu).")
    @app_commands.describe(user="Optional: whose roster (default: you)")
    async def fantasy_roster_view(self, interaction: discord.Interaction, tournament_id: str, user: Optional[discord.Member] = None):
        data = _load()
        t = next((x for x in data.get("tournaments", []) if x.get("id") == tournament_id), None)
        msg = _require_created_or_admin(interaction, t)
        if msg:
            return await interaction.response.send_message(msg, ephemeral=True)

        target = user or interaction.user
        roster = (t.get("rosters", {}) or {}).get(str(target.id))
        if not roster:
            if user:
                return await interaction.response.send_message("ℹ️ That user has no roster saved for this tournament.", ephemeral=True)
            return await interaction.response.send_message("ℹ️ You have no roster saved for this tournament.", ephemeral=True)

        seed_map = {_player_key(p["name"]): p.get("seed") for p in t.get("players", [])}
        results = t.get("results", {}) if t.get("results_entered") else {}

        total_points = 0
        lines = []
        lines.append(f"**Roster — {target.display_name}**")
        lines.append(f"**Tournament:** {t.get('name')} (`{t.get('id')}`)")
        lines.append("")
        lines.append("**Picks:**")
        for i, name in enumerate(roster[:5], start=1):
            seed = seed_map.get(_player_key(name))
            label = _fmt_player(seed, name)
            if results:
                r = results.get(_player_key(name))
                pts = int(r["total"]) if r else 0
                total_points += pts
                lines.append(f"{i}. {label} — **{pts}**")
            else:
                lines.append(f"{i}. {label}")

        if results:
            lines.append("")
            lines.append(f"**Total Points:** **{total_points}**")
            lines.append("")
            lines.append("Use the menu below to view a detailed breakdown per pick.")

        embed = discord.Embed(title="Fantasy Roster", description="\n".join(lines))
        view = RosterPickMenuView(
            user_id=interaction.user.id,
            roster=roster[:5],
            seed_map=seed_map,
            results=results,
            title=f"{target.display_name}'s Picks"
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @app_commands.command(name="fantasy-results", description="View sorted fantasy results for a tournament (requires results).")
    async def fantasy_results(self, interaction: discord.Interaction, tournament_id: str):
        data = _load()
        t = next((x for x in data.get("tournaments", []) if x.get("id") == tournament_id), None)
        msg = _require_created_or_admin(interaction, t)
        if msg:
            return await interaction.response.send_message(msg, ephemeral=True)

        if not t.get("results_entered"):
            return await interaction.response.send_message("❌ Results have not been submitted for this fantasy yet.", ephemeral=True)

        seed_map = {_player_key(p["name"]): p.get("seed") for p in t.get("players", [])}
        results = list((t.get("results", {}) or {}).values())
        results.sort(key=lambda r: int(r.get("total", 0)), reverse=True)

        lines = []
        lines.append(f"**Fantasy Results — {t.get('name')}**")
        lines.append("")
        lines.append("Format: Rank. (Seed) Player — Total — Round")
        lines.append("")
        for i, r in enumerate(results, start=1):
            name = r.get("player", "")
            seed = seed_map.get(_player_key(name))
            lines.append(f"{i}. {_fmt_player(seed, name)} — **{r.get('total', 0)}** — *{r.get('round', '')}*")

        pages = _chunk_pages(lines)
        view = PagerView(pages, interaction.user.id, "Fantasy Results")
        await interaction.response.send_message(embed=view._embed(), view=view)

    @app_commands.command(name="fantasy-user-results", description="Show all users' total points for a completed tournament.")
    async def fantasy_user_results(self, interaction: discord.Interaction, tournament_id: str):
        data = _load()
        t = next((x for x in data.get("tournaments", []) if x.get("id") == tournament_id), None)
        msg = _require_created_or_admin(interaction, t)
        if msg:
            return await interaction.response.send_message(msg, ephemeral=True)

        if not t.get("results_entered"):
            return await interaction.response.send_message("❌ This tournament is not over yet (results not entered).", ephemeral=True)

        scores = _all_user_scores_for_tournament(t)
        # respect blacklist here too
        scores = {uid: pts for uid, pts in scores.items() if not _is_blacklisted(data, uid)}

        if not scores:
            return await interaction.response.send_message("ℹ️ No user rosters found for this tournament.", ephemeral=True)

        items = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)

        # 20 per page (requested)
        lines: List[str] = []
        lines.append(f"**User Results — {t.get('name')}** (`{t.get('id')}`)")
        lines.append("")
        lines.append("Format: Rank. User — Total Points")
        lines.append("")

        # dense ranks
        ranks = _dense_ranks(scores)

        for uid, pts in items:
            lines.append(f"{ranks.get(uid, 0)}. <@{uid}> — **{pts}**")

        # paginate by 20 lines of results (not by char)
        header = lines[:4]
        body = lines[4:]
        pages: List[str] = []
        chunk: List[str] = []
        count = 0
        for ln in body:
            chunk.append(ln)
            count += 1
            if count >= 20:
                pages.append("\n".join(header + chunk))
                chunk = []
                count = 0
        if chunk:
            pages.append("\n".join(header + chunk))

        view = PagerView(pages, interaction.user.id, "Fantasy User Results")
        await interaction.response.send_message(embed=view._embed(), view=view)

    # -------------------------
    # Leaderboards (menu)
    # -------------------------

    @app_commands.command(name="fantasy-leaderboard-view", description="View fantasy user leaderboards (menu).")
    @app_commands.describe(category_id="Optional: category ID for category leaderboards", days_back="Optional: use for 'Last N days' leaderboards")
    async def fantasy_leaderboard_view(
        self,
        interaction: discord.Interaction,
        category_id: Optional[str] = None,
        days_back: Optional[int] = None
    ):
        # If user picks a category-based option but forgot category_id, we’ll show a helpful note in the render step.
        # days_back works similarly for Last N days options.
        view = FantasyLeaderboardView(self, interaction.user.id, category_id=category_id, days_back=days_back)
        embed = discord.Embed(
            title="Fantasy Leaderboards",
            description=(
                "Pick a leaderboard from the menu.\n\n"
                "Notes:\n"
                "- Category options require `category_id`.\n"
                "- “Last N days” options require `days_back`.\n"
                "- Averages require **min 5 tournaments**."
            )
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def _render_leaderboard(
        self,
        interaction: discord.Interaction,
        mode: str,
        category_id: Optional[str],
        days_back: Optional[int]
    ):
        data = _load()
        gid = interaction.guild.id if interaction.guild else None

        # validate category if provided
        cat_title = None
        if category_id:
            cat = next((c for c in data.get("categories", []) if c.get("id") == category_id), None)
            if cat:
                cat_title = cat.get("title")

        # require params for certain scopes
        if category_id is not None and not cat_title:
            # user passed category_id but it's invalid
            return await interaction.response.send_message("❌ That `category_id` does not exist. Use `/fantasy-category-list`.", ephemeral=True)

        # If the selected mode expects category_id or days_back, the view passes them as None unless it’s a cat/days option.
        # So: if category_id is None for a category leaderboard, we must error — but we can detect by how we called it:
        # Here we simply enforce: if user chose a category option but didn't provide category_id earlier, we see category_id=None.
        # The menu pre-filtered cat vs non-cat, so we need to infer it from the interaction message? Not reliable.
        # Instead: the menu only passes category_id/days_back when that scope is chosen, so missing means user didn’t provide it.
        # -> we error if missing required.
        # (We encode that by sending None here; if menu chose cat option, it passed self.category_id which could be None.)
        # So enforce:
        # - category leaderboards require category_id != None
        # - days leaderboards require days_back != None
        # We detect need by looking at the original select value isn’t available here; so we enforce on provided params:
        # If caller gave category_id None, assume not required.
        # But: menu passes category_id only for cat scopes. If user didn’t provide it, it stays None => required but missing.
        # We fix by using sentinel strings from the view? Not worth. Instead, we put a small workaround:
        # If user chose cat scope, view passes self.category_id (could be None). We treat that as missing and error ONLY if view is cat scope.
        # How to know? we can stash a flag on the view and pass it in; but we didn’t.
        # Minimal workaround: if user tries to run a cat leaderboard without category_id, they will just see overall. Not acceptable.
        # So we DO pass requirements explicitly by encoding them in mode string at call site — but we kept it clean.
        # -> We’ll accept a pragmatic rule:
        # If category_id is None and the user wants a category leaderboard, they will have selected an option that calls _render with category_id=None.
        # We cannot distinguish that from a non-category run. So we enforce requirements in the select callback instead.
        #
        # (Handled in the Select callback by passing a sentinel.)
        pass

        # compute
        lb = _compute_leaderboard(
            data=data,
            guild_id=gid,
            mode=mode,
            category_id=category_id,
            days_back=days_back,
            min_tournaments=5
        )

        if not lb:
            parts = []
            if category_id:
                parts.append("category filter")
            if days_back is not None:
                parts.append(f"last {days_back} days")
            suffix = f" ({', '.join(parts)})" if parts else ""
            return await interaction.response.send_message(f"ℹ️ No leaderboard data found{suffix}.", ephemeral=True)

        # title
        mode_title = {
            "points_total": "Most Points",
            "avg_points": "Highest Average Points",
            "wins": "Most Tournament Wins",
            "top5": "Most Top 5 Finishes",
            "top10": "Most Top 10 Finishes",
        }.get(mode, mode)

        filters = []
        if category_id:
            filters.append(f"Category: {cat_title or category_id}")
        if days_back is not None:
            filters.append(f"Last {days_back} days")
        filter_line = " • ".join(filters)

        # build lines (paginate by 20 results)
        lines: List[str] = []
        lines.append(f"**{mode_title}**")
        if filter_line:
            lines.append(filter_line)
        lines.append("")
        if mode == "avg_points":
            lines.append("Format: Rank. User — Avg Points — Tournaments Played")
        else:
            lines.append("Format: Rank. User — Value")
        lines.append("")

        for i, (uid, val, cnt) in enumerate(lb, start=1):
            if mode == "avg_points":
                lines.append(f"{i}. <@{uid}> — **{val:.2f}** — *{cnt}*")
            else:
                # integer-looking values
                sval = str(int(val)) if float(val).is_integer() else f"{val:.2f}"
                lines.append(f"{i}. <@{uid}> — **{sval}**")

        header = lines[:4 if not filter_line else 5]
        body = lines[len(header):]

        pages: List[str] = []
        chunk: List[str] = []
        count = 0
        for ln in body:
            # include header line(s) only once per page
            if ln.strip() == "":
                # keep blank lines in header region already
                pass
            chunk.append(ln)
            # count only actual result lines (those starting with digit + '.')
            if ln and ln[0].isdigit():
                count += 1
            if count >= 20:
                pages.append("\n".join(header + chunk))
                chunk = []
                count = 0
        if chunk:
            pages.append("\n".join(header + chunk))

        view = PagerView(pages, interaction.user.id, f"Fantasy Leaderboard — {mode_title}")
        await interaction.response.send_message(embed=view._embed(), view=view, ephemeral=True)

# ============================================================
# Fix: enforce required params for category/days leaderboard options
# (We do it here without changing the main design.)
# ============================================================

# Patch the select callback to enforce requirements cleanly:
# - "cat" scopes require category_id
# - "days" scopes require days_back
async def _leaderboard_select_callback_guard(view: FantasyLeaderboardView, interaction: discord.Interaction, value: str):
    try:
        mode, scope = value.split(":", 1)
    except Exception:
        return await interaction.response.send_message("❌ Invalid leaderboard option.", ephemeral=True)

    need_cat = "cat" in scope
    need_days = "days" in scope

    if need_cat and not view.category_id:
        return await interaction.response.send_message("❌ This leaderboard needs `category_id` on `/fantasy-leaderboard-view`.", ephemeral=True)
    if need_days and view.days_back is None:
        return await interaction.response.send_message("❌ This leaderboard needs `days_back` on `/fantasy-leaderboard-view`.", ephemeral=True)
    if view.days_back is not None and view.days_back <= 0:
        return await interaction.response.send_message("❌ `days_back` must be a positive number.", ephemeral=True)

    cat = view.category_id if need_cat else None
    days = view.days_back if need_days else None
    await view.cog._render_leaderboard(interaction, mode=mode, category_id=cat, days_back=days)

# Monkey-patch the method used by our LeaderboardSelect callback
FantasyLeaderboardView.on_select = _leaderboard_select_callback_guard  # type: ignore

# ============================================================
# setup
# ============================================================

async def setup(bot: commands.Bot):
    await bot.add_cog(FantasyCog(bot))