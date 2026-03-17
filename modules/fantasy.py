# modules/fantasy.py
from __future__ import annotations

import re
import uuid
import time
import os
import asyncio
from dataclasses import dataclass
from typing import Optional, List, Dict, Tuple, Any

import aiohttp
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
    data.setdefault("categories", [])
    data.setdefault("tournaments", [])
    data.setdefault("ldb_blacklist", [])
    return data

def _save(data: dict) -> None:
    p = _path()
    print(f"[fantasy] _save path={p!r} tournaments={[t.get('id') for t in data.get('tournaments',[])]}")
    save_json(p, data)
    # Verify write succeeded (Railway ephemeral FS guard)
    try:
        check = load_json(p, None)
        if check is None:
            print(f"[fantasy] WARNING: save to {p!r} may not have persisted!")
    except Exception as e:
        print(f"[fantasy] WARNING: verify-read failed: {e}")

# Log path on import so Railway logs show exactly where data lives
print(f"[fantasy] DATA_PATH={_path()!r}")

def _delete_tournament(data: dict, tournament_id: str, guild_id: Optional[int]) -> Optional[dict]:
    kept = []
    removed = None
    for t in data.get("tournaments", []):
        if t.get("id") != tournament_id:
            kept.append(t); continue
        if guild_id is not None and t.get("guild_id") not in (0, guild_id):
            kept.append(t); continue
        removed = t
    if removed is None:
        return None
    data["tournaments"] = kept
    return removed


def _find_tournament(data: dict, tournament_id: str) -> Optional[dict]:
    """Find tournament by ID, falling back to case-insensitive name match."""
    ts = data.get("tournaments", [])
    # Exact ID match first
    t = next((x for x in ts if x.get("id") == tournament_id), None)
    if t:
        return t
    # Name fallback (for when autocomplete times out and user typed the name)
    q = tournament_id.strip().lower()
    return next((x for x in ts if x.get("name", "").strip().lower() == q), None)

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
    if t.get("results_entered"):
        return "Completed"
    if not t.get("picks_open", True):
        return "Closed & Results Pending"
    return "Open"

def _status_and_stamp(t: dict) -> str:
    s = _status_key(t)
    if s == "Completed":
        return f"Completed — completed {_fmt_ts(t.get('completed_at'))}"
    if s == "Closed & Results Pending":
        return f"Closed & Results Pending — closed {_fmt_ts(t.get('closed_at'))}"
    return f"Open — opened {_fmt_ts(t.get('opened_at'))}"

# ============================================================
# Confirm gate
# ============================================================

def _is_created(t: dict) -> bool:
    return t.get("created", True) is True

def _require_created_or_admin(interaction: discord.Interaction, t: Optional[dict]) -> Optional[str]:
    if not t:
        return "❌ Tournament not found."
    if _is_created(t):
        return None
    try:
        if interaction.guild and isinstance(interaction.user, discord.Member) and _is_admin(interaction.user):
            return None
    except Exception:
        pass
    return "❌ This tournament is not confirmed yet."

def _mark_created(t: dict) -> None:
    t["created"] = True

# ============================================================
# Round name helpers
# ============================================================

ROUND_CANONICAL = ["Champion", "Finalist", "Semi-Final", "Quarter-Final", "R16", "R32", "R64", "R128"]

# Higher index = earlier exit (used for determining furthest round reached)
ROUND_ORDER: Dict[str, int] = {r: i for i, r in enumerate(reversed(ROUND_CANONICAL))}

_ROUND_ALIASES: Dict[str, str] = {
    "champion": "Champion", "winner": "Champion", "w": "Champion",
    "finalist": "Finalist", "final": "Finalist", "f": "Finalist",
    "semi-final": "Semi-Final", "semifinal": "Semi-Final", "semi": "Semi-Final", "sf": "Semi-Final",
    "quarter-final": "Quarter-Final", "quarterfinal": "Quarter-Final", "quarter": "Quarter-Final", "qf": "Quarter-Final",
    "r16": "R16", "round of 16": "R16", "ro16": "R16",
    "r32": "R32", "round of 32": "R32", "ro32": "R32",
    "r64": "R64", "round of 64": "R64", "ro64": "R64",
    "r128": "R128", "round of 128": "R128", "ro128": "R128",
    # API-style names
    "1st round": "R64", "2nd round": "R32", "3rd round": "R16",
    "round of 64": "R64", "round of 32": "R32", "round of 16": "R16",
    "quarterfinals": "Quarter-Final", "semifinals": "Semi-Final",
    "the final": "Finalist",
}

def _normalize_round(s: str) -> Optional[str]:
    return _ROUND_ALIASES.get(_norm(s))

def _parse_round_points_text(text: str) -> Tuple[Dict[str, int], List[str]]:
    """
    Accept either:
      A) 'Round: Points' format  (e.g. 'Champion: 500')
      B) Plain numbers, one per line in canonical round order
         (Champion, Finalist, Semi-Final, Quarter-Final, R16, R32, R64, R128)
    """
    lines = [l.strip() for l in (text or "").splitlines() if l.strip()]
    if not lines:
        return {}, []

    # Detect format: if first non-empty line has no ':' and is a plain number → plain mode
    plain_mode = all(re.match(r'^-?\d+$', l) for l in lines)

    result: Dict[str, int] = {}
    errors: List[str] = []

    if plain_mode:
        for i, line in enumerate(lines):
            if i >= len(ROUND_CANONICAL):
                errors.append(f"Line {i+1}: more values than rounds ({len(ROUND_CANONICAL)} max)")
                break
            try:
                result[ROUND_CANONICAL[i]] = int(line)
            except ValueError:
                errors.append(f"Line {i+1}: expected integer, got {line!r}")
    else:
        for idx, line in enumerate(lines, start=1):
            if ":" not in line:
                errors.append(f"Line {idx}: expected 'Round: Points' format, got: {line!r}")
                continue
            key_raw, val_raw = line.split(":", 1)
            canonical = _normalize_round(key_raw.strip())
            if canonical is None:
                errors.append(f"Line {idx}: unrecognised round {key_raw.strip()!r}. Valid: {', '.join(ROUND_CANONICAL)}")
                continue
            try:
                result[canonical] = int(val_raw.strip())
            except ValueError:
                errors.append(f"Line {idx}: points must be an integer, got {val_raw.strip()!r}")

    return result, errors

def _get_tournament_points(data: dict, category_id: str, round_key: str) -> Optional[int]:
    cat = next((c for c in data.get("categories", []) if c.get("id") == category_id), None)
    if not cat:
        return None
    return cat.get("round_points", {}).get(round_key)


# ============================================================
# Results parsing
# ============================================================

def _parse_results_lines(text: str) -> Tuple[List[dict], List[str]]:
    """
    Parse results pasted from Claude chat.
    Short : Player | Round
    Full  : Player | Round | sets_won | sets_lost | performance_pts | upset_pts
    Full+ : Player | Round | sets_won | sets_lost | performance_pts | upset_pts | match log text
    match log is semicolon-separated match summaries, e.g.:
      d. Djokovic 6-3 7-5 +120; d. Zverev 4-6 6-3 7-5 +80

    SCORING FORMULAS (calculated externally, pasted in):
      Set pts (auto): sets_won * 5 - sets_lost * 2
      Performance pts (favourite wins, player_rank < opp_rank):
        sets_won^3 * (player_rank/opp_rank)^0.5 * dominance * 25
      Upset pts WIN (underdog, player_rank > opp_rank):
        sets_won^3 * (player_rank/opp_rank)^1.5 * dominance * 3
      Upset pts LOSS: sets_won * (player_rank/opp_rank)^1.5 * dominance * 0.6
      Dominance = player_games_won / total_games_in_match
      W/O and retirements: 0 performance/upset pts, set pts only
    """
    rows = []
    errors = []
    for idx, raw in enumerate((text or "").splitlines(), start=1):
        line = raw.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split("|")]
        if len(parts) < 2:
            errors.append(f"Line {idx}: need at least: Player | Round")
            continue
        name       = parts[0]
        round_text = parts[1]
        if not name:
            errors.append(f"Line {idx}: player name is empty.")
            continue
        if _normalize_round(round_text) is None:
            errors.append(f"Line {idx}: unrecognised round '{round_text}'. Valid: {', '.join(ROUND_CANONICAL)}")
            continue
        sets_won = sets_lost = performance_pts = upset_pts = 0
        match_log = ""
        if len(parts) >= 5:
            try:
                sets_won        = int(parts[2])
                sets_lost       = int(parts[3])
                performance_pts = int(parts[4])
                upset_pts       = int(parts[5]) if len(parts) >= 6 else 0
            except ValueError:
                errors.append(f"Line {idx}: numeric fields must be integers.")
                continue
            if len(parts) >= 7:
                match_log = " | ".join(parts[6:]).strip()
        rows.append({"player": name, "round": round_text,
                     "sets_won": sets_won, "sets_lost": sets_lost,
                     "performance_pts": performance_pts,
                     "upset_pts": upset_pts, "match_log": match_log})
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
            return await interaction.response.send_message("❌ This menu isn't for you.", ephemeral=True)
        if self._locked: return
        self._locked = True
        try:
            self.i = (self.i - 1) % len(self.pages)
            await self._edit(interaction)
        finally:
            self._locked = False

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ This menu isn't for you.", ephemeral=True)
        if self._locked: return
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
    if seed is None: return "wc"
    if 1 <= seed <= 5: return "top5"
    if 1 <= seed <= 20: return "top20"
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
    PAGE_SIZE = 25

    def __init__(self, cog, user_id: int, tournament_id: str, pool: List[PlayerEntry],
                 target_user_id: Optional[int] = None, force_save: bool = False,
                 header: Optional[str] = None):
        super().__init__(timeout=300)
        self.cog = cog
        self.user_id = user_id
        self.target_user_id = target_user_id if target_user_id is not None else user_id
        self.tournament_id = tournament_id
        self.pool = pool
        self.force_save = force_save
        self.header = header
        self.picks: List[PlayerEntry] = []
        self.used_keys: set = set()
        self.top5_used = 0
        self.top20_used = 0
        self.page = 0
        self._refresh_select()

    def _refresh_select(self):
        self.clear_items()
        remaining = [p for p in self.pool if _player_key(p.name) not in self.used_keys]
        remaining.sort(key=lambda p: (p.seed if p.seed is not None else 10_000, p.name.lower()))
        total_pages = max(1, (len(remaining) + self.PAGE_SIZE - 1) // self.PAGE_SIZE)
        self.page = max(0, min(self.page, total_pages - 1))
        start = self.page * self.PAGE_SIZE
        show = remaining[start:start + self.PAGE_SIZE]
        opts = [discord.SelectOption(label=_fmt_player(p.seed, p.name)[:100], value=p.name[:100]) for p in show]
        if opts:
            self.add_item(PickSelect(self, opts))
        # Pagination buttons if needed
        if total_pages > 1:
            prev_btn = discord.ui.Button(label="◀ Prev", style=discord.ButtonStyle.secondary,
                                          disabled=(self.page == 0), row=1)
            next_btn = discord.ui.Button(label=f"Next ▶ ({self.page + 1}/{total_pages})",
                                          style=discord.ButtonStyle.secondary,
                                          disabled=(self.page >= total_pages - 1), row=1)
            async def _on_prev(inter: discord.Interaction, v=self):
                if inter.user.id != v.user_id:
                    return await inter.response.send_message("❌ Not for you.", ephemeral=True)
                v.page -= 1; v._refresh_select()
                await inter.response.edit_message(content=v._status_text(), view=v)
            async def _on_next(inter: discord.Interaction, v=self):
                if inter.user.id != v.user_id:
                    return await inter.response.send_message("❌ Not for you.", ephemeral=True)
                v.page += 1; v._refresh_select()
                await inter.response.edit_message(content=v._status_text(), view=v)
            prev_btn.callback = _on_prev
            next_btn.callback = _on_next
            self.add_item(prev_btn)
            self.add_item(next_btn)
        if len(self.picks) > 0:
            self.add_item(ResetPicksButton())
        self.add_item(ConfirmPicksButton(disabled=(len(self.picks) != 5)))

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This menu isn't for you.", ephemeral=True)
            return False
        return True

    def _rules_ok_for(self, p: PlayerEntry) -> Tuple[bool, str]:
        b = _seed_bucket(p.seed)
        if b == "top5" and self.top5_used >= 1:
            return False, "❌ Only **one** player from seeds 1–5."
        if b in ("top5", "top20") and self.top20_used >= 3:
            return False, "❌ Only **three** players from seeds 1–20."
        return True, ""

    async def on_pick(self, interaction: discord.Interaction, picked_name: str):
        if not await self._guard(interaction): return
        entry = next((p for p in self.pool if _player_key(p.name) == _player_key(picked_name)), None)
        if not entry:
            return await interaction.response.send_message("❌ Player not found.", ephemeral=True)
        if _player_key(entry.name) in self.used_keys:
            return await interaction.response.send_message("❌ Already picked.", ephemeral=True)
        ok, msg = self._rules_ok_for(entry)
        if not ok:
            return await interaction.response.send_message(msg, ephemeral=True)
        self.picks.append(entry)
        self.used_keys.add(_player_key(entry.name))
        b = _seed_bucket(entry.seed)
        if b == "top5": self.top5_used += 1; self.top20_used += 1
        elif b == "top20": self.top20_used += 1
        self._refresh_select()
        await interaction.response.edit_message(content=self._status_text(), view=self)

    def _status_text(self) -> str:
        lines = [self.header or "**Fantasy Join — Pick 5 players**", ""]
        if self.picks:
            lines.append("**Your picks so far:**")
            for i, p in enumerate(self.picks, 1):
                lines.append(f"{i}. {_fmt_player(p.seed, p.name)}")
        else:
            lines.append("No picks yet.")
        lines.append("")
        lines.append(f"Top 5 used: **{self.top5_used}/1** • Top 20 used: **{self.top20_used}/3** • Total: **{len(self.picks)}/5**")
        return "\n".join(lines)

class ResetPicksButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="Reset", style=discord.ButtonStyle.secondary)

    async def callback(self, interaction: discord.Interaction):
        view: JoinFantasyView = self.view
        if interaction.user.id != view.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)
        view.picks = []; view.used_keys = set(); view.top5_used = 0; view.top20_used = 0
        view._refresh_select()
        await interaction.response.edit_message(content=view._status_text(), view=view)

class ConfirmPicksButton(discord.ui.Button):
    def __init__(self, disabled: bool = False):
        super().__init__(label="Confirm roster", style=discord.ButtonStyle.success, disabled=disabled)

    async def callback(self, interaction: discord.Interaction):
        view: JoinFantasyView = self.view
        if interaction.user.id != view.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)
        if len(view.picks) != 5:
            return await interaction.response.send_message("❌ Pick 5 players.", ephemeral=True)
        await view.cog._save_user_roster(interaction, view.tournament_id,
                                          view.target_user_id, view.picks,
                                          force_save=view.force_save)

# ============================================================
# Admin create flow
# ============================================================

class CategoryPointsModal(discord.ui.Modal, title="Category — Round Points"):
    points_text = discord.ui.TextInput(
        label="Points per round (one number per line)",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=1000,
        placeholder=(
            "500\n"
            "300\n"
            "180\n"
            "90\n"
            "45\n"
            "20\n"
            "10\n"
            "5\n"
            "↑ Champion / Finalist / SF / QF / R16 / R32 / R64 / R128"
        ),
    )

    def __init__(self, cog, user_id: int, category_id: str, category_title: str):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.category_id = category_id
        self.category_title = category_title
        # Pre-fill if existing points
        data = _load()
        cat = next((c for c in data.get("categories", []) if c.get("id") == category_id), None)
        if cat and cat.get("round_points"):
            prefill = "\n".join(
                str(cat["round_points"][r])
                for r in ROUND_CANONICAL
                if r in cat["round_points"]
            )
            try:
                self.points_text.default = prefill
            except Exception:
                pass

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)
        pts, errors = _parse_round_points_text(str(self.points_text))
        if errors:
            return await interaction.response.send_message(
                "❌ Errors in points:\n" + "\n".join(errors[:10]), ephemeral=True)
        data = _load()
        cat = next((c for c in data.get("categories", []) if c.get("id") == self.category_id), None)
        if not cat:
            return await interaction.response.send_message("❌ Category not found.", ephemeral=True)
        cat["round_points"] = pts
        _save(data)
        lines = [f"✅ Points saved for **{self.category_title}** (`{self.category_id}`)", ""]
        for r in ROUND_CANONICAL:
            if r in pts:
                lines.append(f"**{r}:** {pts[r]}")
        await interaction.response.send_message("\n".join(lines), ephemeral=True)


class UnseededModal(discord.ui.Modal, title="Fantasy Create — Unseeded"):
    unseeded = discord.ui.TextInput(label="Unseeded players (1 per line)",
                                    style=discord.TextStyle.paragraph, required=False, max_length=4000)
    def __init__(self, cog, user_id: int, tournament_id: str):
        super().__init__(); self.cog = cog; self.user_id = user_id; self.tournament_id = tournament_id
    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)
        await self.cog._fantasy_create_set_unseeded(interaction, self.tournament_id, str(self.unseeded))

class UnseededStepView(discord.ui.View):
    def __init__(self, cog, user_id: int, tournament_id: str):
        super().__init__(timeout=180); self.cog = cog; self.user_id = user_id; self.tournament_id = tournament_id

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Not for you.", ephemeral=True); return False
        return True

    @discord.ui.button(label="Add unseeded", style=discord.ButtonStyle.primary)
    async def add_unseeded(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction): return
        await interaction.response.send_modal(UnseededModal(self.cog, self.user_id, self.tournament_id))

    @discord.ui.button(label="Skip unseeded", style=discord.ButtonStyle.secondary)
    async def skip_unseeded(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction): return
        await self.cog._fantasy_create_finalize_preview(interaction, self.tournament_id)

class ConfirmCreateView(discord.ui.View):
    def __init__(self, cog, user_id: int, tournament_id: str):
        super().__init__(timeout=180); self.cog = cog; self.user_id = user_id; self.tournament_id = tournament_id

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Not for you.", ephemeral=True); return False
        return True

    @discord.ui.button(label="Confirm", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction): return
        await self.cog._fantasy_create_confirm(interaction, self.tournament_id)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction): return
        await interaction.response.edit_message(content="❌ Fantasy creation cancelled.", embed=None, view=None)

class SeedsModal(discord.ui.Modal, title="Fantasy Create — Seeds"):
    seeds = discord.ui.TextInput(label="Seeded players (1 per line, in order 1..N)",
                                  style=discord.TextStyle.paragraph, required=True, max_length=4000)

    def __init__(self, cog, user_id: int, tournament_name: str, category_id: str, category_title: str):
        super().__init__()
        self.cog = cog; self.user_id = user_id; self.tournament_name = tournament_name
        self.category_id = category_id; self.category_title = category_title

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)
        seeds = _parse_multiline_list(str(self.seeds))
        if not seeds:
            return await interaction.response.send_message("❌ Must provide at least 1 seeded player.", ephemeral=True)
        seen = set(); clean = []
        for name in seeds:
            k = _player_key(name)
            if k in seen: continue
            seen.add(k); clean.append(name)
        data = _load(); tid = _mk_id("fantasy")
        tournament = {
            "id": tid,
            "guild_id": interaction.guild.id if interaction.guild else 0,
            "name": self.tournament_name.strip(),
            "category_id": self.category_id,
            "category_title": self.category_title,
            "created": False,
            "picks_open": True,
            "results_entered": False,
            "opened_at": _now_unix(),
            "closed_at": None,
            "completed_at": None,
            "players": [{"name": name, "seed": i + 1} for i, name in enumerate(clean)],
            "rosters": {},
            "results": {},
            "display": {"primary": None, "secondary": None, "tertiary": None,
                        "logo_url": None, "background_url": None}
        }
        data["tournaments"].append(tournament); _save(data)
        embed = discord.Embed(title="Seeds saved (draft)",
                              description="✅ Seeds saved.\nChoose **Add unseeded** or **Skip unseeded**.\n\n"
                                          "⚠️ This is a **draft** until you press **Confirm**.")
        view = UnseededStepView(self.cog, interaction.user.id, tid)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

# ============================================================
# Delete confirmation
# ============================================================

class _DeleteTournamentConfirmButton(discord.ui.Button):
    def __init__(self, tournament_id: str):
        super().__init__(label="Delete tournament", style=discord.ButtonStyle.danger,
                         custom_id=f"fantasy_cancel_confirm:{tournament_id}")
        self.tournament_id = tournament_id

    async def callback(self, interaction: discord.Interaction):
        view: ConfirmDeleteTournamentView = self.view
        if interaction.user.id != view.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)
        data = _load(); gid = interaction.guild.id if interaction.guild else None
        removed = _delete_tournament(data, self.tournament_id, gid)
        if removed is None:
            return await interaction.response.edit_message(content="❌ Not found.", embed=None, view=None)
        _save(data)
        await interaction.response.edit_message(
            content=f"✅ Deleted: **{removed.get('name', 'Unknown')}** (`{self.tournament_id}`)",
            embed=None, view=None)

class _DeleteTournamentAbortButton(discord.ui.Button):
    def __init__(self, tournament_id: str):
        super().__init__(label="Keep tournament", style=discord.ButtonStyle.secondary,
                         custom_id=f"fantasy_cancel_abort:{tournament_id}")
        self.tournament_id = tournament_id

    async def callback(self, interaction: discord.Interaction):
        view: ConfirmDeleteTournamentView = self.view
        if interaction.user.id != view.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)
        await interaction.response.edit_message(content="❌ Cancelled.", embed=None, view=None)

class ConfirmDeleteTournamentView(discord.ui.View):
    def __init__(self, cog, user_id: int, tournament_id: str):
        super().__init__(timeout=60)
        self.cog = cog; self.user_id = user_id; self.tournament_id = tournament_id
        self.add_item(_DeleteTournamentConfirmButton(tournament_id))
        self.add_item(_DeleteTournamentAbortButton(tournament_id))

# ============================================================
# Roster breakdown menu
# ============================================================

class RosterPickSelect(discord.ui.Select):
    def __init__(self, owner_view, options: List[discord.SelectOption]):
        super().__init__(placeholder="Select a player for detailed breakdown…",
                         min_values=1, max_values=1, options=options)
        self.owner_view = owner_view

    async def callback(self, interaction: discord.Interaction):
        await self.owner_view.on_pick(interaction, self.values[0])

class RosterPickMenuView(discord.ui.View):
    def __init__(self, user_id: int, roster: List[str], seed_map: Dict[str, Optional[int]],
                 results: Dict[str, dict], title: str = "Roster Breakdown"):
        super().__init__(timeout=240)
        self.user_id = user_id; self.roster = roster[:5]; self.seed_map = seed_map
        self.results = results; self.title = title
        opts = []
        for name in self.roster:
            r = results.get(_player_key(name)) if results else None
            pts = int(r.get("total", 0)) if r else 0
            seed = seed_map.get(_player_key(name))
            opts.append(discord.SelectOption(label=f"{_fmt_player(seed, name)} — {pts}"[:100], value=name[:100]))
        if opts:
            self.add_item(RosterPickSelect(self, opts))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Not for you.", ephemeral=True); return False
        return True

    async def on_pick(self, interaction: discord.Interaction, picked_name: str):
        r = self.results.get(_player_key(picked_name)) if self.results else None
        seed = self.seed_map.get(_player_key(picked_name))
        header = _fmt_player(seed, picked_name)
        if not r:
            embed = discord.Embed(title="Pick Breakdown",
                                  description=f"**{header}**\n\nℹ️ Results not entered yet.")
            return await interaction.response.send_message(embed=embed, ephemeral=True)
        lines = [f"**{header} — {r.get('round','')}**", "",
                 f"**Tournament Pts:** {r.get('tournament_points',0):+}",
                 f"**Set Pts:** {r.get('set_points',0):+}  ({r.get('sets_won',0)}W / {r.get('sets_lost',0)}L sets)",
                 f"**Performance Pts:** {r.get('performance_points',0):+}",
                 f"**Upset Pts:** {r.get('upset_points',0):+}", "",
                 f"**Total: {r.get('total',0)}**"]
        log = r.get("match_log", "")
        if log:
            lines += ["", "**Match Results:**"]
            for match in log.split(";"):
                m = match.strip()
                if not m:
                    continue
                # Extract net points if present at end e.g. "d. Djokovic 6-3 7-5 +120"
                # Split off the last token if it looks like a +/- number
                parts = m.rsplit(" ", 1)
                if len(parts) == 2 and parts[1].lstrip("+-").isdigit():
                    desc, pts = parts[0].strip(), parts[1].strip()
                    sign = "+" if not pts.startswith("-") else ""
                    if pts.startswith("+") or pts.startswith("-"):
                        sign = ""
                    lines.append(f"{desc} | {sign}{pts}")
                else:
                    lines.append(m)
        embed = discord.Embed(title="Pick Breakdown", description="\n".join(lines))
        await interaction.response.send_message(embed=embed, ephemeral=True)

# ============================================================
# Results multi-tab view (for /fantasy results)
# ============================================================

class ResultsMainView(discord.ui.View):
    """
    Three-tab view returned by /fantasy results:
      📊 Player Results   — paginated ranked list of all players
      🏆 User Leaderboard — paginated ranked list of all users
      🔍 Player Breakdown — dropdown to inspect any player's detailed stats
    """
    TIMEOUT = 300

    def __init__(self, user_id: int, t: dict, seed_map: Dict[str, Optional[int]]):
        super().__init__(timeout=self.TIMEOUT)
        self.user_id = user_id
        self.t = t
        self.seed_map = seed_map

        # ── Tab 0: Player Results ───────────────────────────────────
        results_sorted = sorted(
            (t.get("results", {}) or {}).values(),
            key=lambda r: int(r.get("total", 0)), reverse=True,
        )
        self.player_results = results_sorted
        p_lines: List[str] = [
            f"**Fantasy Results — {t.get('name')}**", "",
            "Format: Rank. (Seed) Player — Total — Round", "",
        ]
        for i, r in enumerate(results_sorted, 1):
            name = r.get("player", "")
            seed = seed_map.get(_player_key(name))
            p_lines.append(
                f"{i}. {_fmt_player(seed, name)} — **{r.get('total', 0)}** — *{r.get('round', '')}*"
            )
        self.player_pages = _chunk_pages(p_lines)

        # ── Tab 1: User Leaderboard ─────────────────────────────────
        _data = _load()
        scores = {uid: pts for uid, pts in _all_user_scores_for_tournament(t).items()
                  if not _is_blacklisted(_data, uid)}
        items = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        ranks = _dense_ranks(scores)
        u_header = [f"**User Leaderboard — {t.get('name')}**", "",
                    "Format: Rank. User — Total Points", ""]
        u_body = [f"{ranks.get(uid, 0)}. <@{uid}> — **{pts}**" for uid, pts in items]
        u_pages: List[str] = []
        for start in range(0, max(1, len(u_body)), 20):
            u_pages.append("\n".join(u_header + u_body[start:start + 20]))
        self.user_pages = u_pages or [
            f"**User Leaderboard — {t.get('name')}**\n\nℹ️ No user rosters found."
        ]

        # State
        self.tab = 0            # 0 = player results, 1 = user leaderboard, 2 = breakdown
        self.page = 0           # current page for tabs 0 and 1
        self.breakdown_page = 0 # which page of the player select we're on (25 per page)

        self._rebuild()

    # ── Internal helpers ────────────────────────────────────────────

    def _cur_pages(self) -> List[str]:
        return self.player_pages if self.tab == 0 else self.user_pages

    def _embed(self) -> discord.Embed:
        if self.tab == 2:
            total = len(self.player_results)
            bp_total = max(1, (total + 24) // 25)
            desc = (
                f"**Player Breakdown — {self.t.get('name')}**\n\n"
                "Use the menu below to select a player and view their detailed score breakdown."
            )
            e = discord.Embed(title="Fantasy Results", description=desc)
            foot = "Tab: 🔍 Player Breakdown"
            if bp_total > 1:
                foot += f" • Players page {self.breakdown_page + 1}/{bp_total}"
            e.set_footer(text=foot)
            return e
        pages = self._cur_pages()
        tab_name = "📊 Player Results" if self.tab == 0 else "🏆 User Leaderboard"
        e = discord.Embed(title="Fantasy Results", description=pages[self.page])
        e.set_footer(text=f"Tab: {tab_name} • Page {self.page + 1}/{len(pages)}")
        return e

    def _rebuild(self):
        self.clear_items()

        # ── Row 0: tab buttons ──────────────────────────────────────
        tab_defs = [("📊 Player Results", 0), ("🏆 User Leaderboard", 1), ("🔍 Player Breakdown", 2)]
        for label, idx in tab_defs:
            btn = discord.ui.Button(
                label=label,
                style=discord.ButtonStyle.primary if self.tab == idx else discord.ButtonStyle.secondary,
                row=0,
            )
            async def _tab_cb(inter: discord.Interaction, v=self, t=idx):
                if inter.user.id != v.user_id:
                    return await inter.response.send_message("❌ Not for you.", ephemeral=True)
                v.tab = t; v.page = 0; v._rebuild()
                await inter.response.edit_message(embed=v._embed(), view=v)
            btn.callback = _tab_cb
            self.add_item(btn)

        # ── Tabs 0 & 1: page navigation ────────────────────────────
        if self.tab in (0, 1):
            pages = self._cur_pages()
            total = len(pages)
            if total > 1:
                prev = discord.ui.Button(
                    label="◀ Prev", style=discord.ButtonStyle.secondary,
                    disabled=(self.page == 0), row=1,
                )
                nxt = discord.ui.Button(
                    label=f"Next ▶ ({self.page + 1}/{total})", style=discord.ButtonStyle.secondary,
                    disabled=(self.page >= total - 1), row=1,
                )
                async def _prev_cb(inter: discord.Interaction, v=self):
                    if inter.user.id != v.user_id:
                        return await inter.response.send_message("❌ Not for you.", ephemeral=True)
                    v.page = max(0, v.page - 1); v._rebuild()
                    await inter.response.edit_message(embed=v._embed(), view=v)
                async def _next_cb(inter: discord.Interaction, v=self):
                    if inter.user.id != v.user_id:
                        return await inter.response.send_message("❌ Not for you.", ephemeral=True)
                    v.page = min(total - 1, v.page + 1); v._rebuild()
                    await inter.response.edit_message(embed=v._embed(), view=v)
                prev.callback = _prev_cb
                nxt.callback = _next_cb
                self.add_item(prev)
                self.add_item(nxt)

        # ── Tab 2: player breakdown ─────────────────────────────────
        elif self.tab == 2:
            total = len(self.player_results)
            bp_total = max(1, (total + 24) // 25)
            bp = max(0, min(self.breakdown_page, bp_total - 1))
            self.breakdown_page = bp
            start = bp * 25
            slice_ = self.player_results[start:start + 25]

            opts = []
            for r in slice_:
                name = r.get("player", "")
                seed = self.seed_map.get(_player_key(name))
                pts = int(r.get("total", 0))
                opts.append(discord.SelectOption(
                    label=f"{_fmt_player(seed, name)} — {pts}"[:100],
                    value=name[:100],
                ))
            if opts:
                sel = discord.ui.Select(
                    placeholder="Pick a player for their breakdown…",
                    min_values=1, max_values=1,
                    options=opts,
                    row=1,
                )
                async def _sel_cb(inter: discord.Interaction, v=self):
                    if inter.user.id != v.user_id:
                        return await inter.response.send_message("❌ Not for you.", ephemeral=True)
                    picked = inter.data["values"][0]
                    r_map = v.t.get("results", {}) or {}
                    r = r_map.get(_player_key(picked))
                    seed = v.seed_map.get(_player_key(picked))
                    header = _fmt_player(seed, picked)
                    if not r:
                        emb = discord.Embed(title="Player Breakdown",
                                            description=f"**{header}**\n\nℹ️ No result data found.")
                        return await inter.response.send_message(embed=emb, ephemeral=True)
                    lines = [
                        f"**{header} — {r.get('round', '')}**", "",
                        f"**Tournament Pts:** {r.get('tournament_points', 0):+}",
                        f"**Set Pts:** {r.get('set_points', 0):+}  "
                        f"({r.get('sets_won', 0)}W / {r.get('sets_lost', 0)}L sets)",
                        f"**Performance Pts:** {r.get('performance_points', 0):+}",
                        f"**Upset Pts:** {r.get('upset_points', 0):+}", "",
                        f"**Total: {r.get('total', 0)}**",
                    ]
                    log = r.get("match_log", "")
                    if log:
                        lines += ["", "**Match Results:**"]
                        for match in log.split(";"):
                            m = match.strip()
                            if not m:
                                continue
                            parts_m = m.rsplit(" ", 1)
                            if len(parts_m) == 2 and parts_m[1].lstrip("+-").isdigit():
                                desc_s, pts_s = parts_m[0].strip(), parts_m[1].strip()
                                sign = "" if (pts_s.startswith("+") or pts_s.startswith("-")) else "+"
                                lines.append(f"{desc_s} | {sign}{pts_s}")
                            else:
                                lines.append(m)
                    emb = discord.Embed(title="Player Breakdown", description="\n".join(lines))
                    await inter.response.send_message(embed=emb, ephemeral=True)
                sel.callback = _sel_cb
                self.add_item(sel)

            # Player-select pagination (when >25 players)
            if bp_total > 1:
                bp_prev = discord.ui.Button(
                    label="◀ Prev Players", style=discord.ButtonStyle.secondary,
                    disabled=(bp == 0), row=2,
                )
                bp_next = discord.ui.Button(
                    label=f"Next Players ▶ ({bp + 1}/{bp_total})", style=discord.ButtonStyle.secondary,
                    disabled=(bp >= bp_total - 1), row=2,
                )
                async def _bp_prev(inter: discord.Interaction, v=self):
                    if inter.user.id != v.user_id:
                        return await inter.response.send_message("❌ Not for you.", ephemeral=True)
                    v.breakdown_page = max(0, v.breakdown_page - 1); v._rebuild()
                    await inter.response.edit_message(embed=v._embed(), view=v)
                async def _bp_next(inter: discord.Interaction, v=self):
                    if inter.user.id != v.user_id:
                        return await inter.response.send_message("❌ Not for you.", ephemeral=True)
                    v.breakdown_page = min(bp_total - 1, v.breakdown_page + 1); v._rebuild()
                    await inter.response.edit_message(embed=v._embed(), view=v)
                bp_prev.callback = _bp_prev
                bp_next.callback = _bp_next
                self.add_item(bp_prev)
                self.add_item(bp_next)


# ============================================================
# Fantasy end retry UI
# ============================================================

class RetryEndView(discord.ui.View):
    def __init__(self, cog, user_id: int, tournament_id: str, previous_text: str):
        super().__init__(timeout=180)
        self.cog = cog; self.user_id = user_id
        self.tournament_id = tournament_id; self.previous_text = previous_text or ""

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Not for you.", ephemeral=True); return False
        return True

    @discord.ui.button(label="Retry (reopen modal)", style=discord.ButtonStyle.primary)
    async def retry(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            EndResultsModal(self.cog, self.user_id, self.tournament_id, self.previous_text))

class EndResultsModal(discord.ui.Modal, title="Fantasy End — Paste Results"):
    results = discord.ui.TextInput(
        label="Player|Round|SW|SL|Perf|Upset|Match Log",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=4000,
        placeholder="Alcaraz | Champion | 21 | 3 | 340 | 0 | d. Zverev 6-4 6-2 +85; d. Djokovic 6-3 6-4 +97",
    )

    def __init__(self, cog, user_id: int, tournament_id: str, default_text: str = ""):
        super().__init__()
        self.cog = cog; self.user_id = user_id; self.tournament_id = tournament_id
        try: self.results.default = (default_text or "")[:4000]
        except Exception: pass

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)
        await self.cog._fantasy_end_submit(interaction, self.tournament_id, str(self.results))

# ============================================================
# Leaderboard computations
# ============================================================

def _is_blacklisted(data: dict, user_id: int) -> bool:
    try: return int(user_id) in set(int(x) for x in data.get("ldb_blacklist", []))
    except Exception: return False

def _t_in_scope(t: dict, guild_id: Optional[int], category_id: Optional[str], days_back: Optional[int], ldb_reset_at: int = 0) -> bool:
    if guild_id is not None and t.get("guild_id") not in (0, guild_id): return False
    if category_id and t.get("category_id") != category_id: return False
    completed = int(t.get("completed_at") or 0)
    if ldb_reset_at and completed < ldb_reset_at: return False
    if days_back is not None:
        if not t.get("results_entered"): return False
        if completed <= 0: return False
        cutoff = _now_unix() - int(days_back) * 86400
        if completed < cutoff: return False
    return True

def _score_user_in_tournament(t: dict, user_id: int) -> Optional[int]:
    rosters = t.get("rosters", {}) or {}
    roster = rosters.get(str(user_id))
    if not roster or not t.get("results_entered"): return None
    results = t.get("results", {}) or {}
    return sum(int((results.get(_player_key(n)) or {}).get("total", 0)) for n in roster[:5])

def _all_user_scores_for_tournament(t: dict) -> Dict[int, int]:
    if not t.get("results_entered"): return {}
    results = t.get("results", {}) or {}
    out: Dict[int, int] = {}
    for uid_str, roster in (t.get("rosters", {}) or {}).items():
        try: uid = int(uid_str)
        except Exception: continue
        out[uid] = sum(int((results.get(_player_key(n)) or {}).get("total", 0)) for n in (roster or [])[:5])
    return out

def _dense_ranks(score_map: Dict[int, int]) -> Dict[int, int]:
    items = sorted(score_map.items(), key=lambda kv: kv[1], reverse=True)
    ranks: Dict[int, int] = {}; last_score = None; rank = 0
    for uid, pts in items:
        if last_score is None or pts != last_score: rank += 1; last_score = pts
        ranks[uid] = rank
    return ranks

def _compute_leaderboard(data: dict, guild_id: Optional[int], mode: str,
                          category_id: Optional[str], days_back: Optional[int],
                          min_tournaments: int = 5) -> List[Tuple[int, float, int]]:
    tours = [t for t in data.get("tournaments", [])
             if _is_created(t) and t.get("results_entered")
             and _t_in_scope(t, guild_id, category_id, days_back, int(data.get("ldb_reset_at", 0)))]
    points_total: Dict[int, int] = {}; points_count: Dict[int, int] = {}
    wins: Dict[int, int] = {}; top5: Dict[int, int] = {}; top10: Dict[int, int] = {}
    for t in tours:
        score_map = {uid: pts for uid, pts in _all_user_scores_for_tournament(t).items()
                     if not _is_blacklisted(data, uid)}
        for uid, pts in score_map.items():
            points_total[uid] = points_total.get(uid, 0) + pts
            points_count[uid] = points_count.get(uid, 0) + 1
        if score_map:
            ranks = _dense_ranks(score_map)
            for uid, r in ranks.items():
                if r == 1: wins[uid] = wins.get(uid, 0) + 1
                if r <= 5: top5[uid] = top5.get(uid, 0) + 1
                if r <= 10: top10[uid] = top10.get(uid, 0) + 1

    def as_list(m): return sorted([(uid, float(v), 0) for uid, v in m.items()],
                                   key=lambda x: x[1], reverse=True)
    if mode == "points_total": return as_list(points_total)
    if mode == "avg_points":
        out = [(uid, float(tot)/float(points_count.get(uid,1)), points_count.get(uid,0))
               for uid, tot in points_total.items() if points_count.get(uid, 0) >= min_tournaments]
        return sorted(out, key=lambda x: x[1], reverse=True)
    if mode == "wins": return as_list(wins)
    if mode == "top5": return as_list(top5)
    if mode == "top10": return as_list(top10)
    return []

# ============================================================
# Leaderboard UI
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
        super().__init__(placeholder="Choose a fantasy leaderboard…", min_values=1, max_values=1,
                         options=_LDB_OPTIONS[:25])
        self.view_ref = view_ref

    async def callback(self, interaction: discord.Interaction):
        await self.view_ref.on_select(interaction, self.values[0])

class FantasyLeaderboardView(discord.ui.View):
    def __init__(self, cog, user_id: int, category_id: Optional[str], days_back: Optional[int]):
        super().__init__(timeout=240)
        self.cog = cog; self.user_id = user_id
        self.category_id = category_id; self.days_back = days_back
        self.add_item(LeaderboardSelect(self))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Not for you.", ephemeral=True); return False
        return True

    async def on_select(self, interaction: discord.Interaction, value: str):
        try: mode, scope = value.split(":", 1)
        except Exception: return await interaction.response.send_message("❌ Invalid option.", ephemeral=True)
        if "cat" in scope and not self.category_id:
            return await interaction.response.send_message(
                "❌ This leaderboard needs `category_id` on `/fantasy-leaderboard-view`.", ephemeral=True)
        if "days" in scope and self.days_back is None:
            return await interaction.response.send_message(
                "❌ This leaderboard needs `days_back` on `/fantasy-leaderboard-view`.", ephemeral=True)
        cat  = self.category_id if "cat"  in scope else None
        days = self.days_back   if "days" in scope else None
        await self.cog._render_leaderboard(interaction, mode=mode, category_id=cat, days_back=days)

# ============================================================
# API fetch UI: tournament picker + confirm
# ============================================================

class FetchConfirmView(discord.ui.View):
    def __init__(self, cog, user_id: int, tournament_id: str, rows: List[dict]):
        super().__init__(timeout=300)
        self.cog = cog
        self.user_id = user_id
        self.tournament_id = tournament_id
        self.rows = rows

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Not for you.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="✅ Confirm & Save", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.cog._apply_results_data(
            interaction, self.tournament_id, self.rows, title="Results Auto-Saved")

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="❌ Fetch cancelled.", embed=None, view=None)


# ============================================================
# Cog
# ============================================================


# ============================================================
# Chunked results entry (up to 6 parts + Done early)
# ============================================================

# In-memory staging: {(user_id, tournament_id): [line, line, ...]}
_staged_results: Dict[Tuple[int, str], List[str]] = {}

MAX_CHUNKS = 6

class ChunkedResultsModal(discord.ui.Modal, title="Fantasy End — Results (chunk)"):
    chunk_text = discord.ui.TextInput(
        label="Player|Round|SW|SL|Perf|Upset|Match Log",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=4000,
        placeholder="Alcaraz | Champion | 21 | 3 | 340 | 0 | d. Zverev 6-4 6-2 +85",
    )

    def __init__(self, cog, user_id: int, tournament_id: str, chunk_num: int):
        super().__init__(title=f"Results — Part {chunk_num} of {MAX_CHUNKS}")
        self.cog = cog
        self.user_id = user_id
        self.tournament_id = tournament_id
        self.chunk_num = chunk_num

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)

        raw = str(self.chunk_text).strip()
        key = (self.user_id, self.tournament_id)
        existing = _staged_results.get(key, [])
        new_lines = [l for l in raw.splitlines() if l.strip()]
        _staged_results[key] = existing + new_lines
        total_lines = len(_staged_results[key])
        next_chunk = self.chunk_num + 1

        if next_chunk > MAX_CHUNKS:
            # Auto-finalize after the last chunk
            await interaction.response.edit_message(
                content=f"✅ Part {self.chunk_num} saved ({total_lines} players total). Finalizing…",
                view=None,
            )
            await _finalize_chunked_results(self.cog, interaction, self.tournament_id)
        else:
            view = ChunkedResultsView(
                self.cog, self.user_id, self.tournament_id,
                chunk_num=next_chunk, lines_so_far=total_lines,
            )
            await interaction.response.edit_message(
                content=(
                    f"✅ Part {self.chunk_num} saved — **{total_lines}** players staged so far.\n"
                    f"Click **Part {next_chunk}** to continue, or **Done** if all players are entered."
                ),
                view=view,
            )


async def _finalize_chunked_results(cog, interaction: discord.Interaction, tournament_id: str):
    key = (interaction.user.id, tournament_id)
    lines = _staged_results.pop(key, [])
    combined = "\n".join(lines)
    await cog._fantasy_end_submit(interaction, tournament_id, combined)


class ChunkedResultsView(discord.ui.View):
    """Shown between chunks. Has a 'Next Part' button and an early 'Done' button."""

    def __init__(self, cog, user_id: int, tournament_id: str, chunk_num: int, lines_so_far: int):
        super().__init__(timeout=600)
        self.cog = cog
        self.user_id = user_id
        self.tournament_id = tournament_id
        self.chunk_num = chunk_num
        self.lines_so_far = lines_so_far

        next_btn = discord.ui.Button(
            label=f"Part {chunk_num} →",
            style=discord.ButtonStyle.primary,
        )
        next_btn.callback = self._next_callback
        self.add_item(next_btn)

        done_btn = discord.ui.Button(
            label="✅ Done — Finalize",
            style=discord.ButtonStyle.success,
        )
        done_btn.callback = self._done_callback
        self.add_item(done_btn)

        cancel_btn = discord.ui.Button(
            label="✗ Cancel",
            style=discord.ButtonStyle.danger,
        )
        cancel_btn.callback = self._cancel_callback
        self.add_item(cancel_btn)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Not for you.", ephemeral=True)
            return False
        return True

    async def _next_callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(
            ChunkedResultsModal(self.cog, self.user_id, self.tournament_id, self.chunk_num)
        )

    async def _done_callback(self, interaction: discord.Interaction):
        await interaction.response.edit_message(
            content=f"✅ Finalizing {self.lines_so_far} players…", view=None
        )
        await _finalize_chunked_results(self.cog, interaction, self.tournament_id)

    async def _cancel_callback(self, interaction: discord.Interaction):
        key = (self.user_id, self.tournament_id)
        _staged_results.pop(key, None)
        await interaction.response.edit_message(
            content="❌ Results entry cancelled. Staged data cleared.", view=None
        )


class TournamentEndSelect(discord.ui.Select):
    def __init__(self, cog, user_id: int, tournaments: list):
        self.cog = cog
        self.user_id = user_id
        opts = []
        for t in tournaments[:25]:
            label = f"{t.get('name','?')} [{_status_key(t)}]"[:100]
            opts.append(discord.SelectOption(label=label, value=t.get("id","")))
        super().__init__(placeholder="Pick a tournament…", min_values=1, max_values=1, options=opts)

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)
        # Clear any leftover staged data for this user+tournament
        _staged_results.pop((self.user_id, self.values[0]), None)
        view = ChunkedResultsView(self.cog, self.user_id, self.values[0],
                                  chunk_num=1, lines_so_far=0)
        await interaction.response.edit_message(
            content=(
                "📋 **Enter results in up to 6 parts.**\n"
                "Click **Part 1 →** to open the first text box.\n"
                "After each part, click the next button or **Done** when finished."
            ),
            view=view,
        )

class TournamentEndSelectView(discord.ui.View):
    def __init__(self, cog, user_id: int, tournaments: list):
        super().__init__(timeout=60)
        self.add_item(TournamentEndSelect(cog, user_id, tournaments))


class CloseScheduleTimeModal(discord.ui.Modal, title="Schedule Auto-Close"):
    close_time = discord.ui.TextInput(
        label="Close at (YYYY-MM-DD HH:MM UTC)",
        placeholder="2025-01-20 18:00",
        max_length=20,
    )

    def __init__(self, cog, user_id: int, tournament_id: str):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.tournament_id = tournament_id

    async def on_submit(self, interaction: discord.Interaction):
        import datetime
        raw = str(self.close_time).strip()
        try:
            dt = datetime.datetime.strptime(raw, "%Y-%m-%d %H:%M").replace(
                tzinfo=datetime.timezone.utc)
            ts = int(dt.timestamp())
        except ValueError:
            return await interaction.response.send_message(
                "❌ Invalid format. Use: `YYYY-MM-DD HH:MM` (UTC)", ephemeral=True)
        if ts <= int(time.time()):
            return await interaction.response.send_message(
                "❌ That time is in the past.", ephemeral=True)
        data = _load()
        t = _find_tournament(data, self.tournament_id)
        if not t:
            return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)
        t["auto_close_at"] = ts
        _save(data)
        await interaction.response.send_message(
            f"✅ **{t.get('name')}** will auto-close <t:{ts}:R> (<t:{ts}:f>).", ephemeral=True)


class CloseScheduleSelect(discord.ui.Select):
    def __init__(self, cog, user_id: int, tournaments: list):
        self.cog = cog
        self.user_id = user_id
        opts = [discord.SelectOption(label=t.get("name","?")[:100], value=t.get("id",""))
                for t in tournaments[:25]]
        super().__init__(placeholder="Pick a tournament…", min_values=1, max_values=1, options=opts)

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)
        await interaction.response.send_modal(
            CloseScheduleTimeModal(self.cog, self.user_id, self.values[0]))

class CloseScheduleSelectView(discord.ui.View):
    def __init__(self, cog, user_id: int, tournaments: list):
        super().__init__(timeout=60)
        self.add_item(CloseScheduleSelect(cog, user_id, tournaments))


class CloseScheduleEditSelect(discord.ui.Select):
    def __init__(self, cog, user_id: int, tournaments: list):
        self.cog = cog
        self.user_id = user_id
        self.tournaments = tournaments
        opts = [discord.SelectOption(label=t.get("name","?")[:100], value=t.get("id",""))
                for t in tournaments[:25]]
        super().__init__(placeholder="Select to delete schedule…", min_values=1, max_values=1, options=opts)

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ Not for you.", ephemeral=True)
        tid = self.values[0]
        data = _load()
        t = _find_tournament(data, tid)
        if not t:
            return await interaction.response.send_message("❌ Not found.", ephemeral=True)
        t.pop("auto_close_at", None)
        _save(data)
        await interaction.response.send_message(
            f"🗑️ Auto-close schedule removed for **{t.get('name')}**.", ephemeral=True)

class CloseScheduleEditView(discord.ui.View):
    def __init__(self, cog, user_id: int, tournaments: list):
        super().__init__(timeout=60)
        self.add_item(CloseScheduleEditSelect(cog, user_id, tournaments))

class FantasyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._auto_close_task: Optional[asyncio.Task] = None

    fantasy      = app_commands.Group(name="fantasy",       description="Fantasy tournament commands.")
    f_admin      = app_commands.Group(name="fantasy-admin", description="Admin: manage fantasy tournaments & categories.")

    # ── Autocomplete helpers ──────────────────────────────────────────────────

    async def _ac_category(self, interaction: discord.Interaction, cur: str):
        data = _load(); c = cur.lower(); out = []
        for cat in data.get("categories", []):
            if c in cat.get("id","").lower() or c in cat.get("title","").lower() or not c:
                out.append(app_commands.Choice(
                    name=f"{cat.get('title','?')} ({cat.get('id','?')})"[:100],
                    value=cat.get("id","")))
            if len(out) >= 25: break
        return out

    async def _ac_tournament(self, interaction: discord.Interaction, cur: str):
        """All confirmed tournaments (open, closed, completed) — for roster-view, results, user-results."""
        data = _load()
        gid = interaction.guild.id if interaction.guild else 0
        c = cur.lower(); out = []
        # Sort: completed last so open ones appear first
        tours = sorted(data.get("tournaments", []),
                       key=lambda t: (1 if t.get("results_entered") else 0, t.get("name","")))
        for t in tours:
            if t.get("guild_id") not in (0, gid): continue
            if not _is_created(t): continue
            tid = t.get("id",""); name = t.get("name",""); s = _status_key(t)
            prefix = "✅ " if s == "Completed" else ("🔒 " if s == "Closed & Results Pending" else "")
            label  = f"{prefix}{name} [{s}]"
            if c in tid.lower() or c in name.lower() or not c:
                out.append(app_commands.Choice(name=label[:100], value=tid))
            if len(out) >= 25: break
        return out

    async def _ac_open_tournament(self, interaction: discord.Interaction, cur: str):
        """Only tournaments open for picks."""
        data = _load()
        gid = interaction.guild.id if interaction.guild else 0
        c = cur.lower(); out = []
        for t in data.get("tournaments", []):
            if t.get("guild_id") not in (0, gid): continue
            if not _is_created(t) or not t.get("picks_open", True): continue
            tid = t.get("id",""); name = t.get("name","")
            if c in tid.lower() or c in name.lower() or not c:
                out.append(app_commands.Choice(name=name[:100], value=tid))
            if len(out) >= 25: break
        return out

    async def _ac_any_tournament(self, interaction: discord.Interaction, cur: str):
        """All tournaments including drafts (for admin commands)."""
        try:
            data = _load()
            gid = interaction.guild.id if interaction.guild else 0
            c = cur.lower(); out = []
            for t in data.get("tournaments", []):
                if t.get("guild_id") not in (0, gid): continue
                tid = t.get("id",""); name = t.get("name","")
                draft = "" if _is_created(t) else " [DRAFT]"
                if c in tid.lower() or c in name.lower() or not c:
                    out.append(app_commands.Choice(
                        name=f"{name}{draft} [{_status_key(t)}]"[:100], value=tid))
                if len(out) >= 25: break
            return out
        except Exception:
            return []

    # ── Categories ────────────────────────────────────────────────────────────

    @f_admin.command(name="category-create", description="Admin: create a fantasy tournament category.")
    async def fantasy_category_create(self, interaction: discord.Interaction, title: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load(); cid = _mk_id("fantasy-categ")
        data["categories"].append({"id": cid, "title": title.strip(), "round_points": {}})
        _save(data)
        await interaction.response.send_modal(CategoryPointsModal(self, interaction.user.id, cid, title.strip()))

    @f_admin.command(name="category-set-points", description="Admin: set or update round points for an existing category.")
    @app_commands.autocomplete(category_id=_ac_category)
    async def fantasy_category_set_points(self, interaction: discord.Interaction, category_id: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        cat = next((c for c in data.get("categories", []) if c.get("id") == category_id), None)
        if not cat:
            return await interaction.response.send_message("❌ Category not found.", ephemeral=True)
        await interaction.response.send_modal(CategoryPointsModal(self, interaction.user.id, category_id, cat.get("title", "")))

    @f_admin.command(name="category-list", description="List fantasy tournament categories.")
    async def fantasy_category_list(self, interaction: discord.Interaction):
        data = _load(); cats = data.get("categories", [])
        if not cats: return await interaction.response.send_message("ℹ️ No categories yet.")
        lines = []
        for c in sorted(cats, key=lambda x: x.get("title","").lower()):
            lines.append(f"**{c['title']}** — `{c['id']}`")
            rp = c.get("round_points", {})
            if rp:
                pts_str = "  •  ".join(f"{r}: **{rp[r]}**" for r in ROUND_CANONICAL if r in rp)
                lines.append(f"  ↳ {pts_str}")
            else:
                lines.append("  ↳ *(no points set — use `/fantasy-admin category-set-points`)*")
            lines.append("")
        view = PagerView(_chunk_pages(lines), interaction.user.id, "Fantasy Categories")
        await interaction.response.send_message(embed=view._embed(), view=view)

    @f_admin.command(name="category-delete", description="Admin: delete a fantasy tournament category.")
    @app_commands.autocomplete(category_id=_ac_category)
    async def fantasy_category_delete(self, interaction: discord.Interaction, category_id: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load(); before = len(data["categories"])
        data["categories"] = [c for c in data["categories"] if c.get("id") != category_id]
        if len(data["categories"]) == before:
            return await interaction.response.send_message("❌ Category not found.", ephemeral=True)
        _save(data)
        await interaction.response.send_message(f"✅ Deleted category `{category_id}`")

    # ── Tournaments (admin) ───────────────────────────────────────────────────

    @f_admin.command(name="tournament-create", description="Admin: create a fantasy tournament.")
    @app_commands.autocomplete(category_id=_ac_category)
    async def fantasy_create(self, interaction: discord.Interaction, tournament_name: str, category_id: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        cat = next((c for c in data["categories"] if c.get("id") == category_id), None)
        if not cat:
            return await interaction.response.send_message("❌ Category not found. Use `/fantasy-category-list` to see IDs.", ephemeral=True)
        await interaction.response.send_modal(
            SeedsModal(self, interaction.user.id, tournament_name.strip(), category_id, cat.get("title","")))


    @f_admin.command(name="close-schedule", description="Admin: schedule auto-close for a fantasy tournament.")
    async def fantasy_close_schedule(self, interaction: discord.Interaction):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        gid = interaction.guild.id if interaction.guild else 0
        ts = [t for t in data.get("tournaments", [])
              if t.get("guild_id") in (0, gid) and t.get("picks_open", True) and _is_created(t)]
        if not ts:
            return await interaction.response.send_message("❌ No open tournaments to schedule.", ephemeral=True)
        view = CloseScheduleSelectView(self, interaction.user.id, ts)
        await interaction.response.send_message("Select a tournament to schedule auto-close for:",
                                                 view=view, ephemeral=True)

    @f_admin.command(name="close-schedule-edit", description="Admin: view/edit/delete scheduled auto-closes.")
    async def fantasy_close_schedule_edit(self, interaction: discord.Interaction):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        gid = interaction.guild.id if interaction.guild else 0
        scheduled = [t for t in data.get("tournaments", [])
                     if t.get("guild_id") in (0, gid) and t.get("auto_close_at")]
        if not scheduled:
            return await interaction.response.send_message("ℹ️ No scheduled auto-closes.", ephemeral=True)
        lines = []
        for t in scheduled:
            ts = t.get("auto_close_at", 0)
            lines.append(f"**{t.get('name')}** (`{t.get('id')}`) — closes <t:{ts}:R> (<t:{ts}:f>)")
        view = CloseScheduleEditView(self, interaction.user.id, scheduled)
        embed = discord.Embed(title="Scheduled Auto-Closes", description="\n".join(lines))
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @f_admin.command(name="tournament-close", description="Admin: close a fantasy (no more pick edits).")
    @app_commands.autocomplete(tournament_id=_ac_any_tournament)
    async def fantasy_close(self, interaction: discord.Interaction, tournament_id: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        t = _find_tournament(data, tournament_id)
        if not t: return await interaction.response.send_message("❌ Not found.", ephemeral=True)
        t["picks_open"] = False; t["closed_at"] = t.get("closed_at") or _now_unix(); _save(data)
        await interaction.response.send_message(f"✅ Closed: **{t.get('name')}** (`{t.get('id')}`)")

    @f_admin.command(name="tournament-cancel", description="Admin: permanently delete a fantasy tournament.")
    @app_commands.autocomplete(tournament_id=_ac_any_tournament)
    async def fantasy_cancel(self, interaction: discord.Interaction, tournament_id: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        t = _find_tournament(data, tournament_id)
        if not t: return await interaction.response.send_message("❌ Not found.", ephemeral=True)
        gid = interaction.guild.id if interaction.guild else None
        if gid is not None and t.get("guild_id") not in (0, gid):
            return await interaction.response.send_message("❌ Not found in this server.", ephemeral=True)
        embed = discord.Embed(title="⚠️ Confirm Delete Fantasy Tournament",
                              description=f"**Tournament:** {t.get('name')} (`{t.get('id')}`)\n"
                                          f"**Category:** {t.get('category_title')}\n"
                                          f"**Status:** {_status_and_stamp(t)}\n\n"
                                          f"**Permanently deletes:** players, rosters, results.")
        view = ConfirmDeleteTournamentView(self, interaction.user.id, tournament_id)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @f_admin.command(name="tournament-reassign-category", description="Admin: change a tournament's category (recomputes results if already entered).")
    @app_commands.autocomplete(tournament_id=_ac_any_tournament, category_id=_ac_category)
    async def fantasy_reassign_category(self, interaction: discord.Interaction,
                                         tournament_id: str, category_id: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        t = _find_tournament(data, tournament_id)
        if not t:
            return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)
        cat = next((c for c in data.get("categories", []) if c.get("id") == category_id), None)
        if not cat:
            return await interaction.response.send_message("❌ Category not found.", ephemeral=True)

        old_cat_title = t.get("category_title", t.get("category_id", "?"))
        new_cat_title = cat.get("title", category_id)

        # Update category fields
        t["category_id"]    = category_id
        t["category_title"] = new_cat_title

        recomputed = 0
        if t.get("results_entered") and t.get("results"):
            round_points_map: Dict[str, int] = cat.get("round_points", {})
            new_results = {}
            for key, r in t["results"].items():
                canonical              = r.get("round", "")
                tourn_pts              = round_points_map.get(canonical, 0)
                set_pts                = r.get("set_points", 0)
                perf                   = r.get("performance_points", 0)
                upset                  = r.get("upset_points", 0)
                r["tournament_points"] = tourn_pts
                r["total"]             = tourn_pts + set_pts + perf + upset
                new_results[key]       = r
                recomputed += 1
            t["results"] = new_results

        _save(data)

        lines = [
            f"✅ Category reassigned for **{t.get('name')}** (`{t.get('id')}`)",
            f"**From:** {old_cat_title}",
            f"**To:** {new_cat_title} (`{category_id}`)",
        ]
        if t.get("results_entered"):
            if recomputed:
                lines.append(f"♻️ Recomputed tournament points for **{recomputed}** players using new round-points map.")
            else:
                lines.append("ℹ️ Results are entered but no player data found to recompute.")
        else:
            lines.append("ℹ️ Results not yet entered — new category will apply when results are submitted.")

        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    @f_admin.command(name="tournament-end", description="Admin: submit results and complete a fantasy tournament.")
    async def fantasy_end(self, interaction: discord.Interaction):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        gid = interaction.guild.id if interaction.guild else 0
        ts = [t for t in data.get("tournaments", [])
              if t.get("guild_id") in (0, gid) and not t.get("results_entered")]
        if not ts:
            return await interaction.response.send_message(
                "❌ No tournaments awaiting results.", ephemeral=True)
        view = TournamentEndSelectView(self, interaction.user.id, ts)
        await interaction.response.send_message(
            "Select the tournament to enter results for:", view=view, ephemeral=True)


    async def _fantasy_create_set_unseeded(self, interaction: discord.Interaction, tournament_id: str, unseeded_text: str):
        data = _load()
        t = _find_tournament(data, tournament_id)
        if not t: return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)
        seeds = t.get("players", [])
        seed_keys = {_player_key(p["name"]) for p in seeds}
        names = _parse_multiline_list(unseeded_text)
        t["players"] = seeds + [{"name": n, "seed": None} for n in names if _player_key(n) not in seed_keys]
        _save(data)
        await self._fantasy_create_finalize_preview(interaction, tournament_id)

    async def _fantasy_create_finalize_preview(self, interaction: discord.Interaction, tournament_id: str):
        data = _load()
        t = _find_tournament(data, tournament_id)
        if not t: return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)
        lines = [f"**Tournament:** {t.get('name')} (`{t.get('id')}`)",
                 f"**Category:** {t.get('category_title')} (`{t.get('category_id')}`)", "",
                 "**Players:**"]
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
        t = _find_tournament(data, tournament_id)
        if not t: return await interaction.response.edit_message(content="❌ Not found.", embed=None, view=None)
        _mark_created(t); _save(data)
        await interaction.response.edit_message(
            content=f"✅ Fantasy tournament **confirmed**!\n**Name:** {t.get('name')}\n**ID:** `{t.get('id')}`",
            embed=None, view=None)

    async def _fantasy_end_submit(self, interaction: discord.Interaction, tournament_id: str, results_text: str):
        try:
            data = _load()
            t = _find_tournament(data, tournament_id)
            if not t: return await interaction.response.send_message("❌ Not found.", ephemeral=True)

            category_id = t.get("category_id")
            cat = next((c for c in data.get("categories", []) if c.get("id") == category_id), None)
            round_points_map: Dict[str, int] = (cat or {}).get("round_points", {})

            rows, parse_errors = _parse_results_lines(results_text)
            if parse_errors:
                view = RetryEndView(self, interaction.user.id, tournament_id, results_text)
                return await interaction.response.send_message("❌ Errors:\n" + "\n".join(parse_errors[:30]),
                                                                view=view, ephemeral=True)

            tp_keys = {_player_key(p["name"]) for p in t.get("players", [])}
            unknown = [r["player"] for r in rows if _player_key(r["player"]) not in tp_keys]
            given   = {_player_key(r["player"]) for r in rows}
            missing = [p["name"] for p in t.get("players", []) if _player_key(p["name"]) not in given]
            if unknown or missing:
                msg = ["❌ Validation failed."]
                if unknown: msg.append("\n**Unknown:**"); msg.extend([f"- {n}" for n in unknown[:50]])
                if missing: msg.append("\n**Missing:**");  msg.extend([f"- {n}" for n in missing[:50]])
                view = RetryEndView(self, interaction.user.id, tournament_id, results_text)
                return await interaction.response.send_message("\n".join(msg), view=view, ephemeral=True)

            # Build final rows — uses set/upset pts + match log if provided (full format from Claude chat)
            final_rows = []
            for r in rows:
                canonical = _normalize_round(r["round"]) or r["round"]
                tourn_pts = round_points_map.get(canonical, 0)
                sw        = r.get("sets_won", 0)
                sl        = r.get("sets_lost", 0)
                perf      = r.get("performance_pts", 0)
                upset     = r.get("upset_pts", 0)
                set_pts   = sw * 5 - sl * 2
                total     = tourn_pts + set_pts + perf + upset
                final_rows.append({
                    "player":            r["player"],
                    "round":             canonical,
                    "sets_won":          sw,
                    "sets_lost":         sl,
                    "tournament_points": tourn_pts,
                    "set_points":        set_pts,
                    "performance_points": perf,
                    "upset_points":      upset,
                    "total":             total,
                    "match_log":         r.get("match_log", ""),
                })

            has_full = any(r.get("sets_won") or r.get("upset_pts") or r.get("performance_pts") for r in rows)
            note = "" if has_full else "ℹ️ Set & upset points not included. Paste full format from Claude chat for complete scoring."
            await self._apply_results_data(interaction, tournament_id, final_rows,
                                            title="Results Saved", note=note)
        except Exception as e:
            view = RetryEndView(self, interaction.user.id, tournament_id, results_text)
            await interaction.response.send_message(f"❌ Error: `{type(e).__name__}: {e}`",
                                                     view=view, ephemeral=True)

    @commands.Cog.listener()
    async def on_ready(self):
        if not self._auto_close_task or self._auto_close_task.done():
            self._auto_close_task = asyncio.create_task(self._auto_close_loop())

    async def _auto_close_loop(self):
        await asyncio.sleep(10)
        while True:
            try:
                now = int(time.time())
                data = _load()
                changed = False
                for t in data.get("tournaments", []):
                    ac = t.get("auto_close_at")
                    if ac and now >= ac and t.get("picks_open", True) and _is_created(t):
                        t["picks_open"] = False
                        t["closed_at"] = t.get("closed_at") or now
                        t.pop("auto_close_at", None)
                        changed = True
                        print(f"[fantasy] Auto-closed {t.get('id')} — {t.get('name')}")
                if changed:
                    _save(data)
            except Exception as e:
                print(f"[fantasy] auto-close loop error: {e}")
            await asyncio.sleep(30)

    async def _apply_results_data(
        self,
        interaction: discord.Interaction,
        tournament_id: str,
        rows: List[dict],
        title: str = "Results Saved",
        note: str = "",
    ) -> None:
        data = _load()
        t = _find_tournament(data, tournament_id)
        if not t:
            if interaction.response.is_done():
                await interaction.edit_original_response(content="❌ Tournament not found.", embed=None, view=None)
            else:
                await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)
            return
        t["results"] = {_player_key(r["player"]): r for r in rows}
        t["results_entered"] = True
        t["picks_open"] = False
        t["completed_at"] = t.get("completed_at") or _now_unix()
        _save(data)

        lines = [f"✅ **{title}** — {t.get('name')}", ""]
        if note:
            lines += [note, ""]
        lines.append("Player — Round — Total (Tourn + Sets + Perf + Upset) | W-L")
        lines.append("")
        for r in sorted(rows, key=lambda x: x["total"], reverse=True):
            lines.append(
                f"**{r['player']}** — {r['round']} — **{r['total']}** "
                f"({r['tournament_points']} + {r['set_points']} + {r.get('performance_points',0)} + {r['upset_points']}) "
                f"| {r.get('sets_won', 0)}W-{r.get('sets_lost', 0)}L"
            )
        pager = PagerView(_chunk_pages(lines), interaction.user.id, title)
        if interaction.response.is_done():
            await interaction.edit_original_response(content=None, embed=pager._embed(), view=pager)
        else:
            await interaction.response.send_message(embed=pager._embed(), view=pager, ephemeral=True)

        # DM all users with their roster + total points
        asyncio.create_task(self._dm_results(t, rows))

    async def _dm_results(self, t: dict, rows: List[dict]):
        """DM every user their roster and points after a tournament ends."""
        await asyncio.sleep(2)
        seed_map = {_player_key(p["name"]): p.get("seed") for p in t.get("players", [])}
        results = {_player_key(r["player"]): r for r in rows}
        rosters = t.get("rosters") or {}
        tourn_name = t.get("name", "?")
        print(f"[fantasy] _dm_results: {len(rosters)} users to DM for {tourn_name!r}")

        for uid_str, roster in rosters.items():
            try:
                user = await self.bot.fetch_user(int(uid_str))
                total = 0
                pick_lines = []
                for name in roster[:5]:
                    seed = seed_map.get(_player_key(name))
                    label = _fmt_player(seed, name)
                    r = results.get(_player_key(name))
                    pts = int(r["total"]) if r else 0
                    total += pts
                    round_str = r.get("round", "?") if r else "no result"
                    pick_lines.append(f"{label} - **{pts}** ({round_str})")
                desc = [f"**Fantasy Results - {tourn_name}**", "",
                        "**Your Picks:**"] + pick_lines + ["", f"**Your Total: {total}**"]
                embed = discord.Embed(title="Fantasy Tournament Complete!", description="\n".join(desc))
                await user.send(embed=embed)
                print(f"[fantasy] DM sent to {uid_str}")
            except discord.Forbidden:
                print(f"[fantasy] DM blocked by {uid_str} (DMs disabled)")
            except Exception as e:
                print(f"[fantasy] DM failed for {uid_str}: {type(e).__name__}: {e}")

    # ── Leaderboard admin ─────────────────────────────────────────────────────

    @f_admin.command(name="ldb-clear", description="Admin: clear fantasy leaderboard blacklist.")
    async def fantasy_ldb_clear(self, interaction: discord.Interaction):
        if not _is_admin(interaction.user): return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load(); data["ldb_blacklist"] = []; _save(data)
        await interaction.response.send_message("✅ Fantasy leaderboard blacklist cleared.")

    @f_admin.command(name="ldb-wipe", description="Admin: wipe all leaderboard history (fresh start). Tournament data is preserved.")
    async def fantasy_ldb_wipe(self, interaction: discord.Interaction):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        data["ldb_reset_at"] = _now_unix()
        _save(data)
        await interaction.response.send_message(
            f"✅ Leaderboard wiped. Only tournaments completed after <t:{data['ldb_reset_at']}:F> will count.")

    @f_admin.command(name="ldb-blacklist", description="Admin: blacklist a user from all fantasy leaderboards.")
    async def fantasy_ldb_blacklist(self, interaction: discord.Interaction, user: discord.Member):
        if not _is_admin(interaction.user): return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load(); bl = set(int(x) for x in data.get("ldb_blacklist", [])); bl.add(int(user.id))
        data["ldb_blacklist"] = sorted(bl); _save(data)
        await interaction.response.send_message(f"✅ Blacklisted **{user.display_name}**.")

    @f_admin.command(name="ldb-blacklist-view", description="Admin: view fantasy leaderboard blacklist.")
    async def fantasy_ldb_blacklist_view(self, interaction: discord.Interaction):
        if not _is_admin(interaction.user): return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load(); bl = [int(x) for x in data.get("ldb_blacklist", [])]
        if not bl: return await interaction.response.send_message("ℹ️ No blacklisted users.")
        view = PagerView(_chunk_pages([f"- <@{uid}> (`{uid}`)" for uid in bl]),
                          interaction.user.id, "Fantasy Leaderboard Blacklist")
        await interaction.response.send_message(embed=view._embed(), view=view)

    @f_admin.command(name="ldb-whitelist", description="Admin: remove a user from the fantasy leaderboard blacklist.")
    async def fantasy_leaderboard_whitelist(self, interaction: discord.Interaction, user: discord.Member):
        if not _is_admin(interaction.user): return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load(); bl = set(int(x) for x in data.get("ldb_blacklist", []))
        if int(user.id) not in bl: return await interaction.response.send_message("ℹ️ Not blacklisted.")
        bl.remove(int(user.id)); data["ldb_blacklist"] = sorted(bl); _save(data)
        await interaction.response.send_message(f"✅ Whitelisted **{user.display_name}**.")

    @f_admin.command(name="set-roster", description="Admin: set a user's fantasy roster using the pick menu.")
    @app_commands.autocomplete(tournament_id=_ac_tournament)
    @app_commands.describe(tournament_id="Fantasy tournament ID", user="The user to set the roster for")
    async def fantasy_set_roster(self, interaction: discord.Interaction,
                                  tournament_id: str, user: discord.Member):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        t = _find_tournament(data, tournament_id)
        if not t:
            return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)
        pool = [PlayerEntry(name=p["name"], seed=p.get("seed")) for p in t.get("players", [])]
        if not pool:
            return await interaction.response.send_message("❌ No players in this tournament yet.", ephemeral=True)
        view = JoinFantasyView(
            self, interaction.user.id, tournament_id, pool,
            target_user_id=user.id,
            force_save=True,
            header=f"**Setting roster for {user.display_name} — Pick 5 players**"
        )
        await interaction.response.send_message(content=view._status_text(), view=view, ephemeral=True)

    @f_admin.command(name="edit-roster", description="Admin: edit a user's existing roster (bypasses closed picks).")
    @app_commands.autocomplete(tournament_id=_ac_tournament)
    @app_commands.describe(tournament_id="Fantasy tournament ID", user="The user whose roster to edit")
    async def fantasy_edit_roster(self, interaction: discord.Interaction,
                                   tournament_id: str, user: discord.Member):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        t = _find_tournament(data, tournament_id)
        if not t:
            return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)
        pool = [PlayerEntry(name=p["name"], seed=p.get("seed")) for p in t.get("players", [])]
        if not pool:
            return await interaction.response.send_message("❌ No players in this tournament yet.", ephemeral=True)
        existing = (t.get("rosters") or {}).get(str(user.id))
        note = f" (currently has {len(existing)} picks)" if existing else " (no existing roster)"
        view = JoinFantasyView(
            self, interaction.user.id, tournament_id, pool,
            target_user_id=user.id,
            force_save=True,
            header=f"**Editing roster for {user.display_name}{note} — Pick 5 players**"
        )
        await interaction.response.send_message(content=view._status_text(), view=view, ephemeral=True)
    # ── User commands ─────────────────────────────────────────────────────────

    @f_admin.command(name="overview", description="Admin: see every entrant and their picks at a glance.")
    @app_commands.autocomplete(tournament_id=_ac_tournament)
    async def fantasy_overview(self, interaction: discord.Interaction, tournament_id: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        t = _find_tournament(data, tournament_id)
        if not t:
            return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)

        rosters = t.get("rosters") or {}
        seed_map = {_player_key(p["name"]): p.get("seed") for p in t.get("players", [])}

        if not rosters:
            return await interaction.response.send_message(
                f"ℹ️ No entries yet for **{t.get('name')}**.", ephemeral=True)

        lines = [f"**Fantasy Overview — {t.get('name')}**",
                 f"*{len(rosters)} entrant(s)*", ""]

        for uid_str, picks in rosters.items():
            member = interaction.guild.get_member(int(uid_str)) if interaction.guild else None
            name = member.display_name if member else f"<@{uid_str}>"
            formatted = []
            for pick in (picks or [])[:5]:
                seed = seed_map.get(_player_key(pick))
                formatted.append(_fmt_player(seed, pick))
            picks_str = ",  ".join(formatted) if formatted else "—"
            lines.append(f"**{name}:** {picks_str}")

        # Paginate if needed (Discord message limit)
        pages = _chunk_pages(lines[3:])  # chunk entrant lines
        header = "\n".join(lines[:3])
        full_pages = [header + "\n" + p for p in pages] if pages else [header + "\n*No entries.*"]

        view = PagerView(full_pages, interaction.user.id, f"Fantasy Overview — {t.get('name')}")
        await interaction.response.send_message(embed=view._embed(), view=view, ephemeral=True)

    _STATUS_CHOICES = [
        app_commands.Choice(name="Open",                    value="Open"),
        app_commands.Choice(name="Closed & Results Pending", value="Closed & Results Pending"),
        app_commands.Choice(name="Completed",               value="Completed"),
    ]

    @fantasy.command(name="list", description="List fantasy tournaments.")
    @app_commands.describe(status="Optional: filter by status")
    @app_commands.choices(status=_STATUS_CHOICES)
    async def fantasy_list(self, interaction: discord.Interaction,
                            status: Optional[app_commands.Choice[str]] = None):
        data = _load()
        gid = interaction.guild.id if interaction.guild else 0
        is_admin = isinstance(interaction.user, discord.Member) and _is_admin(interaction.user)
        ts = [t for t in data.get("tournaments", [])
              if t.get("guild_id") in (0, gid) and _require_created_or_admin(interaction, t) is None]
        want = status.value if status else None
        if want: ts = [t for t in ts if _status_key(t) == want]
        if not ts:
            return await interaction.response.send_message(
                f"ℹ️ No fantasy tournaments{' with status ' + want if want else ''}.")
        ts.sort(key=lambda x: (x.get("name") or "").lower())
        lines = []
        for t in ts:
            line = f"- **{t.get('name')}** — `{t.get('id')}` — **{_status_and_stamp(t)}**"
            if is_admin:
                line += f"\n  ↳ Category: **{t.get('category_title', '?')}** (`{t.get('category_id', '?')}`)"
            lines.append(line)
        title = "Fantasy Tournaments" + (f" — {want}" if want else "")
        view = PagerView(_chunk_pages(lines), interaction.user.id, title)
        await interaction.response.send_message(embed=view._embed(), view=view)

    @fantasy.command(name="join", description="Join a fantasy tournament and pick your 5 players.")
    @app_commands.autocomplete(tournament_id=_ac_open_tournament)
    async def fantasy_join(self, interaction: discord.Interaction, tournament_id: str):
        data = _load()
        t = _find_tournament(data, tournament_id)
        msg = _require_created_or_admin(interaction, t)
        if msg: return await interaction.response.send_message(msg, ephemeral=True)
        if not t.get("picks_open", True):
            return await interaction.response.send_message("❌ This fantasy is closed — picks are locked.", ephemeral=True)
        pool = [PlayerEntry(name=p["name"], seed=p.get("seed")) for p in t.get("players", [])]
        has_roster = str(interaction.user.id) in (t.get("rosters") or {})
        header = "✏️ Edit your roster — pick 5 new players to replace your current picks." if has_roster else None
        view = JoinFantasyView(self, interaction.user.id, tournament_id, pool, header=header)
        await interaction.response.send_message(content=view._status_text(), view=view, ephemeral=True)

    async def _save_user_roster(self, interaction: discord.Interaction, tournament_id: str,
                                 user_id: int, picks: List[PlayerEntry],
                                 force_save: bool = False):
        data = _load()
        t = _find_tournament(data, tournament_id)
        if not t: return await interaction.response.send_message("❌ Not found.", ephemeral=True)
        if not force_save and not t.get("picks_open", True):
            return await interaction.response.send_message("❌ Picks are closed for this tournament.", ephemeral=True)
        t.setdefault("rosters", {})[str(user_id)] = [p.name for p in picks]; _save(data)
        seed_map = {_player_key(p["name"]): p.get("seed") for p in t.get("players", [])}
        saved_for = f" (for <@{user_id}>)" if user_id != interaction.user.id else ""
        lines = [f"✅ Roster saved{saved_for}.", "", f"**Tournament:** {t.get('name')}", "**Picks:**"]
        for i, name in enumerate([p.name for p in picks], 1):
            lines.append(f"{i}. {_fmt_player(seed_map.get(_player_key(name)), name)}")
        await interaction.response.edit_message(content="\n".join(lines), view=None)

    @fantasy.command(name="roster-view", description="View a user's fantasy picks.")
    @app_commands.autocomplete(tournament_id=_ac_tournament)
    async def fantasy_roster_view(self, interaction: discord.Interaction, tournament_id: str,
                                   user: Optional[discord.Member] = None):
        data = _load()
        t = _find_tournament(data, tournament_id)
        msg = _require_created_or_admin(interaction, t)
        if msg: return await interaction.response.send_message(msg, ephemeral=True)

        rosters = t.get("rosters") or {}
        is_admin = isinstance(interaction.user, discord.Member) and _is_admin(interaction.user)
        viewer_has_roster = str(interaction.user.id) in rosters

        # Non-admins can only view others' picks if they've submitted their own
        if user and user.id != interaction.user.id and not is_admin and not viewer_has_roster:
            return await interaction.response.send_message(
                "❌ You need to submit your own roster before viewing others' picks.",
                ephemeral=True)

        target = user or interaction.user
        roster = rosters.get(str(target.id))
        if not roster:
            return await interaction.response.send_message(
                f"ℹ️ {'That user has' if user else 'You have'} no roster for this tournament.")
        seed_map = {_player_key(p["name"]): p.get("seed") for p in t.get("players", [])}
        results = t.get("results", {}) if t.get("results_entered") else {}
        total_points = 0
        lines = [f"**Roster — {target.display_name}**",
                 f"**Tournament:** {t.get('name')} (`{t.get('id')}`)", "", "**Picks:**"]
        for i, name in enumerate(roster[:5], 1):
            seed = seed_map.get(_player_key(name))
            label = _fmt_player(seed, name)
            if results:
                r = results.get(_player_key(name)); pts = int(r["total"]) if r else 0
                total_points += pts; lines.append(f"{i}. {label} — **{pts}**")
            else:
                lines.append(f"{i}. {label}")
        if results:
            lines.extend(["", f"**Total Points:** **{total_points}**", "",
                           "Use the menu below for a detailed breakdown."])
        embed = discord.Embed(title="Fantasy Roster", description="\n".join(lines))
        view = RosterPickMenuView(interaction.user.id, roster[:5], seed_map, results,
                                   f"{target.display_name}'s Picks")
        await interaction.response.send_message(embed=embed, view=view)

    @fantasy.command(name="results", description="View sorted fantasy results for a tournament.")
    @app_commands.autocomplete(tournament_id=_ac_tournament)
    async def fantasy_results(self, interaction: discord.Interaction, tournament_id: str):
        data = _load()
        t = _find_tournament(data, tournament_id)
        msg = _require_created_or_admin(interaction, t)
        if msg: return await interaction.response.send_message(msg, ephemeral=True)
        if not t.get("results_entered"):
            return await interaction.response.send_message("❌ Results not submitted yet.", ephemeral=True)
        seed_map = {_player_key(p["name"]): p.get("seed") for p in t.get("players", [])}
        view = ResultsMainView(interaction.user.id, t, seed_map)
        await interaction.response.send_message(embed=view._embed(), view=view)

    @fantasy.command(name="user-results", description="Show all users' total points for a completed tournament.")
    @app_commands.autocomplete(tournament_id=_ac_tournament)
    async def fantasy_user_results(self, interaction: discord.Interaction, tournament_id: str):
        data = _load()
        t = _find_tournament(data, tournament_id)
        msg = _require_created_or_admin(interaction, t)
        if msg: return await interaction.response.send_message(msg, ephemeral=True)
        if not t.get("results_entered"):
            return await interaction.response.send_message("❌ Results not entered yet.", ephemeral=True)
        scores = {uid: pts for uid, pts in _all_user_scores_for_tournament(t).items()
                  if not _is_blacklisted(data, uid)}
        if not scores:
            return await interaction.response.send_message("ℹ️ No user rosters found.", ephemeral=True)
        items = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        ranks = _dense_ranks(scores)
        header = [f"**User Results — {t.get('name')}** (`{t.get('id')}`)", "",
                  "Format: Rank. User — Total Points", ""]
        body = [f"{ranks.get(uid,0)}. <@{uid}> — **{pts}**" for uid, pts in items]
        pages: List[str] = []
        for start in range(0, max(1, len(body)), 20):
            pages.append("\n".join(header + body[start:start+20]))
        view = PagerView(pages, interaction.user.id, "Fantasy User Results")
        await interaction.response.send_message(embed=view._embed(), view=view)

    # ── Leaderboards ──────────────────────────────────────────────────────────

    @fantasy.command(name="leaderboard", description="View fantasy user leaderboards.")
    @app_commands.describe(category_id="Optional: category ID for category leaderboards",
                            days_back="Optional: use for 'Last N days' leaderboards")
    @app_commands.autocomplete(category_id=_ac_category)
    async def fantasy_leaderboard_view(self, interaction: discord.Interaction,
                                        category_id: Optional[str] = None,
                                        days_back: Optional[int] = None):
        view = FantasyLeaderboardView(self, interaction.user.id, category_id=category_id, days_back=days_back)
        embed = discord.Embed(title="Fantasy Leaderboards",
                              description="Pick a leaderboard from the menu.\n\n"
                                          "- Category options require `category_id`.\n"
                                          "- Last N days options require `days_back`.\n"
                                          "- Averages require **min 5 tournaments**.")
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def _render_leaderboard(self, interaction: discord.Interaction, mode: str,
                                   category_id: Optional[str], days_back: Optional[int]):
        data = _load()
        gid = interaction.guild.id if interaction.guild else None
        cat_title = None
        if category_id:
            cat = next((c for c in data.get("categories", []) if c.get("id") == category_id), None)
            if not cat:
                return await interaction.response.send_message("❌ Category not found.", ephemeral=True)
            cat_title = cat.get("title")
        lb = _compute_leaderboard(data, gid, mode, category_id, days_back, min_tournaments=5)
        if not lb:
            return await interaction.response.send_message("ℹ️ No leaderboard data found.", ephemeral=True)
        mode_title = {"points_total": "Most Points", "avg_points": "Highest Average Points",
                      "wins": "Most Tournament Wins", "top5": "Most Top 5 Finishes",
                      "top10": "Most Top 10 Finishes"}.get(mode, mode)
        filters = []
        if category_id: filters.append(f"Category: {cat_title or category_id}")
        if days_back is not None: filters.append(f"Last {days_back} days")
        header = [f"**{mode_title}**"]
        if filters: header.append(" • ".join(filters))
        header.extend(["", "Avg: Rank. User — Avg — Played" if mode == "avg_points"
                           else "Rank. User — Value", ""])
        body = []
        for i, (uid, val, cnt) in enumerate(lb, 1):
            if mode == "avg_points":
                body.append(f"{i}. <@{uid}> — **{val:.2f}** — *{cnt}*")
            else:
                body.append(f"{i}. <@{uid}> — **{int(val) if float(val).is_integer() else f'{val:.2f}'}**")
        pages: List[str] = []
        for start in range(0, max(1, len(body)), 20):
            pages.append("\n".join(header + body[start:start+20]))
        view = PagerView(pages, interaction.user.id, f"Fantasy Leaderboard — {mode_title}")
        await interaction.response.send_message(embed=view._embed(), view=view, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(FantasyCog(bot))