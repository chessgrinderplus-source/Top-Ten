# modules/gear.py
from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands
from collections import Counter

from modules.economy import get_balance, remove_balance

# =========================
# Storage
# =========================
def _norm_title(s: str) -> str:
    return " ".join((s or "").strip().lower().split())

def _racket_title_exists(db, title: str, exclude_id: str | None = None) -> bool:
    want = _norm_title(title)

    for rid, row in (db.get("rackets", {}) or {}).items():
        if exclude_id is not None and str(rid) == str(exclude_id):
            continue

        if isinstance(row, dict):
            if _norm_title(row.get("name", "")) == want:
                return True

    return False

def _data_dir() -> str:
    try:
        import config  # type: ignore
        return str(getattr(config, "DATA_DIR", "data"))
    except Exception:
        return "data"


GEAR_PATH = os.path.join(_data_dir(), "gear.json")


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _load_json(path: str, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path: str, data) -> None:
    _ensure_dir(os.path.dirname(path))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)


def _db() -> Dict[str, Any]:
    return _load_json(
        GEAR_PATH,
        {
            "rackets": {},     # id -> row
            "shoes": {},       # id -> row
            "inv": {},         # guild_id -> user_id -> {"rackets":[frame_ids], "shoes":[ids], "strung_rackets":[...], "racket_access":{racket_id:true}, "equipped_racket": {...}|None, "equipped_shoes": id|None}
        },
    )


def _save_db(db: Dict[str, Any]) -> None:
    _save_json(GEAR_PATH, db)


def _is_admin(member: discord.Member) -> bool:
    return bool(getattr(member.guild_permissions, "administrator", False))


def _make_id(prefix: str) -> str:
    import random
    return f"{prefix}-{random.randint(1000, 9999)}"


def _clamp_1dp(v: float, lo: float, hi: float) -> float:
    try:
        x = float(v)
    except Exception:
        x = lo
    x = max(lo, min(hi, x))
    return round(x, 1)

STRING_PATTERNS = ["16x18", "16x19", "16x20", "18x19", "18x20"]

def _clamp_int(v: Any, lo: int, hi: int, default: int) -> int:
    try:
        x = int(v)
    except Exception:
        x = default
    return max(lo, min(hi, x))

def _owned_frame_counts(db: Dict[str, Any], guild_id: int, user_id: int) -> Dict[str, int]:
    inv = _inv_row(guild_id, user_id)
    ids = list(inv.get("rackets", []) or [])
    out: Dict[str, int] = {}
    for rid in ids:
        out[str(rid)] = out.get(str(rid), 0) + 1
    return out

def _racket_label(db: Dict[str, Any], racket_id: str) -> str:
    row = (db.get("rackets", {}) or {}).get(str(racket_id))
    if isinstance(row, dict):
        return f"{row.get('brand_emoji','')} {row.get('name','Racket')} ({racket_id})"
    return str(racket_id)

# =========================
# Inventory helpers
# =========================
def _inv_row(guild_id: int, user_id: int) -> Dict[str, Any]:
    db = _db()
    grow = db.setdefault("inv", {}).setdefault(str(guild_id), {}).get(str(user_id))
    if not isinstance(grow, dict):
        grow = {
            "rackets": [],
            "shoes": [],
            "strung_rackets": [],
            "racket_access": {},
            "equipped_racket": None,   # strung racket dict or None
            "equipped_shoes": None,    # shoe id or None
        }
        db["inv"][str(guild_id)][str(user_id)] = grow
        _save_db(db)
    # backfill missing keys for old rows
    changed = False
    if "equipped_racket" not in grow:
        grow["equipped_racket"] = None
        changed = True
    if "equipped_shoes" not in grow:
        grow["equipped_shoes"] = None
        changed = True
    if changed:
        db["inv"][str(guild_id)][str(user_id)] = grow
        _save_db(db)
    return grow


def gear_get_equipped(guild_id: int, user_id: int) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """
    Returns (racket_model_row, shoes_row).
    racket_model_row includes the strung racket details (pattern, tension, weight) merged in.
    Used by matchsim.
    """
    db = _db()
    inv = _inv_row(guild_id, user_id)

    # --- Racket ---
    racket_result: Optional[Dict[str, Any]] = None
    eq_racket = inv.get("equipped_racket")
    if isinstance(eq_racket, dict):
        frame_id = str(eq_racket.get("frame_id", ""))
        model = (db.get("rackets", {}) or {}).get(frame_id)
        if isinstance(model, dict):
            racket_result = dict(model)
            racket_result["strung_pattern"] = eq_racket.get("pattern", "")
            racket_result["strung_tension"] = eq_racket.get("tension", 55)
            racket_result["strung_weight"] = eq_racket.get("weight", model.get("weight", ""))

    # --- Shoes ---
    shoes_result: Optional[Dict[str, Any]] = None
    eq_shoes = inv.get("equipped_shoes")
    if eq_shoes:
        s = (db.get("shoes", {}) or {}).get(str(eq_shoes))
        if isinstance(s, dict):
            shoes_result = s

    return (racket_result, shoes_result)


def gear_get_equipped_strung_count_for_frame(guild_id: int, user_id: int) -> Tuple[Optional[str], int]:
    """
    Returns (equipped_frame_id, count_of_strung_rackets_matching_equipped_spec).
    Counts only rackets matching the exact equipped pattern+tension+weight.
    Returns (None, 0) if nothing equipped.
    """
    inv = _inv_row(guild_id, user_id)
    eq_racket = inv.get("equipped_racket")
    if not isinstance(eq_racket, dict):
        return (None, 0)
    frame_id = str(eq_racket.get("frame_id", ""))
    if not frame_id:
        return (None, 0)
    eq_pattern = str(eq_racket.get("pattern", ""))
    eq_tension  = str(eq_racket.get("tension", ""))
    eq_weight   = str(eq_racket.get("weight", ""))
    strung = inv.get("strung_rackets", []) or []
    count = sum(
        1 for r in strung
        if isinstance(r, dict)
        and str(r.get("frame_id", "")) == frame_id
        and str(r.get("pattern", "")) == eq_pattern
        and str(r.get("tension", "")) == eq_tension
        and str(r.get("weight", "")) == eq_weight
    )
    return (frame_id, count)


def gear_has_shoes_equipped(guild_id: int, user_id: int) -> bool:
    """Returns True if the player has shoes equipped."""
    inv = _inv_row(guild_id, user_id)
    eq_shoes = inv.get("equipped_shoes")
    return bool(eq_shoes)


# =========================
# Pagination
# =========================
class SimplePager(discord.ui.View):
    def __init__(self, pages: List[discord.Embed], author_id: int):
        super().__init__(timeout=180)
        self.pages = pages
        self.author_id = author_id
        self.i = 0
        self.prev_btn.disabled = True
        if len(pages) <= 1:
            self.next_btn.disabled = True

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.author_id:
            await interaction.response.send_message("❌ Only the command user can use these buttons.", ephemeral=True)
            return False
        return True

    def _sync(self):
        self.prev_btn.disabled = self.i <= 0
        self.next_btn.disabled = self.i >= (len(self.pages) - 1)

    @discord.ui.button(label="Prev", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.i = max(0, self.i - 1)
        self._sync()
        await interaction.response.edit_message(embed=self.pages[self.i], view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.i = min(len(self.pages) - 1, self.i + 1)
        self._sync()
        await interaction.response.edit_message(embed=self.pages[self.i], view=self)


class _StringRacketModal(discord.ui.Modal, title="String Rackets"):
    qty = discord.ui.TextInput(label="How many frames to string?", placeholder="e.g. 3", required=True, max_length=3)
    tension = discord.ui.TextInput(label="Tension (35–70 lbs)", placeholder="e.g. 55", required=True, max_length=2)

    def __init__(self, view: "StringRacketView"):
        super().__init__()
        self.view_ref = view

    async def on_submit(self, interaction: discord.Interaction):
        self.view_ref.qty_val = _clamp_int(self.qty.value, 1, 999, 1)
        self.view_ref.tension_val = _clamp_int(self.tension.value, 35, 70, 55)
        await interaction.response.edit_message(embed=self.view_ref._embed(), view=self.view_ref)


class StringRacketView(discord.ui.View):
    def __init__(self, *, guild_id: int, user_id: int):
        super().__init__(timeout=180)
        self.guild_id = guild_id
        self.user_id = user_id

        self.racket_id: Optional[str] = None
        self.pattern: str = "16x19"
        self.qty_val: int = 1
        self.tension_val: int = 55

        db = _db()
        counts = _owned_frame_counts(db, guild_id, user_id)
        owned_ids = [rid for rid, c in counts.items() if c > 0]

        # Build racket options by NAME (not id)
        opts: List[discord.SelectOption] = []
        for rid in owned_ids[:25]:
            label = _racket_label(db, rid)
            opts.append(discord.SelectOption(label=label[:100], value=rid, description=f"Frames owned: {counts.get(rid,0)}"[:100]))

        if opts:
            self.racket_id = opts[0].value
        else:
            self.racket_id = None

        self.racket_select = discord.ui.Select(
            placeholder="Choose a racket frame type…",
            min_values=1, max_values=1,
            options=opts if opts else [discord.SelectOption(label="No frames owned", value="__NONE__", description="Buy frames first")],
        )
        self.racket_select.callback = self._on_racket
        self.add_item(self.racket_select)

        self.pattern_select = discord.ui.Select(
            placeholder="Choose string pattern…",
            min_values=1, max_values=1,
            options=[discord.SelectOption(label=p, value=p) for p in STRING_PATTERNS],
        )
        self.pattern_select.callback = self._on_pattern
        self.add_item(self.pattern_select)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Only the command user can use this menu.", ephemeral=True)
            return False
        return True

    def _embed(self) -> discord.Embed:
        db = _db()
        counts = _owned_frame_counts(db, self.guild_id, self.user_id)

        e = discord.Embed(title="🧵 String Rackets", color=discord.Color.blurple())

        if not self.racket_id:
            e.description = "You don't own any racket **frames** yet.\nUse `/racket-shop` then `/racket-buy`."
            return e

        if self.racket_id == "__NONE__":
            e.description = "You don't own any racket **frames** yet."
            return e

        row = (db.get("rackets", {}) or {}).get(str(self.racket_id))
        string_price = int(row.get("string_price", 0)) if isinstance(row, dict) else 0
        total_cost = string_price * int(self.qty_val)

        e.add_field(name="Frame Type", value=_racket_label(db, str(self.racket_id))[:1024], inline=False)
        e.add_field(name="Frames Owned", value=str(counts.get(str(self.racket_id), 0)), inline=True)
        e.add_field(name="Pattern", value=self.pattern, inline=True)
        e.add_field(name="Tension", value=f"{self.tension_val} lbs", inline=True)
        e.add_field(name="Quantity", value=str(self.qty_val), inline=True)
        e.add_field(name="Cost", value=f"{total_cost:,}", inline=True)

        e.set_footer(text="Note: Buying a racket gives a FRAME only. Stringing converts frames into STRUNG rackets.")
        return e

    async def _on_racket(self, interaction: discord.Interaction):
        v = self.racket_select.values[0]
        self.racket_id = None if v == "__NONE__" else v
        await interaction.response.edit_message(embed=self._embed(), view=self)

    async def _on_pattern(self, interaction: discord.Interaction):
        self.pattern = self.pattern_select.values[0]
        await interaction.response.edit_message(embed=self._embed(), view=self)

    @discord.ui.button(label="Set qty + tension…", style=discord.ButtonStyle.primary, row=2)
    async def set_modal(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(_StringRacketModal(self))

    @discord.ui.button(label="String", style=discord.ButtonStyle.success, row=2)
    async def do_string(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.racket_id or self.racket_id == "__NONE__":
            return await interaction.response.send_message("❌ No racket selected.", ephemeral=True)

        db = _db()
        rack_row = (db.get("rackets", {}) or {}).get(str(self.racket_id))
        if not isinstance(rack_row, dict):
            return await interaction.response.send_message("❌ Racket model not found.", ephemeral=True)

        string_price = int(rack_row.get("string_price", 0))
        total_cost = int(string_price) * int(self.qty_val)

        inv = _inv_row(self.guild_id, self.user_id)
        frames = list(inv.get("rackets", []) or [])
        owned = sum(1 for x in frames if str(x) == str(self.racket_id))
        if owned < int(self.qty_val):
            return await interaction.response.send_message(
                f"❌ You only have **{owned}** frames of that type.",
                ephemeral=True
            )

        if get_balance(interaction.user.id) < total_cost or not remove_balance(interaction.user.id, total_cost):
            return await interaction.response.send_message("❌ You don't have enough currency for stringing.", ephemeral=True)

        # Remove frames
        removed = 0
        new_frames: List[Any] = []
        for x in frames:
            if removed < int(self.qty_val) and str(x) == str(self.racket_id):
                removed += 1
                continue
            new_frames.append(x)
        inv["rackets"] = new_frames

        # Add strung rackets — store full details including weight
        inv.setdefault("strung_rackets", [])
        weight = rack_row.get("weight", "") or ""
        for _ in range(int(self.qty_val)):
            inv["strung_rackets"].append({
                "frame_id": str(self.racket_id),
                "pattern": str(self.pattern),
                "tension": int(self.tension_val),
                "weight": str(weight),
            })

        db.setdefault("inv", {}).setdefault(str(self.guild_id), {})[str(self.user_id)] = inv
        _save_db(db)

        await interaction.response.edit_message(content="✅ Stringing complete.", embed=None, view=None)
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, row=2)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="❌ Cancelled.", embed=None, view=None)
        self.stop()


# =========================
# Equip racket select view (for when multiple strung rackets share the same frame_id)
# =========================
class EquipRacketSelectView(discord.ui.View):
    """
    Shown when a player runs /gear-equip with a racket_id that has multiple
    strung versions (different pattern/tension/weight). Lets them pick which one.
    """
    def __init__(
        self,
        *,
        guild_id: int,
        user_id: int,
        racket_id: str,
        candidates: List[Dict[str, Any]],  # list of strung racket dicts sharing the frame_id
        model_name: str,
        shoe_id: Optional[str] = None,  # if the user also wants to equip shoes simultaneously
    ):
        super().__init__(timeout=60)
        self.guild_id = guild_id
        self.user_id = user_id
        self.racket_id = racket_id
        self.candidates = candidates
        self.model_name = model_name
        self.shoe_id = shoe_id

        # Build select options — deduplicate by (pattern, tension, weight) but track counts
        # We group duplicates so the player picks a spec, not an individual copy
        from collections import Counter as _Counter
        spec_counts: Dict[Tuple[str, str, str], int] = {}
        for r in candidates:
            key = (
                str(r.get("pattern", "")),
                str(r.get("tension", "")),
                str(r.get("weight", "")),
            )
            spec_counts[key] = spec_counts.get(key, 0) + 1

        opts: List[discord.SelectOption] = []
        for i, ((pat, ten, wt), cnt) in enumerate(spec_counts.items()):
            label = f"Pattern: {pat} | Tension: {ten} lbs"
            if wt:
                label += f" | {wt}"
            desc = f"x{cnt} available" if cnt > 1 else "1 available"
            # value encodes the spec so we can find the right strung racket on selection
            value = f"{pat}|{ten}|{wt}"
            opts.append(discord.SelectOption(label=label[:100], value=value[:100], description=desc))
            if len(opts) >= 25:
                break

        self.select = discord.ui.Select(
            placeholder="Choose which string setup to equip…",
            min_values=1,
            max_values=1,
            options=opts,
        )
        self.select.callback = self._on_select
        self.add_item(self.select)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Only the command user can use this.", ephemeral=True)
            return False
        return True

    async def _on_select(self, interaction: discord.Interaction):
        chosen = self.select.values[0]
        parts = chosen.split("|", 2)
        chosen_pat = parts[0] if len(parts) > 0 else ""
        chosen_ten = parts[1] if len(parts) > 1 else ""
        chosen_wt  = parts[2] if len(parts) > 2 else ""

        # Find the first strung racket matching this spec
        found: Optional[Dict[str, Any]] = None
        for r in self.candidates:
            if (
                str(r.get("pattern", "")) == chosen_pat
                and str(r.get("tension", "")) == chosen_ten
                and str(r.get("weight", "")) == chosen_wt
            ):
                found = r
                break

        if found is None:
            await interaction.response.edit_message(content="❌ Could not find that racket spec. Try again.", view=None)
            self.stop()
            return

        db = _db()
        inv = _inv_row(self.guild_id, self.user_id)
        lines: List[str] = []

        inv["equipped_racket"] = {
            "frame_id": str(self.racket_id),
            "pattern": str(found.get("pattern", "")),
            "tension": int(found.get("tension", 55)),
            "weight": str(found.get("weight", "")),
        }

        tension_display = found.get("tension", "")
        pattern_display = found.get("pattern", "")
        lines.append(f"🎾 Equipped **{self.model_name}** (Pattern: {pattern_display}, Tension: {tension_display} lbs)")

        # Also equip shoes if requested
        if self.shoe_id is not None:
            owned_shoes = inv.get("shoes", []) or []
            if str(self.shoe_id) in [str(s) for s in owned_shoes]:
                inv["equipped_shoes"] = str(self.shoe_id)
                shoe_row = (db.get("shoes", {}) or {}).get(str(self.shoe_id))
                sname = shoe_row.get("name", self.shoe_id) if isinstance(shoe_row, dict) else self.shoe_id
                lines.append(f"👟 Equipped **{sname}**")
            else:
                lines.append("⚠️ Shoe ID not owned — shoes not equipped.")

        db.setdefault("inv", {}).setdefault(str(self.guild_id), {})[str(self.user_id)] = inv
        _save_db(db)

        await interaction.response.edit_message(
            content="✅ " + "\n".join(lines),
            view=None,
        )
        self.stop()

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, row=1)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="❌ Cancelled.", view=None)
        self.stop()


# =========================
# Equip flow: choice buttons → modal → equip logic
# =========================

async def _do_equip_racket(interaction: discord.Interaction, guild_id: int, user_id: int, racket_id: str) -> None:
    """Shared racket equip logic called after the user supplies a racket frame ID."""
    db = _db()
    inv = _inv_row(guild_id, user_id)
    strung_rows = inv.get("strung_rackets", []) or []

    candidates = [r for r in strung_rows if isinstance(r, dict) and str(r.get("frame_id")) == str(racket_id)]

    if not candidates:
        await interaction.response.edit_message(
            content=(
                "❌ You don't have a strung racket with that frame ID.\n"
                "Use `/my-rackets` to see your strung rackets, or `/string-racket` to string frames."
            ),
            view=None,
        )
        return

    specs = set(
        (str(r.get("pattern", "")), str(r.get("tension", "")), str(r.get("weight", "")))
        for r in candidates
    )

    if len(specs) > 1:
        model = (db.get("rackets", {}) or {}).get(str(racket_id))
        model_name = model.get("name", racket_id) if isinstance(model, dict) else racket_id
        view = EquipRacketSelectView(
            guild_id=guild_id,
            user_id=user_id,
            racket_id=racket_id,
            candidates=candidates,
            model_name=model_name,
        )
        await interaction.response.edit_message(
            content=f"🎾 You have **{len(specs)} different string setups** for this racket.\nChoose which one to equip:",
            view=view,
        )
        return

    # Single spec — equip directly
    found = candidates[0]
    inv["equipped_racket"] = {
        "frame_id": str(found.get("frame_id")),
        "pattern": str(found.get("pattern", "")),
        "tension": int(found.get("tension", 55)),
        "weight": str(found.get("weight", "")),
    }
    db.setdefault("inv", {}).setdefault(str(guild_id), {})[str(user_id)] = inv
    _save_db(db)

    model = (db.get("rackets", {}) or {}).get(str(racket_id))
    name = model.get("name", racket_id) if isinstance(model, dict) else racket_id
    await interaction.response.edit_message(
        content=f"✅ 🎾 Equipped **{name}** (Pattern: {found.get('pattern','')}, Tension: {found.get('tension','')} lbs)",
        view=None,
    )


async def _do_equip_shoes(interaction: discord.Interaction, guild_id: int, user_id: int, shoe_id: str) -> None:
    """Shared shoe equip logic called after the user supplies a shoe ID."""
    db = _db()
    inv = _inv_row(guild_id, user_id)
    owned_shoes = inv.get("shoes", []) or []

    if str(shoe_id) not in [str(s) for s in owned_shoes]:
        await interaction.response.edit_message(
            content="❌ You don't own those shoes. Use `/shoe-shop` and `/shoe-buy` first.",
            view=None,
        )
        return

    inv["equipped_shoes"] = str(shoe_id)
    db.setdefault("inv", {}).setdefault(str(guild_id), {})[str(user_id)] = inv
    _save_db(db)

    shoe_row = (db.get("shoes", {}) or {}).get(str(shoe_id))
    sname = shoe_row.get("name", shoe_id) if isinstance(shoe_row, dict) else shoe_id
    await interaction.response.edit_message(
        content=f"✅ 👟 Equipped **{sname}**",
        view=None,
    )


class _EquipRacketModal(discord.ui.Modal, title="Equip Racket"):
    racket_id_input = discord.ui.TextInput(
        label="Racket Frame ID",
        placeholder="e.g. racket-1234  (find it in /my-rackets)",
        required=True,
        max_length=50,
    )

    def __init__(self, guild_id: int, user_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction):
        racket_id = self.racket_id_input.value.strip()
        await _do_equip_racket(interaction, self.guild_id, self.user_id, racket_id)


class _EquipShoesModal(discord.ui.Modal, title="Equip Shoes"):
    shoe_id_input = discord.ui.TextInput(
        label="Shoe ID",
        placeholder="e.g. shoes-5678  (find it in /my-shoes)",
        required=True,
        max_length=50,
    )

    def __init__(self, guild_id: int, user_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction):
        shoe_id = self.shoe_id_input.value.strip()
        await _do_equip_shoes(interaction, self.guild_id, self.user_id, shoe_id)


class _EquipChoiceView(discord.ui.View):
    """Initial view for /gear-equip — two buttons to choose racket or shoes."""

    def __init__(self, guild_id: int, user_id: int):
        super().__init__(timeout=60)
        self.guild_id = guild_id
        self.user_id = user_id

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ Only the command user can use this.", ephemeral=True)
            return False
        return True

    @discord.ui.button(label="🎾 Equip Racket", style=discord.ButtonStyle.primary)
    async def equip_racket(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(_EquipRacketModal(self.guild_id, self.user_id))

    @discord.ui.button(label="👟 Equip Shoes", style=discord.ButtonStyle.primary)
    async def equip_shoes(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(_EquipShoesModal(self.guild_id, self.user_id))

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.edit_message(content="❌ Cancelled.", view=None)
        self.stop()


# =========================
# Cog
# =========================
class GearCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # ---------- Admin: rackets ----------
    @app_commands.command(name="racket-add", description="(Admin) Add a racket to the shop.")
    @app_commands.guild_only()
    async def racket_add(
        self,
        interaction: discord.Interaction,
        name: str,
        brand_emoji: str,
        power: float,
        spin: float,
        control: float,
        weight: Optional[str] = None,
        access_price: app_commands.Range[int, 0, 10_000_000_000_000] = 0,
        frame_price: app_commands.Range[int, 0, 10_000_000] = 0,
        string_price: app_commands.Range[int, 0, 10_000_000] = 0,
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        db = _db()
        if _racket_title_exists(db, name):
            return await interaction.response.send_message(
                f"❌ A racket titled **{name}** already exists. Titles must be unique.",
                ephemeral=True
            )

        rid = _make_id("racket")
        db.setdefault("rackets", {})[rid] = {
            "id": rid,
            "name": name,
            "brand_emoji": brand_emoji,
            "power": _clamp_1dp(power, 1.0, 100.0),
            "spin": _clamp_1dp(spin, 1.0, 100.0),
            "control": _clamp_1dp(control, 1.0, 100.0),
            "weight": weight or "",
            "access_price": int(access_price),
            "frame_price": int(frame_price),
            "string_price": int(string_price),
        }
        _save_db(db)
        await interaction.response.send_message(f"✅ Racket added: **{name}** (`{rid}`)", ephemeral=False)

    @app_commands.command(name="racket-edit", description="(Admin) Edit a racket.")
    @app_commands.guild_only()
    async def racket_edit(
        self,
        interaction: discord.Interaction,
        racket_id: str,
        name: Optional[str] = None,
        brand_emoji: Optional[str] = None,
        power: Optional[float] = None,
        spin: Optional[float] = None,
        control: Optional[float] = None,
        weight: Optional[str] = None,
        access_price: Optional[int] = None,
        frame_price: Optional[int] = None,
        string_price: Optional[int] = None,
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        db = _db()
        row = (db.get("rackets", {}) or {}).get(racket_id)
        if not isinstance(row, dict):
            return await interaction.response.send_message("❌ Racket not found.", ephemeral=True)

        if name is not None:
            if _racket_title_exists(db, name, exclude_id=racket_id):
                return await interaction.response.send_message(
                    f"❌ A racket titled **{name}** already exists. Titles must be unique.",
                    ephemeral=True
                )
            row["name"] = name
        if brand_emoji is not None:
            row["brand_emoji"] = brand_emoji
        if power is not None:
            row["power"] = _clamp_1dp(power, 1.0, 100.0)
        if spin is not None:
            row["spin"] = _clamp_1dp(spin, 1.0, 100.0)
        if control is not None:
            row["control"] = _clamp_1dp(control, 1.0, 100.0)
        if weight is not None:
            row["weight"] = weight
        if access_price is not None:
            row["access_price"] = int(access_price)
        if frame_price is not None:
            row["frame_price"] = int(frame_price)
        if string_price is not None:
            row["string_price"] = int(string_price)

        db["rackets"][racket_id] = row
        _save_db(db)
        await interaction.response.send_message(f"✅ Racket updated: `{racket_id}`", ephemeral=False)

    @app_commands.command(name="racket-delete", description="(Admin) Delete a racket.")
    @app_commands.guild_only()
    async def racket_delete(self, interaction: discord.Interaction, racket_id: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        db = _db()
        rackets = db.get("rackets", {}) or {}
        if racket_id not in rackets:
            return await interaction.response.send_message("❌ Racket not found.", ephemeral=True)
        rackets.pop(racket_id, None)
        db["rackets"] = rackets
        _save_db(db)
        await interaction.response.send_message(f"🗑️ Deleted racket `{racket_id}`.", ephemeral=False)

    # ---------- Admin: shoes ----------
    @app_commands.command(name="shoe-add", description="(Admin) Add shoes to the shop.")
    @app_commands.guild_only()
    async def shoe_add(
        self,
        interaction: discord.Interaction,
        name: str,
        brand_emoji: str,
        footwork_impact: float,
        price: app_commands.Range[int, 0, 10_000_000] = 0,
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        db = _db()
        sid = _make_id("shoes")
        db.setdefault("shoes", {})[sid] = {
            "id": sid,
            "name": name,
            "brand_emoji": brand_emoji,
            "footwork_impact": _clamp_1dp(footwork_impact, 1.0, 100.0),
            "price": int(price),
        }
        _save_db(db)
        await interaction.response.send_message(f"✅ Shoes added: **{name}** (`{sid}`)", ephemeral=False)

    @app_commands.command(name="shoe-edit", description="(Admin) Edit shoes.")
    @app_commands.guild_only()
    async def shoe_edit(
        self,
        interaction: discord.Interaction,
        shoe_id: str,
        name: Optional[str] = None,
        brand_emoji: Optional[str] = None,
        footwork_impact: Optional[float] = None,
        price: Optional[int] = None,
    ):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        db = _db()
        row = (db.get("shoes", {}) or {}).get(shoe_id)
        if not isinstance(row, dict):
            return await interaction.response.send_message("❌ Shoes not found.", ephemeral=True)

        if name is not None:
            row["name"] = name
        if brand_emoji is not None:
            row["brand_emoji"] = brand_emoji
        if footwork_impact is not None:
            row["footwork_impact"] = _clamp_1dp(footwork_impact, 1.0, 100.0)
        if price is not None:
            row["price"] = int(price)

        db["shoes"][shoe_id] = row
        _save_db(db)
        await interaction.response.send_message(f"✅ Shoes updated: `{shoe_id}`", ephemeral=False)

    @app_commands.command(name="shoe-delete", description="(Admin) Delete shoes.")
    @app_commands.guild_only()
    async def shoe_delete(self, interaction: discord.Interaction, shoe_id: str):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        db = _db()
        shoes = db.get("shoes", {}) or {}
        if shoe_id not in shoes:
            return await interaction.response.send_message("❌ Shoes not found.", ephemeral=True)
        shoes.pop(shoe_id, None)
        db["shoes"] = shoes
        _save_db(db)
        await interaction.response.send_message(f"🗑️ Deleted shoes `{shoe_id}`.", ephemeral=False)

    # ---------- Player: shops ----------
    @app_commands.command(name="racket-shop", description="View all rackets available.")
    @app_commands.guild_only()
    async def racket_shop(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        db = _db()
        rackets = list((db.get("rackets", {}) or {}).values())
        rackets = [r for r in rackets if isinstance(r, dict)]
        if not rackets:
            return await interaction.response.send_message("ℹ️ No rackets in the shop yet.", ephemeral=True)

        rackets.sort(key=lambda r: str(r.get("name", "")).lower())
        pages: List[discord.Embed] = []
        chunk = 6
        for i in range(0, len(rackets), chunk):
            sub = rackets[i:i+chunk]
            e = discord.Embed(title="🎾 Racket Shop", color=discord.Color.gold())
            for r in sub:
                e.add_field(
                    name=f"{r.get('brand_emoji','')} {r.get('name')} — `{r.get('id')}`",
                    value=(
                        f"Power/Spin/Control: **{r.get('power')} / {r.get('spin')} / {r.get('control')}**\n"
                        f"Weight: {r.get('weight','') or '—'}\n"
                        f"Access Price (one-time): **{r.get('access_price',0)}**\n"
                        f"Frame Price: **{r.get('frame_price',0)}**\n"
                        f"String Price: **{r.get('string_price',0)}**"
                    ),
                    inline=False,
                )
            e.set_footer(text=f"Page {len(pages)+1}/{(len(rackets)+chunk-1)//chunk}")
            pages.append(e)

        view = SimplePager(pages, author_id=interaction.user.id)
        await interaction.response.send_message(embed=pages[0], view=view, ephemeral=False)

    @app_commands.command(name="shoe-shop", description="View all shoes available.")
    @app_commands.guild_only()
    async def shoe_shop(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        db = _db()
        shoes = list((db.get("shoes", {}) or {}).values())
        shoes = [s for s in shoes if isinstance(s, dict)]
        if not shoes:
            return await interaction.response.send_message("ℹ️ No shoes in the shop yet.", ephemeral=True)

        shoes.sort(key=lambda r: str(r.get("name", "")).lower())
        pages: List[discord.Embed] = []
        chunk = 8
        for i in range(0, len(shoes), chunk):
            sub = shoes[i:i+chunk]
            e = discord.Embed(title="👟 Shoe Shop", color=discord.Color.green())
            for s in sub:
                e.add_field(
                    name=f"{s.get('brand_emoji','')} {s.get('name')} — `{s.get('id')}`",
                    value=f"Footwork Impact: **{s.get('footwork_impact')}**\nPrice: **{s.get('price',0)}**",
                    inline=False,
                )
            e.set_footer(text=f"Page {len(pages)+1}/{(len(shoes)+chunk-1)//chunk}")
            pages.append(e)

        view = SimplePager(pages, author_id=interaction.user.id)
        await interaction.response.send_message(embed=pages[0], view=view, ephemeral=False)

    # ---------- Player: buy ----------
    @app_commands.command(name="racket-buy", description="Buy a racket by ID.")
    @app_commands.guild_only()
    async def racket_buy(
        self,
        interaction: discord.Interaction,
        racket_id: str,
        quantity: app_commands.Range[int, 1, 100] = 1,
    ):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        db = _db()
        row = (db.get("rackets", {}) or {}).get(str(racket_id))
        if not isinstance(row, dict):
            return await interaction.response.send_message("❌ Racket not found.", ephemeral=True)

        qty = int(quantity)

        inv = _inv_row(interaction.guild.id, interaction.user.id)
        inv.setdefault("rackets", [])
        inv.setdefault("racket_access", {})
        inv.setdefault("strung_rackets", [])

        access_map = inv.get("racket_access", {})
        if not isinstance(access_map, dict):
            access_map = {}
            inv["racket_access"] = access_map

        has_access = bool(access_map.get(str(racket_id), False))

        access_price = int(row.get("access_price", 0))
        frame_price = int(row.get("frame_price", 0))

        total = (frame_price * qty) + (0 if has_access else access_price)

        if get_balance(interaction.user.id) < total or not remove_balance(interaction.user.id, total):
            return await interaction.response.send_message("❌ You don't have enough currency.", ephemeral=True)

        if not has_access:
            access_map[str(racket_id)] = True

        inv["rackets"].extend([str(racket_id)] * qty)

        db.setdefault("inv", {}).setdefault(str(interaction.guild.id), {})[str(interaction.user.id)] = inv
        _save_db(db)

        name = str(row.get("name", "Racket"))
        if has_access:
            await interaction.response.send_message(
                f"✅ Bought **{qty}** × **{name}** frame(s) for **{(frame_price * qty):,}**.",
                ephemeral=False
            )
        else:
            await interaction.response.send_message(
                f"✅ Bought **{name}** access + **{qty}** frame(s) for **{total:,}** "
                f"(access **{access_price:,}** + frames **{(frame_price * qty):,}**).",
                ephemeral=False
            )

    @app_commands.command(name="shoe-buy", description="Buy shoes by ID.")
    @app_commands.guild_only()
    async def shoe_buy(self, interaction: discord.Interaction, shoe_id: str):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        db = _db()
        row = (db.get("shoes", {}) or {}).get(shoe_id)
        if not isinstance(row, dict):
            return await interaction.response.send_message("❌ Shoes not found.", ephemeral=True)

        price = int(row.get("price", 0))
        if get_balance(interaction.user.id) < price or not remove_balance(interaction.user.id, price):
            return await interaction.response.send_message("❌ You don't have enough currency.", ephemeral=True)

        inv = _inv_row(interaction.guild.id, interaction.user.id)
        inv.setdefault("shoes", []).append(shoe_id)
        db = _db()
        db.setdefault("inv", {}).setdefault(str(interaction.guild.id), {})[str(interaction.user.id)] = inv
        _save_db(db)

        await interaction.response.send_message(f"✅ Bought **{row.get('name')}** for **{price}**.", ephemeral=False)

    # ---------- Player: string rackets ----------
    @app_commands.command(name="string-racket", description="String racket frames into strung rackets.")
    @app_commands.guild_only()
    async def string_racket(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        view = StringRacketView(guild_id=interaction.guild.id, user_id=interaction.user.id)
        await interaction.response.send_message(embed=view._embed(), view=view, ephemeral=True)

    # ---------- Player: inventories ----------
    @app_commands.command(name="my-rackets", description="View your owned rackets.")
    @app_commands.guild_only()
    async def my_rackets(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        db = _db()
        inv = _inv_row(interaction.guild.id, interaction.user.id)

        frame_ids = inv.get("rackets", []) or []
        strung_rows = inv.get("strung_rackets", []) or []

        frame_counts: Counter = Counter()
        for rid in frame_ids:
            frame_counts[(str(rid), "unstrung", "", "", "")] += 1

        strung_counts: Counter = Counter()
        for row in strung_rows:
            model_id = str(row.get("frame_id", ""))
            if model_id:
                key = (
                    model_id,
                    "strung",
                    str(row.get("pattern", "")),
                    str(row.get("tension", "")),
                    str(row.get("weight", "")),
                )
                strung_counts[key] += 1

        all_counts = dict(frame_counts)
        all_counts.update(strung_counts)

        if not all_counts:
            return await interaction.response.send_message(
                "ℹ️ You don't own any rackets.",
                ephemeral=True
            )

        eq_racket = inv.get("equipped_racket")
        eq_frame_id = str(eq_racket.get("frame_id", "")) if isinstance(eq_racket, dict) else ""
        eq_pattern = str(eq_racket.get("pattern", "")) if isinstance(eq_racket, dict) else ""
        eq_tension = str(eq_racket.get("tension", "")) if isinstance(eq_racket, dict) else ""

        pages = []
        chunk = 6
        rows = list(all_counts.items())

        for i in range(0, len(rows), chunk):
            sub = rows[i:i+chunk]
            embed = discord.Embed(
                title="🎾 Your Rackets",
                color=discord.Color.blurple()
            )

            for (model_id, status, pattern, tension, weight), qty in sub:
                model = db.get("rackets", {}).get(model_id)
                if not model:
                    continue

                if status == "strung":
                    status_label = "✅ Strung"
                    detail = f"Pattern: **{pattern}** | Tension: **{tension} lbs**"
                    if weight:
                        detail += f" | Weight: **{weight}**"
                    is_equipped = (
                        model_id == eq_frame_id and
                        pattern == eq_pattern and
                        tension == eq_tension
                    )
                    if is_equipped:
                        status_label = "🎯 Equipped"
                else:
                    status_label = "🧱 Unstrung"
                    detail = "Frame only — use `/string-racket` to string it"
                    is_equipped = False

                name_line = f"{model.get('brand_emoji','')} {model.get('name')} ({status_label}) `{model.get('id')}`"
                if qty > 1:
                    name_line += f" [x{qty}]"

                embed.add_field(
                    name=name_line,
                    value=(
                        f"Power/Spin/Control: **{model.get('power')} / {model.get('spin')} / {model.get('control')}**\n"
                        f"{detail}"
                    ),
                    inline=False
                )

            embed.set_footer(text=f"Page {len(pages)+1}/{(len(rows)+chunk-1)//chunk}")
            pages.append(embed)

        view = SimplePager(pages, author_id=interaction.user.id)
        await interaction.response.send_message(embed=pages[0], view=view, ephemeral=False)

    @app_commands.command(name="my-shoes", description="View your owned shoes.")
    @app_commands.guild_only()
    async def my_shoes(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        db = _db()
        inv = _inv_row(interaction.guild.id, interaction.user.id)
        ids = list(inv.get("shoes", []) or [])
        if not ids:
            return await interaction.response.send_message("ℹ️ You don't own any shoes.", ephemeral=True)

        eq_shoes = inv.get("equipped_shoes")

        shoes = []
        for sid in ids:
            s = (db.get("shoes", {}) or {}).get(str(sid))
            if isinstance(s, dict):
                shoes.append((sid, s))

        pages: List[discord.Embed] = []
        chunk = 8
        for i in range(0, len(shoes), chunk):
            sub = shoes[i:i+chunk]
            e = discord.Embed(title="👟 Your Shoes", color=discord.Color.blurple())
            for sid, s in sub:
                equipped_tag = " 🎯 **[Equipped]**" if str(sid) == str(eq_shoes) else ""
                e.add_field(
                    name=f"{s.get('brand_emoji','')} {s.get('name')} — `{s.get('id')}`{equipped_tag}",
                    value=f"Footwork Impact: **{s.get('footwork_impact')}**",
                    inline=False,
                )
            e.set_footer(text=f"Page {len(pages)+1}/{(len(shoes)+chunk-1)//chunk}")
            pages.append(e)

        view = SimplePager(pages, author_id=interaction.user.id)
        await interaction.response.send_message(embed=pages[0], view=view, ephemeral=False)

    # ---------- Equip ----------
    @app_commands.command(name="gear-equip", description="Equip a strung racket or shoes.")
    @app_commands.guild_only()
    async def gear_equip(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        view = _EquipChoiceView(guild_id=interaction.guild.id, user_id=interaction.user.id)
        await interaction.response.send_message(
            "🎽 **Equip Gear** — What would you like to equip?",
            view=view,
            ephemeral=True,
        )

    @app_commands.command(name="gear-status", description="Check your currently equipped gear.")
    @app_commands.guild_only()
    async def gear_status(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        db = _db()
        inv = _inv_row(interaction.guild.id, interaction.user.id)

        e = discord.Embed(title="🎯 Equipped Gear", color=discord.Color.blurple())

        # Racket
        eq_racket = inv.get("equipped_racket")
        if isinstance(eq_racket, dict):
            frame_id = str(eq_racket.get("frame_id", ""))
            model = (db.get("rackets", {}) or {}).get(frame_id)
            if isinstance(model, dict):
                strung = inv.get("strung_rackets", []) or []
                eq_pattern = str(eq_racket.get("pattern", ""))
                eq_tension  = str(eq_racket.get("tension", ""))
                eq_weight   = str(eq_racket.get("weight", ""))
                spec_count = sum(
                    1 for r in strung
                    if isinstance(r, dict)
                    and str(r.get("frame_id", "")) == frame_id
                    and str(r.get("pattern", "")) == eq_pattern
                    and str(r.get("tension", "")) == eq_tension
                    and str(r.get("weight", "")) == eq_weight
                )
                e.add_field(
                    name=f"🎾 {model.get('brand_emoji','')} {model.get('name')} [x{spec_count}]",
                    value=(
                        f"Frame ID: `{frame_id}`\n"
                        f"Pattern: **{eq_racket.get('pattern', '—')}** | "
                        f"Tension: **{eq_racket.get('tension', '—')} lbs** | "
                        f"Weight: **{eq_racket.get('weight', '—') or '—'}**\n"
                        f"Power/Spin/Control: **{model.get('power')} / {model.get('spin')} / {model.get('control')}**\n"
                    ),
                    inline=False
                )
            else:
                e.add_field(name="🎾 Racket", value=f"Frame `{frame_id}` (model data not found)", inline=False)
        else:
            e.add_field(name="🎾 Racket", value="None equipped", inline=False)

        # Shoes
        eq_shoes = inv.get("equipped_shoes")
        if eq_shoes:
            shoe_row = (db.get("shoes", {}) or {}).get(str(eq_shoes))
            if isinstance(shoe_row, dict):
                e.add_field(
                    name=f"👟 {shoe_row.get('brand_emoji','')} {shoe_row.get('name')}",
                    value=f"Footwork Impact: **{shoe_row.get('footwork_impact')}**",
                    inline=False
                )
            else:
                e.add_field(name="👟 Shoes", value=f"ID `{eq_shoes}` (not found)", inline=False)
        else:
            e.add_field(name="👟 Shoes", value="None equipped", inline=False)

        await interaction.response.send_message(embed=e, ephemeral=False)


async def setup(bot: commands.Bot):
    await bot.add_cog(GearCog(bot))