# modules/loadouts.py
from __future__ import annotations

import os
from typing import Optional, Dict, Any, List

import discord
from discord.ext import commands
from discord import app_commands

import config
from utils import ensure_dir, load_json, save_json

# =========================
# EASY EDIT CONSTANT
# =========================
CUSTOM_LOADOUT_PRICE = 10  # <-- change this anytime

# =========================
# SLIDERS (MATCH YOUR JSON KEYS)
# =========================
SLIDER_LABELS: dict[str, str] = {
    "fh_power":           "Forehand Power",
    "fh_spin":            "Forehand Spin",
    "bh_power":           "Backhand Power",
    "bh_spin":            "Backhand Spin",
    "serve_power":        "Serve Power",
    "serve_spin":         "Serve Spin",
    "shot_dir_risk":      "Shot Direction Risk",
    "serve_variety":      "Serve Variety",
    "drop_frequency":     "Drop Shot Frequency",
    "slice_usage":        "Slice Usage",
    "deuce_spin":         "Deuce Serve Spin",
    "deuce_place":        "Deuce Serve Placement",
    "ad_spin":            "Ad Serve Spin",
    "ad_place":           "Ad Serve Placement",
    "pressure_play_risk": "Pressure Play Risk",
    "return_position":    "Return Position",
    "movement_aggression":"Movement Aggression",
    "time_btwn_points":   "Time Between Points",
}

# Tooltip hints shown in the editor (single-line)
SLIDER_HINTS: dict[str, str] = {
    "fh_power":           "High = more pace, needs good accuracy/timing",
    "fh_spin":            "High = more topspin margin, needs good power",
    "bh_power":           "High = more pace, needs good accuracy/timing",
    "bh_spin":            "High = more topspin margin, needs good power",
    "serve_power":        "High = faster serve, needs good accuracy",
    "serve_spin":         "High = kick/topspin, fewer faults but less pace",
    "shot_dir_risk":      "High = more risky angles (DTL), needs accuracy",
    "serve_variety":      "High = unpredictable placement, minor accuracy cost",
    "drop_frequency":     "High = more drop shots when at the net/short ball",
    "slice_usage":        "High = slice groundstrokes instead of topspin",
    "deuce_spin":         "High = kick/side-spin on deuce side serves",
    "deuce_place":        "0=Wide  50=Center  100=T",
    "ad_spin":            "High = kick/side-spin on ad side serves",
    "ad_place":           "0=Wide  50=Center  100=T",
    "pressure_play_risk": "High = more aggressive under pressure (more winners AND errors)",
    "return_position":    "High = stand inside (less time, cramps center); Low = deep (more time, spin angles harder)",
    "movement_aggression":"High = closer to baseline, net rushing; Low = farther back, defensive",
    "time_btwn_points":   "High = more rest between YOUR service points (both players recover)",
}

def serve_place_bucket(v: int) -> str:
    if v <= 33:
        return "Wide"
    if v <= 66:
        return "Center"
    return "T"


def return_pos_bucket(v: int) -> str:
    if v <= 33:
        return "Deep"
    if v <= 66:
        return "Balanced"
    return "Inside"


def slider_line(k: str, v: int) -> str:
    name = SLIDER_LABELS.get(k, k)
    hint = SLIDER_HINTS.get(k, "")
    if k in ("deuce_place", "ad_place"):
        return f"• **{name}**: `{v}` → **{serve_place_bucket(v)}**"
    if k == "return_position":
        return f"• **{name}**: `{v}` → **{return_pos_bucket(v)}**"
    return f"• **{name}**: `{v}`"


SLIDER_KEYS: list[str] = [
    "fh_power",
    "fh_spin",
    "bh_power",
    "bh_spin",
    "serve_power",
    "serve_spin",
    "shot_dir_risk",
    "serve_variety",
    "drop_frequency",
    "deuce_spin",
    "deuce_place",
    "ad_spin",
    "ad_place",
    "pressure_play_risk",
    "return_position",
    "movement_aggression",
    "time_btwn_points",
    "slice_usage",
]

PRESETS_PATH = os.path.join(config.DATA_DIR, "loadout_presets.json")
INV_PATH     = os.path.join(config.DATA_DIR, "loadout_inventory.json")


# =========================
# STORAGE HELPERS
# =========================
def _clamp_0_100(v: Any) -> int:
    try:
        x = int(v)
    except Exception:
        x = 50
    return max(0, min(100, x))


def normalize_sliders(sliders: Optional[Dict[str, Any]]) -> Dict[str, int]:
    sliders = sliders or {}
    return {k: _clamp_0_100(sliders.get(k, 50)) for k in SLIDER_KEYS}


def _load_presets_raw() -> Dict[str, Any]:
    ensure_dir(config.DATA_DIR)
    return load_json(PRESETS_PATH, {}) or {}


def load_presets_map() -> Dict[str, Any]:
    raw = _load_presets_raw()
    if isinstance(raw, dict) and isinstance(raw.get("presets"), dict):
        return raw["presets"]
    if isinstance(raw, dict):
        return raw
    return {}


def save_presets_map(presets: Dict[str, Any]) -> None:
    ensure_dir(config.DATA_DIR)
    save_json(PRESETS_PATH, presets)


def generate_preset_id(presets: Dict[str, Any]) -> str:
    best = 0
    for pid in (presets or {}).keys():
        if isinstance(pid, str) and pid.startswith("preset-"):
            suf = pid.replace("preset-", "").strip()
            if suf.isdigit():
                best = max(best, int(suf))
    return f"preset-{best + 1:03d}"


def default_preset_ids(presets: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for pid, row in (presets or {}).items():
        if isinstance(row, dict) and row.get("is_default") is True:
            out.append(pid)
    return out


def _load_inv_db() -> Dict[str, Any]:
    ensure_dir(config.DATA_DIR)
    return load_json(INV_PATH, {"inv": {}}) or {"inv": {}}


def _save_inv_db(db: Dict[str, Any]) -> None:
    save_json(INV_PATH, db)


def inv_row(guild_id: int, user_id: int) -> Dict[str, Any]:
    db = _load_inv_db()
    g  = db.setdefault("inv", {}).setdefault(str(guild_id), {})
    return g.setdefault(
        str(user_id),
        {
            "has_custom":     False,
            "custom_name":    "Custom Loadout",
            "custom_sliders": {k: 50 for k in SLIDER_KEYS},
        },
    )


def set_inv_row(guild_id: int, user_id: int, row: Dict[str, Any]) -> None:
    db = _load_inv_db()
    db.setdefault("inv", {}).setdefault(str(guild_id), {})[str(user_id)] = row
    _save_inv_db(db)


async def preset_id_autocomplete(
    interaction: discord.Interaction, current: str
) -> List[app_commands.Choice[str]]:
    cur = (current or "").lower()
    presets = load_presets_map()
    hits: List[tuple[str, str]] = []
    for pid, row in presets.items():
        if not isinstance(row, dict):
            continue
        title = str(row.get("title") or row.get("name") or pid)
        if cur in pid.lower() or cur in title.lower():
            hits.append((pid, title))
    hits = hits[:25]
    return [app_commands.Choice(name=f"{t} — {pid}", value=pid) for pid, t in hits]


# =========================
# MODAL (CUSTOM LOADOUT EDIT — legacy text input)
# =========================
class CustomLoadoutEditModal(discord.ui.Modal, title="Edit Custom Loadout"):
    sliders_lines = discord.ui.TextInput(
        label="Sliders (one per line: key=value)",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=1500,
        placeholder=(
            "fh_power=55\n"
            "serve_power=60\n"
            "time_btwn_points=40\n"
            "\n"
            "Keys: " + ", ".join(SLIDER_KEYS)
        ),
    )

    def __init__(self, guild_id: int, user_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.user_id  = user_id

    async def on_submit(self, interaction: discord.Interaction):
        row = inv_row(self.guild_id, self.user_id)
        if not row.get("has_custom", False):
            return await interaction.response.send_message(
                "❌ You don't own a custom loadout slot yet. Use `/loadout custom-buy`.",
                ephemeral=True,
            )

        current = normalize_sliders(row.get("custom_sliders", {}))
        text    = str(self.sliders_lines.value or "")

        changed = 0
        ignored = 0

        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                ignored += 1
                continue
            k, v = line.split("=", 1)
            k = k.strip()
            v = v.strip()
            if k not in SLIDER_KEYS:
                ignored += 1
                continue
            current[k] = _clamp_0_100(v)
            changed += 1

        row["custom_sliders"] = current
        set_inv_row(self.guild_id, self.user_id, row)

        await interaction.response.send_message(
            f"✅ Custom loadout updated (**{changed}** changed, **{ignored}** ignored).",
            ephemeral=True,
        )


# =========================
# RENAME MODAL
# =========================
class RenameLoadoutModal(discord.ui.Modal, title="Rename Custom Loadout"):
    new_name = discord.ui.TextInput(
        label="New Name",
        style=discord.TextStyle.short,
        required=True,
        max_length=50,
        placeholder="e.g. Aggressor, Baseline Grinder, Net Rusher…",
    )

    def __init__(self, guild_id: int, user_id: int):
        super().__init__()
        self.guild_id = guild_id
        self.user_id  = user_id

    async def on_submit(self, interaction: discord.Interaction):
        row = inv_row(self.guild_id, self.user_id)
        if not row.get("has_custom", False):
            return await interaction.response.send_message(
                "❌ You don't own a custom loadout slot.", ephemeral=True
            )
        name = str(self.new_name.value or "").strip() or "Custom Loadout"
        row["custom_name"] = name
        set_inv_row(self.guild_id, self.user_id, row)
        await interaction.response.send_message(
            f"✅ Custom loadout renamed to **{name}**.", ephemeral=True
        )


# =========================
class _SetSliderValueModal(discord.ui.Modal, title="Set Slider Value"):
    value = discord.ui.TextInput(label="Value (0–100)", required=True, max_length=3)

    def __init__(self, on_set):
        super().__init__()
        self._on_set = on_set

    async def on_submit(self, interaction: discord.Interaction):
        try:
            v = int(str(self.value.value).strip())
        except Exception:
            v = 50
        v = max(0, min(100, v))
        await self._on_set(interaction, v)


class LoadoutSliderEditorView(discord.ui.View):
    def __init__(self, *, title: str, sliders: dict, on_save, is_admin: bool, show_rename: bool = False, guild_id: int = 0, user_id: int = 0):
        super().__init__(timeout=300)
        self.title        = title
        self.sliders      = sliders
        self.on_save      = on_save
        self.is_admin     = is_admin
        self.show_rename  = show_rename
        self.guild_id     = guild_id
        self.user_id      = user_id
        self.selected_key = SLIDER_KEYS[0]

        self.select = discord.ui.Select(
            placeholder="Pick a slider to edit…",
            min_values=1,
            max_values=1,
            options=[
                discord.SelectOption(
                    label=SLIDER_LABELS.get(k, k)[:100],
                    value=k,
                    description=SLIDER_HINTS.get(k, "")[:100] or None,
                )
                for k in SLIDER_KEYS
            ],
        )
        self.select.callback = self._on_select
        self.add_item(self.select)

        if show_rename:
            rename_btn = discord.ui.Button(
                label="Rename Loadout",
                style=discord.ButtonStyle.secondary,
                row=2,
            )
            rename_btn.callback = self._on_rename
            self.add_item(rename_btn)

    def _embed(self) -> discord.Embed:
        e = discord.Embed(title=self.title, color=discord.Color.blurple())

        lines = [slider_line(x, int(self.sliders.get(x, 50))) for x in SLIDER_KEYS]
        e.add_field(name="All Sliders", value="\n".join(lines)[:1024], inline=False)

        k    = self.selected_key
        v    = int(self.sliders.get(k, 50))
        name = SLIDER_LABELS.get(k, k)
        hint = SLIDER_HINTS.get(k, "")

        if k in ("deuce_place", "ad_place"):
            txt = f"**{name}**\nValue: `{v}` → **{serve_place_bucket(v)}**"
        elif k == "return_position":
            txt = f"**{name}**\nValue: `{v}` → **{return_pos_bucket(v)}**"
        else:
            txt = f"**{name}**\nValue: `{v}`"

        if hint:
            txt += f"\n> *{hint}*"

        e.add_field(name="Now Editing", value=txt[:1024], inline=False)
        return e

    async def _on_select(self, interaction: discord.Interaction):
        self.selected_key = self.select.values[0]
        await interaction.response.edit_message(embed=self._embed(), view=self)

    async def _bump(self, interaction: discord.Interaction, delta: int):
        k = self.selected_key
        v = max(0, min(100, int(self.sliders.get(k, 50)) + delta))
        self.sliders[k] = v
        await interaction.response.edit_message(embed=self._embed(), view=self)

    async def _on_rename(self, interaction: discord.Interaction):
        await interaction.response.send_modal(
            RenameLoadoutModal(self.guild_id, self.user_id)
        )

    @discord.ui.button(label="-5", style=discord.ButtonStyle.secondary, row=1)
    async def minus5(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._bump(interaction, -5)

    @discord.ui.button(label="-1", style=discord.ButtonStyle.secondary, row=1)
    async def minus1(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._bump(interaction, -1)

    @discord.ui.button(label="+1", style=discord.ButtonStyle.secondary, row=1)
    async def plus1(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._bump(interaction, +1)

    @discord.ui.button(label="+5", style=discord.ButtonStyle.secondary, row=1)
    async def plus5(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self._bump(interaction, +5)

    @discord.ui.button(label="Set…", style=discord.ButtonStyle.primary, row=2)
    async def set_exact(self, interaction: discord.Interaction, button: discord.ui.Button):
        async def _on_set(ix: discord.Interaction, v: int):
            self.sliders[self.selected_key] = v
            await ix.response.edit_message(embed=self._embed(), view=self)
        await interaction.response.send_modal(_SetSliderValueModal(_on_set))

    @discord.ui.button(label="Save", style=discord.ButtonStyle.success, row=2)
    async def save(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.on_save(interaction, self.sliders)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.danger, row=2)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.stop()
        await interaction.response.edit_message(content="❌ Cancelled.", embed=None, view=None)


# =========================
# GROUP COG
# =========================
class LoadoutsCog(commands.GroupCog, group_name="loadout", group_description="Loadouts"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    def _is_admin(self, m: discord.Member) -> bool:
        return bool(m.guild_permissions.administrator)

    # -------------------------
    # /loadout my
    # -------------------------
    @app_commands.command(name="my", description="View base presets + custom slot status.")
    @app_commands.guild_only()
    async def my(self, interaction: discord.Interaction):
        presets  = load_presets_map()
        defaults = set(default_preset_ids(presets))

        inv       = inv_row(interaction.guild.id, interaction.user.id)
        has_custom = bool(inv.get("has_custom", False))

        e = discord.Embed(title="My Loadouts", color=discord.Color.blurple())

        if presets:
            lines: List[str] = []
            for pid, row in presets.items():
                if not isinstance(row, dict):
                    continue
                title = str(row.get("title") or row.get("name") or pid)
                tag   = " *(Default)*" if pid in defaults else ""
                desc  = str(row.get("description") or "")
                desc_str = f" — {desc[:60]}" if desc else ""
                lines.append(f"• **{title}**`{tag}`{desc_str} — `{pid}`")
            e.add_field(name="Base Presets", value="\n".join(lines)[:1024], inline=False)
        else:
            e.add_field(name="Base Presets", value="No presets exist yet.", inline=False)

        if has_custom:
            cname  = str(inv.get("custom_name") or "Custom Loadout")
            sliders = normalize_sliders(inv.get("custom_sliders", {}))
            sl_preview = ", ".join(
                f"{SLIDER_LABELS.get(k,k)[:12]}={v}"
                for k, v in sliders.items() if v != 50
            )
            custom_text = f"✅ **{cname}** — *use `/loadout custom-edit` to adjust*"
            if sl_preview:
                custom_text += f"\nModified: {sl_preview[:200]}"
        else:
            custom_text = (
                f"❌ Not owned — buy for **{CUSTOM_LOADOUT_PRICE:,}** coins with `/loadout custom-buy`"
            )

        e.add_field(name="Custom Slot", value=custom_text, inline=False)

        e.set_footer(text="Slider range: 0–100 (50 = neutral). Effects scale ±20 stat points max.")
        await interaction.response.send_message(embed=e, ephemeral=False)

    # -------------------------
    # /loadout custom-buy
    # -------------------------
    @app_commands.command(name="custom-buy", description="Buy 1 custom loadout slot (one-time).")
    @app_commands.guild_only()
    async def custom_buy(self, interaction: discord.Interaction):
        from modules.economy import get_balance, remove_balance

        row = inv_row(interaction.guild.id, interaction.user.id)
        if row.get("has_custom", False):
            return await interaction.response.send_message(
                "✅ You already own a custom loadout slot.", ephemeral=True
            )

        bal = int(get_balance(interaction.user.id))
        if bal < CUSTOM_LOADOUT_PRICE:
            return await interaction.response.send_message(
                f"❌ You need **{CUSTOM_LOADOUT_PRICE:,}** coins. You have **{bal:,}**.",
                ephemeral=True,
            )

        ok = remove_balance(interaction.user.id, CUSTOM_LOADOUT_PRICE)
        if not ok:
            return await interaction.response.send_message(
                "❌ Purchase failed (insufficient funds).", ephemeral=True
            )

        row["has_custom"]     = True
        row["custom_name"]    = "Custom Loadout"
        row["custom_sliders"] = {k: 50 for k in SLIDER_KEYS}
        set_inv_row(interaction.guild.id, interaction.user.id, row)

        await interaction.response.send_message(
            "✅ Purchased **Custom Loadout Slot**.\n"
            "Use `/loadout custom-edit` to tune your sliders, "
            "or `/loadout custom-name` to give it a name.",
            ephemeral=False,
        )

    # -------------------------
    # /loadout custom-edit
    # -------------------------
    @app_commands.command(name="custom-edit", description="Edit your custom loadout sliders with the interactive editor.")
    @app_commands.guild_only()
    async def custom_edit(self, interaction: discord.Interaction):
        row = inv_row(interaction.guild.id, interaction.user.id)
        if not row.get("has_custom", False):
            return await interaction.response.send_message(
                f"❌ You don't own a custom slot. Buy it for **{CUSTOM_LOADOUT_PRICE:,}** with `/loadout custom-buy`.",
                ephemeral=True,
            )

        cname   = str(row.get("custom_name") or "Custom Loadout")
        sliders = normalize_sliders(row.get("custom_sliders", {}))

        async def _save(ix: discord.Interaction, new_sliders: dict):
            row2 = inv_row(ix.guild.id, ix.user.id)
            row2["has_custom"]     = True
            row2["custom_sliders"] = normalize_sliders(new_sliders)
            set_inv_row(ix.guild.id, ix.user.id, row2)
            new_name = str(row2.get("custom_name") or "Custom Loadout")
            await ix.response.edit_message(
                content=f"✅ Saved **{new_name}**.", embed=None, view=None
            )

        view = LoadoutSliderEditorView(
            title=f"Custom Loadout: {cname}",
            sliders=sliders,
            on_save=_save,
            is_admin=False,
            show_rename=True,
            guild_id=interaction.guild.id,
            user_id=interaction.user.id,
        )
        await interaction.response.send_message(embed=view._embed(), view=view, ephemeral=True)

    # -------------------------
    # /loadout custom-name
    # -------------------------
    @app_commands.command(name="custom-name", description="Rename your custom loadout slot.")
    @app_commands.guild_only()
    async def custom_name(self, interaction: discord.Interaction):
        row = inv_row(interaction.guild.id, interaction.user.id)
        if not row.get("has_custom", False):
            return await interaction.response.send_message(
                f"❌ You don't own a custom loadout slot yet.", ephemeral=True
            )
        await interaction.response.send_modal(
            RenameLoadoutModal(interaction.guild.id, interaction.user.id)
        )

    # -------------------------
    # /loadout preset-add (Admin)
    # -------------------------
    @app_commands.command(name="preset-add", description="(Admin) Create a new base loadout preset.")
    @app_commands.guild_only()
    async def preset_add(
        self,
        interaction: discord.Interaction,
        title: str,
        description: str = "",
        is_default: bool = False,
    ):
        if not isinstance(interaction.user, discord.Member) or not self._is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        presets = load_presets_map()
        new_id  = generate_preset_id(presets)

        presets[new_id] = {
            "id":          new_id,
            "title":       title,
            "description": description,
            "is_default":  bool(is_default),
            "sliders":     {k: 50 for k in SLIDER_KEYS},
            "created_at":  discord.utils.utcnow().isoformat(),
        }
        save_presets_map(presets)

        await interaction.response.send_message(
            f"✅ Created preset **{title}** — `{new_id}`",
            ephemeral=False,
        )

    # -------------------------
    # /loadout preset-view (Admin)
    # -------------------------
    @app_commands.command(name="preset-view", description="(Admin) View a base preset slider settings.")
    @app_commands.guild_only()
    @app_commands.autocomplete(preset_id=preset_id_autocomplete)
    async def preset_view(self, interaction: discord.Interaction, preset_id: str):
        if not isinstance(interaction.user, discord.Member) or not self._is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        presets = load_presets_map()
        p = presets.get(preset_id)
        if not isinstance(p, dict):
            return await interaction.response.send_message("❌ Preset not found.", ephemeral=True)

        title   = str(p.get("title") or p.get("name") or preset_id)
        sliders = normalize_sliders(p.get("sliders", {}))

        e = discord.Embed(title=f"Preset: {title}", color=discord.Color.blurple())
        e.add_field(name="Preset ID", value=f"`{preset_id}`", inline=False)
        e.add_field(name="Default",   value="✅ Yes" if p.get("is_default") else "❌ No", inline=True)
        desc = str(p.get("description") or "")
        if desc:
            e.add_field(name="Description", value=desc[:512], inline=False)

        lines = [slider_line(k, int(sliders.get(k, 50))) for k in SLIDER_KEYS]
        e.add_field(name="Sliders", value="\n".join(lines)[:1024], inline=False)

        await interaction.response.send_message(embed=e, ephemeral=True)

    # -------------------------
    # /loadout preset-edit (Admin)
    # -------------------------
    @app_commands.command(name="preset-edit", description="(Admin) Edit a base loadout preset (interactive editor).")
    @app_commands.guild_only()
    @app_commands.autocomplete(preset_id=preset_id_autocomplete)
    async def preset_edit(self, interaction: discord.Interaction, preset_id: str):
        if not isinstance(interaction.user, discord.Member) or not self._is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        presets = load_presets_map()
        p = presets.get(preset_id)
        if not isinstance(p, dict):
            return await interaction.response.send_message("❌ Preset not found.", ephemeral=True)

        title   = str(p.get("title") or p.get("name") or preset_id)
        sliders = normalize_sliders(p.get("sliders", {}))

        async def _save(ix: discord.Interaction, new_sliders: dict):
            presets2 = load_presets_map()
            p2 = presets2.get(preset_id)
            if not isinstance(p2, dict):
                return await ix.response.edit_message(content="❌ Preset missing.", embed=None, view=None)
            p2["sliders"] = normalize_sliders(new_sliders)
            presets2[preset_id] = p2
            save_presets_map(presets2)
            await ix.response.edit_message(
                content=f"✅ Saved preset **{title}** (`{preset_id}`)", embed=None, view=None
            )

        view = LoadoutSliderEditorView(
            title=f"Preset: {title}",
            sliders=sliders,
            on_save=_save,
            is_admin=True,
        )
        await interaction.response.send_message(embed=view._embed(), view=view, ephemeral=True)

    # -------------------------
    # /loadout preset-delete (Admin)
    # -------------------------
    @app_commands.command(name="preset-delete", description="(Admin) Delete a base loadout preset.")
    @app_commands.guild_only()
    @app_commands.autocomplete(preset_id=preset_id_autocomplete)
    async def preset_delete(self, interaction: discord.Interaction, preset_id: str):
        if not isinstance(interaction.user, discord.Member) or not self._is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        presets = load_presets_map()
        if preset_id not in presets:
            return await interaction.response.send_message("❌ Preset not found.", ephemeral=True)

        title = str(presets[preset_id].get("title") or preset_id)
        del presets[preset_id]
        save_presets_map(presets)

        await interaction.response.send_message(f"🗑️ Deleted preset **{title}** (`{preset_id}`).", ephemeral=False)


async def setup(bot: commands.Bot):
    await bot.add_cog(LoadoutsCog(bot))