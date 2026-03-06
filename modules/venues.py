# modules/venues.py
from __future__ import annotations

import os
from typing import Dict, Any, Optional, List, Tuple

import discord
from discord import app_commands
from discord.ext import commands

import config
from utils import ensure_dir, load_json, save_json

from modules.economy import get_balance, remove_balance


# -------------------------
# Storage
# -------------------------
WEATHER_UNITS  = ("C", "F")
SPEED_UNITS    = ("KMH", "MPH")
ALTITUDE_UNITS = ("M", "FT")


def _norm_unit(v: str, allowed: Tuple[str, ...], default: str) -> str:
    s = (v or "").strip().upper()
    return s if s in allowed else default


def _data_dir() -> str:
    return str(getattr(config, "DATA_DIR", "data"))


VENUES_PATH    = os.path.join(_data_dir(), "venues.json")
VENUE_INV_PATH = os.path.join(_data_dir(), "venue_inventory.json")
TOURN_PATH     = os.path.join(_data_dir(), "tournaments.json")


def _venues_db() -> Dict[str, Any]:
    ensure_dir(_data_dir())
    return load_json(VENUES_PATH, {"venues": {}})


def _venues_save(db: Dict[str, Any]) -> None:
    ensure_dir(_data_dir())
    save_json(VENUES_PATH, db)


def _inv_db() -> Dict[str, Any]:
    ensure_dir(_data_dir())
    return load_json(VENUE_INV_PATH, {"inv": {}})


def _inv_save(db: Dict[str, Any]) -> None:
    ensure_dir(_data_dir())
    save_json(VENUE_INV_PATH, db)


def _tourn_db() -> Dict[str, Any]:
    ensure_dir(_data_dir())
    return load_json(TOURN_PATH, {"tournaments": {}})


def _tourn_save(db: Dict[str, Any]) -> None:
    ensure_dir(_data_dir())
    save_json(TOURN_PATH, db)


# -------------------------
# Surface system
# -------------------------
SURFACES = ("hard", "clay", "grass")

_SURFACE_DEFAULTS: Dict[str, Tuple[int, int]] = {
    "hard":  (38, 50),
    "clay":  (65, 78),
    "grass": (20, 22),
}

_SURFACE_CPI_RANGE: Dict[str, str] = {
    "hard":  "28–52  (indoor: 26–44)",
    "clay":  "52–82  (indoor: 50–68, rare)",
    "grass": "14–30  (indoor: 12–24)",
}

_SURFACE_BOUNCE_NOTE: Dict[str, str] = {
    "hard":  "Medium — true, consistent hop",
    "clay":  "High — heavy topspin kick, slows pace after bounce",
    "grass": "Low — skids through, stays low, punishes late swings",
}

_CONDITIONS_EXPLAINED = (
    "**How conditions shift effective CPI in-match:**\n"
    "• 🌡️ +1°C above 20°C → +0.25 CPI  (heat slows the ball after bounce)\n"
    "• 🏔️ Altitude 1000m → +2 CPI  (thinner air = faster flight, but heavier bounce on clay)\n"
    "• 💧 +10% humidity above 50% → −0.6 CPI  (damp felt grips more)\n"
    "• 💨 Wind >10 km/h → up to +8 CPI  (wind-assisted flat hitting pace)\n"
    "• 🌧️ Rain → −5 CPI  (wet ball, wet felt, slower)\n"
    "• 🏠 Indoor/roof closed → weather sealed, CPI stays very close to base"
)


def _is_admin(member: discord.Member) -> bool:
    return bool(getattr(member.guild_permissions, "administrator", False))


def _clamp_int(v: int, lo: int, hi: int) -> int:
    return lo if v < lo else hi if v > hi else v


def _venue_id_from_name(tournament_id: str, venue_name: str) -> str:
    base = f"{tournament_id}:{venue_name}".lower().strip()
    base = "".join(ch if ch.isalnum() else "-" for ch in base)
    while "--" in base:
        base = base.replace("--", "-")
    return f"venue-{base[:80]}"


def _get_venues() -> Dict[str, Dict[str, Any]]:
    return _venues_db().get("venues", {})


def _get_venue(venue_id: str) -> Optional[Dict[str, Any]]:
    return _get_venues().get(venue_id)


def _set_venue(venue_id: str, row: Dict[str, Any]) -> None:
    db = _venues_db()
    db.setdefault("venues", {})[venue_id] = row
    _venues_save(db)


def _del_venue(venue_id: str) -> bool:
    db = _venues_db()
    venues = db.get("venues", {})
    if venue_id in venues:
        del venues[venue_id]
        _venues_save(db)
        return True
    return False


def _get_user_inv(guild_id: int, user_id: int) -> List[str]:
    db = _inv_db()
    g  = db.setdefault("inv", {}).setdefault(str(guild_id), {})
    return list(g.get(str(user_id), []))


def _set_user_inv(guild_id: int, user_id: int, items: List[str]) -> None:
    db = _inv_db()
    g  = db.setdefault("inv", {}).setdefault(str(guild_id), {})
    g[str(user_id)] = list(dict.fromkeys(items))
    _inv_save(db)


def _owned(guild_id: int, user_id: int, venue_id: str) -> bool:
    return venue_id in _get_user_inv(guild_id, user_id)


def _format_money(n: int) -> str:
    if n >= 1_000_000:
        s = f"{n/1_000_000:.1f}".rstrip("0").rstrip(".")
        return f"{s}M"
    if n >= 1_000:
        s = f"{n/1_000:.1f}".rstrip("0").rstrip(".")
        return f"{s}K"
    return str(n)


# -------------------------
# Autocomplete helpers
# -------------------------
async def venue_autocomplete(
    interaction: discord.Interaction, current: str
) -> List[app_commands.Choice[str]]:
    cur    = (current or "").lower().strip()
    venues = _get_venues()
    items: List[Tuple[str, Dict[str, Any]]] = list(venues.items())

    def hit(v_id: str, row: Dict[str, Any]) -> bool:
        if not cur:
            return True
        name = str(row.get("name", "")).lower()
        tid  = str(row.get("tournament_id", "")).lower()
        return (cur in v_id.lower()) or (cur in name) or (cur in tid)

    hits = [(vid, row) for (vid, row) in items if hit(vid, row)][:25]
    out: List[app_commands.Choice[str]] = []
    for vid, row in hits:
        label = f"{row.get('name','(unnamed)')} [{row.get('tournament_id','?')}]"
        out.append(app_commands.Choice(name=label[:100], value=vid))
    return out


async def tourn_autocomplete(
    interaction: discord.Interaction, current: str
) -> List[app_commands.Choice[str]]:
    cur  = (current or "").lower().strip()
    db   = _tourn_db().get("tournaments", {})
    hits = []
    for tid, row in db.items():
        if (not cur
                or cur in tid.lower()
                or cur in str(row.get("title",    "")).lower()
                or cur in str(row.get("location", "")).lower()):
            hits.append((tid, row))
    hits = hits[:25]
    return [
        app_commands.Choice(name=f"{row.get('title', tid)} ({tid})"[:100], value=tid)
        for tid, row in hits
    ]


# -------------------------
# Cog
# -------------------------
class VenuesCog(commands.Cog):

    # ── Tournament sub-group (nested under /venue tournament) ─────────────
    class _TournamentSubGroup(app_commands.Group):
        def __init__(self):
            super().__init__(
                name="tournament",
                description="Manage venue tournaments (location/event metadata)"
            )

        # ── /venue tournament create ──────────────────────────────────────
        @app_commands.command(name="create", description="(Admin) Create a venue tournament/event.")
        @app_commands.guild_only()
        async def tournament_create(
            self,
            interaction: discord.Interaction,
            tournament_id: str,
            title: str,
            location: str,
            altitude_m: int = 0,
            weather_unit: str = "C",
            speed_unit: str = "KMH",
            altitude_unit: str = "M",
            humidity_pct_min: int = 30,
            humidity_pct_max: int = 70,
        ):
            if not isinstance(interaction.user, discord.Member) or not _is_admin(interaction.user):
                return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

            tid = tournament_id.strip()
            if not tid:
                return await interaction.response.send_message("❌ tournament_id required.", ephemeral=True)

            db   = _tourn_db()
            tmap = db.setdefault("tournaments", {})
            if tid in tmap:
                return await interaction.response.send_message("❌ tournament_id already exists.", ephemeral=True)

            hmin = _clamp_int(int(humidity_pct_min), 0, 100)
            hmax = _clamp_int(int(humidity_pct_max), 0, 100)
            if hmax < hmin:
                hmin, hmax = hmax, hmin

            tmap[tid] = {
                "tournament_id":    tid,
                "title":            title.strip(),
                "location":         location.strip(),
                "altitude_m":       int(altitude_m),
                "weather_unit":     _norm_unit(weather_unit,  WEATHER_UNITS,  "C"),
                "speed_unit":       _norm_unit(speed_unit,    SPEED_UNITS,    "KMH"),
                "altitude_unit":    _norm_unit(altitude_unit, ALTITUDE_UNITS, "M"),
                "humidity_pct_min": int(hmin),
                "humidity_pct_max": int(hmax),
            }
            _tourn_save(db)
            await interaction.response.send_message(
                f"✅ Venue tournament **{title}** created as `{tid}`.", ephemeral=False
            )

        # ── /venue tournament view ────────────────────────────────────────
        @app_commands.command(name="view", description="View a venue tournament.")
        @app_commands.guild_only()
        @app_commands.autocomplete(tournament_id=tourn_autocomplete)
        async def tournament_view(self, interaction: discord.Interaction, tournament_id: str):
            if not isinstance(interaction.user, discord.Member) or not _is_admin(interaction.user):
                return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

            row = _tourn_db().get("tournaments", {}).get(tournament_id)
            if not row:
                return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)

            txt = (
                f"🏟️ **{row.get('title', tournament_id)}**\n"
                f"ID: `{tournament_id}`\n"
                f"Location: **{row.get('location','—')}**\n"
                f"Altitude: **{row.get('altitude_m', 0)} m**\n"
                f"Humidity: **{row.get('humidity_pct_min',30)}–{row.get('humidity_pct_max',70)}%**\n"
                f"Units: Weather **{row.get('weather_unit','C')}**, "
                f"Speed **{row.get('speed_unit','KMH')}**, "
                f"Altitude **{row.get('altitude_unit','M')}**"
            )
            await interaction.response.send_message(txt, ephemeral=False)

        # ── /venue tournament edit ────────────────────────────────────────
        @app_commands.command(name="edit", description="(Admin) Edit a venue tournament.")
        @app_commands.guild_only()
        @app_commands.autocomplete(tournament_id=tourn_autocomplete)
        @app_commands.choices(
            weather_unit  =[app_commands.Choice(name="C",   value="C"),   app_commands.Choice(name="F",   value="F")],
            speed_unit    =[app_commands.Choice(name="KMH", value="KMH"), app_commands.Choice(name="MPH", value="MPH")],
            altitude_unit =[app_commands.Choice(name="M",   value="M"),   app_commands.Choice(name="FT",  value="FT")],
        )
        async def tournament_edit(
            self,
            interaction: discord.Interaction,
            tournament_id: str,
            title: Optional[str] = None,
            location: Optional[str] = None,
            altitude_m: Optional[int] = None,
            weather_unit: Optional[str] = None,
            speed_unit: Optional[str] = None,
            altitude_unit: Optional[str] = None,
            humidity_pct_min: Optional[int] = None,
            humidity_pct_max: Optional[int] = None,
        ):
            if not isinstance(interaction.user, discord.Member) or not _is_admin(interaction.user):
                return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

            db   = _tourn_db()
            tmap = db.get("tournaments", {})
            row  = tmap.get(tournament_id)
            if not row:
                return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)

            if title           is not None: row["title"]            = title.strip()
            if location        is not None: row["location"]         = location.strip()
            if altitude_m      is not None: row["altitude_m"]       = int(altitude_m)
            if weather_unit    is not None: row["weather_unit"]     = _norm_unit(weather_unit,  WEATHER_UNITS,  row.get("weather_unit",  "C"))
            if speed_unit      is not None: row["speed_unit"]       = _norm_unit(speed_unit,    SPEED_UNITS,    row.get("speed_unit",    "KMH"))
            if altitude_unit   is not None: row["altitude_unit"]    = _norm_unit(altitude_unit, ALTITUDE_UNITS, row.get("altitude_unit", "M"))
            if humidity_pct_min is not None: row["humidity_pct_min"] = _clamp_int(int(humidity_pct_min), 0, 100)
            if humidity_pct_max is not None: row["humidity_pct_max"] = _clamp_int(int(humidity_pct_max), 0, 100)

            hmin = int(row.get("humidity_pct_min", 30))
            hmax = int(row.get("humidity_pct_max", 70))
            if hmax < hmin:
                row["humidity_pct_min"], row["humidity_pct_max"] = hmax, hmin

            tmap[tournament_id] = row
            _tourn_save(db)
            await interaction.response.send_message(f"✅ Venue tournament `{tournament_id}` updated.", ephemeral=False)

        # ── /venue tournament delete ──────────────────────────────────────
        @app_commands.command(name="delete", description="(Admin) Delete a venue tournament (only if no venues reference it).")
        @app_commands.guild_only()
        @app_commands.autocomplete(tournament_id=tourn_autocomplete)
        async def tournament_delete(self, interaction: discord.Interaction, tournament_id: str):
            if not isinstance(interaction.user, discord.Member) or not _is_admin(interaction.user):
                return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

            venues = _get_venues()
            used   = [v for v in venues.values() if v.get("tournament_id") == tournament_id]
            if used:
                return await interaction.response.send_message(
                    f"❌ Can't delete: {len(used)} venue(s) still reference this tournament.", ephemeral=True
                )

            db   = _tourn_db()
            tmap = db.get("tournaments", {})
            if tournament_id not in tmap:
                return await interaction.response.send_message("❌ Tournament not found.", ephemeral=True)

            del tmap[tournament_id]
            _tourn_save(db)
            await interaction.response.send_message(f"🗑️ Deleted venue tournament `{tournament_id}`.", ephemeral=False)

    # ── Set up the groups ─────────────────────────────────────────────────
    venue = app_commands.Group(name="venue", description="Venues: create/edit/shop/buy/inventory")
    venue.add_command(_TournamentSubGroup())

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # ─── venue commands ───────────────────────────────────────────────────

    @venue.command(name="create", description="(Admin) Create a venue. CPI and bounce auto-default by surface.")
    @app_commands.guild_only()
    @app_commands.autocomplete(tournament_id=tourn_autocomplete)
    @app_commands.choices(surface=[
        app_commands.Choice(name="Hard  — medium pace, medium bounce",   value="hard"),
        app_commands.Choice(name="Clay  — slow, high kicking bounce",    value="clay"),
        app_commands.Choice(name="Grass — fast, low skidding bounce",    value="grass"),
    ])
    async def venue_create(
        self,
        interaction: discord.Interaction,
        tournament_id: str,
        venue_name: str,
        surface: str,
        price: int,
        cpi_base: Optional[int] = None,
        bounce_height: Optional[app_commands.Range[int, 1, 100]] = None,
        roof: bool = False,
        roof_closed: bool = False,
        stadium_capacity: int = 5000,
        temp_c_min: int = 10,
        temp_c_max: int = 28,
        wind_kmh_min: int = 0,
        wind_kmh_max: int = 18,
        rain_chance_pct: int = 10,
    ):
        if not isinstance(interaction.user, discord.Member) or not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        surface_n = surface.strip().lower()
        if surface_n not in SURFACES:
            return await interaction.response.send_message(
                f"❌ Surface must be one of: {', '.join(SURFACES)}", ephemeral=True
            )

        tdb = _tourn_db().get("tournaments", {})
        if tournament_id.strip() not in tdb:
            return await interaction.response.send_message(
                "❌ tournament_id not found. Create it first with `/venue tournament create`.", ephemeral=True
            )

        vid = _venue_id_from_name(tournament_id.strip(), venue_name.strip())
        if _get_venue(vid):
            return await interaction.response.send_message(
                "❌ Venue already exists (same tournament + name).", ephemeral=True
            )

        surf_cpi_default, surf_bh_default = _SURFACE_DEFAULTS[surface_n]
        final_cpi = _clamp_int(int(cpi_base),      1, 100) if cpi_base      is not None else surf_cpi_default
        final_bh  = _clamp_int(int(bounce_height), 1, 100) if bounce_height is not None else surf_bh_default

        if roof and roof_closed:
            wind_kmh_min = 0
            wind_kmh_max = 0
            rain_chance_pct = 0

        row = {
            "venue_id":           vid,
            "tournament_id":      tournament_id.strip(),
            "name":               venue_name.strip(),
            "surface":            surface_n,
            "cpi_base":           final_cpi,
            "bounce_height_base": final_bh,
            "roof":               bool(roof),
            "roof_closed":        bool(roof_closed) if roof else False,
            "stadium_capacity":   int(max(0, stadium_capacity)),
            "price":              int(max(0, price)),
            "weather": {
                "temp_c_min":      int(temp_c_min),
                "temp_c_max":      int(temp_c_max),
                "wind_kmh_min":    int(wind_kmh_min),
                "wind_kmh_max":    int(wind_kmh_max),
                "rain_chance_pct": int(_clamp_int(int(rain_chance_pct), 0, 100)),
            },
        }
        _set_venue(vid, row)

        auto_notes: List[str] = []
        if cpi_base is None:
            auto_notes.append(f"CPI auto → **{final_cpi}**")
        if bounce_height is None:
            auto_notes.append(f"Bounce auto → **{final_bh}**")
        note = "  *(auto: " + ", ".join(auto_notes) + ")*" if auto_notes else ""

        await interaction.response.send_message(
            f"🏟️ ✅ Venue **{venue_name}** created as `{vid}`.{note}", ephemeral=False
        )

    @venue.command(name="edit", description="(Admin) Edit a venue.")
    @app_commands.guild_only()
    @app_commands.autocomplete(venue_id=venue_autocomplete)
    @app_commands.choices(surface=[
        app_commands.Choice(name="Hard",  value="hard"),
        app_commands.Choice(name="Clay",  value="clay"),
        app_commands.Choice(name="Grass", value="grass"),
    ])
    async def venue_edit(
        self,
        interaction: discord.Interaction,
        venue_id: str,
        venue_name: Optional[str] = None,
        surface: Optional[str] = None,
        cpi_base: Optional[int] = None,
        bounce_height: Optional[app_commands.Range[int, 1, 100]] = None,
        price: Optional[int] = None,
        roof: Optional[bool] = None,
        roof_closed: Optional[bool] = None,
        stadium_capacity: Optional[int] = None,
        temp_c_min: Optional[int] = None,
        temp_c_max: Optional[int] = None,
        wind_kmh_min: Optional[int] = None,
        wind_kmh_max: Optional[int] = None,
        rain_chance_pct: Optional[int] = None,
    ):
        if not isinstance(interaction.user, discord.Member) or not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        row = _get_venue(venue_id)
        if not row:
            return await interaction.response.send_message("❌ Venue not found.", ephemeral=True)

        if venue_name  is not None: row["name"]              = venue_name.strip()
        if surface     is not None:
            s = surface.strip().lower()
            if s not in SURFACES:
                return await interaction.response.send_message(
                    f"❌ Surface must be one of: {', '.join(SURFACES)}", ephemeral=True
                )
            row["surface"] = s
        if cpi_base        is not None: row["cpi_base"]           = _clamp_int(int(cpi_base),      1, 100)
        if bounce_height   is not None: row["bounce_height_base"] = _clamp_int(int(bounce_height), 1, 100)
        if price           is not None: row["price"]              = int(max(0, price))
        if roof            is not None: row["roof"]               = bool(roof)
        if roof_closed     is not None:
            row["roof_closed"] = bool(roof_closed) if row.get("roof") else False
        if stadium_capacity is not None: row["stadium_capacity"]  = int(max(0, stadium_capacity))

        w = row.setdefault("weather", {})
        if temp_c_min      is not None: w["temp_c_min"]      = int(temp_c_min)
        if temp_c_max      is not None: w["temp_c_max"]      = int(temp_c_max)
        if wind_kmh_min    is not None: w["wind_kmh_min"]    = int(wind_kmh_min)
        if wind_kmh_max    is not None: w["wind_kmh_max"]    = int(wind_kmh_max)
        if rain_chance_pct is not None: w["rain_chance_pct"] = int(_clamp_int(int(rain_chance_pct), 0, 100))

        _set_venue(venue_id, row)
        await interaction.response.send_message(f"✅ Venue `{venue_id}` updated.", ephemeral=False)

    @venue.command(name="delete", description="(Admin) Delete a venue.")
    @app_commands.guild_only()
    @app_commands.autocomplete(venue_id=venue_autocomplete)
    async def venue_delete(self, interaction: discord.Interaction, venue_id: str):
        if not isinstance(interaction.user, discord.Member) or not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        ok = _del_venue(venue_id)
        if not ok:
            return await interaction.response.send_message("❌ Venue not found.", ephemeral=True)
        await interaction.response.send_message(f"🗑️ Deleted venue `{venue_id}`.", ephemeral=False)

    @venue.command(name="view", description="View a venue's full details including conditions impact.")
    @app_commands.guild_only()
    @app_commands.autocomplete(venue_id=venue_autocomplete)
    async def venue_view(self, interaction: discord.Interaction, venue_id: str):
        row = _get_venue(venue_id)
        if not row:
            return await interaction.response.send_message("❌ Venue not found.", ephemeral=True)

        surface  = str(row.get("surface", "hard"))
        cpi_base = int(row.get("cpi_base", _SURFACE_DEFAULTS.get(surface, (38, 50))[0]))
        bh_base  = int(row.get("bounce_height_base", _SURFACE_DEFAULTS.get(surface, (38, 50))[1]))
        has_roof = bool(row.get("roof", False))
        rc       = bool(row.get("roof_closed", False))
        w        = row.get("weather", {})

        cpi_range  = _SURFACE_CPI_RANGE.get(surface, "—")
        bh_note    = _SURFACE_BOUNCE_NOTE.get(surface, "—")
        roof_line  = "🏠 **Indoor** (roof closed — weather sealed)" if (has_roof and rc) else (
                     "🏟️ Retractable roof (currently open)" if has_roof else
                     "🌤️ Open-air outdoor")

        wind_line = "No wind (sealed indoor)" if (has_roof and rc) else (
            f"💨 **{w.get('wind_kmh_min',0)}–{w.get('wind_kmh_max',18)} km/h** (up to +8 CPI at high gusts)")

        txt = (
            f"🏟️ **{row.get('name','(unnamed)')}**\n"
            f"ID: `{venue_id}` · Tournament: `{row.get('tournament_id','?')}`\n\n"
            f"**Surface: {surface.title()}**\n"
            f"Base CPI: **{cpi_base}**  ·  Expected in-match range: {cpi_range}\n"
            f"Bounce height: **{bh_base}/100** — {bh_note}\n\n"
            f"{roof_line}\n"
            f"Capacity: **{row.get('stadium_capacity',0):,}** · "
            f"Price: **{_format_money(int(row.get('price',0)))}**\n\n"
            f"🌦️ **Weather Profile**\n"
            f"🌡️ Temp: **{w.get('temp_c_min','?')}–{w.get('temp_c_max','?')}°C** (each °C above 20 → +0.25 CPI)\n"
            f"{wind_line}\n"
            f"🌧️ Rain chance: **{w.get('rain_chance_pct','?')}%** (rain → −5 CPI)\n\n"
            f"{_CONDITIONS_EXPLAINED}"
        )
        await interaction.response.send_message(txt[:1980], ephemeral=False)

    @venue.command(name="shop", description="Browse venues for sale.")
    @app_commands.guild_only()
    async def venue_shop(self, interaction: discord.Interaction):
        venues = _get_venues()
        if not venues:
            return await interaction.response.send_message("No venues created yet.", ephemeral=True)

        rows = sorted(
            venues.values(),
            key=lambda r: (str(r.get("tournament_id", "")), str(r.get("name", "")).lower()),
        )

        lines = ["🏪 **Venue Shop** (use `/venue buy <venue_id>`)\n"]
        for r in rows[:20]:
            vid     = r.get("venue_id", "?")
            surface = str(r.get("surface", "hard"))
            cpi     = int(r.get("cpi_base", _SURFACE_DEFAULTS.get(surface, (38,))[0]))
            price   = int(r.get("price", 0))
            roof_tag = " 🏠" if r.get("roof_closed") else (" 🏟️" if r.get("roof") else "")
            lines.append(
                f"• **{r.get('name','(unnamed)')}** — `{vid}` — "
                f"{surface.title()}{roof_tag} — Base CPI **{cpi}** — "
                f"Price **{_format_money(price)}**"
            )
        if len(rows) > 20:
            lines.append(f"\n…and {len(rows) - 20} more.")

        await interaction.response.send_message("\n".join(lines)[:1900], ephemeral=False)

    @venue.command(name="buy", description="Buy a venue (adds it to your venue inventory).")
    @app_commands.guild_only()
    @app_commands.autocomplete(venue_id=venue_autocomplete)
    async def venue_buy(self, interaction: discord.Interaction, venue_id: str):
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        row = _get_venue(venue_id)
        if not row:
            return await interaction.response.send_message("❌ Venue not found.", ephemeral=True)

        if _owned(interaction.guild.id, interaction.user.id, venue_id):
            return await interaction.response.send_message("❌ You already own this venue.", ephemeral=True)

        price = int(row.get("price", 0))
        bal   = int(get_balance(interaction.user.id))
        if bal < price:
            return await interaction.response.send_message(
                f"❌ Not enough coins. Need **{_format_money(price)}**, you have **{_format_money(bal)}**.",
                ephemeral=True,
            )

        ok = remove_balance(interaction.user.id, price)
        if not ok:
            return await interaction.response.send_message(
                f"❌ Purchase failed — insufficient funds.", ephemeral=True
            )

        inv = _get_user_inv(interaction.guild.id, interaction.user.id)
        inv.append(venue_id)
        _set_user_inv(interaction.guild.id, interaction.user.id, inv)

        await interaction.response.send_message(
            f"✅ Bought **{row.get('name','(unnamed)')}** (`{venue_id}`).", ephemeral=False
        )

    @venue.command(name="inventory", description="View your owned venues.")
    @app_commands.guild_only()
    async def venue_inventory(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
    ):
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message("❌ Guild only.", ephemeral=True)

        target = user or interaction.user
        inv    = _get_user_inv(interaction.guild.id, target.id)
        if not inv:
            who = "You have" if target.id == interaction.user.id else f"{target.display_name} has"
            return await interaction.response.send_message(f"{who} no venues.", ephemeral=True)

        venues = _get_venues()
        lines  = [f"🎟️ **Venue Inventory — {target.display_name}**\n"]
        for vid in inv[:30]:
            r = venues.get(vid)
            if not r:
                lines.append(f"• `{vid}` *(missing)*")
            else:
                surface  = str(r.get("surface", "hard"))
                cpi      = int(r.get("cpi_base", _SURFACE_DEFAULTS.get(surface, (38,))[0]))
                roof_tag = " 🏠" if r.get("roof_closed") else (" 🏟️" if r.get("roof") else "")
                lines.append(
                    f"• **{r.get('name','(unnamed)')}** — `{vid}` — "
                    f"{surface.title()}{roof_tag} — Base CPI **{cpi}**"
                )
        if len(inv) > 30:
            lines.append(f"\n…and {len(inv) - 30} more.")

        await interaction.response.send_message("\n".join(lines)[:1900], ephemeral=False)


async def setup(bot: commands.Bot):
    await bot.add_cog(VenuesCog(bot))