import discord
from discord.ext import commands
from discord import app_commands
from datetime import datetime
import uuid

import config
from utils import load_json, save_json, ensure_dir
from modules.economy import get_balance, remove_balance


def _is_admin(member: discord.Member) -> bool:
    return member.guild_permissions.administrator


# -------------------------
# settings (cardlog + emojis)
# -------------------------

def _settings_path():
    return f"{config.DATA_DIR}/settings.json"


def _get_settings():
    ensure_dir(config.DATA_DIR)
    return load_json(_settings_path(), {
        "cardlog_channel_id": None,
        "rarity_emojis": {
            "silver": "🥈",
            "gold": "🥇",
            "elite": "💠",
            "legendary": "👑"
        }
    })


def _set_cardlog_channel_id(cid: int):
    s = _get_settings()
    s["cardlog_channel_id"] = cid
    save_json(_settings_path(), s)


async def _cardlog(guild: discord.Guild, text: str):
    s = _get_settings()
    cid = s.get("cardlog_channel_id")
    if not cid:
        return
    ch = guild.get_channel(int(cid))
    if not ch:
        return
    try:
        await ch.send(text)
    except Exception:
        pass


def _get_rarity_emojis() -> dict[str, str]:
    s = _get_settings()
    r = s.get("rarity_emojis") or {}
    return {
        "silver": str(r.get("silver", "🥈")),
        "gold": str(r.get("gold", "🥇")),
        "elite": str(r.get("elite", "💠")),
        "legendary": str(r.get("legendary", "👑")),
    }


def _set_rarity_emojis(silver: str, gold: str, elite: str, legendary: str):
    s = _get_settings()
    s["rarity_emojis"] = {
        "silver": silver.strip(),
        "gold": gold.strip(),
        "elite": elite.strip(),
        "legendary": legendary.strip(),
    }
    save_json(_settings_path(), s)


# -------------------------
# cards storage
# -------------------------

def _cards_path():
    return f"{config.DATA_DIR}/cards.json"


def _load():
    ensure_dir(config.DATA_DIR)
    data = load_json(_cards_path(), {})

    if not isinstance(data, dict):
        data = {}

    # ---- categories must be a list ----
    cats = data.get("categories")
    if isinstance(cats, dict):
        data["categories"] = list(cats.values())
    elif not isinstance(cats, list):
        data["categories"] = []

    # ---- cards must be a list ----
    cards = data.get("cards")
    if isinstance(cards, dict):
        data["cards"] = list(cards.values())
    elif not isinstance(cards, list):
        data["cards"] = []

    # ---- inventory must be dict[user_id] -> list ----
    inv = data.get("inventory")
    if not isinstance(inv, dict):
        data["inventory"] = {}
    else:
        for uid, items in list(inv.items()):
            if not isinstance(items, list):
                data["inventory"][uid] = []

    # Save repaired structure so this never happens again
    save_json(_cards_path(), data)
    return data


def _save(data):
    save_json(_cards_path(), data)


# -------------------------
# interaction dedupe
# -------------------------

def _dedupe_path():
    return f"{config.DATA_DIR}/interaction_dedupe.json"


def _seen_interaction(interaction_id: int) -> bool:
    ensure_dir(config.DATA_DIR)
    data = load_json(_dedupe_path(), {"seen": []})

    seen = data.get("seen")
    if not isinstance(seen, list):
        seen = []
        data["seen"] = seen

    key = str(interaction_id)
    if key in seen:
        return True

    seen.append(key)

    # keep file from growing forever
    if len(seen) > 3000:
        data["seen"] = seen[-2000:]

    save_json(_dedupe_path(), data)
    return False


# -------------------------
# helpers
# -------------------------

def _rarity_key(rarity: str) -> str:
    return (rarity or "").strip().lower()


def _emoji_for_rarity(rarity: str) -> str:
    rkey = _rarity_key(rarity)
    e = _get_rarity_emojis()
    return e.get(rkey, "❔")


def _abbr(n: int) -> str:
    n = int(n)
    if n >= 1_000_000:
        s = f"{n/1_000_000:.1f}M"
        return s.replace(".0M", "M")
    if n >= 1_000:
        s = f"{n/1_000:.1f}K"
        return s.replace(".0K", "K")
    return str(n)


def _fmt_prices(pr):
    e = _get_rarity_emojis()
    return (
        "Prices: "
        f"{e['silver']} {_abbr(pr['silver'])}  |  "
        f"{e['gold']} {_abbr(pr['gold'])}  |  "
        f"{e['elite']} {_abbr(pr['elite'])}  |  "
        f"{e['legendary']} {_abbr(pr['legendary'])}"
    )


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def _matches(card: dict, cat: dict | None, query: str) -> bool:
    q = _norm(query)
    if not q:
        return True
    hay = " ".join([
        _norm(card.get("title", "")),
        _norm(card.get("description", "")),
        _norm((cat or {}).get("title", "")),
        _norm((cat or {}).get("description", "")),
    ])
    for part in q.split():
        if part not in hay:
            return False
    return True


def _is_enabled(card: dict) -> bool:
    """
    Your shop uses: active (bool)  -> this is THE main flag
    Also supports: enabled / disabled (legacy)
    """
    if not isinstance(card, dict):
        return False

    # legacy disabled:true
    dis = card.get("disabled", False)
    if isinstance(dis, bool) and dis:
        return False
    if isinstance(dis, str) and dis.strip().lower() in ("1", "true", "yes", "on"):
        return False

    # main flag (your JSON): active
    if "active" in card:
        a = card.get("active", True)
        if isinstance(a, bool):
            return a
        if isinstance(a, (int, float)):
            return a != 0
        if isinstance(a, str):
            return a.strip().lower() in ("1", "true", "yes", "on")

    # fallback: enabled
    e = card.get("enabled", True)
    if isinstance(e, bool):
        return e
    if isinstance(e, (int, float)):
        return e != 0
    if isinstance(e, str):
        return e.strip().lower() in ("1", "true", "yes", "on")

    return True


# -------------------------
# views
# -------------------------

class PagerView(discord.ui.View):
    def __init__(self, pages, user_id: int, title="Menu"):
        super().__init__(timeout=180)
        self.pages = pages
        self.user_id = user_id
        self.i = 0
        self.title = title
        self._locked = False

    def _embed(self):
        embed = discord.Embed(title=self.title, description=self.pages[self.i])
        embed.set_footer(text=f"Page {self.i+1}/{len(self.pages)}")
        return embed

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
            return await interaction.response.send_message("❌ This menu isn't for you.", ephemeral=True)
        if self._locked:
            return
        self._locked = True
        try:
            self.i = (self.i + 1) % len(self.pages)
            await self._edit(interaction)
        finally:
            self._locked = False


class CategorySelect(discord.ui.Select):
    def __init__(self, options, user_id: int):
        super().__init__(placeholder="Select a category...", min_values=1, max_values=1, options=options)
        self.user_id = user_id

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ This menu isn't for you.", ephemeral=True)
        await self.view.on_pick_category(interaction, self.values[0])


class SearchModal(discord.ui.Modal, title="Search Cards"):
    query = discord.ui.TextInput(
        label="Search",
        placeholder="Type words to search (title/description/category)…",
        required=True,
        max_length=100
    )

    def __init__(self, cog, user_id: int, category_id: str | None = None, public: bool = False):
        super().__init__()
        self.cog = cog
        self.user_id = user_id
        self.category_id = category_id
        self.public = public

    async def on_submit(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ This menu isn't for you.", ephemeral=True)
        await self.cog.show_search_results(interaction, str(self.query), self.user_id, self.category_id, public=self.public)


class CategorySelectView(discord.ui.View):
    def __init__(self, cog, user_id: int, categories):
        super().__init__(timeout=180)
        self.cog = cog
        self.user_id = user_id
        opts = [discord.SelectOption(label=c["title"], value=c["id"]) for c in categories]
        self.add_item(CategorySelect(opts, user_id))

    @discord.ui.button(label="Search", style=discord.ButtonStyle.primary)
    async def search(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.user_id:
            return await interaction.response.send_message("❌ This menu isn't for you.", ephemeral=True)
        await interaction.response.send_modal(SearchModal(self.cog, self.user_id, category_id=None, public=False))

    async def on_pick_category(self, interaction: discord.Interaction, category_id: str):
        await self.cog.show_store_for_category(interaction, category_id)


class BuyView(discord.ui.View):
    def __init__(self, cog, card_id: str, user_id: int):
        super().__init__(timeout=180)
        self.cog = cog
        self.card_id = card_id
        self.user_id = user_id

    async def _guard(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("❌ This menu isn't for you.", ephemeral=True)
            return False
        return True

    async def _defer_buy(self, interaction: discord.Interaction):
        try:
            if not interaction.response.is_done():
                await interaction.response.defer(ephemeral=True, thinking=True)
        except Exception:
            pass

    @discord.ui.button(label="Buy Silver", style=discord.ButtonStyle.success)
    async def buy_silver(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction):
            return
        await self._defer_buy(interaction)
        await self.cog.buy_card(interaction, self.card_id, "Silver")

    @discord.ui.button(label="Buy Gold", style=discord.ButtonStyle.success)
    async def buy_gold(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction):
            return
        await self._defer_buy(interaction)
        await self.cog.buy_card(interaction, self.card_id, "Gold")

    @discord.ui.button(label="Buy Elite", style=discord.ButtonStyle.success)
    async def buy_elite(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction):
            return
        await self._defer_buy(interaction)
        await self.cog.buy_card(interaction, self.card_id, "Elite")

    @discord.ui.button(label="Buy Legendary", style=discord.ButtonStyle.success)
    async def buy_legendary(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not await self._guard(interaction):
            return
        await self._defer_buy(interaction)
        await self.cog.buy_card(interaction, self.card_id, "Legendary")


# -------------------------
# main cog
# -------------------------

class CardsCog(commands.Cog):

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # -------------------------
    # MAIN CARD GROUP
    # -------------------------
    card_group = app_commands.Group(name="card", description="Card system commands")

    # -------------------------
    # TYPE SUBGROUP
    # -------------------------
    type_group = app_commands.Group(name="type", description="Manage card types/categories", parent=card_group)

    @type_group.command(name="create", description="Admin: create a card category (type).")
    async def type_create(
        self,
        interaction: discord.Interaction,
        title: str,
        description: str,
        stats1: str,
        stats2: str | None = None,
        stats3: str | None = None,
        stats4: str | None = None,
        stats5: str | None = None,
        stats6: str | None = None,
        stats7: str | None = None,
        stats8: str | None = None,
        stats9: str | None = None,
        stats10: str | None = None,
    ):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        if _seen_interaction(interaction.id):
            return await interaction.response.send_message("✅ (Already processed)", ephemeral=True)

        stats_titles = [stats1.strip()]
        for s in [stats2, stats3, stats4, stats5, stats6, stats7, stats8, stats9, stats10]:
            if s and s.strip():
                stats_titles.append(s.strip())

        data = _load()
        cid = f"card-categ-{uuid.uuid4().hex[:8]}"
        data["categories"].append({
            "id": cid,
            "title": title.strip(),
            "description": description.strip(),
            "stats_titles": stats_titles
        })
        _save(data)

        await interaction.response.send_message(f"✅ Created category **{title.strip()}** with ID `{cid}`.")
        await _cardlog(interaction.guild, f"🃏 {interaction.user.mention} created card category **{title.strip()}** (`{cid}`).")

    @type_group.command(name="clear-all", description="Admin: delete ALL card categories and cards (keeps inventories).")
    async def type_clear_all(self, interaction: discord.Interaction):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        data = _load()
        data["categories"] = []
        data["cards"] = []
        _save(data)

        await interaction.response.send_message("🧨 All card categories and cards cleared (inventories kept).")

    # -------------------------
    # INVENTORY SUBGROUP
    # -------------------------
    inventory_group = app_commands.Group(name="inventory", description="Manage card inventories", parent=card_group)

    @inventory_group.command(name="view", description="View a user's card inventory (public, grouped).")
    @app_commands.describe(user="Optional: whose inventory (default: you)")
    async def inventory_view(self, interaction: discord.Interaction, user: discord.Member | None = None):
        data = _load()
        target = user or interaction.user
        inv = data.get("inventory", {}).get(str(target.id), [])
        if not inv:
            return await interaction.response.send_message("ℹ️ Inventory is empty.")

        cards_map = {c["id"]: c for c in data.get("cards", [])}
        cats_map = {c["id"]: c for c in data.get("categories", [])}

        grouped: dict[tuple[str, str], dict] = {}
        for item in inv:
            cid = item.get("card_id")
            rkey = _rarity_key(item.get("rarity", ""))
            key = (cid, rkey)
            grouped.setdefault(key, {"count": 0, "paid": 0, "rarity": item.get("rarity", ""), "when": 0})
            grouped[key]["count"] += 1
            grouped[key]["paid"] = int(item.get("paid", 0))
            grouped[key]["when"] = max(grouped[key]["when"], int(item.get("when", 0)))

        blocks = []
        items_sorted = sorted(grouped.items(), key=lambda kv: kv[1]["when"], reverse=True)

        for (card_id, rkey), meta in items_sorted:
            card = cards_map.get(card_id)
            if not card:
                # missing card record: still show something basic
                count = meta["count"]
                rarity = meta["rarity"]
                emoji = _emoji_for_rarity(rarity)
                when_ts = int(meta.get("when", 0))
                paid = meta.get("paid", 0)
                blocks.append(
                    f"**(Deleted Card)** [{count}] — {emoji} **{rarity}**\n"
                    f"Last Paid: **{_abbr(paid)}**\n"
                    + (f"Latest Purchase: <t:{when_ts}:F> (<t:{when_ts}:R>)\n" if when_ts else "")
                    + f"ID: `{card_id}`\n"
                )
                continue

            cat = cats_map.get(card.get("category_id"))
            count = meta["count"]
            rarity = meta["rarity"]
            paid = int(meta["paid"])

            emoji = _emoji_for_rarity(rarity)
            when_ts = int(meta.get("when", 0))

            block = (
                f"**{card['title']}** [{count}] — {emoji} **{rarity}**\n"
                f"*{card['description']}*\n"
                f"Last Paid: **{_abbr(paid)}**\n"
                + (f"Latest Purchase: <t:{when_ts}:F> (<t:{when_ts}:R>)\n" if when_ts else "")
                + f"ID: `{card_id}`\n"
            )
            if cat:
                for t, v in zip(cat["stats_titles"], card["stats_values"]):
                    block += f"**{t}**: {v}\n"
            blocks.append(block)

        pages, cur = [], ""
        for b in blocks:
            if len(cur) + len(b) > 3500:
                pages.append(cur)
                cur = ""
            cur += b + "\n"
        if cur:
            pages.append(cur)

        embed = discord.Embed(title=f"{target.display_name}'s Card Inventory", description=pages[0])
        embed.set_footer(text=f"Page 1/{len(pages)}")
        view = PagerView(pages, interaction.user.id, title=f"{target.display_name}'s Card Inventory")
        await interaction.response.send_message(embed=embed, view=view)

    @inventory_group.command(name="summary", description="Inventory summary (public).")
    @app_commands.describe(user="Optional: whose inventory (default: you)")
    async def inventory_summary(self, interaction: discord.Interaction, user: discord.Member | None = None):
        data = _load()
        target = user or interaction.user
        inv = data.get("inventory", {}).get(str(target.id), [])
        if not inv:
            return await interaction.response.send_message("ℹ️ Inventory is empty.")

        cards_map = {c["id"]: c for c in data.get("cards", [])}
        cats_map = {c["id"]: c for c in data.get("categories", [])}
        e = _get_rarity_emojis()

        def item_value(it: dict) -> int:
            paid = int(it.get("paid", 0) or 0)
            if paid > 0:
                return paid
            card = cards_map.get(it.get("card_id"))
            if not card:
                return 0
            rkey = _rarity_key(it.get("rarity", ""))
            try:
                return int(card.get("prices", {}).get(rkey, 0) or 0)
            except Exception:
                return 0

        counts = {"silver": 0, "gold": 0, "elite": 0, "legendary": 0}
        totals = {"silver": 0, "gold": 0, "elite": 0, "legendary": 0}
        times: list[int] = []

        copies_by_card: dict[str, int] = {}
        value_by_variant: dict[tuple[str, str], int] = {}

        for it in inv:
            cid = it.get("card_id")
            rkey = _rarity_key(it.get("rarity", ""))
            val = item_value(it)

            if rkey in counts:
                counts[rkey] += 1
                totals[rkey] += val

            if cid:
                copies_by_card[cid] = copies_by_card.get(cid, 0) + 1
                value_by_variant[(cid, rkey)] = value_by_variant.get((cid, rkey), 0) + val

            w = int(it.get("when", 0) or 0)
            if w:
                times.append(w)

        total_value = sum(totals.values())
        first_ts = min(times) if times else 0
        last_ts = max(times) if times else 0

        def card_label(cid: str) -> str:
            card = cards_map.get(cid)
            if not card:
                return f"**(Deleted Card)** (`{cid}`)"
            cat = cats_map.get(card.get("category_id"))
            cat_name = cat.get("title") if cat else "Unknown Category"
            return f"**{card.get('title','(untitled)')}** — *{cat_name}* (`{cid}`)"

        def variant_label(cid: str, rkey: str) -> str:
            card = cards_map.get(cid)
            if not card:
                return f"**(Deleted Card)** {_emoji_for_rarity(rkey)} (`{cid}`)"
            cat = cats_map.get(card.get("category_id"))
            cat_name = cat.get("title") if cat else "Unknown Category"
            return f"**{card.get('title','(untitled)')}** {_emoji_for_rarity(rkey)} — *{cat_name}*"

        top_variants = sorted(value_by_variant.items(), key=lambda kv: kv[1], reverse=True)[:5]
        least_variant = sorted(value_by_variant.items(), key=lambda kv: kv[1])[0] if value_by_variant else None

        fav_id = None
        if copies_by_card:
            fav_id = sorted(copies_by_card.items(), key=lambda kv: kv[1], reverse=True)[0][0]

        lines = []
        lines.append(f"**Inventory Summary — {target.display_name}**")
        lines.append("")
        lines.append(f"**Total Value:** {_abbr(total_value)} coins")
        lines.append("")
        lines.append("**Owned (by rarity):**")
        lines.append(
            f"{e['silver']} {counts['silver']}   "
            f"{e['gold']} {counts['gold']}   "
            f"{e['elite']} {counts['elite']}   "
            f"{e['legendary']} {counts['legendary']}"
        )
        lines.append("")
        lines.append("**Value by rarity:**")
        lines.append(
            f"{e['silver']} {_abbr(totals['silver'])}  |  "
            f"{e['gold']} {_abbr(totals['gold'])}  |  "
            f"{e['elite']} {_abbr(totals['elite'])}  |  "
            f"{e['legendary']} {_abbr(totals['legendary'])}"
        )
        lines.append("")
        if first_ts:
            lines.append(f"**First purchase:** <t:{first_ts}:F> (<t:{first_ts}:R>)")
        if last_ts:
            lines.append(f"**Latest purchase:** <t:{last_ts}:F> (<t:{last_ts}:R>)")

        lines.append("")
        lines.append("**Top 5 most valuable cards:**")
        if top_variants:
            for i, ((cid, rkey), v) in enumerate(top_variants, start=1):
                lines.append(f"{i}. {variant_label(cid, rkey)} — **{_abbr(v)}**")
        else:
            lines.append("—")

        lines.append("")
        lines.append("**Least valuable card:**")
        if least_variant:
            (cid, rkey), v = least_variant
            lines.append(f"{variant_label(cid, rkey)} — **{_abbr(v)}**")
        else:
            lines.append("—")

        lines.append("")
        lines.append("**Favourite card:**")
        if fav_id:
            lines.append(f"{card_label(fav_id)} — **{copies_by_card.get(fav_id, 0)} copies**")
        else:
            lines.append("—")

        embed = discord.Embed(title="Inventory Summary", description="\n".join(lines))
        await interaction.response.send_message(embed=embed)

    @inventory_group.command(name="add", description="Admin: add one card to a user's inventory.")
    async def inventory_add(self, interaction: discord.Interaction, user: discord.Member, card_id: str, rarity: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        data = _load()
        card = next((c for c in data["cards"] if c["id"] == card_id), None)
        if not card:
            return await interaction.response.send_message("❌ Invalid card ID.", ephemeral=True)

        inv = data.setdefault("inventory", {}).setdefault(str(user.id), [])
        inv.append({
            "card_id": card_id,
            "rarity": rarity,
            "paid": 0,
            "when": int(datetime.utcnow().timestamp())
        })
        _save(data)

        await interaction.response.send_message(f"✅ Added **{rarity}** **{card['title']}** to {user.mention}.")

    @inventory_group.command(name="remove", description="Admin: remove one card from a user's inventory.")
    async def inventory_remove(self, interaction: discord.Interaction, user: discord.Member, card_id: str, rarity: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        data = _load()
        inv = data.get("inventory", {}).get(str(user.id), [])
        if not inv:
            return await interaction.response.send_message("❌ That inventory is empty.", ephemeral=True)

        rkey = _rarity_key(rarity)
        idx = next((i for i, it in enumerate(inv)
                    if it.get("card_id") == card_id and _rarity_key(it.get("rarity", "")) == rkey), None)
        if idx is None:
            return await interaction.response.send_message("❌ That user doesn't have that card/rarity.", ephemeral=True)

        inv.pop(idx)
        _save(data)
        await interaction.response.send_message(f"✅ Removed one `{card_id}` ({rarity}) from {user.mention}.")

    @inventory_group.command(name="clear", description="Admin: clear inventories (optional: one user).")
    @app_commands.describe(user="Optional: clear only this user's inventory (default: everyone)")
    async def inventory_clear(self, interaction: discord.Interaction, user: discord.Member | None = None):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        data = _load()
        inv = data.setdefault("inventory", {})

        if user is None:
            inv.clear()
            _save(data)
            return await interaction.response.send_message("🧹 Cleared **ALL** inventories.", ephemeral=True)

        inv[str(user.id)] = []
        _save(data)
        await interaction.response.send_message(f"🧹 Cleared inventory for {user.mention}.", ephemeral=True)

    # -------------------------
    # MAIN CARD COMMANDS
    # -------------------------

    @card_group.command(name="set-log-channel", description="Admin: set the card-logs channel by channel ID.")
    async def set_log_channel(self, interaction: discord.Interaction, channel_id: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        if not channel_id.isdigit():
            return await interaction.response.send_message("❌ Invalid channel ID.", ephemeral=True)

        cid = int(channel_id)
        ch = interaction.guild.get_channel(cid) if interaction.guild else None
        if not ch:
            return await interaction.response.send_message("❌ That channel ID is not in this server.", ephemeral=True)

        _set_cardlog_channel_id(cid)
        await interaction.response.send_message(f"✅ Card log channel set to <#{cid}>.")

    @card_group.command(name="create", description="Admin: create a card under a category.")
    async def card_create(
        self,
        interaction: discord.Interaction,
        category_id: str,
        card_title: str,
        card_description: str,
        silver_price: int,
        gold_price: int,
        elite_price: int,
        legendary_price: int,
        stats_values: str
    ):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        if _seen_interaction(interaction.id):
            return await interaction.response.send_message("✅ (Already processed)", ephemeral=True)

        data = _load()
        cat = next((c for c in data["categories"] if c["id"] == category_id), None)
        if not cat:
            return await interaction.response.send_message("❌ Category does not exist.", ephemeral=True)

        values = [v.strip() for v in stats_values.split(",")] if stats_values else []
        if len(values) != len(cat["stats_titles"]):
            return await interaction.response.send_message(
                f"❌ stats_values mismatch. Category expects **{len(cat['stats_titles'])}** values.",
                ephemeral=True
            )

        pr = {
            "silver": int(silver_price),
            "gold": int(gold_price),
            "elite": int(elite_price),
            "legendary": int(legendary_price),
        }
        if any(v <= 0 for v in pr.values()):
            return await interaction.response.send_message("❌ All prices must be > 0.", ephemeral=True)

        card_id = f"card-{uuid.uuid4().hex[:8]}"
        data["cards"].append({
            "id": card_id,
            "category_id": category_id,
            "title": card_title.strip(),
            "description": card_description.strip(),
            "stats_values": values,
            "prices": pr,
            "active": True
        })
        _save(data)

        await interaction.response.send_message(f"✅ Created card **{card_title.strip()}** with ID `{card_id}`.")
        await _cardlog(interaction.guild, f"🆕 {interaction.user.mention} created card **{card_title.strip()}** (`{card_id}`).")

    @card_group.command(name="edit-emojis", description="Admin: set rarity emojis used in shop/inventory/logs.")
    async def edit_emojis(
        self,
        interaction: discord.Interaction,
        silver: str,
        gold: str,
        elite: str,
        legendary: str
    ):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        for val, label in [(silver, "Silver"), (gold, "Gold"), (elite, "Elite"), (legendary, "Legendary")]:
            if not val or not val.strip():
                return await interaction.response.send_message(f"❌ {label} emoji can't be empty.", ephemeral=True)
            if len(val.strip()) > 64:
                return await interaction.response.send_message(f"❌ {label} emoji too long.", ephemeral=True)

        _set_rarity_emojis(silver, gold, elite, legendary)
        e = _get_rarity_emojis()

        await interaction.response.send_message(
            "✅ Rarity emojis updated:\n"
            f"{e['silver']} Silver\n"
            f"{e['gold']} Gold\n"
            f"{e['elite']} Elite\n"
            f"{e['legendary']} Legendary",
            ephemeral=True
        )

    @card_group.command(name="edit", description="Admin: edit a card (same fields as create).")
    async def card_edit(
        self,
        interaction: discord.Interaction,
        card_id: str,
        category_id: str,
        card_title: str,
        card_description: str,
        silver_price: int,
        gold_price: int,
        elite_price: int,
        legendary_price: int,
        stats_values: str
    ):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        data = _load()
        card = next((c for c in data["cards"] if c["id"] == card_id), None)
        if not card:
            return await interaction.response.send_message("❌ Card not found.", ephemeral=True)

        cat = next((c for c in data["categories"] if c["id"] == category_id), None)
        if not cat:
            return await interaction.response.send_message("❌ Category does not exist.", ephemeral=True)

        values = [v.strip() for v in stats_values.split(",")] if stats_values else []
        if len(values) != len(cat["stats_titles"]):
            return await interaction.response.send_message(
                f"❌ stats_values mismatch. Category expects **{len(cat['stats_titles'])}** values.",
                ephemeral=True
            )

        pr = {
            "silver": int(silver_price),
            "gold": int(gold_price),
            "elite": int(elite_price),
            "legendary": int(legendary_price),
        }
        if any(v <= 0 for v in pr.values()):
            return await interaction.response.send_message("❌ All prices must be > 0.", ephemeral=True)

        card["category_id"] = category_id
        card["title"] = card_title.strip()
        card["description"] = card_description.strip()
        card["stats_values"] = values
        card["prices"] = pr
        _save(data)

        await interaction.response.send_message(f"✅ Updated card `{card_id}`.")

    @card_group.command(name="disable", description="Admin: remove a card from the shop but keep it for inventories.")
    async def card_disable(self, interaction: discord.Interaction, card_id: str):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)

        data = _load()
        card = next((c for c in data["cards"] if c["id"] == card_id), None)
        if not card:
            return await interaction.response.send_message("❌ Card not found.", ephemeral=True)

        card["active"] = False
        _save(data)
        await interaction.response.send_message(f"✅ Card `{card_id}` removed from shop (kept for inventories).")

    @card_group.command(name="clear-all", description="Admin: delete ALL cards (keeps categories + inventories).")
    async def card_clear_all(self, interaction: discord.Interaction):
        if not _is_admin(interaction.user):
            return await interaction.response.send_message("❌ Admin only.", ephemeral=True)
        data = _load()
        data["cards"] = []
        _save(data)
        await interaction.response.send_message("🧨 All cards cleared (categories + inventories kept).")

    @card_group.command(name="store", description="Browse the card store by category (ephemeral).")
    async def card_store(self, interaction: discord.Interaction):
        data = _load()
        cats = data["categories"]
        if not cats:
            return await interaction.response.send_message("ℹ️ No card categories yet.", ephemeral=True)

        view = CategorySelectView(self, interaction.user.id, cats)
        await interaction.response.send_message("Select a card category (or Search):", view=view, ephemeral=True)

    @card_group.command(name="search", description="Search the card store (public).")
    @app_commands.describe(query="Words to search", category_id="Optional: restrict to a category ID")
    async def card_search(self, interaction: discord.Interaction, query: str, category_id: str | None = None):
        await self.show_search_results(interaction, query, interaction.user.id, category_id=category_id, public=True)

    @card_group.command(name="view", description="View a card and buy it (public).")
    async def card_view(self, interaction: discord.Interaction, card_id: str):
        data = _load()
        card = next((c for c in data.get("cards", []) if c.get("id") == card_id), None)
        if not card:
            return await interaction.response.send_message("❌ Invalid card ID.", ephemeral=True)

        # IMPORTANT: do NOT allow viewing disabled cards
        if not _is_enabled(card):
            return await interaction.response.send_message("❌ That card is currently disabled.", ephemeral=True)

        cat = next((c for c in data.get("categories", []) if c.get("id") == card.get("category_id")), None)
        if not cat:
            return await interaction.response.send_message("❌ Card category missing.", ephemeral=True)

        desc = f"**{card['title']}**\n*{card['description']}*\n\n"
        for t, v in zip(cat["stats_titles"], card["stats_values"]):
            desc += f"**{t}**: {v}\n"
        desc += f"\n{_fmt_prices(card['prices'])}\nID: `{card['id']}`"

        embed = discord.Embed(title="Card View", description=desc)
        view = BuyView(self, card_id, interaction.user.id)
        await interaction.response.send_message(embed=embed, view=view)

    # -------------------------
    # HELPER METHODS
    # -------------------------

    async def show_search_results(self, interaction: discord.Interaction, query: str, user_id: int, category_id: str | None, public: bool):
        data = _load()
        cats_map = {c["id"]: c for c in data["categories"]}

        # ONLY enabled/active cards should show in shop/search
        cards = [c for c in data.get("cards", []) if _is_enabled(c)]

        if category_id:
            cards = [c for c in cards if c.get("category_id") == category_id]

        hits = []
        for c in cards:
            cat = cats_map.get(c.get("category_id"))
            if _matches(c, cat, query):
                hits.append((c, cat))

        if not hits:
            try:
                if not interaction.response.is_done():
                    return await interaction.response.send_message("🔎 No cards found.", ephemeral=(not public))
                return await interaction.edit_original_response(content="🔎 No cards found.", embed=None, view=None)
            except Exception:
                return

        blocks = []
        for card, cat in hits:
            cat_name = cat["title"] if cat else "(missing category)"
            blocks.append(
                f"**{card['title']}**  — *{cat_name}*\n"
                f"*{card['description']}*\n"
                f"{_fmt_prices(card['prices'])}\n"
                f"ID: `{card['id']}`\n"
            )

        pages = []
        cur = ""
        for b in blocks:
            if len(cur) + len(b) > 3500:
                pages.append(cur)
                cur = ""
            cur += b + "\n"
        if cur:
            pages.append(cur)

        title = f"Search: {query}"
        embed = discord.Embed(title=title, description=pages[0])
        embed.set_footer(text=f"Page 1/{len(pages)}")
        view = PagerView(pages, user_id, title=title)

        if not interaction.response.is_done():
            await interaction.response.send_message(embed=embed, view=view, ephemeral=(not public))
        else:
            await interaction.edit_original_response(content=None, embed=embed, view=view)

    async def show_store_for_category(self, interaction: discord.Interaction, category_id: str):
        data = _load()
        cat = next((c for c in data["categories"] if c["id"] == category_id), None)
        if not cat:
            return await interaction.response.send_message("❌ Category not found.", ephemeral=True)

        cards = [
            c for c in data.get("cards", [])
            if c.get("category_id") == category_id and _is_enabled(c)
        ]
        if not cards:
            return await interaction.response.edit_message(content="ℹ️ No cards in this category yet.", view=None, embed=None)

        blocks = []
        for c in cards:
            blocks.append(f"**{c['title']}**\n*{c['description']}*\n{_fmt_prices(c['prices'])}\nID: `{c['id']}`\n")

        pages, cur = [], ""
        for b in blocks:
            if len(cur) + len(b) > 3500:
                pages.append(cur)
                cur = ""
            cur += b + "\n"
        if cur:
            pages.append(cur)

        embed = discord.Embed(title=f"Store: {cat['title']}", description=pages[0])
        embed.set_footer(text=f"Page 1/{len(pages)}")
        view = PagerView(pages, interaction.user.id, title=f"Store: {cat['title']}")
        await interaction.response.edit_message(content=None, embed=embed, view=view)

    async def buy_card(self, interaction: discord.Interaction, card_id: str, rarity: str):
        data = _load()

        async def _msg(text: str):
            # button clicks are deferred in BuyView; edit that deferred message
            try:
                await interaction.edit_original_response(content=text, embed=None, view=None)
            except Exception:
                try:
                    await interaction.followup.send(text, ephemeral=True)
                except Exception:
                    pass

        card = next((c for c in data.get("cards", []) if c.get("id") == card_id), None)
        if not card:
            await _msg("❌ Invalid card ID.")
            return

        # IMPORTANT: do NOT allow purchasing disabled cards
        if not _is_enabled(card):
            await _msg("❌ This card is currently disabled and cannot be purchased.")
            return

        rkey = _rarity_key(rarity)
        prices = card.get("prices", {}) or {}
        if rkey not in prices:
            await _msg("❌ Invalid rarity.")
            return

        price = int(prices.get(rkey, 0) or 0)
        if price <= 0:
            await _msg("❌ This card has an invalid price.")
            return

        bal = get_balance(interaction.user.id)
        if bal < price:
            await _msg("❌ You don't have enough coins.")
            return

        if not remove_balance(interaction.user.id, price):
            await _msg("❌ You don't have enough coins.")
            return

        ts = int(datetime.utcnow().timestamp())
        emoji = _emoji_for_rarity(rarity)

        inv = data.setdefault("inventory", {}).setdefault(str(interaction.user.id), [])
        inv.append({"card_id": card_id, "rarity": rarity, "paid": price, "when": ts})
        _save(data)

        await _msg(f"✅ Purchased {emoji} **{rarity}** **{card['title']}** for **{_abbr(price)}** coins.")

        await _cardlog(
            interaction.guild,
            "🧾 **CARD PURCHASE**\n"
            f"Buyer: {interaction.user.mention} (`{interaction.user.id}`)\n"
            f"Card: **{card['title']}** (`{card_id}`)\n"
            f"Rarity: {emoji} **{rarity}**\n"
            f"Price: **{_abbr(price)}** coins\n"
            f"When: <t:{ts}:F> (<t:{ts}:R>)"
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(CardsCog(bot))