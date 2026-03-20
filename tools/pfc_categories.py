# tools/pfc_categories.py
# ─────────────────────────────────────────────────────────────────────────────
# Perfect Fit Challenge — Category Registry
#
# REQUIRES: pip install beautifulsoup4
#
# Every category has a fetch_fn(player) that auto-fetches from Wikipedia.
# Returns None if parsing fails — the generator then prompts for manual entry.
# Wikipedia HTML is cached per player slug (one fetch per player per run).
# ─────────────────────────────────────────────────────────────────────────────

import re
import requests
from bs4 import BeautifulSoup, Tag

# ─────────────────────────────────────────────────────────────────────────────
# WIKIPEDIA FETCH + CACHE
# ─────────────────────────────────────────────────────────────────────────────

_session = requests.Session()
_session.headers.update({"User-Agent": "TennisBotPFC/1.0 (tennis Discord bot)"})

_wiki_cache: dict[str, BeautifulSoup | None] = {}


def _get_soup(slug: str) -> BeautifulSoup | None:
    if not slug:
        return None
    if slug in _wiki_cache:
        return _wiki_cache[slug]
    try:
        r = _session.get(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action": "parse",
                "page": slug,
                "prop": "text",
                "format": "json",
                "formatversion": "2",
            },
            timeout=20,
        )
        r.raise_for_status()
        html  = r.json()["parse"]["text"]
        soup  = BeautifulSoup(html, "html.parser")
        _wiki_cache[slug] = soup
    except Exception:
        _wiki_cache[slug] = None
    return _wiki_cache[slug]


# ─────────────────────────────────────────────────────────────────────────────
# PARSING UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def _cell_text(tag) -> str:
    if tag is None:
        return ""
    for unwanted in tag.find_all(["sup", "span"], class_=lambda c: c and "reference" in c):
        unwanted.decompose()
    return tag.get_text(separator=" ", strip=True)


def _parse_wl(s: str) -> tuple[int, int] | None:
    m = re.search(r'(\d+)\s*[–\-/]\s*(\d+)', s)
    return (int(m.group(1)), int(m.group(2))) if m else None


def _parse_int(s: str) -> int | None:
    s = re.sub(r'[^\d]', '', s.split(".")[0])
    return int(s) if s else None


def _parse_money(s: str) -> float | None:
    s = re.sub(r'[^\d.]', '', s)
    if not s:
        return None
    val = float(s)
    if val > 1_000_000:
        val = round(val / 1_000_000, 2)
    return val


def _win_pct(w: int, l: int) -> float:
    return round(w / (w + l) * 100, 1) if (w + l) > 0 else 0.0


def _wikitables(soup: BeautifulSoup):
    return soup.find_all("table", class_=lambda c: c and "wikitable" in c.split())


def _table_headers(table: Tag) -> list[str]:
    rows = table.find_all("tr")
    return [_cell_text(th).lower() for th in rows[0].find_all(["th", "td"])] if rows else []


# ─────────────────────────────────────────────────────────────────────────────
# INFOBOX PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _infobox_field(soup: BeautifulSoup, *labels: str) -> str | None:
    infobox = soup.find("table", class_=lambda c: c and "infobox" in c.split())
    if not infobox:
        return None
    for row in infobox.find_all("tr"):
        th = row.find("th")
        td = row.find("td")
        if not (th and td):
            continue
        th_text = _cell_text(th).lower()
        for label in labels:
            if label.lower() in th_text:
                return _cell_text(td)
    return None


def _infobox_int(soup: BeautifulSoup, *labels: str) -> int | None:
    val = _infobox_field(soup, *labels)
    return _parse_int(val) if val else None


# ─────────────────────────────────────────────────────────────────────────────
# YEAR-BY-YEAR TABLE PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_year_table(soup: BeautifulSoup) -> list[dict] | None:
    for table in _wikitables(soup):
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue
        headers = _table_headers(table)
        if not any("year" in h for h in headers):
            continue

        col: dict[str, int] = {}
        for i, h in enumerate(headers):
            if "year" in h and "year" not in col:       col["year"] = i
            if re.search(r'^w$|^wins$', h):             col.setdefault("w", i)
            if re.search(r'^l$|^losses$', h):           col.setdefault("l", i)
            if re.search(r'w.?l|w.l|win.?loss', h):    col["wl"] = i
            if "title" in h:                            col["titles"] = i
            if "prize" in h or "earning" in h:         col["prize"] = i
            if "rank" in h and "end" in h:             col.setdefault("yr_rank", i)
            if h == "rank":                             col.setdefault("yr_rank", i)
            if "top" in h and "10" in h:               col["top10"] = i

        results = []
        for row in rows[1:]:
            cells = row.find_all(["th", "td"])
            if not cells:
                continue
            year_raw = _cell_text(cells[col["year"]]) if "year" in col and col["year"] < len(cells) else ""
            year_m   = re.search(r'(19|20)\d{2}', year_raw)
            if year_m:
                year_val: int | str = int(year_m.group())
            elif re.search(r'career|total', year_raw, re.I):
                year_val = "career"
            else:
                continue

            row_data: dict = {"year": year_val}

            def _get(key: str):
                return _cell_text(cells[col[key]]) if key in col and col[key] < len(cells) else None

            if "wl" in col:
                wl = _parse_wl(_get("wl") or "")
                if wl: row_data["w"], row_data["l"] = wl
            else:
                w = _parse_int(_get("w") or "")
                l = _parse_int(_get("l") or "")
                if w is not None: row_data["w"] = w
                if l is not None: row_data["l"] = l

            for key in ("titles", "yr_rank", "top10"):
                raw = _get(key)
                if raw:
                    v = _parse_int(raw)
                    if v is not None: row_data[key] = v

            raw_prize = _get("prize")
            if raw_prize:
                v = _parse_money(raw_prize)
                if v: row_data["prize"] = v

            results.append(row_data)

        if results:
            return results
    return None


def _career_row(soup: BeautifulSoup) -> dict | None:
    rows = _parse_year_table(soup)
    return next((r for r in rows if r.get("year") == "career"), None) if rows else None


def _season_row(soup: BeautifulSoup, year: int) -> dict | None:
    rows = _parse_year_table(soup)
    return next((r for r in rows if r.get("year") == year), None) if rows else None


def _count_yr_end_top_n(soup: BeautifulSoup, n: int) -> int | None:
    rows = _parse_year_table(soup)
    if not rows:
        return None
    count, found = 0, False
    for r in rows:
        rank = r.get("yr_rank")
        if rank is not None:
            found = True
            if rank <= n:
                count += 1
    return count if found else None


# ─────────────────────────────────────────────────────────────────────────────
# SURFACE TABLE PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_surface_table(soup: BeautifulSoup) -> dict[str, tuple[int, int]] | None:
    target = {"hard", "clay", "grass", "carpet", "overall",
              "indoor", "outdoor", "indoor hard", "outdoor hard"}
    for table in _wikitables(soup):
        data: dict[str, tuple[int, int]] = {}
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if not cells:
                continue
            first = _cell_text(cells[0]).lower().strip()
            if first not in target:
                continue
            for cell in cells[1:]:
                wl = _parse_wl(_cell_text(cell))
                if wl and wl[0] + wl[1] > 0:
                    data[first] = wl
                    break
        if len(data) >= 2:
            return data
    return None


# ─────────────────────────────────────────────────────────────────────────────
# GRAND SLAM TIMELINE PARSER
# ─────────────────────────────────────────────────────────────────────────────

_SLAM_ALIASES = {
    "australian open": "ao",
    "roland garros":   "rg",
    "french open":     "rg",
    "wimbledon":       "wimbledon",
    "us open":         "uso",
}


def _parse_slam_timeline(soup: BeautifulSoup) -> dict[str, dict] | None:
    for table in _wikitables(soup):
        rows = table.find_all("tr")
        if len(rows) < 3:
            continue

        slam_rows: dict[str, list] = {}
        for row in rows:
            cells = row.find_all(["th", "td"])
            if not cells:
                continue
            first = _cell_text(cells[0]).lower()
            for alias, key in _SLAM_ALIASES.items():
                if alias in first:
                    slam_rows[key] = cells
                    break

        if len(slam_rows) < 2:
            continue

        headers = [_cell_text(th) for th in rows[0].find_all(["th", "td"])]
        wl_col  = next((i for i, h in enumerate(headers) if re.search(r'w.?l|w.l', h, re.I)), None)

        results: dict[str, dict] = {}
        for slam_key, cells in slam_rows.items():
            d: dict = {}
            if wl_col and wl_col < len(cells):
                wl = _parse_wl(_cell_text(cells[wl_col]))
                if wl: d["wins"], d["losses"] = wl
            if "wins" not in d:
                for cell in reversed(cells[-4:]):
                    wl = _parse_wl(_cell_text(cell))
                    if wl and wl[0] + wl[1] > 3:
                        d["wins"], d["losses"] = wl
                        break
            titles = finals = 0
            for cell in cells[1:]:
                t = _cell_text(cell).strip()
                if t == "W":   titles += 1; finals += 1
                elif t == "F": finals += 1
            if titles > 0 or finals > 0:
                d["titles"]        = titles
                d["finals_apps"]   = finals
                d["finals_wins"]   = titles
                d["finals_losses"] = finals - titles
            results[slam_key] = d

        return results if results else None
    return None


# ─────────────────────────────────────────────────────────────────────────────
# GENERIC TOURNAMENT RECORD PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_tournament_record(soup: BeautifulSoup, tournament_name: str) -> dict | None:
    name_lower = tournament_name.lower()
    for table in _wikitables(soup):
        rows    = table.find_all("tr")
        headers = _table_headers(table)
        wl_col     = next((i for i, h in enumerate(headers) if re.search(r'w.?l|w.l', h)), None)
        titles_col = next((i for i, h in enumerate(headers) if "title" in h), None)

        for row in rows[1:]:
            cells = row.find_all(["th", "td"])
            if not cells:
                continue
            if not any(name_lower in _cell_text(c).lower() for c in cells[:3]):
                continue

            d: dict = {}
            if wl_col and wl_col < len(cells):
                wl = _parse_wl(_cell_text(cells[wl_col]))
                if wl: d["wins"], d["losses"] = wl
            if "wins" not in d:
                for cell in cells:
                    wl = _parse_wl(_cell_text(cell))
                    if wl and wl[0] + wl[1] > 2:
                        d["wins"], d["losses"] = wl
                        break
            titles = finals = 0
            for cell in cells[1:]:
                t = _cell_text(cell).strip()
                if t == "W":   titles += 1; finals += 1
                elif t == "F": finals += 1
            if titles_col and titles_col < len(cells):
                v = _parse_int(_cell_text(cells[titles_col]))
                if v is not None: titles = v
            if titles > 0 or finals > 0:
                d["titles"]        = titles
                d["finals_apps"]   = finals
                d["finals_wins"]   = titles
                d["finals_losses"] = finals - titles
            if d:
                return d
    return None


# ─────────────────────────────────────────────────────────────────────────────
# TOP-10 WINS PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_top10_wins(soup: BeautifulSoup, tournament_name: str | None = None) -> int | None:
    for table in _wikitables(soup):
        rows    = table.find_all("tr")
        headers = _table_headers(table)
        top10_col = next((i for i, h in enumerate(headers) if "top" in h and "10" in h), None)
        if top10_col is None:
            continue
        target_lower = tournament_name.lower() if tournament_name else None
        for row in rows[1:]:
            cells = row.find_all(["th", "td"])
            if not cells or top10_col >= len(cells):
                continue
            first = _cell_text(cells[0]).lower()
            if target_lower:
                if target_lower not in first: continue
            else:
                if not re.search(r'career|total', first): continue
            return _parse_int(_cell_text(cells[top10_col]))
    return None


# ─────────────────────────────────────────────────────────────────────────────
# H2H PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_h2h(soup: BeautifulSoup, opponent_name: str) -> tuple[int, int] | None:
    last_name = opponent_name.lower().split()[-1]
    for table in _wikitables(soup):
        for row in table.find_all("tr"):
            cells    = row.find_all(["th", "td"])
            row_text = " ".join(_cell_text(c) for c in cells)
            if last_name not in row_text.lower():
                continue
            for cell in cells:
                wl = _parse_wl(_cell_text(cell))
                if wl: return wl
            wl = _parse_wl(row_text)
            if wl: return wl
    return None


# ─────────────────────────────────────────────────────────────────────────────
# WIN STREAK PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_win_streak(soup: BeautifulSoup, surface: str | None = None) -> int | None:
    text = soup.get_text()
    patterns = (
        [
            rf'(\d+)[- ]match winning streak on {surface}',
            rf'(\d+) consecutive.*?{surface}',
            rf'{surface}.*?(\d+)[- ]match winning streak',
        ] if surface else [
            r'(\d+)[- ]match winning streak',
            r'winning streak of (\d+)',
            r'(\d+) consecutive match',
        ]
    )
    vals = []
    for pat in patterns:
        for m in re.finditer(pat, text, re.IGNORECASE):
            v = _parse_int(m.group(1))
            if v and v > 5: vals.append(v)
    return max(vals) if vals else None


# ─────────────────────────────────────────────────────────────────────────────
# FETCH FUNCTION FACTORIES
# ─────────────────────────────────────────────────────────────────────────────

def _slug(player: dict) -> str:
    return player.get("wikipedia_slug", "")


def _fetch_infobox_int(*labels):
    def fn(player):
        soup = _get_soup(_slug(player))
        return _infobox_int(soup, *labels) if soup else None
    return fn


def _fetch_career_titles(player):
    soup = _get_soup(_slug(player))
    if not soup: return None
    v = _infobox_int(soup, "career titles", "singles titles")
    if v is not None: return v
    row = _career_row(soup)
    return row.get("titles") if row else None


def _fetch_career_prize(player):
    soup = _get_soup(_slug(player))
    if not soup: return None
    raw = _infobox_field(soup, "prize money", "career prize")
    if raw: return _parse_money(raw)
    row = _career_row(soup)
    return row.get("prize") if row else None


def _fetch_yr_end_top_n(n: int):
    def fn(player):
        soup = _get_soup(_slug(player))
        return _count_yr_end_top_n(soup, n) if soup else None
    return fn


def _fetch_weeks_no1(player):
    soup = _get_soup(_slug(player))
    return _infobox_int(soup, "weeks at world no. 1", "weeks at no. 1") if soup else None


def _fetch_year_end_no1(player):
    soup = _get_soup(_slug(player))
    if not soup: return None
    v = _infobox_int(soup, "year-end no. 1", "year end no. 1")
    return v if v is not None else _count_yr_end_top_n(soup, 1)


def _fetch_slam_titles(player):
    soup = _get_soup(_slug(player))
    return _infobox_int(soup, "grand slam singles titles", "grand slam titles") if soup else None


def _fetch_surface(surface_key: str, stat: str):
    def fn(player):
        soup = _get_soup(_slug(player))
        if not soup: return None
        table = _parse_surface_table(soup)
        if not table: return None
        wl = table.get(surface_key)
        if not wl: return None
        w, l = wl
        if stat == "wins":   return w
        if stat == "losses": return l
        if stat == "pct":    return _win_pct(w, l)
    return fn


def _fetch_slam(slam_key: str, stat: str):
    def fn(player):
        soup = _get_soup(_slug(player))
        if not soup: return None
        timeline = _parse_slam_timeline(soup)
        if not timeline or slam_key not in timeline: return None
        d = timeline[slam_key]
        if stat == "pct":
            w, l = d.get("wins", 0), d.get("losses", 0)
            return _win_pct(w, l) if (w + l) > 0 else None
        return d.get(stat)
    return fn


def _fetch_tournament(tournament_name: str, stat: str):
    def fn(player):
        soup = _get_soup(_slug(player))
        if not soup: return None
        d = _parse_tournament_record(soup, tournament_name)
        if not d: return None
        if stat == "pct":
            w, l = d.get("wins", 0), d.get("losses", 0)
            return _win_pct(w, l) if (w + l) > 0 else None
        return d.get(stat)
    return fn


def _fetch_top10(tournament_name: str | None = None):
    def fn(player):
        soup = _get_soup(_slug(player))
        return _parse_top10_wins(soup, tournament_name) if soup else None
    return fn


def _fetch_season(year: int, stat: str):
    def fn(player):
        soup = _get_soup(_slug(player))
        if not soup: return None
        row = _season_row(soup, year)
        if not row: return None
        if stat == "pct":
            w, l = row.get("w", 0), row.get("l", 0)
            return _win_pct(w, l) if (w + l) > 0 else None
        return row.get(stat)
    return fn


def _fetch_streak(surface: str | None = None):
    def fn(player):
        soup = _get_soup(_slug(player))
        return _parse_win_streak(soup, surface) if soup else None
    return fn


def _fetch_h2h(reference_name: str, stat: str):
    def fn(player):
        if player["name"].lower() == reference_name.lower(): return None
        soup = _get_soup(_slug(player))
        if not soup: return None
        wl = _parse_h2h(soup, reference_name)
        if not wl: return None
        w, l = wl
        if stat == "wins":   return w
        if stat == "losses": return l
        if stat == "pct":    return _win_pct(w, l)
        if stat == "net":    return w - l
    return fn


# ─────────────────────────────────────────────────────────────────────────────
# TOURNAMENT DEFINITIONS
# Each tuple: (display_name, id_key, wikipedia_search_name)
# ─────────────────────────────────────────────────────────────────────────────

GRAND_SLAMS = [
    ("Australian Open", "ao",        "Australian Open"),
    ("Roland Garros",   "rg",        "Roland Garros"),
    ("Wimbledon",       "wimbledon", "Wimbledon"),
    ("US Open",         "uso",       "US Open"),
]

MASTERS_1000 = [
    ("Indian Wells", "indian_wells", "Indian Wells"),
    ("Miami",        "miami",        "Miami"),
    ("Monte Carlo",  "monte_carlo",  "Monte Carlo"),
    ("Madrid",       "madrid",       "Madrid"),
    ("Rome",         "rome",         "Rome"),
    ("Canada",       "canada",       "Canada"),
    ("Cincinnati",   "cincinnati",   "Cincinnati"),
    ("Shanghai",     "shanghai",     "Shanghai"),
    ("Paris Bercy",  "paris_bercy",  "Paris"),
]

ATP_500 = [
    ("Dubai / Doha",   "dubai",       "Dubai"),
    ("Acapulco",       "acapulco",    "Acapulco"),
    ("Rotterdam",      "rotterdam",   "Rotterdam"),
    ("Rio de Janeiro", "rio",         "Rio de Janeiro"),
    ("Barcelona",      "barcelona",   "Barcelona"),
    ("Queen's Club",   "queens",      "Queen's Club"),
    ("Halle",          "halle",       "Halle"),
    ("Hamburg",        "hamburg",     "Hamburg"),
    ("Washington",     "washington",  "Washington"),
    ("Beijing",        "beijing",     "Beijing"),
    ("Tokyo",          "tokyo",       "Tokyo"),
    ("Vienna",         "vienna",      "Vienna"),
    ("Basel",          "basel",       "Basel"),
]

SPECIAL_EVENTS = [
    ("ATP Finals",         "atp_finals", "ATP Finals"),
    ("Olympics (Singles)", "olympics",   "Olympics"),
]

SURFACES = [
    ("Clay",         "clay"),
    ("Hard",         "hard"),
    ("Grass",        "grass"),
    ("Indoor Hard",  "indoor hard"),
    ("Outdoor Hard", "outdoor hard"),
]


# ─────────────────────────────────────────────────────────────────────────────
# CATEGORY BUILDER
# ─────────────────────────────────────────────────────────────────────────────

def _cat(id: str, display_name: str, group: str, fetch_fn) -> dict:
    return {"id": id, "display_name": display_name, "group": group,
            "source": "wikipedia", "fetch_fn": fetch_fn}


def _wl_family(tournaments):
    cats = []
    for display, key, wiki_name in tournaments:
        is_slam = key in ("ao", "rg", "wimbledon", "uso")
        fn_w = _fetch_slam(key, "wins")   if is_slam else _fetch_tournament(wiki_name, "wins")
        fn_l = _fetch_slam(key, "losses") if is_slam else _fetch_tournament(wiki_name, "losses")
        fn_p = _fetch_slam(key, "pct")    if is_slam else _fetch_tournament(wiki_name, "pct")
        cats += [
            _cat(f"wl_{key}_wins",   f"{display} — Career Wins",   "W/L Record", fn_w),
            _cat(f"wl_{key}_losses", f"{display} — Career Losses",  "W/L Record", fn_l),
            _cat(f"wl_{key}_pct",    f"{display} — Career Win %",   "W/L Record", fn_p),
        ]
    return cats


def _finals_family(tournaments):
    cats = []
    for display, key, wiki_name in tournaments:
        is_slam  = key in ("ao", "rg", "wimbledon", "uso")
        src      = _fetch_slam if is_slam else _fetch_tournament
        name_arg = key if is_slam else wiki_name
        cats += [
            _cat(f"finals_{key}_wins",   f"{display} — Finals Won",         "Finals Record", src(name_arg, "finals_wins")),
            _cat(f"finals_{key}_losses", f"{display} — Finals Lost",         "Finals Record", src(name_arg, "finals_losses")),
            _cat(f"finals_{key}_apps",   f"{display} — Finals Appearances",  "Finals Record", src(name_arg, "finals_apps")),
        ]
    return cats


def _titles_family(tournaments):
    cats = []
    for display, key, wiki_name in tournaments:
        is_slam = key in ("ao", "rg", "wimbledon", "uso")
        fn      = _fetch_slam(key, "titles") if is_slam else _fetch_tournament(wiki_name, "titles")
        cats.append(_cat(f"titles_{key}", f"{display} — Titles", "Titles", fn))
    return cats


def _top10_family(tournaments):
    return [
        _cat(f"top10_{key}", f"{display} — Top 10 Wins", "Top 10 Wins", _fetch_top10(wiki_name))
        for display, key, wiki_name in tournaments
    ]


def _surface_family():
    cats = []
    for display, key in SURFACES:
        cats += [
            _cat(f"surface_wins_{key}",   f"{display} — Career Wins",  "Surface W/L", _fetch_surface(key, "wins")),
            _cat(f"surface_losses_{key}", f"{display} — Career Losses", "Surface W/L", _fetch_surface(key, "losses")),
            _cat(f"surface_pct_{key}",    f"{display} — Career Win %",  "Surface W/L", _fetch_surface(key, "pct")),
        ]
    return cats


def _streak_family():
    cats = [_cat("streak_overall", "Longest Win Streak (Overall)", "Win Streaks", _fetch_streak())]
    for display, key in SURFACES:
        cats.append(_cat(f"streak_{key}", f"Longest Win Streak — {display}", "Win Streaks", _fetch_streak(key)))
    return cats


# ─────────────────────────────────────────────────────────────────────────────
# DYNAMIC BUILDERS (called at runtime in the generator)
# ─────────────────────────────────────────────────────────────────────────────

def build_h2h_categories(reference_name: str, reference_wikipedia_slug: str) -> list[dict]:
    safe_id = re.sub(r"[^a-z0-9]", "_", reference_name.lower())
    return [
        _cat(f"h2h_{safe_id}_wins", f"H2H vs {reference_name} — Wins",          "Head to Head", _fetch_h2h(reference_name, "wins")),
        _cat(f"h2h_{safe_id}_pct",  f"H2H vs {reference_name} — Win %",          "Head to Head", _fetch_h2h(reference_name, "pct")),
        _cat(f"h2h_{safe_id}_net",  f"H2H vs {reference_name} — Net Wins (W-L)", "Head to Head", _fetch_h2h(reference_name, "net")),
    ]


def build_season_categories(year: int) -> list[dict]:
    y = str(year)
    return [
        _cat(f"season_{y}_wins",    f"{y} Season — Wins",              f"Season {y}", _fetch_season(year, "w")),
        _cat(f"season_{y}_losses",  f"{y} Season — Losses",            f"Season {y}", _fetch_season(year, "l")),
        _cat(f"season_{y}_pct",     f"{y} Season — Win %",             f"Season {y}", _fetch_season(year, "pct")),
        _cat(f"season_{y}_titles",  f"{y} Season — Titles",            f"Season {y}", _fetch_season(year, "titles")),
        _cat(f"season_{y}_top10",   f"{y} Season — Top 10 Wins",       f"Season {y}", _fetch_season(year, "top10")),
        _cat(f"season_{y}_yr_rank", f"{y} Season — Year-End Ranking",  f"Season {y}", _fetch_season(year, "yr_rank")),
        _cat(f"season_{y}_prize",   f"{y} Season — Prize Money (USD)", f"Season {y}", _fetch_season(year, "prize")),
    ]


# ─────────────────────────────────────────────────────────────────────────────
# STATIC REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

def _build_registry() -> list[dict]:
    cats = [
        _cat("slam_singles",       "Grand Slam Singles Titles",         "Career Overview", _fetch_slam_titles),
        _cat("slam_doubles",       "Grand Slam Doubles Titles",         "Career Overview", _fetch_infobox_int("grand slam doubles")),
        _cat("slam_mixed",         "Grand Slam Mixed Doubles Titles",   "Career Overview", _fetch_infobox_int("grand slam mixed")),
        _cat("career_titles",      "Career Singles Titles",             "Career Overview", _fetch_career_titles),
        _cat("career_prize",       "Career Prize Money (USD millions)", "Career Overview", _fetch_career_prize),
        _cat("career_top10_wins",  "Career Top 10 Wins",                "Career Overview", _fetch_top10(None)),
        _cat("weeks_no1",          "Weeks at World No. 1",              "Career Overview", _fetch_weeks_no1),
        _cat("year_end_no1",       "Year-End No. 1 Finishes",           "Career Overview", _fetch_year_end_no1),
        _cat("yr_end_top10_count", "Times Finishing Year-End Top 10",   "Career Overview", _fetch_yr_end_top_n(10)),
        _cat("yr_end_top5_count",  "Times Finishing Year-End Top 5",    "Career Overview", _fetch_yr_end_top_n(5)),
        _cat("masters_titles",     "Masters 1000 Titles",               "Career Overview", _fetch_infobox_int("masters")),
        _cat("atp_finals_titles",  "ATP Finals Titles",                 "Career Overview", _fetch_tournament("ATP Finals", "titles")),
        _cat("olympic_gold",       "Olympic Gold Medals (Singles)",     "Career Overview", _fetch_infobox_int("olympic")),
        _cat("davis_cup_apps",     "Davis Cup Appearances",             "Career Overview", _fetch_infobox_int("davis cup")),
        _cat("davis_cup_wins",     "Davis Cup Wins (Team)",             "Career Overview", _fetch_infobox_int("davis cup win")),
    ]
    cats += _surface_family()
    cats += _wl_family(GRAND_SLAMS)
    cats += _finals_family(GRAND_SLAMS)
    cats += _titles_family(GRAND_SLAMS)
    cats += _top10_family(GRAND_SLAMS)
    cats += _wl_family(MASTERS_1000)
    cats += _finals_family(MASTERS_1000)
    cats += _titles_family(MASTERS_1000)
    cats += _top10_family(MASTERS_1000)
    cats += _wl_family(ATP_500)
    cats += _finals_family(ATP_500)
    cats += _titles_family(ATP_500)
    cats += _top10_family(ATP_500)
    cats += _wl_family(SPECIAL_EVENTS)
    cats += _finals_family(SPECIAL_EVENTS)
    cats += _titles_family(SPECIAL_EVENTS)
    cats += _top10_family(SPECIAL_EVENTS)
    cats += _streak_family()
    return cats


CATEGORIES: list[dict] = _build_registry()