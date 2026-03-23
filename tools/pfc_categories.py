# tools/pfc_categories.py
# ─────────────────────────────────────────────────────────────────────────────
# Perfect Fit Challenge — Category Registry
#
# REQUIRES: pip install beautifulsoup4
#
# TWO-LAYER FETCH STRATEGY:
#   Layer 1 — Wikitext API (raw template text):
#     Used for infobox fields. Parsing {{Infobox tennis biography}} directly
#     is far more reliable than scraping rendered HTML.
#     Fields like weeksno1, grandslamssingles, masterstitles, careerprize, etc.
#
#   Layer 2 — Parse API (rendered HTML via BeautifulSoup):
#     Used for all tables (career stats, surface breakdown, slam timeline,
#     tournament records, H2H, win streaks).
#
# Both are cached per slug — one HTTP request per layer per player per run.
# On any failure the fetch_fn returns None → generator prompts manual entry.
# ─────────────────────────────────────────────────────────────────────────────

import re
import requests
from bs4 import BeautifulSoup, Tag

# ─────────────────────────────────────────────────────────────────────────────
# HTTP SESSION + CACHES
# ─────────────────────────────────────────────────────────────────────────────

_session = requests.Session()
_session.headers.update({"User-Agent": "TennisBotPFC/1.0 (tennis Discord bot)"})

_wikitext_cache: dict[str, str | None]          = {}
_soup_cache:     dict[str, BeautifulSoup | None] = {}


def _get_wikitext(slug: str) -> str | None:
    """Fetch raw wikitext. Cached per slug."""
    if not slug:
        return None
    if slug in _wikitext_cache:
        return _wikitext_cache[slug]
    try:
        r = _session.get(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action": "query", "prop": "revisions",
                "rvprop": "content", "rvslots": "main",
                "titles": slug, "format": "json", "formatversion": "2",
            },
            timeout=20,
        )
        r.raise_for_status()
        pages = r.json()["query"]["pages"]
        _wikitext_cache[slug] = pages[0]["revisions"][0]["slots"]["main"]["content"]
    except Exception:
        _wikitext_cache[slug] = None
    return _wikitext_cache[slug]


def _get_soup(slug: str) -> BeautifulSoup | None:
    """Fetch rendered HTML as BeautifulSoup. Cached per slug."""
    if not slug:
        return None
    if slug in _soup_cache:
        return _soup_cache[slug]
    try:
        r = _session.get(
            "https://en.wikipedia.org/w/api.php",
            params={
                "action": "parse", "page": slug,
                "prop": "text", "format": "json", "formatversion": "2",
            },
            timeout=20,
        )
        r.raise_for_status()
        html = r.json()["parse"]["text"]
        _soup_cache[slug] = BeautifulSoup(html, "html.parser")
    except Exception:
        _soup_cache[slug] = None
    return _soup_cache[slug]


# ─────────────────────────────────────────────────────────────────────────────
# WIKITEXT INFOBOX PARSER
# ─────────────────────────────────────────────────────────────────────────────
# {{Infobox tennis biography}} field examples:
#   | weeksno1          = 428
#   | grandslamssingles = 24
#   | masterstitles     = 40
#   | yearendno1        = 8
#   | careerprize       = US$241,295,668
# ─────────────────────────────────────────────────────────────────────────────

def _wt_field(wikitext: str, *keys: str) -> str | None:
    """Extract a raw field value from a wikitext template."""
    for key in keys:
        pattern = rf'^\s*\|\s*{re.escape(key)}\s*=\s*(.+?)(?=\n\s*\||\n\s*\}}|\Z)'
        m = re.search(pattern, wikitext, re.IGNORECASE | re.MULTILINE | re.DOTALL)
        if m:
            raw = m.group(1).strip()
            # Strip wiki markup
            raw = re.sub(r'<ref[^>]*>.*?</ref>', '', raw, flags=re.DOTALL)
            raw = re.sub(r'<[^>]+>', '', raw)
            raw = re.sub(r'\[\[(?:[^|\]]+\|)?([^\]]+)\]\]', r'\1', raw)
            raw = re.sub(r'\{\{[^}]+\}\}', '', raw)
            raw = raw.strip()
            if raw:
                return raw
    return None


def _wt_int(wikitext: str, *keys: str) -> int | None:
    val = _wt_field(wikitext, *keys)
    if not val:
        return None
    digits = re.sub(r'[^\d]', '', val.split('.')[0])
    return int(digits) if digits else None


def _wt_money(wikitext: str, *keys: str) -> float | None:
    val = _wt_field(wikitext, *keys)
    if not val:
        return None
    digits = re.sub(r'[^\d.]', '', val)
    if not digits:
        return None
    v = float(digits)
    return round(v / 1_000_000, 2) if v > 1_000_000 else v


# ─────────────────────────────────────────────────────────────────────────────
# HTML TABLE UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def _clean(tag) -> str:
    """Get clean text, stripping refs/superscripts."""
    if tag is None:
        return ""
    for junk in tag.find_all(["sup", "span"],
            class_=lambda c: c and ("reference" in c or "noprint" in c)):
        junk.decompose()
    return tag.get_text(separator=" ", strip=True)


def _parse_wl(s: str) -> tuple[int, int] | None:
    m = re.search(r'(\d+)\s*[–\-/]\s*(\d+)', s)
    return (int(m.group(1)), int(m.group(2))) if m else None


def _to_int(s: str) -> int | None:
    d = re.sub(r'[^\d]', '', s.split('.')[0])
    return int(d) if d else None


def _to_money(s: str) -> float | None:
    d = re.sub(r'[^\d.]', '', s)
    if not d:
        return None
    v = float(d)
    return round(v / 1_000_000, 2) if v > 1_000_000 else v


def _win_pct(w: int, l: int) -> float:
    return round(w / (w + l) * 100, 1) if (w + l) > 0 else 0.0


def _wikitables(soup: BeautifulSoup) -> list[Tag]:
    return soup.find_all("table", class_=lambda c: c and "wikitable" in c.split())


# ─────────────────────────────────────────────────────────────────────────────
# CAREER YEAR-BY-YEAR TABLE
# ─────────────────────────────────────────────────────────────────────────────

def _parse_career_table(soup: BeautifulSoup) -> list[dict] | None:
    """
    Find the singles career statistics table (year rows + career total).
    Returns list of row dicts: year, w, l, pct, titles, prize, yr_rank, top10
    """
    for table in _wikitables(soup):
        rows = table.find_all("tr")
        if len(rows) < 4:
            continue

        header_cells = rows[0].find_all(["th", "td"])
        headers      = [_clean(c).lower() for c in header_cells]

        has_year = any("year" in h for h in headers)
        has_wl   = any(re.search(r'w[–\-]l|^w$|^wins?$', h) for h in headers)
        if not (has_year and has_wl):
            continue

        col: dict[str, int] = {}
        for i, h in enumerate(headers):
            if "year" in h and "year" not in col:                    col["year"] = i
            if re.fullmatch(r'w\.?|wins?', h):                      col.setdefault("w", i)
            if re.fullmatch(r'l\.?|losses?', h):                    col.setdefault("l", i)
            if re.search(r'w[–\-]l') and "wl" not in col:          col["wl"] = i
            if "title" in h and "titles" not in col:                 col["titles"] = i
            if re.search(r'prize|earn', h) and "prize" not in col:  col["prize"] = i
            if re.search(r'rank', h) and "rank" not in col:         col["rank"] = i
            if re.search(r'top.?10', h) and "top10" not in col:     col["top10"] = i

        # Re-check wl column using direct pattern
        for i, h in enumerate(headers):
            if re.search(r'w[–\-]l', h) and "wl" not in col:
                col["wl"] = i

        def _get(cells, key):
            idx = col.get(key, -1)
            return _clean(cells[idx]) if 0 <= idx < len(cells) else None

        results = []
        for row in rows[1:]:
            cells = row.find_all(["th", "td"])
            if not cells:
                continue
            year_raw = _get(cells, "year") or ""
            ym = re.search(r'(19|20)\d{2}', year_raw)
            if ym:
                year_val: int | str = int(ym.group())
            elif re.search(r'career|total', year_raw, re.I):
                year_val = "career"
            else:
                continue

            d: dict = {"year": year_val}

            if "wl" in col:
                wl = _parse_wl(_get(cells, "wl") or "")
                if wl:
                    d["w"], d["l"] = wl
            else:
                for k in ("w", "l"):
                    v = _to_int(_get(cells, k) or "")
                    if v is not None:
                        d[k] = v

            if "w" in d and "l" in d:
                d["pct"] = _win_pct(d["w"], d["l"])

            for src, dst in (("titles","titles"), ("rank","yr_rank"), ("top10","top10")):
                raw = _get(cells, src)
                if raw:
                    v = _to_int(raw)
                    if v is not None:
                        d[dst] = v

            raw_p = _get(cells, "prize")
            if raw_p:
                v = _to_money(raw_p)
                if v:
                    d["prize"] = v

            results.append(d)

        if len(results) >= 2:
            return results

    return None


def _career_row(soup: BeautifulSoup) -> dict | None:
    rows = _parse_career_table(soup)
    return next((r for r in rows if r.get("year") == "career"), None) if rows else None


def _season_row(soup: BeautifulSoup, year: int) -> dict | None:
    rows = _parse_career_table(soup)
    return next((r for r in rows if r.get("year") == year), None) if rows else None


def _count_yr_end_top_n(soup: BeautifulSoup, n: int) -> int | None:
    rows = _parse_career_table(soup)
    if not rows:
        return None
    count, found = 0, False
    for r in rows:
        rank = r.get("yr_rank")
        if isinstance(rank, int):
            found = True
            if rank <= n:
                count += 1
    return count if found else None


# ─────────────────────────────────────────────────────────────────────────────
# SURFACE BREAKDOWN TABLE
# ─────────────────────────────────────────────────────────────────────────────

_SURFACE_KEYS = {
    "hard", "clay", "grass", "carpet", "overall",
    "indoor", "outdoor", "indoor hard", "outdoor hard",
}

_SURFACE_NORM = {
    "hard court": "hard",
    "clay court": "clay",
    "grass court": "grass",
    "hard courts": "hard",
    "clay courts": "clay",
    "grass courts": "grass",
}


def _parse_surface_table(soup: BeautifulSoup) -> dict[str, tuple[int, int]] | None:
    for table in _wikitables(soup):
        data: dict[str, tuple[int, int]] = {}
        for row in table.find_all("tr"):
            cells = row.find_all(["th", "td"])
            if not cells:
                continue
            raw = _clean(cells[0]).lower().strip()
            raw = re.sub(r'\[.*?\]', '', raw).strip()
            key = _SURFACE_NORM.get(raw, raw)
            if key not in _SURFACE_KEYS:
                continue
            for cell in cells[1:]:
                wl = _parse_wl(_clean(cell))
                if wl and wl[0] + wl[1] > 0:
                    data[key] = wl
                    break
        if len(data) >= 2:
            return data
    return None


# ─────────────────────────────────────────────────────────────────────────────
# GRAND SLAM PERFORMANCE TIMELINE
# ─────────────────────────────────────────────────────────────────────────────

_SLAM_ALIASES = {
    "australian open": "ao",
    "french open":     "rg",
    "roland garros":   "rg",
    "wimbledon":       "wimbledon",
    "us open":         "uso",
}


def _parse_slam_timeline(soup: BeautifulSoup) -> dict[str, dict] | None:
    for table in _wikitables(soup):
        rows = table.find_all("tr")
        if len(rows) < 5:
            continue

        slam_rows: dict[str, list[Tag]] = {}
        for row in rows:
            cells = row.find_all(["th", "td"])
            if not cells:
                continue
            first = re.sub(r'\[.*?\]', '', _clean(cells[0]).lower()).strip()
            for alias, key in _SLAM_ALIASES.items():
                if alias in first:
                    slam_rows[key] = cells
                    break

        if len(slam_rows) < 2:
            continue

        # Find W-L column
        headers = [_clean(c).lower() for c in rows[0].find_all(["th", "td"])]
        wl_col  = next((i for i, h in enumerate(headers) if re.search(r'w[–\-]l', h)), None)

        results: dict[str, dict] = {}
        for slam_key, cells in slam_rows.items():
            d: dict = {}

            if wl_col and wl_col < len(cells):
                wl = _parse_wl(_clean(cells[wl_col]))
                if wl:
                    d["wins"], d["losses"] = wl

            # Fallback: scan last few cells
            if "wins" not in d:
                for cell in reversed(cells[-5:]):
                    wl = _parse_wl(_clean(cell))
                    if wl and (wl[0] + wl[1]) > 3:
                        d["wins"], d["losses"] = wl
                        break

            titles = finals_w = finals_l = 0
            for cell in cells[1:]:
                t = _clean(cell).strip().upper()
                if re.search(r'\d', t):   # skip cells with numbers (e.g. W-L summaries)
                    continue
                if t == "W":
                    titles   += 1
                    finals_w += 1
                elif t == "F":
                    finals_l += 1

            if titles > 0 or (finals_w + finals_l) > 0:
                d["titles"]        = titles
                d["finals_wins"]   = finals_w
                d["finals_losses"] = finals_l
                d["finals_apps"]   = finals_w + finals_l

            results[slam_key] = d

        if results:
            return results

    return None


# ─────────────────────────────────────────────────────────────────────────────
# GENERIC TOURNAMENT RECORD PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_tournament_record(soup: BeautifulSoup, tournament_name: str) -> dict | None:
    name_lower = tournament_name.lower()

    for table in _wikitables(soup):
        rows    = table.find_all("tr")
        if len(rows) < 3:
            continue

        headers  = [_clean(c).lower() for c in rows[0].find_all(["th", "td"])]
        wl_col     = next((i for i, h in enumerate(headers) if re.search(r'w[–\-]l', h)), None)
        titles_col = next((i for i, h in enumerate(headers) if "title" in h), None)

        for row in rows[1:]:
            cells = row.find_all(["th", "td"])
            if not cells:
                continue
            row_text = " ".join(_clean(c).lower() for c in cells[:3])
            if name_lower not in row_text:
                continue

            d: dict = {}

            if wl_col and wl_col < len(cells):
                wl = _parse_wl(_clean(cells[wl_col]))
                if wl:
                    d["wins"], d["losses"] = wl

            if "wins" not in d:
                for cell in cells[1:]:
                    wl = _parse_wl(_clean(cell))
                    if wl and (wl[0] + wl[1]) > 2:
                        d["wins"], d["losses"] = wl
                        break

            # Count W/F cells for timeline-style tables
            titles = finals_w = finals_l = 0
            for cell in cells[1:]:
                t = _clean(cell).strip().upper()
                if re.search(r'\d', t):
                    continue
                if t == "W":
                    titles += 1; finals_w += 1
                elif t == "F":
                    finals_l += 1

            if titles_col and titles_col < len(cells):
                v = _to_int(_clean(cells[titles_col]))
                if v is not None:
                    titles = v

            if titles > 0 or (finals_w + finals_l) > 0:
                d["titles"]        = titles
                d["finals_wins"]   = finals_w
                d["finals_losses"] = finals_l
                d["finals_apps"]   = finals_w + finals_l

            if d:
                return d

    return None


# ─────────────────────────────────────────────────────────────────────────────
# TOP-10 WINS PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_top10(soup: BeautifulSoup, tournament_name: str | None = None) -> int | None:
    for table in _wikitables(soup):
        rows    = table.find_all("tr")
        headers = [_clean(c).lower() for c in rows[0].find_all(["th","td"])] if rows else []
        top10_col = next((i for i,h in enumerate(headers) if re.search(r'top.?10', h)), None)
        if top10_col is None:
            continue
        target = tournament_name.lower() if tournament_name else None
        for row in rows[1:]:
            cells = row.find_all(["th", "td"])
            if not cells or top10_col >= len(cells):
                continue
            first = re.sub(r'\[.*?\]', '', _clean(cells[0]).lower()).strip()
            if target:
                if target not in first: continue
            else:
                if not re.search(r'career|total', first): continue
            v = _to_int(_clean(cells[top10_col]))
            if v is not None:
                return v
    return None


# ─────────────────────────────────────────────────────────────────────────────
# H2H PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_h2h(soup: BeautifulSoup, opponent_name: str) -> tuple[int, int] | None:
    name_parts = opponent_name.lower().split()
    last_name  = name_parts[-1]

    # Find H2H section first
    h2h_tables: list[Tag] = []
    for heading in soup.find_all(["h2", "h3"]):
        if "head" in heading.get_text().lower():
            for sib in heading.find_next_siblings():
                if sib.name in ("h2", "h3"): break
                if hasattr(sib, "find_all"):
                    for tbl in sib.find_all("table", class_=lambda c: c and "wikitable" in c.split()):
                        h2h_tables.append(tbl)

    search = h2h_tables or _wikitables(soup)

    for table in search:
        for row in table.find_all("tr"):
            cells    = row.find_all(["th", "td"])
            row_text = " ".join(_clean(c) for c in cells)
            if last_name not in row_text.lower():
                continue
            for cell in cells:
                wl = _parse_wl(_clean(cell))
                if wl: return wl
            wl = _parse_wl(row_text)
            if wl: return wl

    return None


# ─────────────────────────────────────────────────────────────────────────────
# WIN STREAK PARSER
# ─────────────────────────────────────────────────────────────────────────────

def _parse_win_streak(soup: BeautifulSoup, surface: str | None = None) -> int | None:
    text = soup.get_text(" ", strip=True)
    if surface:
        patterns = [
            rf'(\d+)[–\- ]match winning streak[^.]*{surface}',
            rf'{surface}[^.]*(\d+)[–\- ]match winning streak',
            rf'(\d+) consecutive[^.]*(?:win|match|victory)[^.]*{surface}',
        ]
    else:
        patterns = [
            r'(\d+)[–\- ]match winning streak',
            r'winning streak of (\d+)',
            r'won (\d+) consecutive',
            r'(\d+) consecutive (?:match wins?|wins?|matches)',
        ]
    vals = []
    for pat in patterns:
        for m in re.finditer(pat, text, re.IGNORECASE):
            v = _to_int(m.group(1))
            if v and 5 < v < 500:
                vals.append(v)
    return max(vals) if vals else None


# ─────────────────────────────────────────────────────────────────────────────
# FETCH FUNCTION FACTORIES
# ─────────────────────────────────────────────────────────────────────────────

def _slug(player: dict) -> str:
    return player.get("wikipedia_slug", "")


def _fetch_wt_int(*keys):
    def fn(player):
        wt = _get_wikitext(_slug(player))
        return _wt_int(wt, *keys) if wt else None
    return fn


def _fetch_slam_titles(player):
    wt = _get_wikitext(_slug(player))
    if not wt: return None
    return _wt_int(wt, "grandslamssingles", "grand slam singles titles",
                   "grandslam_singles", "gs_singles")


def _fetch_weeks_no1(player):
    wt = _get_wikitext(_slug(player))
    if not wt: return None
    return _wt_int(wt, "weeksno1", "weeks at world no. 1", "weeks_no1")


def _fetch_year_end_no1(player):
    wt = _get_wikitext(_slug(player))
    if wt:
        v = _wt_int(wt, "yearendno1", "year-end no. 1", "year_end_no1")
        if v is not None: return v
    soup = _get_soup(_slug(player))
    return _count_yr_end_top_n(soup, 1) if soup else None


def _fetch_career_titles(player):
    wt = _get_wikitext(_slug(player))
    if wt:
        v = _wt_int(wt, "careertitles", "career titles", "career_titles")
        if v is not None: return v
    soup = _get_soup(_slug(player))
    if not soup: return None
    row = _career_row(soup)
    return row.get("titles") if row else None


def _fetch_career_prize(player):
    wt = _get_wikitext(_slug(player))
    if wt:
        v = _wt_money(wt, "careerprize", "career prize", "prize money", "prize_money")
        if v is not None: return v
    soup = _get_soup(_slug(player))
    if not soup: return None
    row = _career_row(soup)
    return row.get("prize") if row else None


def _fetch_yr_end_top_n(n: int):
    def fn(player):
        soup = _get_soup(_slug(player))
        return _count_yr_end_top_n(soup, n) if soup else None
    return fn


def _fetch_surface(surface_key: str, stat: str):
    norm = {"indoor hard": "indoor hard", "outdoor hard": "outdoor hard"}.get(surface_key, surface_key)
    def fn(player):
        soup = _get_soup(_slug(player))
        if not soup: return None
        table = _parse_surface_table(soup)
        if not table: return None
        wl = table.get(norm)
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
        tl = _parse_slam_timeline(soup)
        if not tl or slam_key not in tl: return None
        d = tl[slam_key]
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
        return _parse_top10(soup, tournament_name) if soup else None
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
# CATEGORY BUILDER HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _cat(id, display_name, group, fetch_fn):
    return {"id": id, "display_name": display_name, "group": group,
            "source": "wikipedia", "fetch_fn": fetch_fn}


def _wl_family(t):
    cats = []
    for display, key, wiki in t:
        sl = key in ("ao","rg","wimbledon","uso")
        cats += [
            _cat(f"wl_{key}_wins",   f"{display} — Career Wins",  "W/L Record", _fetch_slam(key,"wins")   if sl else _fetch_tournament(wiki,"wins")),
            _cat(f"wl_{key}_losses", f"{display} — Career Losses", "W/L Record", _fetch_slam(key,"losses") if sl else _fetch_tournament(wiki,"losses")),
            _cat(f"wl_{key}_pct",    f"{display} — Career Win %",  "W/L Record", _fetch_slam(key,"pct")    if sl else _fetch_tournament(wiki,"pct")),
        ]
    return cats


def _finals_family(t):
    cats = []
    for display, key, wiki in t:
        sl = key in ("ao","rg","wimbledon","uso"); n = key if sl else wiki
        src = _fetch_slam if sl else _fetch_tournament
        cats += [
            _cat(f"finals_{key}_wins",   f"{display} — Finals Won",         "Finals Record", src(n,"finals_wins")),
            _cat(f"finals_{key}_losses", f"{display} — Finals Lost",         "Finals Record", src(n,"finals_losses")),
            _cat(f"finals_{key}_apps",   f"{display} — Finals Appearances",  "Finals Record", src(n,"finals_apps")),
        ]
    return cats


def _titles_family(t):
    return [
        _cat(f"titles_{key}", f"{display} — Titles", "Titles",
             _fetch_slam(key,"titles") if key in ("ao","rg","wimbledon","uso") else _fetch_tournament(wiki,"titles"))
        for display, key, wiki in t
    ]


def _top10_family(t):
    return [_cat(f"top10_{key}", f"{display} — Top 10 Wins", "Top 10 Wins", _fetch_top10(wiki))
            for display, key, wiki in t]


def _surface_family():
    cats = []
    for display, key in SURFACES:
        cats += [
            _cat(f"surface_wins_{key}",   f"{display} — Career Wins",  "Surface W/L", _fetch_surface(key,"wins")),
            _cat(f"surface_losses_{key}", f"{display} — Career Losses", "Surface W/L", _fetch_surface(key,"losses")),
            _cat(f"surface_pct_{key}",    f"{display} — Career Win %",  "Surface W/L", _fetch_surface(key,"pct")),
        ]
    return cats


def _streak_family():
    cats = [_cat("streak_overall", "Longest Win Streak (Overall)", "Win Streaks", _fetch_streak())]
    for display, key in SURFACES:
        cats.append(_cat(f"streak_{key}", f"Longest Win Streak — {display}", "Win Streaks", _fetch_streak(key)))
    return cats


# ─────────────────────────────────────────────────────────────────────────────
# DYNAMIC BUILDERS
# ─────────────────────────────────────────────────────────────────────────────

def build_h2h_categories(reference_name: str, reference_wikipedia_slug: str) -> list[dict]:
    safe_id = re.sub(r"[^a-z0-9]", "_", reference_name.lower())
    return [
        _cat(f"h2h_{safe_id}_wins", f"H2H vs {reference_name} — Wins",          "Head to Head", _fetch_h2h(reference_name,"wins")),
        _cat(f"h2h_{safe_id}_pct",  f"H2H vs {reference_name} — Win %",          "Head to Head", _fetch_h2h(reference_name,"pct")),
        _cat(f"h2h_{safe_id}_net",  f"H2H vs {reference_name} — Net Wins (W-L)", "Head to Head", _fetch_h2h(reference_name,"net")),
    ]


def build_season_categories(year: int) -> list[dict]:
    y = str(year)
    return [
        _cat(f"season_{y}_wins",    f"{y} Season — Wins",              f"Season {y}", _fetch_season(year,"w")),
        _cat(f"season_{y}_losses",  f"{y} Season — Losses",            f"Season {y}", _fetch_season(year,"l")),
        _cat(f"season_{y}_pct",     f"{y} Season — Win %",             f"Season {y}", _fetch_season(year,"pct")),
        _cat(f"season_{y}_titles",  f"{y} Season — Titles",            f"Season {y}", _fetch_season(year,"titles")),
        _cat(f"season_{y}_top10",   f"{y} Season — Top 10 Wins",       f"Season {y}", _fetch_season(year,"top10")),
        _cat(f"season_{y}_yr_rank", f"{y} Season — Year-End Ranking",  f"Season {y}", _fetch_season(year,"yr_rank")),
        _cat(f"season_{y}_prize",   f"{y} Season — Prize Money (USD)", f"Season {y}", _fetch_season(year,"prize")),
    ]


# ─────────────────────────────────────────────────────────────────────────────
# STATIC REGISTRY
# ─────────────────────────────────────────────────────────────────────────────

def _build_registry() -> list[dict]:
    cats = [
        _cat("slam_singles",       "Grand Slam Singles Titles",         "Career Overview", _fetch_slam_titles),
        _cat("slam_doubles",       "Grand Slam Doubles Titles",         "Career Overview", _fetch_wt_int("grandslams_doubles","grand slam doubles titles","grandslam_doubles")),
        _cat("slam_mixed",         "Grand Slam Mixed Doubles Titles",   "Career Overview", _fetch_wt_int("grandslams_mixed","grand slam mixed doubles titles")),
        _cat("career_titles",      "Career Singles Titles",             "Career Overview", _fetch_career_titles),
        _cat("career_prize",       "Career Prize Money (USD millions)", "Career Overview", _fetch_career_prize),
        _cat("career_top10_wins",  "Career Top 10 Wins",                "Career Overview", _fetch_top10(None)),
        _cat("weeks_no1",          "Weeks at World No. 1",              "Career Overview", _fetch_weeks_no1),
        _cat("year_end_no1",       "Year-End No. 1 Finishes",           "Career Overview", _fetch_year_end_no1),
        _cat("yr_end_top10_count", "Times Finishing Year-End Top 10",   "Career Overview", _fetch_yr_end_top_n(10)),
        _cat("yr_end_top5_count",  "Times Finishing Year-End Top 5",    "Career Overview", _fetch_yr_end_top_n(5)),
        _cat("masters_titles",     "Masters 1000 Titles",               "Career Overview", _fetch_wt_int("masterstitles","masters_titles","masters titles")),
        _cat("atp_finals_titles",  "ATP Finals Titles",                 "Career Overview", _fetch_wt_int("atpfinalstitles","atp_finals_titles","yearendchamp","wctitles")),
        _cat("olympic_gold",       "Olympic Gold Medals (Singles)",     "Career Overview", _fetch_wt_int("olympicgold","olympic gold","olympics_gold")),
        _cat("davis_cup_wins",     "Davis Cup Wins (Team)",             "Career Overview", _fetch_wt_int("daviscup","davis cup","davis_cup")),
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