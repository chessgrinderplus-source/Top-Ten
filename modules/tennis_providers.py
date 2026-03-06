# modules/tennis_providers.py
from __future__ import annotations

import asyncio
import csv
import io
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import aiohttp

import config
from utils import ensure_dir, load_json, save_json


# ---------------- Errors ----------------
class ProviderError(Exception):
    pass


class ProviderHTTPError(ProviderError):
    pass


class ProviderDataError(ProviderError):
    pass


# ---------------- Small utilities ----------------
def _now() -> int:
    return int(time.time())


def _data_dir() -> str:
    return getattr(config, "DATA_DIR", "./data")


def _tennis_cache_dir() -> str:
    return os.path.join(_data_dir(), "tennis_cache")


def _safe_filename(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", name.strip())


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _parse_int(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        s = str(x).strip()
        if s == "":
            return default
        return int(float(s))
    except Exception:
        return default


def _parse_date_yyyymmdd(s: str) -> Optional[datetime]:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y%m%d")
    except Exception:
        return None


def _guess_years_window() -> List[int]:
    # Keep this reasonable: career-ish window without downloading 30+ years by default.
    # You can widen by setting TENNIS_YEARS_BACK in config (e.g., 25).
    years_back = int(getattr(config, "TENNIS_YEARS_BACK", 15))
    this_year = datetime.utcnow().year
    start = max(1991, this_year - years_back)
    # Sackmann data often lags the current calendar year; include this_year and last year.
    return list(range(start, this_year + 1))


# ---------------- HTTP + disk cache ----------------
@dataclass
class _CacheEntry:
    path: str
    fetched_at: int


async def _http_get_text(url: str, timeout: int = 20) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; TennisBot/1.0; +https://discord.com)",
        "Accept": "text/plain,text/csv,*/*",
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(url, timeout=timeout) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise ProviderHTTPError(f"GET {url} -> {resp.status}: {text[:200]}")
            return text


async def _get_cached_text(url: str, ttl: int) -> str:
    """
    Cache remote text to disk (DATA_DIR/tennis_cache) + small JSON index for timestamps.
    """
    ensure_dir(_tennis_cache_dir())
    index_path = os.path.join(_tennis_cache_dir(), "_index.json")
    index = load_json(index_path, {})

    key = _safe_filename(url)
    path = os.path.join(_tennis_cache_dir(), key + ".txt")

    fetched_at = int(index.get(url, 0) or 0)
    if os.path.exists(path) and fetched_at and (_now() - fetched_at) < ttl:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()

    text = await _http_get_text(url)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

    index[url] = _now()
    save_json(index_path, index)
    return text


def _read_csv_dicts(text: str) -> List[Dict[str, str]]:
    f = io.StringIO(text)
    reader = csv.DictReader(f)
    return [dict(r) for r in reader]


# ---------------- Provider ----------------
class TennisProvider:
    source_name = "JeffSackmann (GitHub raw CSV)"

    async def get_rankings(self, tour: str, kind: str, limit: int) -> List[Dict[str, Any]]:
        raise NotImplementedError

    async def search_player(self, name: str, tour: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    async def get_player(self, player_id: int, tour: str, kind: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    async def get_player_stats(self, player_id: int, tour: str, season: Optional[int] = None) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    async def get_h2h(self, p1_id: int, p2_id: int, tour: str, limit_matches: int = 10) -> Optional[Dict[str, Any]]:
        raise NotImplementedError


class SackmannProvider(TennisProvider):
    """
    Uses:
      ATP:
        https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_players.csv
        https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_rankings_current.csv
        https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master/atp_matches_YYYY.csv
      WTA:
        https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_players.csv
        https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_rankings_current.csv
        https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master/wta_matches_YYYY.csv
    """

    def __init__(self) -> None:
        self._players_cache: Dict[str, Dict[int, Dict[str, Any]]] = {"ATP": {}, "WTA": {}}
        self._rankings_cache: Dict[str, Dict[int, Dict[str, Any]]] = {"ATP": {}, "WTA": {}}
        self._loaded_players: Dict[str, bool] = {"ATP": False, "WTA": False}
        self._loaded_rankings: Dict[str, bool] = {"ATP": False, "WTA": False}

    def _base(self, tour: str) -> str:
        t = (tour or "").strip().upper()
        if t == "WTA":
            return "https://raw.githubusercontent.com/JeffSackmann/tennis_wta/master"
        return "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master"

    def _players_url(self, tour: str) -> str:
        t = (tour or "").strip().upper()
        return f"{self._base(t)}/{'wta' if t=='WTA' else 'atp'}_players.csv"

    def _rankings_url(self, tour: str) -> str:
        t = (tour or "").strip().upper()
        return f"{self._base(t)}/{'wta' if t=='WTA' else 'atp'}_rankings_current.csv"

    def _matches_url(self, tour: str, year: int) -> str:
        t = (tour or "").strip().upper()
        return f"{self._base(t)}/{'wta' if t=='WTA' else 'atp'}_matches_{int(year)}.csv"

    async def _load_players(self, tour: str) -> None:
        t = (tour or "").strip().upper()
        if t not in ("ATP", "WTA"):
            t = "ATP"
        if self._loaded_players.get(t):
            return

        url = self._players_url(t)
        text = await _get_cached_text(url, ttl=60 * 60 * 24)  # 24h
        rows = _read_csv_dicts(text)

        out: Dict[int, Dict[str, Any]] = {}
        for r in rows:
            pid = _parse_int(r.get("player_id"))
            if not pid:
                continue
            first = (r.get("name_first") or r.get("first_name") or "").strip()
            last = (r.get("name_last") or r.get("last_name") or "").strip()
            name = (r.get("name_full") or "").strip() or (" ".join([first, last]).strip())
            out[pid] = {
                "player_id": pid,
                "first": first,
                "last": last,
                "name": name,
                "hand": (r.get("hand") or "").strip(),
                "dob": (r.get("dob") or r.get("birth_date") or "").strip(),
                "ioc": (r.get("ioc") or r.get("country_code") or "").strip(),
            }

        self._players_cache[t] = out
        self._loaded_players[t] = True

    async def _load_rankings(self, tour: str) -> None:
        t = (tour or "").strip().upper()
        if t not in ("ATP", "WTA"):
            t = "ATP"
        if self._loaded_rankings.get(t):
            return

        url = self._rankings_url(t)
        text = await _get_cached_text(url, ttl=60 * 60)  # 1h
        rows = _read_csv_dicts(text)

        out: Dict[int, Dict[str, Any]] = {}
        for r in rows:
            pid = _parse_int(r.get("player_id"))
            rk = _parse_int(r.get("ranking"))
            pts = _parse_int(r.get("ranking_points"))
            if pid and rk:
                out[pid] = {"rank": rk, "points": pts}

        self._rankings_cache[t] = out
        self._loaded_rankings[t] = True

    async def get_rankings(self, tour: str, kind: str, limit: int) -> List[Dict[str, Any]]:
        t = (tour or "").strip().upper()
        k = (kind or "singles").strip().lower()
        if k != "singles":
            # Sackmann has doubles rankings in other files, but we’re keeping this simple.
            raise ProviderDataError("Only singles rankings are supported in the free dataset provider.")

        await self._load_players(t)
        await self._load_rankings(t)

        players = self._players_cache[t]
        ranks = self._rankings_cache[t]

        # ranks: pid -> {rank, points}
        items = []
        for pid, rp in ranks.items():
            p = players.get(pid)
            if not p:
                continue
            items.append(
                {
                    "rank": int(rp["rank"]),
                    "name": p["name"],
                    "country": p.get("ioc", ""),
                    "points": int(rp.get("points", 0)),
                    "movement": 0,  # not available in current file
                    "player_id": pid,
                }
            )

        items.sort(key=lambda x: x["rank"])
        return items[: max(1, min(int(limit), 200))]

    async def search_player(self, name: str, tour: str) -> Optional[Dict[str, Any]]:
        t = (tour or "").strip().upper()
        await self._load_players(t)
        await self._load_rankings(t)

        q = _norm(name)
        if not q:
            return None

        players = self._players_cache[t]
        ranks = self._rankings_cache[t]

        best: Tuple[int, int, int] | None = None  # (score, has_rank, rank)
        best_pid: Optional[int] = None

        for pid, p in players.items():
            full = _norm(p.get("name", ""))
            if not full:
                continue

            # scoring:
            # 0 = perfect, lower is better
            if full == q:
                score = 0
            elif full.startswith(q):
                score = 1
            elif q in full:
                score = 2
            else:
                continue

            rp = ranks.get(pid)
            has_rank = 1 if rp else 0
            rnk = int(rp["rank"]) if rp else 9999

            cand = (score, -has_rank, rnk)
            if best is None or cand < best:
                best = cand
                best_pid = pid

        if best_pid is None:
            return None

        p = players[best_pid]
        return {"player_id": best_pid, "name": p.get("name", ""), "tour": t}

    async def get_player(self, player_id: int, tour: str, kind: str) -> Optional[Dict[str, Any]]:
        t = (tour or "").strip().upper()
        await self._load_players(t)
        await self._load_rankings(t)

        pid = int(player_id)
        p = self._players_cache[t].get(pid)
        if not p:
            return None

        rp = self._rankings_cache[t].get(pid, {})
        dob = p.get("dob") or ""
        age = None
        d = _parse_date_yyyymmdd(dob)
        if d:
            today = datetime.utcnow().date()
            age = today.year - d.date().year - ((today.month, today.day) < (d.date().month, d.date().day))

        return {
            "player_id": pid,
            "name": p.get("name", ""),
            "country": p.get("ioc", ""),
            "hand": p.get("hand", ""),
            "age": age,
            "rank": rp.get("rank"),
            "points": rp.get("points"),
            "wl": None,  # filled by get_player_stats
        }

    async def _load_matches_year(self, tour: str, year: int) -> List[Dict[str, str]]:
        t = (tour or "").strip().upper()
        url = self._matches_url(t, year)
        # match files are stable; cache longer
        text = await _get_cached_text(url, ttl=60 * 60 * 24 * 7)
        return _read_csv_dicts(text)

    async def get_player_stats(self, player_id: int, tour: str, season: Optional[int] = None) -> Optional[Dict[str, Any]]:
        t = (tour or "").strip().upper()
        await self._load_players(t)
        pid = int(player_id)

        years = [int(season)] if season else _guess_years_window()

        wins = 0
        losses = 0
        by_surface = {"Hard": [0, 0], "Clay": [0, 0], "Grass": [0, 0], "Carpet": [0, 0], "Unknown": [0, 0]}

        # recent matches list (for context)
        recent: List[Tuple[str, str, str, str]] = []  # (date, event, round, vs + result)

        for y in reversed(years):
            try:
                rows = await self._load_matches_year(t, y)
            except ProviderHTTPError:
                # year file might not exist (future or missing) — just skip
                continue

            for r in rows:
                w_id = _parse_int(r.get("winner_id"))
                l_id = _parse_int(r.get("loser_id"))
                if w_id != pid and l_id != pid:
                    continue

                surf = (r.get("surface") or "").strip() or "Unknown"
                if surf not in by_surface:
                    surf = "Unknown"

                if w_id == pid:
                    wins += 1
                    by_surface[surf][0] += 1
                    result = "W"
                    opp_id = l_id
                else:
                    losses += 1
                    by_surface[surf][1] += 1
                    result = "L"
                    opp_id = w_id

                if len(recent) < 10:
                    date = (r.get("tourney_date") or "").strip()
                    opp = self._players_cache[t].get(opp_id, {}).get("name", f"#{opp_id}")
                    event = (r.get("tourney_name") or "").strip()
                    rnd = (r.get("round") or "").strip()
                    score = (r.get("score") or "").strip()
                    recent.append((date, event, rnd, f"{result} vs {opp} ({score})"))

        p = await self.get_player(pid, t, "singles")
        if not p:
            return None

        # format surfaces
        surf_lines = {}
        for s, (w, l) in by_surface.items():
            if w == 0 and l == 0:
                continue
            surf_lines[s] = f"{w}-{l}"

        return {
            "name": p.get("name", "Player"),
            "player_id": pid,
            "tour": t,
            "season": season,
            "stats": {
                "Matches (W-L)": f"{wins}-{losses}",
                "Win %": (f"{(wins / (wins + losses) * 100):.1f}%" if (wins + losses) else "0.0%"),
                "By surface": ", ".join([f"{s}: {wl}" for s, wl in surf_lines.items()]) if surf_lines else "No matches found",
            },
            "recent": recent,
        }

    async def get_h2h(self, p1_id: int, p2_id: int, tour: str, limit_matches: int = 10) -> Optional[Dict[str, Any]]:
        t = (tour or "").strip().upper()
        await self._load_players(t)

        p1 = int(p1_id)
        p2 = int(p2_id)

        years = _guess_years_window()

        w1 = 0
        w2 = 0
        matches: List[Dict[str, Any]] = []

        for y in reversed(years):
            try:
                rows = await self._load_matches_year(t, y)
            except ProviderHTTPError:
                continue

            for r in rows:
                w_id = _parse_int(r.get("winner_id"))
                l_id = _parse_int(r.get("loser_id"))
                if {w_id, l_id} != {p1, p2}:
                    continue

                if w_id == p1:
                    w1 += 1
                    winner = p1
                else:
                    w2 += 1
                    winner = p2

                if len(matches) < max(1, int(limit_matches)):
                    matches.append(
                        {
                            "date": (r.get("tourney_date") or "").strip(),
                            "event": (r.get("tourney_name") or "").strip(),
                            "round": (r.get("round") or "").strip(),
                            "surface": (r.get("surface") or "").strip(),
                            "score": (r.get("score") or "").strip(),
                            "winner_id": winner,
                        }
                    )

        p1_name = self._players_cache[t].get(p1, {}).get("name", f"#{p1}")
        p2_name = self._players_cache[t].get(p2, {}).get("name", f"#{p2}")

        return {
            "tour": t,
            "p1_id": p1,
            "p2_id": p2,
            "p1_name": p1_name,
            "p2_name": p2_name,
            "record": f"{p1_name} leads {w1}-{w2}" if w1 >= w2 else f"{p2_name} leads {w2}-{w1}",
            "p1_wins": w1,
            "p2_wins": w2,
            "recent": matches,
        }


def get_provider() -> TennisProvider:
    # You can later add config switch here (e.g. TENNIS_PROVIDER="sackmann")
    return SackmannProvider()