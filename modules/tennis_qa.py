# modules/tennis_qa.py
from __future__ import annotations
from typing import Any

import config

# Optional OpenAI (only used if OPENAI_API_KEY exists)
try:
    from openai import OpenAI
except Exception:
    OpenAI = None


async def answer_question(question: str, provider) -> tuple[str, str]:
    q = (question or "").strip()
    if not q:
        return ("Ask me something like: “Show ATP top 20” or “Who’s live right now?”", f"Source: {provider.source_name}")

    # If no OpenAI key, do a simple deterministic router (still grounded)
    if not config.OPENAI_API_KEY or OpenAI is None:
        text = await _basic_router(q, provider)
        return (text, f"Source: {provider.source_name} (no GPT key set)")

    client = OpenAI(api_key=config.OPENAI_API_KEY)

    tools = [
        {
            "type": "function",
            "name": "get_rankings",
            "description": "Get current rankings for a tour and type.",
            "parameters": {
                "type": "object",
                "properties": {
                    "tour": {"type": "string", "description": "ATP or WTA"},
                    "kind": {"type": "string", "description": "singles or doubles"},
                    "limit": {"type": "integer", "description": "max results"},
                },
                "required": ["tour", "kind", "limit"],
            },
        },
        {
            "type": "function",
            "name": "get_live",
            "description": "Get live matches (optionally filtered).",
            "parameters": {
                "type": "object",
                "properties": {
                    "tour": {"type": ["string", "null"]},
                    "tournament": {"type": ["string", "null"]},
                },
                "required": ["tour", "tournament"],
            },
        },
        {
            "type": "function",
            "name": "get_match",
            "description": "Get a match by id or query.",
            "parameters": {
                "type": "object",
                "properties": {"query": {"type": "string"}},
                "required": ["query"],
            },
        },
        {
            "type": "function",
            "name": "get_player",
            "description": "Search and return quick player info.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "tour": {"type": "string"},
                },
                "required": ["name", "tour"],
            },
        },
    ]

    async def run_tool(name: str, args: dict[str, Any]) -> Any:
        if name == "get_rankings":
            return await provider.get_rankings(tour=args["tour"], kind=args["kind"], limit=int(args["limit"]))
        if name == "get_live":
            return await provider.get_live_matches(tour=args.get("tour"), tournament=args.get("tournament"))
        if name == "get_match":
            return await provider.get_match(args["query"])
        if name == "get_player":
            p = await provider.search_player(args["name"], tour=args["tour"])
            if not p:
                return None
            return await provider.get_player(p["player_id"], tour=args["tour"], kind="singles")
        return None

    resp = client.responses.create(
        model=config.OPENAI_MODEL,
        input=f"You are a tennis data assistant. Use tools for facts. If data is missing, say so.\n\nUser: {q}",
        tools=tools,
    )

    tool_calls = []
    for item in getattr(resp, "output", []) or []:
        if getattr(item, "type", None) == "function_call":
            tool_calls.append(item)

    if not tool_calls:
        text = getattr(resp, "output_text", None) or "I couldn’t generate an answer."
        return (text, f"Source: {provider.source_name} • GPT: {config.OPENAI_MODEL}")

    call = tool_calls[0]
    tool_name = getattr(call, "name", "")
    tool_args = getattr(call, "arguments", {}) or {}

    tool_result = await run_tool(tool_name, tool_args if isinstance(tool_args, dict) else {})

    resp2 = client.responses.create(
        model=config.OPENAI_MODEL,
        input=[
            {"role": "user", "content": q},
            {"role": "tool", "name": tool_name, "content": str(tool_result)},
        ],
    )
    text2 = getattr(resp2, "output_text", None) or "I couldn’t generate an answer from the tool result."
    return (text2, f"Source: {provider.source_name} • GPT: {config.OPENAI_MODEL}")


    async def _basic_router(q: str, provider) -> str:
        low = (q or "").strip().lower()

        # If the user is clearly asking for live
        if "live" in low or "score" in low:
            matches = await provider.get_live_matches(tour=None, tournament=None)
            if not matches:
                return "No live matches right now (this bot isn’t connected to a live-score provider yet)."
            out = ["**Live matches:**"]
            for m in matches[:10]:
                out.append(f"- {m.get('p1')} vs {m.get('p2')} — {m.get('score','')} (ID `{m.get('match_id','')}`)")
            return "\n".join(out)

        # Rankings shortcut
        if "rank" in low or "ranking" in low or "top" in low:
            tour = "ATP"
            if "wta" in low:
                tour = "WTA"
            rows = await provider.get_rankings(tour=tour, kind="singles", limit=20)
            if not rows:
                return "No rankings data found."
            return "\n".join([f"**Top {tour} (singles):**"] + [f"{r['rank']}. {r['name']}" for r in rows[:20]])

        # If it's a "real question", don’t try to search_player() using the whole sentence.
        # (No GPT key mode is meant to be tool-ish, not full QA.)
        looks_like_question = ("?" in q) or any(w in low for w in ["how", "what", "why", "when", "does", "do ", "is ", "are "])
        if looks_like_question:
            return (
                "I can answer **rankings/live/player lookups** without GPT.\n"
                "Try:\n"
                "- `ATP top 20`\n"
                "- `WTA top 20`\n"
                "- `live`\n"
                "- a player name like `Jannik Sinner`"
            )

        # Otherwise treat it as player name search
        try:
            p = await provider.search_player(q, tour="ATP")
        except Exception:
            p = None

        if not p:
            return "Try: “ATP top 20”, “live”, or a player name like “Jannik Sinner”."

        info = await provider.get_player(p["player_id"], tour="ATP", kind="singles")
        if not info:
            return "Player found but no info available."

        return (
            f"**{info.get('name','Player')}**\n"
            f"Rank: {info.get('rank','?')} | Points: {info.get('points','?')}\n"
            f"Country: {info.get('country','?')} | Season W/L: {info.get('wl','?')}"
        )