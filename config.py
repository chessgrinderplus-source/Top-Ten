"""
config.py — reads from environment variables.
On Replit: set values in the Secrets tab.
On Railway: set values in the Variables tab.
Locally: create a .env file (it's gitignored).
"""
import os
import json
import tempfile

# Load .env file if present (local dev / Replit)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Discord ──────────────────────────────────────────────────────────────────
DISCORD_TOKEN   = os.environ["TOKEN"]          # Required — bot token
HOME_GUILD_ID   = int(os.getenv("HOME_GUILD_ID", "1333962919536492607"))

# ── Data / files ─────────────────────────────────────────────────────────────
DATA_DIR              = os.getenv("DATA_DIR", "data")
COMMAND_TIMEOUT_FILE  = os.getenv("COMMAND_TIMEOUT_FILE", "data/command_timeouts.json")

# ── Google Sheets ─────────────────────────────────────────────────────────────
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")

# Support two ways to provide the service account:
#   1. GOOGLE_SERVICE_ACCOUNT_JSON  = path to a JSON file  (Replit / local)
#   2. GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT = raw JSON string (Railway)
_sa_path    = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
_sa_content = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT", "")

if _sa_content and not _sa_path:
    # Write the raw JSON to a temp file so gspread can read it normally
    _tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    _tmp.write(_sa_content)
    _tmp.close()
    GOOGLE_SERVICE_ACCOUNT_JSON = _tmp.name
else:
    GOOGLE_SERVICE_ACCOUNT_JSON = _sa_path