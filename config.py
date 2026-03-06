import os, tempfile

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── Discord ───────────────────────────────────────────────────────────────────
DISCORD_TOKEN   = os.environ["TOKEN"]
HOME_GUILD_ID   = int(os.getenv("HOME_GUILD_ID", "1333962919536492607"))

# ── Data directory ────────────────────────────────────────────────────────────
DATA_DIR = os.getenv("DATA_DIR", "data")

def _d(filename): return os.path.join(DATA_DIR, filename)

# ── Per-module file paths ─────────────────────────────────────────────────────
COMMAND_TIMEOUT_FILE    = os.getenv("COMMAND_TIMEOUT_FILE",    _d("command_timeouts.json"))
TARGET_SERVER_FILE      = os.getenv("TARGET_SERVER_FILE",      _d("settings.json"))
ECONOMY_FILE            = os.getenv("ECONOMY_FILE",            _d("economy.json"))
YOUTUBE_FILE            = os.getenv("YOUTUBE_FILE",            _d("youtube.json"))
CARDS_FILE              = os.getenv("CARDS_FILE",              _d("cards.json"))
FANTASY_FILE            = os.getenv("FANTASY_FILE",            _d("fantasy.json"))
PLAYERS_FILE            = os.getenv("PLAYERS_FILE",            _d("players.json"))
ACADEMY_FILE            = os.getenv("ACADEMY_FILE",            _d("academy.json"))
COACHES_FILE            = os.getenv("COACHES_FILE",            _d("coaches.json"))
GEAR_FILE               = os.getenv("GEAR_FILE",               _d("gear.json"))
VENUES_FILE             = os.getenv("VENUES_FILE",             _d("venues.json"))
LOADOUTS_FILE           = os.getenv("LOADOUTS_FILE",           _d("loadouts_presets.json"))
TRAINING_FILE           = os.getenv("TRAINING_FILE",           _d("training.json"))
TOURNAMENTS_FILE        = os.getenv("TOURNAMENTS_FILE",        _d("tournaments.json"))
MATCHSIM_FILE           = os.getenv("MATCHSIM_FILE",           _d("matchsim_bots.json"))
SHOP_FILE               = os.getenv("SHOP_FILE",               _d("shop_statpacks.json"))

# ── Google Sheets ─────────────────────────────────────────────────────────────
GOOGLE_DRIVE_FOLDER_ID  = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")

_sa_path    = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
_sa_content = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT", "")

if _sa_content and not _sa_path:
    _tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    _tmp.write(_sa_content)
    _tmp.close()
    GOOGLE_SERVICE_ACCOUNT_JSON = _tmp.name
else:
    GOOGLE_SERVICE_ACCOUNT_JSON = _sa_path