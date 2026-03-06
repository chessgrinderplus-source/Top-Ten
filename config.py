import os
import tempfile

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Debug: print available env var names so we can see what Railway provides
print("[config] Available env vars:", sorted(os.environ.keys()))

DISCORD_TOKEN          = os.environ["TOKEN"]
HOME_GUILD_ID          = int(os.getenv("HOME_GUILD_ID", "1333962919536492607"))
DATA_DIR               = os.getenv("DATA_DIR", "data")
COMMAND_TIMEOUT_FILE   = os.getenv("COMMAND_TIMEOUT_FILE", "data/command_timeouts.json")
GOOGLE_DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "")

_sa_path    = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
_sa_content = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_CONTENT", "")

if _sa_content and not _sa_path:
    _tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    _tmp.write(_sa_content)
    _tmp.close()
    GOOGLE_SERVICE_ACCOUNT_JSON = _tmp.name
else:
    GOOGLE_SERVICE_ACCOUNT_JSON = _sa_path