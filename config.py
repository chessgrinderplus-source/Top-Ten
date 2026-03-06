import os

DISCORD_TOKEN = "MTQ2NTM3MDg0NTk2NTcxNzU2Nw.GB1smc.4t5O1h2PgLev468qgOphNYFKs0k91teeW8P5rY"
HOME_GUILD_ID = 1333962919536492607
MEMBERSHIP_CHECK_MINUTES = 5

DATA_DIR = "data"
TARGET_SERVER_FILE = f"{DATA_DIR}/target_server.json"
YOUTUBE_FILE = f"{DATA_DIR}/youtube.json"
ECONOMY_FILE = f"{DATA_DIR}/economy.json"
COMMAND_TIMEOUT_FILE = f"{DATA_DIR}/command_timeouts.json"
FANTASY_FILE = f"{DATA_DIR}/fantasy.json"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")  # pick whatever you want later
TENNIS_PROVIDER = "sofascore"
TENNIS_RANKINGS_MODE = "sofascore"   # or "sofascore" if you want the non-official list
TENNIS_API_KEY = os.getenv("TENNIS_API_KEY", "")

GOOGLE_SERVICE_ACCOUNT_JSON = "keys/google_sa.json"
GOOGLE_DRIVE_FOLDER_ID = "1BK5KqIONBc613D5MkJuuxy04_RcMGL4R"