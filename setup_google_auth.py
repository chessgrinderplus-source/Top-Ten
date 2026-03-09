"""
Run this ONCE in Replit shell: python setup_google_auth.py
"""
import os
from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

CLIENT_SECRETS = "keys/oauth_client.json"

if not os.path.exists(CLIENT_SECRETS):
    print(f"❌ Missing {CLIENT_SECRETS}")
    exit(1)

os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS, SCOPES)

# Run local server on port 8090 — Replit will proxy it
creds = flow.run_local_server(port=8090, open_browser=False)

os.makedirs("keys", exist_ok=True)
token_path = "keys/google_token.json"
with open(token_path, "w") as f:
    f.write(creds.to_json())

print(f"\n✅ Saved to {token_path}")
print("\nNow run:  cat keys/google_token.json")
print("Copy everything and add as GOOGLE_TOKEN_CONTENT in Railway Variables.")