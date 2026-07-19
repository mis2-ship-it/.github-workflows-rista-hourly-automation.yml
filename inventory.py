import os
import json
import time
import jwt
print("PyJWT version:", jwt.__version__)
import requests
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials

print("🚀 Live Script Started")

# ---------------- AUTH ---------------- #

API_KEY = os.environ["API_KEY"]
SECRET_KEY = os.environ["SECRET_KEY"]
GOOGLE_CREDENTIALS = os.environ["GOOGLE_CREDENTIALS"]

def get_token():
    payload = {
        "iss": API_KEY,
        "iat": int(time.time()),
        "exp": int(time.time()) + 60
    }
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def headers():
    # 👉 This is the critical fix
    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "Content-Type": "application/json"
    }

# ---------------- GOOGLE ---------------- #

creds = Credentials.from_service_account_info(
    json.loads(GOOGLE_CREDENTIALS),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)
sheet_url = "https://docs.google.com/spreadsheets/d/1YAzHR1djQQSyW8Cz9-y6HxLV7XQY9xSm6mVnBy8a7lc/edit"

retry = 5
for i in range(retry):
    try:
        spreadsheet = client.open_by_url(sheet_url)
        sheet = spreadsheet.worksheet("Sample_Data")
        print("✅ Connected to Google Sheet")
        break
    except Exception as e:
        print(f"⚠️ Google Sheet connection failed ({i+1}/{retry})")
        print(str(e))
        time.sleep(10)
else:
    raise Exception("❌ Failed to connect Google Sheet after retries")

# ---------------- API CALL ---------------- #

BASE_URL = "https://api.ristaapps.com"

def fetch_data(endpoint, method="GET", payload=None):
    url = BASE_URL + endpoint
    print(f"Calling: {url}")
    if method == "GET":
        response = requests.get(url, headers=headers())
    else:
        response = requests.post(url, headers=headers(), json=payload or {})

    if response.status_code != 200:
        print("Response body:", response.text)
        raise Exception(f"API call failed: {response.status_code}")
    return response.json()

# ---------------- FETCH & ENRICH ---------------- #

transfer = fetch_data("/inventory/transfer/page", "GET")
grn = fetch_data("/inventory/grn/page", "GET")
stock = fetch_data("/inventory/item/stock", "POST")

all_data = []
all_data += transfer.get("data", [])
all_data += grn.get("data", [])
all_data += stock.get("data", [])

if not all_data:
    raise Exception("❌ No data returned from API")

for item in all_data:
    item["source"] = "Rista"
    item["fetched_at"] = datetime.utcnow().isoformat()

# ---------------- PUSH TO SHEET ---------------- #

df = pd.DataFrame(all_data)
sheet.clear()
sheet.update([df.columns.tolist()] + df.values.tolist())

print("✅ Inventory data pushed successfully!")
