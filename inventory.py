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
BASE_URL = "https://api.ristaapps.com/v1"

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
today_str = datetime.utcnow().strftime("%Y-%m-%d")
BRANCH_ID = "FZBBLR023" 
query_params = f"?branch={BRANCH_ID}&day={today_str}&page=1&size=50"

transfer = fetch_data(f"/inventory/transfer/page{query_params}", "GET")
grn = fetch_data(f"/inventory/grn/page{query_params}", "GET")

# 💡 Dynamically collect unique SKU codes from the transfer and GRN data
collected_skus = set()

# Extract from transfer items
for record in transfer.get("data", []):
    # Adjust "skuCode" or "sku" key based on your API's actual response structure
    if "skuCode" in record:
        collected_skus.add(record["skuCode"])
    elif "items" in record:
        for item in record["items"]:
            if "skuCode" in item:
                collected_skus.add(item["skuCode"])

# Extract from GRN items
for record in grn.get("data", []):
    if "skuCode" in record:
        collected_skus.add(record["skuCode"])
    elif "items" in record:
        for item in record["items"]:
            if "skuCode" in item:
                collected_skus.add(item["skuCode"])

# Convert set back to a list
sku_list = list(collected_skus)

# Fallback: If no transfers or GRNs happened today, use a dummy or specific SKU to prevent 400 error
if not sku_list:
    sku_list = ["ALL"] # Or substitute a known active SKU from your inventory

# Build the payload with the gathered SKUs
stock_payload = {
    "branchCode": BRANCH_ID,
    "skuCodes": sku_list
}

stock = fetch_data("/inventory/item/stock", "POST", payload=stock_payload)

# ---------------- PUSH TO SHEET ---------------- #

df = pd.DataFrame(all_data)
sheet.clear()
sheet.update([df.columns.tolist()] + df.values.tolist())

print("✅ Inventory data pushed successfully!")

