import os
import json
import time
import jwt
import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

print("🚀 Rista Inventory & Consumption Script Started")

# =========================================================
# AUTHENTICATION
# =========================================================
API_KEY = os.environ["API_KEY"]
SECRET_KEY = os.environ["SECRET_KEY"]
RISTA_BASE_URL = "https://api.ristaapps.com/v1"

def get_token():
    payload = {
        "iss": API_KEY,
        "iat": int(time.time())
    }
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def headers():
    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "content-type": "application/json"
    }

# =========================================================
# GOOGLE SHEETS CONNECTOR
# =========================================================
creds = Credentials.from_service_account_info(
    json.loads(os.environ["GOOGLE_CREDENTIALS"]),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)
client = gspread.authorize(creds)
SPREADSHEET_ID = "1umqb0k_G0F-cAzMbrmqSYnEz06-NjmCANWtWEa_NS9w"
spreadsheet = client.open_by_key(SPREADSHEET_ID)
print("✅ Connected Google Sheet")

# =========================================================
# DATE FRAMEWORK
# =========================================================
fetch_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
print("📅 Processing Target Business Day:", fetch_date)

# =========================================================
# LOAD AND FILTER COCO BRANCHES
# =========================================================
try:
    help_ws = spreadsheet.worksheet("Help_Sheet")
except Exception:
    # Fallback to spaced version if named differently
    help_ws = spreadsheet.worksheet("Help Sheet")

help_data = help_ws.get()
if not help_data:
    print("❌ Help Sheet Empty")
    exit()

raw_headers = [str(h).strip().lower().replace(" ", "") for h in help_data[0]]
rows = help_data[1:]
help_df = pd.DataFrame(rows, columns=raw_headers[:len(rows[0])])

# Safe validation for filtering ownership
if "ownership" in help_df.columns:
    help_df = help_df[help_df["ownership"].astype(str).str.upper().str.strip() == "COCO"].copy()
else:
    print("⚠️ Warning: 'ownership' column missing. Continuing with all stores.")

if "branchcode" not in help_df.columns:
    print("❌ branchcode column missing in Help Sheet.")
    exit()

branches = help_df["branchcode"].dropna().astype(str).str.strip().unique().tolist()
print(f"🏪 Active COCO Branch Count found: {len(branches)}")

# =========================================================
# DATA EXTRACTION LOOPS
# =========================================================
availability_list = []
inventory_list = []
consumption_list = []

for idx, branch in enumerate(branches):
    print(f"🔄 Processing Branch [{idx+1}/{len(branches)}]: {branch}")
    
    # --- 1. ITEM AVAILABILITY (Sold-out Current State) ---
    try:
        res = requests.get(f"{RISTA_BASE_URL}/items/soldout", headers=headers(), params={"branch": branch}, timeout=60)
        if res.status_code == 200:
            data = res.json().get("data", [])
            if data:
                df = pd.json_normalize(data)
                df["branchCode"] = branch
                availability_list.append(df)
    except Exception as e:
        print(f"⚠️ Error item availability ({branch}): {e}")

    # --- 2. INVENTORY TRACKING (Store Items Balances) ---
    try:
        res = requests.get(f"{RISTA_BASE_URL}/inventory/store/items", headers=headers(), params={"branch": branch}, timeout=60)
        if res.status_code == 200:
            data = res.json().get("data", [])
            if data:
                df = pd.json_normalize(data)
                df["branchCode"] = branch
                inventory_list.append(df)
    except Exception as e:
        print(f"⚠️ Error inventory balances ({branch}): {e}")

    # --- 3. CONSUMPTION REPORTING (Via Sales Page Elements) ---
    try:
        res = requests.get(f"{RISTA_BASE_URL}/sales/page", headers=headers(), params={"branch": branch, "day": fetch_date}, timeout=60)
        if res.status_code == 200:
            data = res.json().get("data", [])
            if data:
                df = pd.json_normalize(data)
                if "items" in df.columns:
                    df = df.explode("items").reset_index(drop=True)
                    items_df = pd.json_normalize(df["items"]).add_prefix("item_")
                    df = pd.concat([df.drop(columns=["items"]), items_df], axis=1)
                df["branchCode"] = branch
                consumption_list.append(df)
    except Exception as e:
        print(f"⚠️ Error consumption data ({branch}): {e}")

# =========================================================
# FORMAT AND WRITE DATA TO SHEETS
# =========================================================
def update_spreadsheet_tab(tab_name, data_frames):
    try:
        ws = spreadsheet.worksheet(tab_name)
    except Exception:
        ws = spreadsheet.add_worksheet(title=tab_name, rows="100", cols="20")
        
    ws.clear()
    
    if not data_frames:
        ws.update([["Status"] , ["No records found for target day."]], "A1")
        print(f"⚠️ No data compiled for tab: {tab_name}")
        return
        
    final_df = pd.concat(data_frames, ignore_index=True)
    
    # Fill empty values cleanly for GSheet compatibility
    final_df = final_df.fillna("")
    
    # Convert dataframe to nested arrays
    sheet_output = [final_df.columns.tolist()] + final_df.values.tolist()
    
    ws.update(sheet_output, "A1")
    print(f"✅ Successfully exported data to tab: {tab_name}")

# Executing sheet updates for visual review
update_spreadsheet_tab("Raw_Availability", availability_list)
update_spreadsheet_tab("Raw_Inventory", inventory_list)
update_spreadsheet_tab("Raw_Consumption", consumption_list)

print("🏁 Process Complete. Review raw datasets to confirm metric layout rules.")
