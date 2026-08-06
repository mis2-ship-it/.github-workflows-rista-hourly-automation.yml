import os
import json
import time
import jwt
import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

print("🚀 Rista Inventory Activity MTD Automation Started")

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
# GOOGLE SHEETS CONNECTOR (NEW GSHEET)
# =========================================================
creds = Credentials.from_service_account_info(
    json.loads(os.environ["GOOGLE_CREDENTIALS"]),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)
client = gspread.authorize(creds)
SPREADSHEET_ID = "130C3oQsVmONGVUulhGbDWroRKpkebwgnFhq3uiny_O0"
spreadsheet = client.open_by_key(SPREADSHEET_ID)
print("✅ Connected to Google Sheet:", SPREADSHEET_ID)

# =========================================================
# CURRENT MONTH DATE RANGE (1st of month to Yesterday)
# =========================================================
today = datetime.now()
start_date = today.replace(day=1)
end_date = today - timedelta(days=1)

date_list = []
curr = start_date
while curr <= end_date:
    date_list.append(curr.strftime("%Y-%m-%d"))
    curr += timedelta(days=1)

print(f"📅 Fetching MTD Range: {date_list[0]} to {date_list[-1]} ({len(date_list)} Days)")

# =========================================================
# LOAD COCO BRANCHES FROM HELP SHEET
# =========================================================
try:
    help_ws = spreadsheet.worksheet("Help_Sheet")
except Exception:
    help_ws = spreadsheet.worksheet("Help Sheet")

help_data = help_ws.get()
if not help_data:
    print("❌ Help Sheet Empty")
    exit()

raw_headers = [str(h).strip().lower().replace(" ", "") for h in help_data[0]]
rows = help_data[1:]
help_df = pd.DataFrame(rows, columns=raw_headers[:len(rows[0])])

if "ownership" in help_df.columns:
    help_df = help_df[help_df["ownership"].astype(str).str.upper().str.strip() == "COCO"].copy()

if "branchcode" not in help_df.columns:
    print("❌ branchcode column missing in Help Sheet.")
    exit()

branches = help_df["branchcode"].dropna().astype(str).str.strip().unique().tolist()
print(f"🏪 Active COCO Branch Count: {len(branches)}")

# =========================================================
# FETCH & PROCESS INVENTORY ACTIVITY DATA
# =========================================================
inv_items_list_activity = []

def safe_fetch(url, params):
    try:
        res = requests.get(url, headers=headers(), params=params, timeout=60)
        if res.status_code == 200:
            return res.json().get("data", [])
    except Exception as e:
        print(f"⚠️ API Fetch Request failed on {url}: {e}")
    return []

for idx, branch in enumerate(branches):
    print(f"🔄 Processing Branch [{idx+1}/{len(branches)}]: {branch}")
    
    for day_str in date_list:
        data = safe_fetch(
            f"{RISTA_BASE_URL}/inventory/item/activity/page", 
            {"branch": branch, "day": day_str, "date": day_str}
        )
        
        if data:
            df = pd.json_normalize(data)
            df["branchCode"] = branch
            df["activityDate"] = day_str
            
            # 🌟 EXPLODE & SPLIT THE 'activities' COLUMN
            if "activities" in df.columns:
                # Remove rows with empty or missing activities
                df = df.dropna(subset=["activities"]).copy()
                
                # Explode nested array into individual rows
                df = df.explode("activities").reset_index(drop=True)
                
                # Flatten dict keys into separate columns (e.g. activity_type, activity_quantity, activity_cost)
                activities_df = pd.json_normalize(df["activities"]).add_prefix("activity_")
                
                # Merge back with the original dataframe
                df = pd.concat([df.drop(columns=["activities"]), activities_df], axis=1)
                
            inv_items_list_activity.append(df)

# =========================================================
# EXPORT TO GOOGLE SHEET
# =========================================================
TARGET_TAB = "Inventory_Activity_MTD"

try:
    ws = spreadsheet.worksheet(TARGET_TAB)
except Exception:
    ws = spreadsheet.add_worksheet(title=TARGET_TAB, rows="5000", cols="30")

ws.clear()

if not inv_items_list_activity:
    ws.update([["Status"], ["No activity records found for current month MTD."]], "A1")
    print("⚠️ No data compiled for current month MTD.")
else:
    final_df = pd.concat(inv_items_list_activity, ignore_index=True)
    final_df = final_df.fillna("")
    
    # Sanitize remaining complex structures (if any)
    for col in final_df.columns:
        if final_df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            final_df[col] = final_df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
            
    sheet_output = [final_df.columns.tolist()] + final_df.values.tolist()
    
    try:
        ws.update(sheet_output, "A1")
        print(f"✅ Successfully exported {len(final_df)} rows to tab: '{TARGET_TAB}'")
    except Exception as err:
        print(f"❌ Target update failure on sheet {TARGET_TAB}: {err}")

print("🏁 Inventory Activity MTD execution complete.")
