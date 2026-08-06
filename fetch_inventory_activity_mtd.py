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
# LOAD COCO & WAREHOUSE BRANCHES FROM HELP SHEET
# =========================================================
try:
    help_ws = spreadsheet.worksheet("Help_Sheet")
except Exception:
    help_ws = spreadsheet.worksheet("Help Sheet")

help_data = help_ws.get()
if not help_data:
    print("❌ Help Sheet Empty")
    exit()

# Handle duplicate and blank column headers cleanly
raw_headers = [str(h).strip().lower().replace(" ", "") for h in help_data[0]]
safe_headers = []
for i, h in enumerate(raw_headers):
    if not h:
        h = f"blank_col_{i}"
    if h in safe_headers:
        h = f"{h}_{i}"
    safe_headers.append(h)

rows = help_data[1:]
normalized_rows = []
header_len = len(safe_headers)

for row in rows:
    row_list = list(row)
    if len(row_list) < header_len:
        row_list.extend([""] * (header_len - len(row_list)))
    elif len(row_list) > header_len:
        row_list = row_list[:header_len]
    normalized_rows.append(row_list)

help_df = pd.DataFrame(normalized_rows, columns=safe_headers)

# Find ownership column (matches 'ownership' or deduplicated versions)
ownership_col = [c for c in help_df.columns if "ownership" in c]
if ownership_col:
    allowed_ownership = ["COCO", "WARE HOUSE", "WAREHOUSE"]
    help_df = help_df[
        help_df[ownership_col[0]]
        .astype(str)
        .str.upper()
        .str.strip()
        .isin(allowed_ownership)
    ].copy()

# Find branchcode column
branch_cols = [c for c in help_df.columns if "branchcode" in c]
if not branch_cols:
    print("❌ branchcode column missing in Help Sheet.")
    exit()

branch_series = help_df[branch_cols[0]]

branches = (
    branch_series
    .dropna()
    .astype(str)
    .str.strip()
    .loc[lambda x: x != ""]
    .unique()
    .tolist()
)

print(f"🏪 Active Filtered Branch Count (COCO & WARE HOUSE): {len(branches)}")

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
            
            # Explode & Split the 'activities' array
            if "activities" in df.columns:
                df = df.dropna(subset=["activities"]).copy()
                df = df.explode("activities").reset_index(drop=True)
                activities_df = pd.json_normalize(df["activities"]).add_prefix("activity_")
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
