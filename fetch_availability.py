import os
import json
import time
import jwt
import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

print("🚀 Rista Inventory, Availability, Consumption & Analytics Automation Started")

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
print("📅 Target Business Day for Sync:", fetch_date)

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
# COLLECTORS FOR TARGET DATASETS
# =========================================================
avail_current_list = []
avail_hist_list = []
inv_store_items_list = []
inv_items_list = []
cons_sales_list = []
cons_shrink_list = []

# New Collectors
coupon_utilization_list = []
outlet_timings_list = []

# Helper logic to extract data safely
def safe_fetch(url, params):
    try:
        res = requests.get(url, headers=headers(), params=params, timeout=60)
        if res.status_code == 200:
            return res.json().get("data", []) or res.json().get("transactions", []) or res.json().get("timings", [])
    except Exception as e:
        print(f"⚠️ API Fetch Request failed on {url}: {e}")
    return []

# Loop branches and capture data structures
for idx, branch in enumerate(branches):
    print(f"🔄 Fetching data blocks [{idx+1}/{len(branches)}] for Branch: {branch}")
    
    # --- 1. AVAILABILITY BLOCKS ---
    data = safe_fetch(f"{RISTA_BASE_URL}/items/soldout", {"branch": branch})
    if data:
        df = pd.json_normalize(data)
        df["branchCode"] = branch
        avail_current_list.append(df)
        
    data = safe_fetch(f"{RISTA_BASE_URL}/items/soldout/history", {"branch": branch, "day": fetch_date, "date": fetch_date})
    if data:
        df = pd.json_normalize(data)
        df["branchCode"] = branch
        avail_hist_list.append(df)

    # --- 2. INVENTORY BLOCKS ---
    data = safe_fetch(f"{RISTA_BASE_URL}/inventory/store/items", {"branch": branch})
    if data:
        df = pd.json_normalize(data)
        df["branchCode"] = branch
        inv_store_items_list.append(df)
        
    data = safe_fetch(f"{RISTA_BASE_URL}/inventory/items", {"branch": branch, "day": fetch_date, "date": fetch_date})
    if data:
        df = pd.json_normalize(data)
        df["branchCode"] = branch
        inv_items_list.append(df)

    data = safe_fetch(f"{RISTA_BASE_URL}/inventory/item/activity", {"branch": branch, "day": fetch_date, "date": fetch_date})
    if data:
        df = pd.json_normalize(data)
        df["branchCode"] = branch
        inv_items_list_activity.append(df)

    # --- 3. CONSUMPTION BLOCKS ---
    data = safe_fetch(f"{RISTA_BASE_URL}/sales/page", {"branch": branch, "day": fetch_date})
    if data:
        df = pd.json_normalize(data)
        if "items" in df.columns:
            df = df.explode("items").reset_index(drop=True)
            items_df = pd.json_normalize(df["items"]).add_prefix("item_")
            df = pd.concat([df.drop(columns=["items"]), items_df], axis=1)
        df["branchCode"] = branch
        cons_sales_list.append(df)
        
    data = safe_fetch(f"{RISTA_BASE_URL}/inventory/shrinkage/page", {"branch": branch, "day": fetch_date, "date": fetch_date})
    if data:
        df = pd.json_normalize(data)
        df["branchCode"] = branch
        cons_shrink_list.append(df)

    # --- 4. COUPON UTILIZATION DASHBOARD ---
    data = safe_fetch(f"{RISTA_BASE_URL}/analytics/discount/transactions", {"branch": branch, "day": fetch_date, "date": fetch_date})
    if data:
        df = pd.json_normalize(data)
        df["branchCode"] = branch
        coupon_utilization_list.append(df)

    # --- 5. OUTLET DELIVERY TIMINGS ---
    data = safe_fetch(f"{RISTA_BASE_URL}/outlet/delivery/timings", {"branch": branch})
    if data:
        df = pd.json_normalize(data)
        df["branchCode"] = branch
        outlet_timings_list.append(df)

# =========================================================
# EXPORT TO TARGET DATA TABS
# =========================================================
def update_spreadsheet_tab(tab_name, data_frames):
    try:
        ws = spreadsheet.worksheet(tab_name)
    except Exception:
        ws = spreadsheet.add_worksheet(title=tab_name, rows="100", cols="20")
        
    ws.clear()
    
    if not data_frames:
        ws.update([["Status"], [f"No data structures returned for {tab_name} on target date."]], "A1")
        print(f"⚠️ No fields compiled for tab: {tab_name}")
        return
        
    final_df = pd.concat(data_frames, ignore_index=True)
    final_df = final_df.fillna("")
    
    # Sanitize data frames from containing un-flattened structural components (lists/dicts)
    for col in final_df.columns:
        if final_df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            final_df[col] = final_df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
            
    sheet_output = [final_df.columns.tolist()] + final_df.values.tolist()
    
    try:
        ws.update(sheet_output, "A1")
        print(f"✅ Sheet Tab Updated successfully: {tab_name}")
    except Exception as err:
        print(f"❌ Target update failure on sheet {tab_name}: {err}")

# Update the workspace sheets
update_spreadsheet_tab("Raw_Availability_Current", avail_current_list)
update_spreadsheet_tab("Raw_Availability_History", avail_hist_list)
update_spreadsheet_tab("Raw_Inventory_StoreItems", inv_store_items_list)
update_spreadsheet_tab("Raw_Inventory_Items", inv_items_list)
update_spreadsheet_tab("Raw_Inventory_Activity", inv_items_list_activity)
update_spreadsheet_tab("Raw_Consumption_Sales", cons_sales_list)
update_spreadsheet_tab("Raw_Consumption_Shrinkage", cons_shrink_list)

# Push the new requested targets
update_spreadsheet_tab("Raw_Analytics_Coupons", coupon_utilization_list)
update_spreadsheet_tab("Raw_Outlet_DeliveryTimings", outlet_timings_list)

print("🏁 All available data models updated on target workspace tabs. Ready for operational layout review!")
