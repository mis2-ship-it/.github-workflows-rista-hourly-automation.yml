import os
import json
import time
import jwt
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

print("🚀 Rista Inventory Activity Pipeline Started")

# =========================================================
# CONFIGURATION & AUTH
# =========================================================
API_KEY = os.environ["API_KEY"]
SECRET_KEY = os.environ["SECRET_KEY"]
RISTA_BASE_URL = "https://api.ristaapps.com/v1"

SPREADSHEET_ID = "130C3oQsVmONGVUulhGbDWroRKpkebwgnFhq3uiny_O0"
DRIVE_FOLDER_ID = "1g5ap7nXTNwYl3RYyilKZIh0XrYhvCrwe"

def get_token():
    payload = {"iss": API_KEY, "iat": int(time.time())}
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")

def headers():
    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "content-type": "application/json"
    }

# Google Credentials
google_creds_json = json.loads(os.environ["GOOGLE_CREDENTIALS"])
scopes = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
creds = Credentials.from_service_account_info(google_creds_json, scopes=scopes)
client = gspread.authorize(creds)
spreadsheet = client.open_by_key(SPREADSHEET_ID)

# =========================================================
# DATE RANGE & DYNAMIC MONTHLY FILE NAME
# =========================================================
today = datetime.now()
start_date = today.replace(day=1)
end_date = today - timedelta(days=1)

date_list = []
curr = start_date
while curr <= end_date:
    date_list.append(curr.strftime("%Y-%m-%d"))
    curr += timedelta(days=1)

print(f"📅 MTD Range: {date_list[0]} to {date_list[-1]} ({len(date_list)} Days)")

# Dynamic Monthly File Name: e.g. "2026-08.csv"
month_year_filename = f"{today.strftime('%Y-%m')}.csv"

# =========================================================
# LOAD HELP SHEET & MAP STORES
# =========================================================
try:
    help_ws = spreadsheet.worksheet("Help_Sheet")
except Exception:
    help_ws = spreadsheet.worksheet("Help Sheet")

help_data = help_ws.get()
raw_headers = [str(h).strip().lower().replace(" ", "") for h in help_data[0]]

safe_headers = []
for i, h in enumerate(raw_headers):
    h = f"blank_col_{i}" if not h else h
    if h in safe_headers:
        h = f"{h}_{i}"
    safe_headers.append(h)

rows = help_data[1:]
normalized_rows = [list(r) + [""] * (len(safe_headers) - len(r)) for r in rows]
help_df = pd.DataFrame(normalized_rows, columns=safe_headers)

branch_cols = [c for c in help_df.columns if "branchcode" in c]
store_cols = [c for c in help_df.columns if "storename" in c or "store" in c]
region_cols = [c for c in help_df.columns if "region" in c]
ownership_cols = [c for c in help_df.columns if "ownership" in c]

branch_col_name = branch_cols[0]
lookup_cols = [branch_col_name]
rename_dict = {branch_col_name: "branchCode"}

if store_cols:
    lookup_cols.append(store_cols[0])
    rename_dict[store_cols[0]] = "Store Name"
if region_cols:
    lookup_cols.append(region_cols[0])
    rename_dict[region_cols[0]] = "Region"

# Must-include store codes
explicit_stores = ["90003-2221", "90003-2216", "90003-2218", "90003-2214", "90003-2215", "DW"]

# Ownership Filtering + Explicit Inclusion
if ownership_cols:
    ownership_series = help_df[ownership_cols[0]].astype(str).str.upper().str.strip()
    branch_series = help_df[branch_col_name].astype(str).str.strip()
    
    is_coco_wh = ownership_series.str.contains("COCO|WARE|WH", na=False)
    is_explicit = branch_series.isin(explicit_stores)
    
    help_df = help_df[is_coco_wh | is_explicit].copy()

help_lookup = help_df[lookup_cols].copy().rename(columns=rename_dict)
help_lookup["branchCode"] = help_lookup["branchCode"].astype(str).str.strip()
help_lookup = help_lookup.drop_duplicates(subset=["branchCode"])

branches = help_lookup["branchCode"].loc[lambda x: x != ""].tolist()
print(f"🏪 Active Branches Loaded: {len(branches)}")

# Verify specific stores in queue
for check_code in explicit_stores:
    status = "✅ Present" if check_code in branches else "❌ Missing"
    print(f"Store {check_code}: {status}")

# =========================================================
# FETCH MTD INVENTORY ACTIVITY DATA
# =========================================================
inv_items_list_activity = []

def safe_fetch(url, params):
    try:
        res = requests.get(url, headers=headers(), params=params, timeout=60)
        if res.status_code == 200:
            return res.json().get("data", [])
    except Exception as e:
        print(f"⚠️ API Error ({url}): {e}")
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
            
            if "activities" in df.columns:
                df = df.dropna(subset=["activities"]).copy()
                df = df.explode("activities").reset_index(drop=True)
                activities_df = pd.json_normalize(df["activities"]).add_prefix("activity_")
                df = pd.concat([df.drop(columns=["activities"]), activities_df], axis=1)
                
            inv_items_list_activity.append(df)

if not inv_items_list_activity:
    print("❌ No activity data retrieved.")
    exit()

final_df = pd.concat(inv_items_list_activity, ignore_index=True)
final_df["branchCode"] = final_df["branchCode"].astype(str).str.strip()
final_df = final_df.merge(help_lookup, on="branchCode", how="left")

# Sanitize numbers and convert activity quantities/costs to absolute positive values
num_fields = ["activity_quantity", "activity_cost", "openingBalance", "openingCost", "closingBalance", "closingCost"]
for col in num_fields:
    if col in final_df.columns:
        final_df[col] = pd.to_numeric(final_df[col], errors="coerce").fillna(0)
    else:
        final_df[col] = 0.0

final_df["activity_quantity"] = final_df["activity_quantity"].abs()
final_df["activity_cost"] = final_df["activity_cost"].abs()

# Reorder key columns to front
lead_cols = [c for c in ["branchCode", "Store Name", "Region", "activityDate"] if c in final_df.columns]
final_df = final_df[lead_cols + [c for c in final_df.columns if c not in lead_cols]]

# =========================================================
# 1. SAVE MASTER DATA TO CSV & UPLOAD/OVERWRITE ON GOOGLE DRIVE
# =========================================================
final_df.to_csv(month_year_filename, index=False)
print(f"📁 Local Master CSV generated: {month_year_filename} ({len(final_df)} rows)")

try:
    drive_service = build('drive', 'v3', credentials=creds)
    query = f"name = '{month_year_filename}' and '{DRIVE_FOLDER_ID}' in parents and trashed = false"
    results = drive_service.files().list(q=query, fields="files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
    existing_files = results.get('files', [])
    
    media = MediaFileUpload(month_year_filename, mimetype='text/csv', resumable=True)
    
    if existing_files:
        file_id = existing_files[0]['id']
        drive_service.files().update(fileId=file_id, media_body=media, supportsAllDrives=True).execute()
        print(f"🔄 Updated existing Google Drive file: '{month_year_filename}' (ID: {file_id})")
    else:
        file_metadata = {'name': month_year_filename, 'parents': [DRIVE_FOLDER_ID]}
        new_file = drive_service.files().create(body=file_metadata, media_body=media, fields='id', supportsAllDrives=True).execute()
        print(f"☁️ Created new Google Drive file: '{month_year_filename}' (ID: {new_file.get('id')})")
        
except Exception as e:
    print(f"❌ Google Drive API Error: {e}")

# =========================================================
# FILTER SALES ACTIVITY TYPE FOR STOCK ON HAND CALCULATIONS
# =========================================================
sales_df = final_df[final_df["activity_type"].astype(str).str.strip().str.upper() == "SALES"].copy()

# =========================================================
# 2. GENERATE STORE-LEVEL SUMMARY
# =========================================================
min_date, max_date = date_list[0], date_list[-1]

opening_df = final_df[final_df["activityDate"] == min_date].groupby(["Region", "Store Name"], as_index=False)[["openingCost", "openingBalance"]].sum()
closing_df = final_df[final_df["activityDate"] == max_date].groupby(["Region", "Store Name"], as_index=False)[["closingCost", "closingBalance"]].sum()

sales_activity_sums = sales_df.groupby(["Region", "Store Name"], as_index=False)[["activity_cost", "activity_quantity"]].sum()

store_summary = opening_df.merge(closing_df, on=["Region", "Store Name"], how="outer").merge(sales_activity_sums, on=["Region", "Store Name"], how="outer").fillna(0)

store_summary["Stock on Hand Cost %"] = np.where(store_summary["closingCost"] > 0, (store_summary["activity_cost"] / store_summary["closingCost"]) * 100, 0)
store_summary["Stock on Hand Qty %"] = np.where(store_summary["closingBalance"] > 0, (store_summary["activity_quantity"] / store_summary["closingBalance"]) * 100, 0)

store_summary = store_summary.rename(columns={
    "openingCost": "Opening Cost",
    "openingBalance": "Opening Qty",
    "closingCost": "Closing Cost",
    "closingBalance": "Closing Qty"
}).sort_values(by=["Region", "Store Name"])

store_summary_display = store_summary[["Region", "Store Name", "Opening Cost", "Opening Qty", "Closing Cost", "Closing Qty", "Stock on Hand Cost %", "Stock on Hand Qty %"]].copy()
store_summary_display["Stock on Hand Cost %"] = store_summary_display["Stock on Hand Cost %"].round(2).astype(str) + "%"
store_summary_display["Stock on Hand Qty %"] = store_summary_display["Stock on Hand Qty %"].round(2).astype(str) + "%"

# =========================================================
# 3. GENERATE OVERALL REGION SUMMARY
# =========================================================
region_agg = store_summary.groupby("Region", as_index=False)[["Opening Cost", "Opening Qty", "Closing Cost", "Closing Qty", "activity_cost", "activity_quantity"]].sum()

region_agg["Stock on Hand Cost %"] = np.where(region_agg["Closing Cost"] > 0, (region_agg["activity_cost"] / region_agg["Closing Cost"]) * 100, 0)
region_agg["Stock on Hand Qty %"] = np.where(region_agg["Closing Qty"] > 0, (region_agg["activity_quantity"] / region_agg["Closing Qty"]) * 100, 0)

kpi_cols = ["Opening Cost", "Opening Qty", "Closing Cost", "Closing Qty", "Stock on Hand Cost %", "Stock on Hand Qty %"]
region_summary = pd.DataFrame({"KPI Metrics": kpi_cols})

for _, row in region_agg.iterrows():
    reg = row["Region"]
    region_summary[reg] = [
        round(row["Opening Cost"], 2),
        round(row["Opening Qty"], 2),
        round(row["Closing Cost"], 2),
        round(row["Closing Qty"], 2),
        f"{round(row['Stock on Hand Cost %'], 2)}%",
        f"{round(row['Stock on Hand Qty %'], 2)}%"
    ]

# =========================================================
# 4. GENERATE DAILY STOCK ON HAND SHEET (SALES ONLY)
# =========================================================
daily_sales = sales_df.groupby(["Region", "Store Name", "activityDate"], as_index=False)["activity_cost"].sum()
daily_closing = final_df.groupby(["Region", "Store Name", "activityDate"], as_index=False)["closingCost"].sum()

daily_merged = daily_closing.merge(daily_sales, on=["Region", "Store Name", "activityDate"], how="left").fillna(0)
daily_merged["SOH_Cost_Pct"] = np.where(daily_merged["closingCost"] > 0, (daily_merged["activity_cost"] / daily_merged["closingCost"]) * 100, 0)

daily_pivot = daily_merged.pivot(index=["Region", "Store Name"], columns="activityDate", values="SOH_Cost_Pct").fillna(0).reset_index()

for c in date_list:
    if c in daily_pivot.columns:
        daily_pivot[c] = daily_pivot[c].round(2).astype(str) + "%"

# =========================================================
# 5. EXPORT DASHBOARD TABS TO GOOGLE SHEET
# =========================================================
def update_tab(tab_name, df):
    try:
        ws = spreadsheet.worksheet(tab_name)
    except Exception:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=str(len(df) + 100), cols=str(len(df.columns) + 10))
    ws.clear()
    sheet_data = [df.columns.tolist()] + df.values.tolist()
    ws.update(sheet_data, "A1")
    print(f"✅ Dashboard updated: '{tab_name}' ({len(df)} rows)")

update_tab("Region_Summary", region_summary)
update_tab("Store_Summary", store_summary_display)
update_tab("Daily_Stock_On_Hand", daily_pivot)

print("🏁 Execution complete. Master data safely updated in Google Drive!")
