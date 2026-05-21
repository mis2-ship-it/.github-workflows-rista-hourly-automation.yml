import os
import json
import time
import jwt
import requests
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

print("🚀 Script Started")

# =========================================
# AUTH
# =========================================

API_KEY = os.environ["API_KEY"]
SECRET_KEY = os.environ["SECRET_KEY"]

def get_token():

    payload = {
        "iss": API_KEY,
        "iat": int(time.time())
    }

    return jwt.encode(
        payload,
        SECRET_KEY,
        algorithm="HS256"
    )

def headers():

    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "content-type": "application/json"
    }

# =========================================
# GOOGLE AUTH
# =========================================

creds = Credentials.from_service_account_info(

    json.loads(os.environ["GOOGLE_CREDENTIALS"]),

    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)

# =========================================
# GOOGLE SHEET
# =========================================

spreadsheet = client.open_by_key(
    "19z6KkVBFoLC33_wcNqVhDLyQEC2dDQ8YQE0gE38BhVg"
)

print("✅ Connected to Google Sheet")

# =========================================
# DATE
# =========================================

yesterday = (
    datetime.now() - timedelta(days=1)
).strftime("%Y-%m-%d")

sheet_name = f"{yesterday}_sample"

print("📅 Fetching Date:", yesterday)

# =========================================
# CREATE / OPEN SHEET
# =========================================

try:

    ws = spreadsheet.worksheet(sheet_name)

    ws.clear()

except:

    ws = spreadsheet.add_worksheet(
        title=sheet_name,
        rows="500",
        cols="200"
    )

print(f"✅ Worksheet Ready: {sheet_name}")

# =========================================
# FETCH ACTIVE BRANCHES
# =========================================

b_url = "https://api.ristaapps.com/v1/branch/list"

b_resp = requests.get(
    b_url,
    headers=headers()
)

b_json = b_resp.json()

if isinstance(b_json, dict):

    b_data = b_json.get("data", [])

else:

    b_data = b_json

branch_df = pd.DataFrame(b_data)

branch_df = branch_df[
    branch_df["status"] == "Active"
]

branches = branch_df["branchCode"].tolist()

print("🏪 Active Branches:", len(branches))

# =========================================
# FETCH SALES PAGE SAMPLE
# =========================================

s_url = "https://api.ristaapps.com/v1/sales/page"

sales_data = []

total_rows = 0

for branch in branches:

    print(f"Fetching: {branch}")

    params = {
        "branch": branch,
        "day": yesterday
    }

    try:

        response = requests.get(
            s_url,
            headers=headers(),
            params=params,
            timeout=60
        )

        if response.status_code != 200:

            print(f"❌ Failed: {branch}")

            continue

        js = response.json()

        data = js.get("data", [])

        if not data:
            continue

        df = pd.json_normalize(data)

        sales_data.append(df)

        total_rows += len(df)

        print(f"✅ Rows: {len(df)}")

        # =====================================
        # ONLY 100 SAMPLE ROWS
        # =====================================

        if total_rows >= 100:
            break

    except Exception as e:

        print(f"❌ Error: {str(e)}")

# =========================================
# CONCAT
# =========================================

if not sales_data:

    print("❌ No data fetched")

    exit()

sales_df = pd.concat(
    sales_data,
    ignore_index=True
)

# =========================================
# LIMIT 100 ROWS
# =========================================

sales_df = sales_df.head(100)

print("✅ Raw Rows:", len(sales_df))
print("✅ Raw Columns:", len(sales_df.columns))

# =========================================
# EXPLODE ITEMS
# =========================================

if "items" in sales_df.columns:

    exploded_df = sales_df.explode("items")

    item_df = pd.json_normalize(
        exploded_df["items"]
    ).add_prefix("item_")

    exploded_df = exploded_df.drop(
        columns=["items"]
    )

    final_df = pd.concat(
        [
            exploded_df.reset_index(drop=True),
            item_df.reset_index(drop=True)
        ],
        axis=1
    )

else:

    final_df = sales_df.copy()

print("✅ Final Rows:", len(final_df))
print("✅ Final Columns:", len(final_df.columns))

# =========================================
# CLEAN
# =========================================

final_df = final_df.fillna("")

final_df = final_df.astype(str)

# =========================================
# PUSH TO SHEET
# =========================================

ws.update(
    [final_df.columns.tolist()] +
    final_df.values.tolist(),
    value_input_option="USER_ENTERED"
)

print("✅ Data Uploaded Successfully")

print("📄 Sheet URL:")
print(spreadsheet.url)
