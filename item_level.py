# =========================================================
# IMPORTS
# =========================================================

import os
import re
import json
import time
import jwt
import requests
import pandas as pd
import numpy as np

from datetime import (
    datetime,
    timedelta
)

import gspread

from google.oauth2.service_account import Credentials

print("🚀 Product Dashboard Started")


# =========================================================
# AUTH
# =========================================================

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


# =========================================================
# GOOGLE AUTH
# =========================================================

creds = Credentials.from_service_account_info(
    json.loads(
        os.environ[
            "GOOGLE_CREDENTIALS"
        ]
    ),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)


spreadsheet = client.open_by_key(
    "1ldgnNMdeubDx_ImtCC1uCD7FGz8gt_edmWEkmO_xRNk"
)

print("✅ Connected Google Sheet")

# =========================================================
# BUSINESS WINDOW
# =========================================================

now = datetime.now()

print("🕒 Current Time:", now)

# =========================================================
# BUSINESS DATE
# =========================================================

if now.hour < 5:

    business_date = (
        now - timedelta(days=1)
    ).date()

else:

    business_date = now.date()

print(
    "📅 Business Date:",
    business_date
)


# =========================================================
# HELP SHEET
# =========================================================

help_ws = spreadsheet.worksheet(
    "Help Sheet"
)

help_data = help_ws.get_all_values()

if not help_data:

    print("❌ Help Sheet Empty")
    exit()

help_rows = []

for row in help_data[1:]:

    row = list(row)

    if len(row) < 7:
        row.extend(
            [""] * (7 - len(row))
        )

    help_rows.append([
        row[0],  # A branchCode
        row[1],  # B Store Name
        row[2],  # C Ownership
        row[3],  # D Region
        row[5],  # F Channel
        row[6],  # G Source
    ])

help_df = pd.DataFrame(
    help_rows,
    columns=[
        "branchCode",
        "Store Name",
        "Ownership",
        "Region",
        "Channel",
        "Source"
    ]
)

help_df["Ownership"] = (
    help_df["Ownership"]
    .astype(str)
    .str.upper()
    .str.strip()
)

help_df["branchCode"] = (
    help_df["branchCode"]
    .astype(str)
    .str.strip()
)

# COCO only
help_df = help_df[
    help_df["Ownership"] == "COCO"
].copy()

print("✅ Help Sheet Loaded")
print(
    "📋 Help Columns:",
    help_df.columns.tolist()
)

print(
    "✅ COCO Stores:",
    len(help_df)
)

# =========================================================
# REQUIRED COLUMNS CHECK
# =========================================================

required_cols = [
    "branchCode",
    "Store Name",
    "Ownership",
    "Region",
    "Channel",
    "Source"
]

missing_cols = [
    c for c in required_cols
    if c not in help_df.columns
]

if missing_cols:

    print(
        "❌ Missing Help Columns:",
        missing_cols
    )

    print(
        "📋 Available Columns:",
        help_df.columns.tolist()
    )

    exit()


# =========================================================
# RENAME
# =========================================================

help_df = help_df.rename(
    columns={
        "Channel":
        "Help Channel",

        "Source":
        "Help Source"
    }
)

# =========================================================
# CLEAN BRANCH CODE
# =========================================================

help_df["branchCode"] = (
    help_df["branchCode"]
    .astype(str)
    .str.strip()
)

branches = (
    help_df["branchCode"]
    .dropna()
    .unique()
    .tolist()
)

print(
    "🏪 Branch Count:",
    len(branches)
)

# =========================================================
# CHANNEL GROUP MAPPING
# =========================================================

if (
    "Help Channel" in help_df.columns
    and
    "Help Source" in help_df.columns
):

    channel_map = dict(
        zip(
            help_df["Help Channel"],
            help_df["Help Source"]
        )
    )

    print(
        "✅ Channel Mapping:",
        len(channel_map)
    )

else:

    print(
        "⚠️ Channel Mapping Skipped"
    )


# =========================================================
# ITEM GROUP TAB
# =========================================================

item_ws = spreadsheet.worksheet(
    "Item Group"
)

item_data = item_ws.get_all_values()

if not item_data:

    print("❌ Item Group Empty")
    exit()

# =========================================================
# FIXED HEADERS (A:E ONLY)
# =========================================================

item_headers = [
    "Item Name",
    "Item Group Name",
    "Variant(s)",
    "Product Mix",
    "Category Group"
]

normalized_rows = []

for row in item_data[1:]:

    row = list(row)

    # only take A:E
    row = row[:5]

    # fill blanks if less columns
    if len(row) < 5:

        row.extend(
            [""] * (5 - len(row))
        )

    normalized_rows.append(row)

item_df = pd.DataFrame(
    normalized_rows,
    columns=item_headers
)

# =========================================================
# CLEAN COLUMNS
# =========================================================

item_df.columns = (
    item_df.columns
    .astype(str)
    .str.strip()
)

# =========================================================
# CLEAN ITEM NAME
# =========================================================

item_df["Item Name"] = (
    item_df["Item Name"]
    .astype(str)
    .str.strip()
    .str.lower()
)

print(
    "✅ Item Group Loaded"
)

print(
    "📋 Item Rows:",
    len(item_df)
)

# =========================================================
# STANDARDIZE COLUMN NAMES
# =========================================================

item_df = item_df.rename(
    columns={
        "Item Name":
        "Item Name",

        "Item Group Name":
        "Item Group Name",

        "Variant(s)":
        "Variant",

        "Product Mix":
        "Product Mix",

        "Category Group":
        "Category Group"
    }
)

# =========================================================
# CLEAN ITEM NAME
# =========================================================

item_df["Item Name"] = (
    item_df["Item Name"]
    .astype(str)
    .str.strip()
    .str.upper()
)

print(
    "✅ Item Mapping Rows:",
    len(item_df)
)

# =========================================================
# SALES API
# =========================================================

sales_url = (
    "https://api.ristaapps.com/v1/sales/page"
)


# =========================================================
# FETCH SALES
# =========================================================

def fetch_sales_window(
    start_datetime,
    end_datetime,
    tag
):

    all_sales = []

    print(
        f"\n📦 Fetching {tag}"
    )

    print(
        "Window:",
        start_datetime,
        "to",
        end_datetime
    )

    # =====================================================
    # LOOP BRANCHES
    # =====================================================

    for branch in branches:

        print(
            "Fetching:",
            branch
        )

        params = {
            "branch": branch,
        
            "day": start_datetime.strftime(
                "%Y-%m-%d"
            ),
        
            "page": 1,
            "limit": 5000
        }
        print("Params:", params)
        
        try:

            response = requests.get(
                sales_url,
                headers=headers(),
                params=params,
                timeout=180
            )

            print(
                "Status:",
                response.status_code
            )

            print(
                "Response:",
                response.text[:500]
            )

            if (
                response.status_code
                != 200
            ):
                continue

            response_json = (
                response.json()
            )

            data = (
                response_json
                .get("data", [])
            )

            if not data:

                print(
                    "No Data:",
                    branch
                )
                continue

            df = pd.json_normalize(
                data
            )

            
            # =========================================
            # SAFE BRANCH CODE
            # =========================================

            if (
                "branchCode"
                not in df.columns
            ):

                if (
                    "branch"
                    in df.columns
                ):

                    df[
                        "branchCode"
                    ] = df[
                        "branch"
                    ]

            # =========================================
            # TAG DATASET
            # =========================================

            df["DATASET"] = tag

            all_sales.append(df)

        except Exception as e:

            print(
                f"❌ Error {branch}:",
                str(e)
            )

    # =====================================================
    # FINAL CONCAT
    # =====================================================

    if not all_sales:

        print(
            f"❌ No {tag} Data"
        )

        return pd.DataFrame()

    final_df = pd.concat(
        all_sales,
        ignore_index=True
    )

    print(
        f"✅ {tag} Rows:",
        len(final_df)
    )

    return final_df

# =========================================================
# BUSINESS WINDOW
# =========================================================

from zoneinfo import ZoneInfo

ist_now = datetime.now(
    ZoneInfo("Asia/Kolkata")
)

print("🕒 Current Time:", ist_now)

# =========================================================
# BUSINESS DATE
# =========================================================

if ist_now.hour < 9:

    business_date = (
        ist_now.date()
        - timedelta(days=1)
    )

else:

    business_date = (
        ist_now.date()
    )

print(
    "📅 Business Date:",
    business_date
)

# =========================================================
# CURRENT WINDOW
# 9 AM → CURRENT HOUR
# SUPPORTS AFTER MIDNIGHT
# =========================================================

from zoneinfo import ZoneInfo
from datetime import datetime, timedelta

ist = ZoneInfo("Asia/Kolkata")

ist_now = datetime.now(ist)

# =========================================================
# BUSINESS DATE LOGIC
# AFTER MIDNIGHT → STILL SAME BUSINESS DAY
# =========================================================

if ist_now.hour < 9:

    business_date = (
        ist_now.date()
        - timedelta(days=1)
    )

else:

    business_date = (
        ist_now.date()
    )

# =========================================================
# WINDOW START
# =========================================================

current_window_start = datetime.combine(
    business_date,
    datetime.min.time()
).replace(
    hour=9,
    minute=0,
    second=0,
    tzinfo=ist
)

# =========================================================
# WINDOW END
# =========================================================

current_window_end = ist_now.replace(
    minute=59,
    second=59,
    microsecond=0
)

# =========================================================
# LAST WEEK WINDOW
# =========================================================

lw_window_start = (
    current_window_start
    - timedelta(days=7)
)

lw_window_end = (
    current_window_end
    - timedelta(days=7)
)

print(
    "🟢 Current Window:",
    current_window_start,
    "to",
    current_window_end
)

print(
    "🟡 LW Window:",
    lw_window_start,
    "to",
    lw_window_end
)
# =========================================================
# FETCH DATA
# =========================================================

current_df = fetch_sales_window(
    current_window_start,
    current_window_end,
    "CURRENT"
)

lw_df = fetch_sales_window(
    lw_window_start,
    lw_window_end,
    "LW"
)

# =========================================================
# FLATTEN ITEMS
# =========================================================

def flatten_items(df):

    print("📦 Flattening Item Data...")

    # ==========================================
    # EMPTY CHECK
    # ==========================================

    if df is None or len(df) == 0:

        print("⚠ Empty dataframe")

        return pd.DataFrame()

    # ==========================================
    # COLUMN CHECK
    # ==========================================

    if "items" not in df.columns:

        print("❌ 'items' column missing")

        print("Available Columns:")
        print(df.columns.tolist())

        return pd.DataFrame()

    # ==========================================
    # NULL ITEMS CHECK
    # ==========================================

    df["items"] = df["items"].apply(
        lambda x: x
        if isinstance(x, list)
        else []
    )

    # ==========================================
    # EXPLODE ITEMS
    # ==========================================

    df = df.explode("items")

    # ==========================================
    # REMOVE EMPTY ITEMS
    # ==========================================

    df = df[
        df["items"].notna()
    ].copy()

    if df.empty:

        print("⚠ No item rows found")

        return pd.DataFrame()

    # ==========================================
    # NORMALIZE JSON
    # ==========================================

    item_df = pd.json_normalize(
        df["items"]
    )

    print("ITEM DF COLUMNS")
    print(item_df.columns.tolist())
    
    print("ITEM SAMPLE")
    print(item_df.head(3))

    # ==========================================
    # COMBINE
    # ==========================================

    df = pd.concat(
        [
            df.drop(columns=["items"])
            .reset_index(drop=True),

            item_df.reset_index(drop=True)
        ],
        axis=1
    )

    print(
        f"✅ Flatten Completed | Rows: {len(df)}"
    )

    return df

# =========================================================
# CREATE ITEM LEVEL DATA
# =========================================================

current_df = flatten_items(
    current_df
)

lw_df = flatten_items(
    lw_df
)

print(
    "✅ Item Level Data Ready"
)

# =========================================================
# DEBUG FLATTEN OUTPUT
# =========================================================

print("CURRENT SAMPLE DATA")

print("CURRENT ITEM COLUMNS")
print(
[c for c in current_df.columns
if "item_" in str(c)]
)

print("LW ITEM COLUMNS")
print(
[c for c in lw_df.columns
if "item_" in str(c)]
)

print("CURRENT DUPLICATE COLS")
print(
current_df.columns[
current_df.columns.duplicated()
]
)

print("LW DUPLICATE COLS")
print(
lw_df.columns[
lw_df.columns.duplicated()
]
)

print("CURRENT ITEM SAMPLE")
print(
current_df[
[
c for c in [
"item_quantity",
"item_baseGrossAmount",
"item_baseNetAmount",
"item_baseNetDiscountAmount"
]
if c in current_df.columns
]
].head(20)
)


sample_cols = [
    "item_shortName",
    "item_quantity",
    "item_baseGrossAmount",
    "item_baseNetAmount",
    "item_baseNetDiscountAmount"
]

available_cols = [
    c for c in sample_cols
    if c in current_df.columns
]

print(
    current_df[
        available_cols
    ].head(10)
)

print("LW SAMPLE DATA")

available_cols_lw = [
    c for c in sample_cols
    if c in lw_df.columns
]

print(
    lw_df[
        available_cols_lw
    ].head(10)
)
# =========================================================
# REMOVE DUPLICATE COLUMNS
# =========================================================

current_df = current_df.loc[
    :,
    ~current_df.columns.duplicated()
]

lw_df = lw_df.loc[
    :,
    ~lw_df.columns.duplicated()
]

print(
    "✅ Duplicate Columns Removed"
)

# =========================================================
# CHECK IMPORTANT ITEM COLUMNS
# =========================================================

required_cols = {
    "item_quantity": 0,
    "item_baseNetAmount": 0,
    "item_baseNetDiscountAmount": 0,
    "item_discounts": "",
    "item_shortName": "",
    "item_categoryName": ""
}

for col, default_value in required_cols.items():

    # ---------------- CURRENT ---------------- #

    if col in current_df.columns:

        print(
            f"✅ Current Found: {col}"
        )

    else:

        print(
            f"❌ Current Missing: {col}"
        )

        current_df[col] = (
            default_value
        )

    # ---------------- LW ---------------- #

    if col in lw_df.columns:

        print(
            f"✅ LW Found: {col}"
        )

    else:

        print(
            f"❌ LW Missing: {col}"
        )

        lw_df[col] = (
            default_value
        )

# =========================================================
# CONCAT SALES DATA
# =========================================================

sales_df = pd.concat(
    [
        current_df,
        lw_df
    ],
    ignore_index=True,
    sort=False
)

print(
    "✅ Total Rows:",
    len(sales_df)
)

# =========================================================
# DEBUG
# =========================================================

print("📋 API Columns:")
print(
    sales_df.columns.tolist()
)

print("CURRENT DF COLUMNS")
print(
    current_df.columns.tolist()
)

print("LW DF COLUMNS")
print(
    lw_df.columns.tolist()
)

# =========================================================
# STANDARD DATAFRAME NAMES
# =========================================================

current_sales = current_df.copy()

lw_sales = lw_df.copy()

print("✅ Sales DataFrames Created")

# =========================================================

# FIX ITEM METRIC VALUES

# =========================================================

column_mapping = {
"quantity": "item_quantity",
"baseGrossAmount": "item_baseGrossAmount",
"baseNetAmount": "item_baseNetAmount",
"baseNetDiscountAmount": "item_baseNetDiscountAmount",
"shortName": "item_shortName",
"categoryName": "item_categoryName",
"createdTime": "item_createdTime"
}

for source_col, target_col in column_mapping.items():

# ---------------- CURRENT ---------------- #

    if source_col in current_sales.columns:
    
        current_sales[target_col] = (
            current_sales[source_col]
        )
    
    else:
    
        if target_col not in current_sales.columns:
    
            current_sales[target_col] = 0
    
    # ---------------- LW ---------------- #
    
    if source_col in lw_sales.columns:
    
        lw_sales[target_col] = (
            lw_sales[source_col]
        )
    
    else:
    
        if target_col not in lw_sales.columns:
    
            lw_sales[target_col] = 0
    
    print("✅ Item Metric Values Fixed")
    
    debug_cols = [
    c for c in [
    "item_quantity",
    "item_baseGrossAmount",
    "item_baseNetAmount",
    "item_baseNetDiscountAmount"
    ]
    if c in current_sales.columns
    ]
    
    print("CURRENT SALES SAMPLE")
    
    print(
    current_sales[
    debug_cols
    ].head(10)
    )
    
    print("LW SALES SAMPLE")
    
    debug_cols_lw = [
    c for c in [
    "item_quantity",
    "item_baseGrossAmount",
    "item_baseNetAmount",
    "item_baseNetDiscountAmount"
    ]
    if c in lw_sales.columns
    ]
    
    print(
    lw_sales[
    debug_cols_lw
    ].head(10)
    )


# =========================================================
# EMPTY CHECK
# =========================================================

if sales_df.empty:

    print("❌ No Sales Data")
    exit()



# =========================================================
# SAFE COLUMN CREATION
# =========================================================

required_cols = {
    "branchCode": "",
    "branchName": "",
    "brandName": "",
    "invoiceNumber": "",
    "invoiceDay": "",
    "channel": "",
    "status": "",
    "item_shortName": "",
    "item_categoryName": "",
    "item_quantity": 0,
    "item_createdTime": "",
    "item_baseGrossAmount": 0,
    "item_baseNetDiscountAmount": 0,
    "item_baseNetAmount": 0,
    "item_discounts": "",
    "discounts": "",
    "totalMaterialCost": 0
}

for col, default in required_cols.items():

    if col not in sales_df.columns:

        sales_df[col] = default

print("✅ Safe Columns Created")


# =========================================================
# CLEAN BRANCH CODE
# =========================================================

sales_df["branchCode"] = (
    sales_df["branchCode"]
    .astype(str)
    .str.strip()
)


# =========================================================
# MERGE HELP SHEET
# =========================================================

help_merge = help_df[
    [
        "branchCode",
        "Store Name",
        "Region",
        "Help Channel",
        "Help Source"
    ]
].copy()

sales_df = sales_df.merge(
    help_merge,
    on="branchCode",
    how="left"
)

print("✅ Help Sheet Merged")


# =========================================================
# NUMERIC CLEANING
# =========================================================

numeric_cols = [
    "item_quantity",
    "item_baseGrossAmount",
    "item_baseNetDiscountAmount",
    "item_baseNetAmount",
    "totalMaterialCost"
]

for col in numeric_cols:

    sales_df[col] = pd.to_numeric(
        sales_df[col],
        errors="coerce"
    ).fillna(0)


# =========================================================
# DISCOUNT POSITIVE
# =========================================================

sales_df[
    "item_baseNetDiscountAmount"
] = (
    sales_df[
        "item_baseNetDiscountAmount"
    ]
    .abs()
)


# =========================================================
# CLEAN ITEM NAME
# =========================================================

sales_df["item_shortName"] = (
    sales_df["item_shortName"]
    .astype(str)
    .str.strip()
    .str.upper()
)

item_df["Item Name"] = (
    item_df["Item Name"]
    .astype(str)
    .str.strip()
    .str.upper()
)


# =========================================================
# ITEM GROUP MERGE
# =========================================================

sales_df = sales_df.merge(
    item_df[
        [
            "Item Name",
            "Item Group Name",
            "Product Mix",
            "Category Group"
        ]
    ],
    left_on="item_shortName",
    right_on="Item Name",
    how="left"
)

print("✅ Item Group Merged")


# =========================================================
# REMOVE ADDONS
# =========================================================

sales_df = sales_df[
    sales_df["Product Mix"]
    .astype(str)
    .str.upper()
    != "ADDONS"
].copy()

print(
    "✅ AddOns Removed:",
    len(sales_df)
)


# =========================================================
# CHANNEL GROUPING
# =========================================================

def channel_group(x):

    x = str(x).upper()

    if "SWIGGY" in x:
        return "Swiggy"

    elif "ZOMATO" in x:
        return "Zomato"

    elif (
        "POS" in x
        or "DINE"
        in x
    ):
        return "In Store"

    elif (
        "OWNLY" in x
        or "WEBSITE"
        in x
    ):
        return "Ownly"

    return "Others"


sales_df["Channel Group"] = (
    sales_df["channel"]
    .apply(channel_group)
)

print("✅ Channel Group Created")


# =========================================================
# DISCOUNT EXTRACTION
# =========================================================

def extract_zomato_discount(x):

    x = str(x)

    match = re.search(
        r"Merchant Voucher Code\s*\((.*?)\)",
        x
    )

    if match:
        return match.group(1)

    match2 = re.search(
        r"'name':\s*'(.*?)'",
        x
    )

    if match2:
        return match2.group(1)

    return "No Offer"


def extract_swiggy_discount(x):

    x = str(x)

    match = re.search(
        r"Restaurant Discount\s*\((.*?)\)",
        x
    )

    if match:
        return match.group(1)

    return "No Offer"


sales_df[
    "Zomato Discount Code"
] = sales_df[
    "discounts"
].apply(
    extract_zomato_discount
)

sales_df[
    "Swiggy Discount Code"
] = sales_df[
    "item_discounts"
].apply(
    extract_swiggy_discount
)

print("✅ Discount Codes Extracted")


# =========================================================
# CLOSED / OPEN / VOIDED
# =========================================================

sales_df["status"] = (
    sales_df["status"]
    .astype(str)
    .str.upper()
)

closed_df = sales_df[
    sales_df["status"]
    == "CLOSED"
].copy()

open_orders = (
    sales_df[
        sales_df["status"]
        == "OPEN"
    ]["invoiceNumber"]
    .nunique()
)

voided_orders = (
    sales_df[
        sales_df["status"]
        == "VOIDED"
    ]["invoiceNumber"]
    .nunique()
)

print(
    "✅ Closed Rows:",
    len(closed_df)
)

print(
    "🟡 Open Orders:",
    open_orders
)

print(
    "🔴 Voided Orders:",
    voided_orders
)


# =========================================================
# ORDER TIME CONVERSION
# =========================================================

closed_df["Order Time"] = pd.to_datetime(
    closed_df["createdDate"],
    utc=True,
    errors="coerce"
).dt.tz_convert("Asia/Kolkata")

print("✅ Order Time Converted")


# =========================================================
# CURRENT WINDOW FILTER
# =========================================================

current_sales = closed_df[
    (
        closed_df["Order Time"]
        >= current_window_start
    )
    &
    (
        closed_df["Order Time"]
        <= current_window_end
    )
].copy()


# =========================================================
# LAST WEEK WINDOW FILTER
# =========================================================

lw_sales = closed_df[
    (
        closed_df["Order Time"]
        >= lw_window_start
    )
    &
    (
        closed_df["Order Time"]
        <= lw_window_end
    )
].copy()


print(
    "✅ Current Rows:",
    len(current_sales)
)

print(
    "✅ LW Rows:",
    len(lw_sales)
)

# =========================================================
# DEBUG CHECK - CURRENT VS LW SALES
# =========================================================

print("CURRENT SALES")
print(current_sales.shape)

print(
    current_sales[
        [
            "brandName",
            "Order Time"
        ]
    ]
    .head(20)
)

print("LW SALES")
print(lw_sales.shape)

print(
    lw_sales[
        [
            "brandName",
            "Order Time"
        ]
    ]
    .head(20)
)

print(
    "✅ Current Rows:",
    len(current_sales)
)

print(
    "✅ LW Rows:",
    len(lw_sales)
)

print("Current Brands:")
print(
    current_sales["brandName"]
    .value_counts(dropna=False)
)

print("LW Brands:")
print(
    lw_sales["brandName"]
    .value_counts(dropna=False)
)

print("Current Columns:")
print(
    current_sales.columns.tolist()
)

print("LW Columns:")
print(
    lw_sales.columns.tolist()
)

print(
    "Current Time Range:",
    current_sales["Order Time"].min(),
    "to",
    current_sales["Order Time"].max()
)

print(
    "LW Time Range:",
    lw_sales["Order Time"].min(),
    "to",
    lw_sales["Order Time"].max()
)


# =========================================================
# ITEM GROUP FOR WINDOW DATA
# =========================================================

merge_cols = [
    "Item Name",
    "Item Group Name",
    "Product Mix",
    "Category Group"
]

current_sales["item_shortName"] = (
    current_sales["item_shortName"]
    .astype(str)
    .str.strip()
    .str.upper()
)

lw_sales["item_shortName"] = (
    lw_sales["item_shortName"]
    .astype(str)
    .str.strip()
    .str.upper()
)

current_sales = current_sales.merge(
    item_df[merge_cols],
    left_on="item_shortName",
    right_on="Item Name",
    how="left"
)

lw_sales = lw_sales.merge(
    item_df[merge_cols],
    left_on="item_shortName",
    right_on="Item Name",
    how="left"
)

print("✅ Product Mix Merged")
print("CURRENT PRODUCT MIX RELATED COLUMNS")
print(
    [
        c for c in current_sales.columns
        if "Product Mix" in str(c)
    ]
)

print("LW PRODUCT MIX RELATED COLUMNS")
print(
    [
        c for c in lw_sales.columns
        if "Product Mix" in str(c)
    ]
)

# =========================================================
# SAFE PRODUCT MIX
# =========================================================

for df in [current_df, lw_df]:

    if "Product Mix" not in df.columns:

        print("⚠ Product Mix Missing")

        print(df.columns.tolist())

        df["Product Mix"] = "Others"

    df["Product Mix"] = (
        df["Product Mix"]
        .fillna("Others")
        .astype(str)
        .str.strip()
    )

print("✅ Product Mix Safe")


# =========================================================
# FIX PRODUCT MIX FOR OVERALL DASHBOARD
# =========================================================

for df_name, df in [
    ("Current", current_sales),
    ("LW", lw_sales)
]:

    if "Product Mix" not in df.columns:

        product_mix_cols = [
            c for c in df.columns
            if "Product Mix" in str(c)
        ]

        print(
            f"{df_name} Product Mix Columns:",
            product_mix_cols
        )

        if product_mix_cols:

            df["Product Mix"] = None

            for col in product_mix_cols:

                df["Product Mix"] = (
                    df["Product Mix"]
                    .combine_first(df[col])
                )

        else:

            df["Product Mix"] = "Others"

    df["Product Mix"] = (
        df["Product Mix"]
        .fillna("Others")
        .astype(str)
        .str.strip()
    )

print("✅ Product Mix Fixed For Overall")



# =========================================================
# HOURLY COMPARISON DASHBOARD
# CURRENT VS LAST WEEK
# =========================================================

def create_hourly_dashboard(
    current_sales,
    lw_sales
):

    brands = {
    "Frozen Bottle": "Frozen Bottle",
    "Boba Bar": "Boba Bar",
    "Madno": "Madno",
    "Lubov": "Lubov- Patisserie"
    }

    final_output = []

    for brand, brand_filter in brands.items():

        print(f"Brand: {brand}")

        # =============================================
        # FILTER BRAND
        # =============================================

        curr = current_sales[
            current_sales["brandName"]
            .astype(str)
            .str.strip()
            .str.upper()
            .str.contains(
                brand_filter.upper(),
                na=False
            )
        ].copy()

        lw = lw_sales[
            lw_sales["brandName"]
            .astype(str)
            .str.strip()
            .str.upper()
            .str.contains(
                brand_filter.upper(),
                na=False
            )
        ].copy()

        # =============================================
        # DEBUG
        # =============================================

        print(f"Brand: {brand}")

        print("Curr Columns:")
        print(curr.columns.tolist())
        
        print("LW Columns:")
        print(lw.columns.tolist())
        
        print("Curr Rows:", len(curr))
        print("LW Rows:", len(lw))

        # =============================================
        # FIX MISSING METRIC COLUMNS
        # =============================================

        required_cols = [
            "item_quantity",
            "item_baseNetAmount",
            "item_baseNetDiscountAmount"
        ]

        for col in required_cols:

            if col not in curr.columns:

                print(
                    f"⚠ Missing in Current: {col}"
                )

                curr[col] = 0

            if col not in lw.columns:

                print(
                    f"⚠ Missing in LW: {col}"
                )

                lw[col] = 0
    

        # =================================================
        # CURRENT METRICS
        # =================================================

        current_orders = (
            curr["invoiceNumber"]
            .nunique()
        )

        current_qty = (
            curr["item_quantity"]
            .fillna(0)
            .sum()
        )

        current_gross = (
            curr["item_baseGrossAmount"]
            .fillna(0)
            .sum()
        )

        current_discount = abs(
            curr["item_baseNetDiscountAmount"]
            .fillna(0)
            .sum()
        )

        current_net = (
            curr["item_baseNetAmount"]
            .fillna(0)
            .sum()
        )

        current_aov = (
            current_net /
            current_orders
            if current_orders > 0
            else 0
        )

        # =================================================
        # LAST WEEK METRICS
        # =================================================

        lw_orders = (
            lw["invoiceNumber"]
            .nunique()
        )

        lw_qty = (
            lw["item_quantity"]
            .fillna(0)
            .sum()
        )

        lw_gross = (
            lw["item_baseGrossAmount"]
            .fillna(0)
            .sum()
        )

        lw_discount = abs(
            lw["item_baseNetDiscountAmount"]
            .fillna(0)
            .sum()
        )

        lw_net = (
            lw["item_baseNetAmount"]
            .fillna(0)
            .sum()
        )

        lw_aov = (
            lw_net /
            lw_orders
            if lw_orders > 0
            else 0
        )

        # =================================================
        # GROWTH %
        # =================================================

        def growth(curr, prev):

            if prev == 0:
                return 0

            return round(
                ((curr - prev) / prev)
                * 100,
                1
            )

        final_output.append({

            "Brand": brand,

            "Orders":
            current_orders,

            "LW Orders":
            lw_orders,

            "Orders Growth %":
            growth(
                current_orders,
                lw_orders
            ),

            "Qty":
            round(current_qty, 0),

            "LW Qty":
            round(lw_qty, 0),

            "Qty Growth %":
            growth(
                current_qty,
                lw_qty
            ),

            "Gross Rev":
            round(current_gross, 0),

            "LW Gross":
            round(lw_gross, 0),

            "Gross Growth %":
            growth(
                current_gross,
                lw_gross
            ),

            "Discount":
            round(current_discount, 0),

            "LW Discount":
            round(lw_discount, 0),

            "Discount Growth %":
            growth(
                current_discount,
                lw_discount
            ),

            "Net Rev":
            round(current_net, 0),

            "LW Net Rev":
            round(lw_net, 0),

            "Net Growth %":
            growth(
                current_net,
                lw_net
            ),

            "AOV":
            round(current_aov, 1),

            "LW AOV":
            round(lw_aov, 1),

            "AOV Growth %":
            growth(
                current_aov,
                lw_aov
            )
        })

    dashboard = pd.DataFrame(
        final_output
    )

    return dashboard


# =========================================================
# CREATE DASHBOARD
# =========================================================

hourly_dashboard = (
    create_hourly_dashboard(
        current_sales,
        lw_sales
    )
)

print("✅ Hourly Dashboard Created")
print(hourly_dashboard.head())

# =========================================================
# FIX DASHBOARD COLUMNS
# =========================================================

dashboard_cols = [
    "Product Mix",
    "Category Group",
    "Item Group Name"
]

for col in dashboard_cols:

    x_col = f"{col}_x"
    y_col = f"{col}_y"

    if (
        col not in current_sales.columns
        and x_col in current_sales.columns
    ):

        current_sales[col] = (
            current_sales[x_col]
            .combine_first(
                current_sales.get(y_col)
            )
        )

    if (
        col not in lw_sales.columns
        and x_col in lw_sales.columns
    ):

        lw_sales[col] = (
            lw_sales[x_col]
            .combine_first(
                lw_sales.get(y_col)
            )
        )

print("✅ Dashboard Columns Fixed")

# =========================================================
# FIX PRODUCT MIX COLUMN
# =========================================================

def fix_product_mix(df, df_name):

    print(f"🔍 Checking Product Mix for {df_name}")

    product_mix_cols = [
        c for c in df.columns
        if "Product Mix" in str(c)
    ]

    print(
        f"{df_name} Product Mix Columns:",
        product_mix_cols
    )

    # create blank column first
    df["Product Mix"] = None

    # combine available columns
    for col in product_mix_cols:

        df["Product Mix"] = (
            df["Product Mix"]
            .combine_first(df[col])
        )

    # fallback
    df["Product Mix"] = (
        df["Product Mix"]
        .fillna("Others")
        .astype(str)
        .str.strip()
    )

    print(
        f"✅ {df_name} Product Mix Fixed"
    )

    print(
        df["Product Mix"]
        .value_counts()
        .head(10)
    )

    return df


# =========================================================
# APPLY PRODUCT MIX FIX
# =========================================================

current_sales = fix_product_mix(
    current_sales,
    "Current"
)

lw_sales = fix_product_mix(
    lw_sales,
    "LW"
)


# =========================================================
# PRODUCT MIX DASHBOARD
# =========================================================

def create_product_mix_dashboard(
    current_sales,
    lw_sales
):

    brands = {
        "Frozen Bottle": "Frozen Bottle",
        "Boba Bar": "Boba Bar",
        "Madno": "Madno",
        "Lubov": "Lubov- Patisserie"
    }

    product_mix_dashboard = {}

    for brand, brand_filter in brands.items():

        curr = current_sales[
            current_sales["brandName"]
            .astype(str)
            .str.upper()
            ==
            brand_filter.upper()
        ].copy()

        lw = lw_sales[
            lw_sales["brandName"]
            .astype(str)
            .str.upper()
            ==
            brand_filter.upper()
        ].copy()

        print("================================")
        print("Brand:", brand)
        print("Current Rows:", len(curr))
        print("LW Rows:", len(lw))
        print(
            "Current Product Mix Exists:",
            "Product Mix" in curr.columns
        )
        print(
            "LW Product Mix Exists:",
            "Product Mix" in lw.columns
        )
        print("================================")

        # =============================================
        # REQUIRED METRIC COLUMNS
        # =============================================

        required_cols = [
            "item_quantity",
            "item_baseNetAmount",
            "item_baseNetDiscountAmount",
            "item_baseGrossAmount"
        ]

        for col in required_cols:

            if col not in curr.columns:
                curr[col] = 0

            if col not in lw.columns:
                lw[col] = 0

        # numeric conversion
        for col in required_cols:

            curr[col] = pd.to_numeric(
                curr[col],
                errors="coerce"
            ).fillna(0)

            lw[col] = pd.to_numeric(
                lw[col],
                errors="coerce"
            ).fillna(0)

        # =============================================
        # CURRENT MIX
        # =============================================

        curr_mix = (
            curr.groupby("Product Mix")
            .agg(
                Orders=(
                    "invoiceNumber",
                    "nunique"
                ),

                **{
                    "Gross Rev": (
                        "item_baseGrossAmount",
                        "sum"
                    ),

                    "Qty Sold": (
                        "item_quantity",
                        "sum"
                    ),

                    "Net Rev": (
                        "item_baseNetAmount",
                        "sum"
                    ),

                    "Discount": (
                        "item_baseNetDiscountAmount",
                        lambda x:
                        abs(x.sum())
                    )
                }
            )
            .reset_index()
        )

        curr_mix["Dis %"] = np.where(
            curr_mix["Gross Rev"] > 0,
            (
                curr_mix["Discount"]
                /
                curr_mix["Gross Rev"]
            ) * 100,
            0
        )

        # =============================================
        # LW MIX
        # =============================================

        lw_mix = (
            lw.groupby("Product Mix")
            .agg(
                **{
                    "LW Orders": (
                        "invoiceNumber",
                        "nunique"
                    ),

                    "LW Gross Rev": (
                        "item_baseGrossAmount",
                        "sum"
                    ),

                    "LW Qty Sold": (
                        "item_quantity",
                        "sum"
                    ),

                    "LW Net Rev": (
                        "item_baseNetAmount",
                        "sum"
                    ),

                    "LW Discount": (
                        "item_baseNetDiscountAmount",
                        lambda x:
                        abs(x.sum())
                    )
                }
            )
            .reset_index()
        )

        lw_mix["LW Dis %"] = np.where(
            lw_mix["LW Gross Rev"] > 0,
            (
                lw_mix["LW Discount"]
                /
                lw_mix["LW Gross Rev"]
            ) * 100,
            0
        )

        # =============================================
        # MERGE
        # =============================================

        final_mix = curr_mix.merge(
            lw_mix,
            on="Product Mix",
            how="left"
        ).fillna(0)

        # =============================================
        # GROWTH %
        # =============================================

        final_mix["Growth %"] = np.where(
            final_mix["LW Net Rev"] > 0,
            (
                (
                    final_mix["Net Rev"]
                    -
                    final_mix["LW Net Rev"]
                )
                /
                final_mix["LW Net Rev"]
            ) * 100,
            0
        )

        final_mix = (
            final_mix
            .sort_values(
                "Net Rev",
                ascending=False
            )
            .round(0)
        )

        product_mix_dashboard[
            brand
        ] = final_mix

    return product_mix_dashboard

# =========================================================
# SOURCE + REGION PRODUCT MIX DASHBOARD
# =========================================================

def create_source_region_dashboard(
    current_sales,
    lw_sales
):

    dashboard = {}

    # =====================================================
    # SOURCE FILTERS
    # =====================================================

    source_filters = {
        "In Store":
            ~current_sales["channel"]
            .astype(str)
            .str.upper()
            .str.contains(
                "SWIGGY|ZOMATO",
                na=False
            ),

        "Swiggy":
            current_sales["channel"]
            .astype(str)
            .str.upper()
            .str.contains(
                "SWIGGY",
                na=False
            ),

        "Zomato":
            current_sales["channel"]
            .astype(str)
            .str.upper()
            .str.contains(
                "ZOMATO",
                na=False
            )
    }

    for source, curr_filter in source_filters.items():

        # =================================================
        # CURRENT SOURCE
        # =================================================

        curr = current_sales[
            curr_filter
        ].copy()

        # =================================================
        # LW SOURCE
        # =================================================

        if source == "In Store":

            lw = lw_sales[
                ~lw_sales["channel"]
                .astype(str)
                .str.upper()
                .str.contains(
                    "SWIGGY|ZOMATO",
                    na=False
                )
            ].copy()

        else:

            lw = lw_sales[
                lw_sales["channel"]
                .astype(str)
                .str.upper()
                .str.contains(
                    source.upper(),
                    na=False
                )
            ].copy()

        # =================================================
        # CURRENT SUMMARY
        # =================================================

        curr_mix = (
            curr.groupby(
                ["Region", "Product Mix"]
            )
            .agg(
                **{
                    "Net Sales": (
                        "item_baseNetAmount",
                        "sum"
                    ),

                    "Qty": (
                        "item_quantity",
                        "sum"
                    ),

                    "Gross Sales": (
                        "item_baseGrossAmount",
                        "sum"
                    ),

                    "Discount": (
                        "item_baseNetDiscountAmount",
                        lambda x:
                        abs(x.sum())
                    )
                }
            )
            .reset_index()
        )

        curr_mix["Dis %"] = np.where(
            curr_mix["Gross Sales"] > 0,
            (
                curr_mix["Discount"]
                /
                curr_mix["Gross Sales"]
            ) * 100,
            0
        )

        curr_mix = curr_mix.drop(
            columns=[
                "Gross Sales",
                "Discount"
            ]
        )

        # =================================================
        # LAST WEEK SUMMARY
        # =================================================

        lw_mix = (
            lw.groupby(
                ["Region", "Product Mix"]
            )
            .agg(
                **{
                    "LW Net Sales": (
                        "item_baseNetAmount",
                        "sum"
                    ),

                    "LW Qty": (
                        "item_quantity",
                        "sum"
                    ),

                    "LW Gross": (
                        "item_baseGrossAmount",
                        "sum"
                    ),

                    "LW Discount": (
                        "item_baseNetDiscountAmount",
                        lambda x:
                        abs(x.sum())
                    )
                }
            )
            .reset_index()
        )

        lw_mix["LW Dis %"] = np.where(
            lw_mix["LW Gross"] > 0,
            (
                lw_mix["LW Discount"]
                /
                lw_mix["LW Gross"]
            ) * 100,
            0
        )

        lw_mix = lw_mix.drop(
            columns=[
                "LW Gross",
                "LW Discount"
            ]
        )

        # =================================================
        # MERGE
        # =================================================

        final_df = curr_mix.merge(
            lw_mix,
            on=[
                "Region",
                "Product Mix"
            ],
            how="left"
        ).fillna(0)

        # =================================================
        # GROWTH %
        # =================================================

        final_df["Sales Growth %"] = np.where(
            final_df["LW Net Sales"] > 0,
            (
                (
                    final_df["Net Sales"]
                    -
                    final_df["LW Net Sales"]
                )
                /
                final_df["LW Net Sales"]
            ) * 100,
            0
        )

        final_df["Qty Growth %"] = np.where(
            final_df["LW Qty"] > 0,
            (
                (
                    final_df["Qty"]
                    -
                    final_df["LW Qty"]
                )
                /
                final_df["LW Qty"]
            ) * 100,
            0
        )

        # =================================================
        # TOP 10 PRODUCT MIX
        # =================================================

        final_df = (
            final_df
            .sort_values(
                "Net Sales",
                ascending=False
            )
            .head(10)
        )

        # =================================================
        # ROUND
        # =================================================

        numeric_cols = final_df.select_dtypes(
            include=np.number
        ).columns

        final_df[numeric_cols] = (
            final_df[numeric_cols]
            .round(0)
            .astype(int)
        )

        dashboard[source] = final_df

    return dashboard
    
# Create Product Mix Dashboard #

product_mix_dashboard = (
    create_product_mix_dashboard(
        current_sales,
        lw_sales
    )
)

print(
    "✅ Product Mix Dashboard Created"
)

for k, v in (
    product_mix_dashboard.items()
):
    print(k, len(v))


# =============================================
# OVERALL PRODUCT MIX
# =============================================

curr = current_sales.copy()
lw = lw_sales.copy()

curr_mix = (
    curr.groupby("Product Mix")
    .agg(
        Orders=("invoiceNumber", "nunique"),
        Qty_Sold=("item_quantity", "sum"),
        Gross_Rev=("item_baseGrossAmount", "sum"),
        Net_Rev=("item_baseNetAmount", "sum"),
        Discount=("item_baseNetDiscountAmount", lambda x: abs(x.sum()))
    )
    .reset_index()
)

curr_mix["Dis %"] = np.where(
    curr_mix["Gross_Rev"] > 0,
    (
        curr_mix["Discount"]
        /
        curr_mix["Gross_Rev"]
    ) * 100,
    0
).round(1)

lw_mix = (
    lw.groupby("Product Mix")
    .agg(
        LW_Orders=("invoiceNumber", "nunique"),
        LW_Qty_Sold=("item_quantity", "sum"),
        LW_Gross_Rev=("item_baseGrossAmount", "sum"),
        LW_Net_Rev=("item_baseNetAmount", "sum"),
        LW_Discount=("item_baseNetDiscountAmount", lambda x: abs(x.sum()))
    )
    .reset_index()
)

overall_mix = curr_mix.merge(
    lw_mix,
    on="Product Mix",
    how="left"
).fillna(0)

overall_mix["Growth %"] = np.where(
    overall_mix["LW_Net_Rev"] > 0,
    (
        (
            overall_mix["Net_Rev"]
            -
            overall_mix["LW_Net_Rev"]
        )
        /
        overall_mix["LW_Net_Rev"]
    ) * 100,
    0
).round(1)

product_mix_dashboard[
    "Overall"
] = overall_mix.sort_values(
    "Net_Rev",
    ascending=False
)

print("✅ Overall Product Mix Added")


# =========================================================
# FIX ITEM METRIC COLUMNS
# =========================================================

metric_cols = [
    "item_quantity",
    "item_baseNetAmount",
    "item_baseGrossAmount",
    "item_baseNetDiscountAmount"
]

for col in metric_cols:

    x_col = f"{col}_x"
    y_col = f"{col}_y"

    # CURRENT SALES
    if (
        col not in current_sales.columns
        and x_col in current_sales.columns
    ):

        current_sales[col] = (
            current_sales[x_col]
            .combine_first(
                current_sales.get(y_col)
            )
        )

    # LW SALES
    if (
        col not in lw_sales.columns
        and x_col in lw_sales.columns
    ):

        lw_sales[col] = (
            lw_sales[x_col]
            .combine_first(
                lw_sales.get(y_col)
            )
        )

    # NUMERIC CONVERSION
    current_sales[col] = pd.to_numeric(
        current_sales[col],
        errors="coerce"
    ).fillna(0)

    lw_sales[col] = pd.to_numeric(
        lw_sales[col],
        errors="coerce"
    ).fillna(0)

print("✅ Item Metric Columns Fixed")

# =========================================================
# DEBUG CHECK
# =========================================================

print(
    current_sales[
        [
            "item_quantity",
            "item_baseNetAmount",
            "item_baseNetDiscountAmount"
        ]
    ].head()
)

print("CURRENT SALES PRODUCT MIX CHECK")
print(
    [col for col in current_sales.columns
     if "Product Mix" in col]
)

print("LW SALES PRODUCT MIX CHECK")
print(
    [col for col in lw_sales.columns
     if "Product Mix" in col]
)


# =========================================================
# CATEGORY DASHBOARD
# =========================================================

def create_category_dashboard(
    current_sales,
    lw_sales
):

    brands = {
    "Frozen Bottle": "Frozen Bottle",
    "Boba Bar": "Boba Bar",
    "Madno": "Madno",
    "Lubov": "Lubov- Patisserie"
    }

    category_dashboard = {}

    for brand, brand_filter in brands.items():

        # =============================================
        # BRAND FILTER
        # =============================================

        curr = current_sales[
            current_sales["brandName"]
            .astype(str)
            .str.strip()
            .str.upper()
            ==
            brand_filter.upper()
        ].copy()
        
        
        lw = lw_sales[
            lw_sales["brandName"]
            .astype(str)
            .str.strip()
            .str.upper()
            ==
            brand_filter.upper()
        ].copy()

        # =============================================
        # CURRENT MIX
        # =============================================
        
        curr_cat = (
            curr.groupby("Category Group")
            .agg(
                **{
                    "Orders": (
                        "invoiceNumber",
                        "nunique"
                    ),
        
                    "Qty Sold": (
                        "item_quantity",
                        "sum"
                    ),
        
                    "Net Rev": (
                        "item_baseNetAmount",
                        "sum"
                    ),

                    "Gross Rev": (
                        "item_baseGrossAmount",
                        "sum"
                    ),
        
                    "Discount": (
                        "item_baseNetDiscountAmount",
                        lambda x: abs(x.sum())
                    )
                }
            )
            .reset_index()
        )
        
        curr_cat["Dis %"] = np.where(
            curr_cat["Gross Rev"] > 0,
            (
                curr_cat["Discount"]
                /
                curr_cat["Gross Rev"]
            ) * 100,
            0
        ).round(1)
        
        

        # =============================================
        # LAST WEEK CATEGORY
        # =============================================
        
        lw_cat = (
            lw.groupby(
                "Category Group"
            )
            .agg(
                **{
                    "LW Net Rev": (
                        "item_baseNetAmount",
                        "sum"
                    ),

                    "LW Gross Rev": (
                        "item_baseGrossAmount",
                        "sum"
                    ),
        
                    "LW Qty": (
                        "item_quantity",
                        "sum"
                    ),
        
                    "LW Orders": (
                        "invoiceNumber",
                        "nunique"
                    ),
        
                    "LW Discount": (
                        "item_baseNetDiscountAmount",
                        lambda x:
                        abs(x.sum())
                    )
                }
            )
            .reset_index()
        )
        
        lw_cat["LW Dis %"] = np.where(
            lw_cat["LW Gross Rev"] > 0,
            (
                lw_cat["LW Discount"]
                /
                lw_cat["LW Gross Rev"]
            ) * 100,
            0
        ).round(1)
        
        lw_cat = lw_cat.drop(
            columns="LW Discount"
        )

        # =============================================
        # MERGE CURRENT + LW
        # =============================================

        final_cat = curr_cat.merge(
            lw_cat,
            on="Category Group",
            how="left"
        )

        final_cat = (
            final_cat
            .fillna(0)
        )

        # =============================================
        # GROWTH %
        # =============================================
        
        final_cat["Growth %"] = np.where(
            final_cat["LW Net Rev"] > 0,
            (
                (
                    final_cat["Net Rev"]
                    -
                    final_cat["LW Net Rev"]
                )
                /
                final_cat["LW Net Rev"]
            ) * 100,
            0
        ).round(1)

        # =============================================
        # SORT
        # =============================================

        final_cat = (
            final_cat
            .sort_values(
                "Net Rev",
                ascending=False
            )
        )

        # round values
        numeric_cols = [
            "Net Rev",
            "Qty Sold",
            "Orders",
            "Dis %",
            "Growth %",
            "LW Net Rev",
            "LW Qty",
            "LW Orders",
            "LW Dis %"
        ]

        final_cat[
            numeric_cols
        ] = (
            final_cat[
                numeric_cols
            ]
            .round(1)
        )

        category_dashboard[
            brand
        ] = final_cat

    return category_dashboard

# =========================================================
# FIX CATEGORY GROUP COLUMN
# =========================================================

if (
    "Category Group" not in current_sales.columns
    and
    "Category Group_x" in current_sales.columns
):

    current_sales["Category Group"] = (
        current_sales["Category Group_x"]
        .combine_first(
            current_sales.get(
                "Category Group_y"
            )
        )
    )

if (
    "Category Group" not in lw_sales.columns
    and
    "Category Group_x" in lw_sales.columns
):

    lw_sales["Category Group"] = (
        lw_sales["Category Group_x"]
        .combine_first(
            lw_sales.get(
                "Category Group_y"
            )
        )
    )

print("✅ Category Group Fixed")

# =========================================================
# CREATE CATEGORY DASHBOARD
# =========================================================

category_dashboard = (
    create_category_dashboard(
        current_sales,
        lw_sales
    )
)

print(
    "✅ Category Dashboard Created"
)

for k, v in (
    category_dashboard.items()
):
    print(
        k,
        len(v)
    )

# =========================================================
# TOP 15 ITEM LEVEL DASHBOARD
# =========================================================

def create_item_dashboard(
    current_sales,
    lw_sales
):

    brands = {
    "Frozen Bottle": "Frozen Bottle",
    "Boba Bar": "Boba Bar",
    "Madno": "Madno",
    "Lubov": "Lubov- Patisserie"
    }

    item_dashboard = {}

    for brand, brand_filter in brands.items():

        # =============================================
        # FILTER BRAND
        # =============================================

        curr = current_sales[
            current_sales["brandName"]
            .astype(str)
            .str.upper()
            ==
            brand_filter.upper()
        ].copy()

        lw = lw_sales[
            lw_sales["brandName"]
            .astype(str)
            .str.upper()
            ==
            brand_filter.upper()
        ].copy()

        # =============================================
        # CURRENT ITEM LEVEL
        # =============================================

        curr_item = (
            curr.groupby(
                "Item Group Name"
            )
            .agg(
                **{
                    "Net Rev": (
                        "item_baseNetAmount",
                        "sum"
                    ),

                    "Gross Rev": (
                        "item_baseGrossAmount",
                        "sum"
                    ),

                    "Qty Sold": (
                        "item_quantity",
                        "sum"
                    ),

                    "Orders": (
                        "invoiceNumber",
                        "nunique"
                    ),

                    "Discount": (
                        "item_baseNetDiscountAmount",
                        lambda x:
                        abs(x.sum())
                    )
                }
            )
            .reset_index()
        )

        curr_item["Dis %"] = np.where(
            curr_item["Gross Rev"] > 0,
            (
                curr_item["Discount"]
                /
                curr_item["Gross Rev"]
            ) * 100,
            0
        ).round(1)

        curr_item = curr_item.drop(
            columns="Discount"
        )

        # =============================================
        # LAST WEEK ITEM LEVEL
        # =============================================
        
        lw_item = (
            lw.groupby(
                "Item Group Name"
            )
            .agg(
                **{
                    "LW Net Rev": (
                        "item_baseNetAmount",
                        "sum"
                    ),

                    "LW Gross Rev": (
                        "item_baseGrossAmount",
                        "sum"
                    ),
        
                    "LW Qty": (
                        "item_quantity",
                        "sum"
                    ),
        
                    "LW Orders": (
                        "invoiceNumber",
                        "nunique"
                    ),
        
                    "LW Discount": (
                        "item_baseNetDiscountAmount",
                        lambda x:
                        abs(x.sum())
                    )
                }
            )
            .reset_index()
        )
        
        lw_item["LW Dis %"] = np.where(
            lw_item["LW Gross Rev"] > 0,
            (
                lw_item["LW Discount"]
                /
                lw_item["LW Gross Rev"]
            ) * 100,
            0
        ).round(1)
        
        lw_item = lw_item.drop(
            columns="LW Discount"
        )
        
        # =============================================
        # MERGE
        # =============================================
        
        final_item = curr_item.merge(
            lw_item,
            on="Item Group Name",
            how="left"
        )
        
        final_item = (
            final_item
            .fillna(0)
        )

        # =============================================
        # GROWTH %
        # =============================================
        
        final_item["Growth %"] = np.where(
            final_item["LW Net Rev"] > 0,
            (
                (
                    final_item["Net Rev"]
                    -
                    final_item["LW Net Rev"]
                )
                /
                final_item["LW Net Rev"]
            ) * 100,
            0
        ).round(1)
        # =============================================
        # SORT BY NET REV
        # =============================================

        final_item = (
            final_item
            .sort_values(
                "Net Rev",
                ascending=False
            )
        )

        # =============================================
        # TOP 15 ONLY
        # =============================================

        final_item = (
            final_item
            .head(15)
        )

 
        # =============================================
        # ROUND VALUES
        # =============================================

        numeric_cols = [
            "Net Rev",
            "Qty Sold",
            "Orders",
            "Dis %",
            "Growth %",
            "LW Net Rev",
            "LW Orders",
            "LW Dis %"
        ]

        final_item[
            numeric_cols
        ] = (
            final_item[
                numeric_cols
            ]
            .round(1)
        )

        item_dashboard[
            brand
        ] = final_item

    return item_dashboard

# =========================================================
# FIX ITEM GROUP NAME COLUMN
# =========================================================

if (
    "Item Group Name" not in current_sales.columns
    and
    "Item Group Name_x" in current_sales.columns
):

    current_sales["Item Group Name"] = (
        current_sales["Item Group Name_x"]
        .combine_first(
            current_sales.get(
                "Item Group Name_y"
            )
        )
    )

if (
    "Item Group Name" not in lw_sales.columns
    and
    "Item Group Name_x" in lw_sales.columns
):

    lw_sales["Item Group Name"] = (
        lw_sales["Item Group Name_x"]
        .combine_first(
            lw_sales.get(
                "Item Group Name_y"
            )
        )
    )

print("✅ Item Group Name Fixed")

# =========================================================
# CREATE ITEM DASHBOARD
# =========================================================

item_dashboard = (
    create_item_dashboard(
        current_sales,
        lw_sales
    )
)

print(
    "✅ Item Dashboard Created"
)

for k, v in (
    item_dashboard.items()
):
    print(
        k,
        len(v)
    )

# =========================================================
# CREATE SOURCE REGION DASHBOARD
# =========================================================

source_region_dashboard = (
    create_source_region_dashboard(
        current_sales,
        lw_sales
    )
)

print(
    "✅ Source Region Dashboard Created"
)

for k, v in (
    source_region_dashboard.items()
):
    print(
        k,
        len(v)
    )
    
# =========================================================
# DISCOUNT CODE EXTRACTION
# =========================================================

import ast
import re


def extract_swiggy_code(x):

    try:

        if pd.isna(x):
            return "No Offer"

        if isinstance(x, str):

            x = ast.literal_eval(x)

        if not x:
            return "No Offer"

        first = x[0]

        name = first.get(
            "name",
            ""
        )

        # Example:
        # Restaurant Discount (15% off)

        match = re.search(
            r"\((.*?)\)",
            name
        )

        if match:
            return match.group(1)

        return name

    except:
        return "No Offer"


def extract_zomato_code(x):

    try:

        if pd.isna(x):
            return "No Offer"

        if isinstance(x, str):

            x = ast.literal_eval(x)

        if not x:
            return "No Offer"

        first = x[0]

        name = first.get(
            "name",
            ""
        )

        # Merchant Voucher Code (TRYNEW)

        match = re.search(
            r"Merchant Voucher Code\s*\((.*?)\)",
            name
        )

        if match:
            return match.group(1)

        return name

    except:
        return "No Offer"

# =========================================================
# FIX ITEM METRIC COLUMNS
# =========================================================

for col in [
    "item_quantity",
    "item_baseNetAmount",
    "item_baseNetDiscountAmount"
]:

    if (
        col not in current_sales.columns
        and
        f"{col}_x" in current_sales.columns
    ):

        current_sales[col] = (
            current_sales[f"{col}_x"]
            .combine_first(
                current_sales.get(
                    f"{col}_y"
                )
            )
        )

    # convert numeric
    current_sales[col] = pd.to_numeric(
        current_sales[col],
        errors="coerce"
    ).fillna(0)

print("✅ Item Metric Columns Fixed")


# =========================================================
# FIX DISCOUNT COLUMNS
# =========================================================

if (
    "item_discounts" not in current_sales.columns
    and
    "item_discounts_x" in current_sales.columns
):

    current_sales["item_discounts"] = (
        current_sales["item_discounts_x"]
        .combine_first(
            current_sales.get(
                "item_discounts_y"
            )
        )
    )

if (
    "discounts" not in current_sales.columns
    and
    "discounts_x" in current_sales.columns
):

    current_sales["discounts"] = (
        current_sales["discounts_x"]
        .combine_first(
            current_sales.get(
                "discounts_y"
            )
        )
    )

print("✅ Discount Columns Fixed")
# =========================================================
# CREATE CODE COLUMNS
# =========================================================

current_sales["Swiggy Code"] = (
    current_sales["item_discounts"]
    .apply(extract_swiggy_code)
)

current_sales["Zomato Code"] = (
    current_sales["discounts"]
    .apply(extract_zomato_code)
)
print("✅ Discount Codes Extracted")

# =========================================================
# DASHBOARD FUNCTION
# =========================================================

def create_discount_dashboard(
    current_sales,
    lw_sales,
    code_col,
    channel_name
):

    brands = {
        "Frozen Bottle": "Frozen Bottle",
        "Boba Bar": "Boba Bar",
        "Madno": "Madno",
        "Lubov": "Lubov- Patisserie"
    }

    dashboard = {}

    for brand, brand_filter in brands.items():

        # =====================================================
        # CURRENT SALES
        # =====================================================

        temp = current_sales[
            (
                current_sales["brandName"]
                .astype(str)
                .str.upper()
                ==
                brand_filter.upper()
            )
            &
            (
                current_sales["channel"]
                .astype(str)
                .str.upper()
                .str.contains(
                    channel_name.upper(),
                    na=False
                )
            )
        ].copy()

        # =====================================================
        # LAST WEEK SALES
        # =====================================================

        lw_temp = lw_sales[
            (
                lw_sales["brandName"]
                .astype(str)
                .str.upper()
                ==
                brand_filter.upper()
            )
            &
            (
                lw_sales["channel"]
                .astype(str)
                .str.upper()
                .str.contains(
                    channel_name.upper(),
                    na=False
                )
            )
        ].copy()

        # =====================================================
        # CURRENT SUMMARY
        # =====================================================

        code_df = (
            temp.groupby(code_col)
            .agg(
                **{
                    "Orders": (
                        "invoiceNumber",
                        "nunique"
                    ),

                    "Qty Sold": (
                        "item_quantity",
                        "sum"
                    ),

                    "Gross Rev": (
                        "item_baseGrossAmount",
                        "sum"
                    ),

                    "Net Rev": (
                        "item_baseNetAmount",
                        "sum"
                    ),

                    "Discount Given": (
                        "item_baseNetDiscountAmount",
                        lambda x:
                        abs(x.sum())
                    )
                }
            )
            .reset_index()
        )

        # =====================================================
        # CURRENT DIS %
        # =====================================================

        code_df["Dis %"] = np.where(
            code_df["Gross Rev"] > 0,
            (
                code_df["Discount Given"]
                /
                code_df["Gross Rev"]
            ) * 100,
            0
        ).round(1)

        # =====================================================
        # CURRENT AOV
        # =====================================================

        code_df["AOV"] = np.where(
            code_df["Orders"] > 0,
            code_df["Net Rev"]
            /
            code_df["Orders"],
            0
        ).round(1)

        # =====================================================
        # LW SUMMARY
        # =====================================================

        lw_code_df = (
            lw_temp.groupby(code_col)
            .agg(
                **{
                    "LW Orders": (
                        "invoiceNumber",
                        "nunique"
                    ),

                    "LW Qty": (
                        "item_quantity",
                        "sum"
                    ),

                    "LW Gross Rev": (
                        "item_baseGrossAmount",
                        "sum"
                    ),

                    "LW Net Rev": (
                        "item_baseNetAmount",
                        "sum"
                    ),

                    "LW Discount": (
                        "item_baseNetDiscountAmount",
                        lambda x:
                        abs(x.sum())
                    )
                }
            )
            .reset_index()
        )

        # =====================================================
        # MERGE CURRENT + LW
        # =====================================================

        code_df = code_df.merge(
            lw_code_df,
            on=code_col,
            how="left"
        ).fillna(0)

        # =====================================================
        # GROWTH %
        # =====================================================
        
        code_df["Growth %"] = np.where(
            code_df["LW Net Rev"] > 0,
            (
                (
                    code_df["Net Rev"]
                    -
                    code_df["LW Net Rev"]
                )
                /
                code_df["LW Net Rev"]
            ) * 100,
            0
        ).round(1)

        # =====================================================
        # LW DIS %
        # =====================================================

        code_df["LW Dis %"] = np.where(
            code_df["LW Net Rev"] > 0,
            (
                code_df["LW Discount"]
                /
                code_df["LW Net Rev"]
            ) * 100,
            0
        ).round(1)

        # =====================================================
        # LW AOV
        # =====================================================

        code_df["LW AOV"] = np.where(
            code_df["LW Orders"] > 0,
            code_df["LW Net Rev"]
            /
            code_df["LW Orders"],
            0
        ).round(1)

        # =====================================================
        # SORT
        # =====================================================

        code_df = (
            code_df
            .sort_values(
                "Orders",
                ascending=False
            )
            .head(15)
        )

        # =====================================================
        # FINAL COLUMN ORDER
        # =====================================================

        code_df = code_df[
            [
                code_col,
                "Orders",
                "Qty Sold",
                "Net Rev",
                "Discount Given",
                "Dis %",
                "AOV",
                "Growth %",
                "LW Orders",
                "LW Qty",
                "LW Net Rev",
                "LW Discount",
                "LW Dis %",
                "LW AOV"
            ]
        ]

        dashboard[brand] = code_df

    return dashboard



# =========================================================
# SWIGGY DASHBOARD
# =========================================================

swiggy_discount_dashboard = (
    create_discount_dashboard(
        current_sales,
        lw_sales,
        "Swiggy Discount Code",
        "Swiggy"
    )
)

print(
    "✅ Swiggy Discount Dashboard Created"
)

# =========================================================
# ZOMATO DASHBOARD
# =========================================================

zomato_discount_dashboard = (
    create_discount_dashboard(
        current_sales,
        lw_sales,
        "Zomato Discount Code",
        "Zomato"
    )
)

print(
    "✅ Zomato Discount Dashboard Created"
)

# =========================================================
# CHECK
# =========================================================

for k, v in (
    swiggy_discount_dashboard.items()
):
    print(
        "Swiggy",
        k,
        len(v)
    )

for k, v in (
    zomato_discount_dashboard.items()
):
    print(
        "Zomato",
        k,
        len(v)
    )



# =========================================================
# GOOGLE SHEET DASHBOARD UPDATE
# =========================================================

DASHBOARD_SPREADSHEET_ID = (
    "1ldgnNMdeubDx_ImtCC1uCD7FGz8gt_edmWEkmO_xRNk"
)

dashboard_spreadsheet = (
    client.open_by_key(
        DASHBOARD_SPREADSHEET_ID
    )
)

print("✅ Dashboard File Connected")


# =========================================================
# SAFE TAB FUNCTION
# =========================================================

def get_or_create_sheet(sheet_name):

    try:
        ws = dashboard_spreadsheet.worksheet(
            sheet_name
        )

    except:

        ws = (
            dashboard_spreadsheet
            .add_worksheet(
                title=sheet_name,
                rows=1000,
                cols=50
            )
        )

    return ws


# =========================================================
# UPDATE SHEET FUNCTION
# =========================================================

def update_sheet(
    sheet_name,
    df,
    start_cell="A1"
):

    ws = get_or_create_sheet(
        sheet_name
    )

    # refresh tab
    ws.clear()

    if df.empty:

        ws.update(
            values=data,
            range_name=start_cell
        )

        return

    data = (
        [df.columns.tolist()]
        +
        df.fillna("")
        .values
        .tolist()
    )

    ws.update(
        start_cell,
        data
    )

    print(
        f"✅ Updated: {sheet_name}"
    )


# =========================================================
# 1. HOURLY SUMMARY
# =========================================================

update_sheet(
    "Hourly Summary",
    hourly_dashboard
)


# =========================================================
# 2. PRODUCT MIX
# =========================================================

product_ws = get_or_create_sheet(
    "Product Mix Dashboard"
)

product_ws.clear()

row_num = 1

for brand, df in (
    product_mix_dashboard.items()
):

    product_ws.update(
        f"A{row_num}",
        [[brand]]
    )

    row_num += 1

    data = (
        [df.columns.tolist()]
        +
        df.fillna("")
        .values.tolist()
    )

    product_ws.update(
        f"A{row_num}",
        data
    )

    row_num += len(df) + 4

print(
    "✅ Product Mix Updated"
)

time.sleep(5)

# =========================================================
# 3. CATEGORY DASHBOARD
# =========================================================

category_ws = get_or_create_sheet(
    "Category Dashboard"
)

category_ws.clear()

row_num = 1

for brand, df in (
    category_dashboard.items()
):

    category_ws.update(
        f"A{row_num}",
        [[brand]]
    )

    row_num += 1

    data = (
        [df.columns.tolist()]
        +
        df.fillna("")
        .values.tolist()
    )

    category_ws.update(
        f"A{row_num}",
        data
    )

    row_num += len(df) + 4

print(
    "✅ Category Dashboard Updated"
)

time.sleep(5)

# =========================================================
# 4. TOP ITEMS
# =========================================================

item_ws = get_or_create_sheet(
    "Top 15 Items"
)

item_ws.clear()

row_num = 1

for brand, df in (
    item_dashboard.items()
):

    item_ws.update(
        f"A{row_num}",
        [[brand]]
    )

    row_num += 1

    data = (
        [df.columns.tolist()]
        +
        df.fillna("")
        .values.tolist()
    )

    item_ws.update(
        f"A{row_num}",
        data
    )

    row_num += len(df) + 4

print(
    "✅ Item Dashboard Updated"
)

time.sleep(5)

# =========================================================
# 5. SWIGGY DISCOUNT
# =========================================================

swiggy_ws = get_or_create_sheet(
    "Swiggy Discount Dashboard"
)

swiggy_ws.clear()

row_num = 1

for brand, df in (
    swiggy_discount_dashboard.items()
):

    swiggy_ws.update(
        f"A{row_num}",
        [[brand]]
    )

    row_num += 1

    data = (
        [df.columns.tolist()]
        +
        df.fillna("")
        .values.tolist()
    )

    swiggy_ws.update(
        f"A{row_num}",
        data
    )

    row_num += len(df) + 4

print(
    "✅ Swiggy Dashboard Updated"
)

time.sleep(5)
# =========================================================
# 6. ZOMATO DISCOUNT
# =========================================================

zomato_ws = get_or_create_sheet(
    "Zomato Discount Dashboard"
)

zomato_ws.clear()

row_num = 1

for brand, df in (
    zomato_discount_dashboard.items()
):

    zomato_ws.update(
        f"A{row_num}",
        [[brand]]
    )

    row_num += 1

    data = (
        [df.columns.tolist()]
        +
        df.fillna("")
        .values.tolist()
    )

    zomato_ws.update(
        f"A{row_num}",
        data
    )

    row_num += len(df) + 4

print(
    "✅ Zomato Dashboard Updated"
)

print(
    "✅ ALL DASHBOARDS REFRESHED"
)

time.sleep(5)
# =========================================================
# HOURLY WINDOW
# =========================================================

from datetime import datetime
import pytz

ist = pytz.timezone("Asia/Kolkata")

current_time = datetime.now(ist)

start_hour = "09:00 AM"
end_hour = current_time.strftime("%I:%M %p")

hourly_window = (
    f"{start_hour} - {end_hour}"
)

print(
    "⏰ Hourly Window:",
    hourly_window
)

# =========================================================
# FORMAT DATE
# =========================================================

formatted_date = (
    business_date
    .strftime("%d-%m-%Y")
)

print(
    f"📅 Formatted Date: "
    f"{formatted_date}"
)

# =========================================================
# DISCOUNT HTML TABLE
# =========================================================

def create_discount_html(
    dashboard,
    title
):

    html = f"""
    <hr>
    <h3>{title}</h3>
    """

    for brand, df in dashboard.items():

        html += f"""
        <h4>{brand}</h4>
        """

        if df.empty:

            html += """
            <p>No Data Available</p>
            """
            continue

        html += apply_growth_style(df)

        html += "<br>"

    return html


# =========================================================
# APPLY GROWTH STYLE
# =========================================================

def apply_growth_style(df):

    html = """
    <table border="0"
    style="
    border-collapse:collapse;
    width:100%;
    text-align:center;
    font-family:Arial;
    font-size:12px;
    ">
    """

    # ==========================================
    # HEADER
    # ==========================================

    html += "<tr>"

    for col in df.columns:

        html += f"""
        <th style="
        background:#1f4e78;
        color:white;
        padding:8px;
        border:1px solid #ddd;
        text-align:center;
        ">
        {col}
        </th>
        """

    html += "</tr>"

    # ==========================================
    # ROWS
    # ==========================================

    for _, row in df.iterrows():

        html += "<tr>"

        for col in df.columns:

            val = row[col]

            # ==========================================
            # REMOVE DECIMALS
            # ==========================================
            
            try:
            
                if isinstance(val, (int, float)):
            
                    val = round(val)
            
            except:
                pass
            # Growth Highlight
            if "Growth" in col:

                color = (
                    "#d9ead3"
                    if val >= 0
                    else "#f4cccc"
                )

                html += f"""
                <td style="
                background:{color};
                padding:6px;
                border:1px solid #ddd;
                text-align:center;
                font-weight:bold;
                ">
                {val}%
                </td>
                """

            else:

                html += f"""
                <td style="
                padding:6px;
                border:1px solid #ddd;
                text-align:center;
                ">
                {val}
                </td>
                """

        html += "</tr>"

    html += "</table><br>"

    return html


# =========================================================
# CREATE HTML SUMMARY
# =========================================================

print("📄 Creating Summary HTML...")


# =========================================================
# TIME WINDOW
# =========================================================

from datetime import datetime
from zoneinfo import ZoneInfo

ist = ZoneInfo("Asia/Kolkata")

current_time = datetime.now(ist)

start_hour = "09:00 AM"

end_hour = (
    current_time
    .replace(
        minute=0,
        second=0,
        microsecond=0
    )
    .strftime("%I:%M %p")
)

hourly_window = (
    f"{start_hour} - {end_hour}"
)

print(
    "⏰ Hourly Window:",
    hourly_window
)

table_style = """
<style>

table {
    border-collapse: collapse;
    width: 100%;
    text-align: center;
    font-size: 12px;
}

th {
    background: #1f4e78;
    color: white;
    padding: 8px;
    border: 1px solid #ddd;
}

td {
    padding: 6px;
    border: 1px solid #ddd;
    text-align: center;
}

.growth-positive {
    background-color: #d9ead3;
    font-weight: bold;
}

.growth-negative {
    background-color: #f4cccc;
    font-weight: bold;
}

</style>
"""


# =========================================================
# HTML BODY
# =========================================================

summary_html = f"""
<html>

<head>
{table_style}
</head>

<body>

<h2>📊 Product Level Sales Dashboard</h2>

<p>
<b>Business Date:</b> {business_date}
</p>

<p>
<b>Hourly Window:</b>
{hourly_window}
</p>

<hr>

<h3>📈 Hourly Summary</h3>

{apply_growth_style(hourly_dashboard)}

<hr>

<h3>🌍 Source + Region Product Mix Dashboard</h3>
"""

# =========================================================
# SOURCE + REGION DASHBOARD
# =========================================================

for source, df in source_region_dashboard.items():

    summary_html += f"""
    <h4>{source}</h4>
    """

    if df.empty:

        summary_html += """
        <p>No Data Available</p>
        """

    else:

        summary_html += (
            apply_growth_style(
                df.head(10)
            )
        )

summary_html += """
<hr>

<h3>🍨 Product Mix Dashboard</h3>

"""


# =========================================================
# PRODUCT MIX
# =========================================================

for brand, df in product_mix_dashboard.items():

    summary_html += f"""
    <h4>{brand}</h4>
    """

    if df.empty:

        summary_html += """
        <p>No Data Available</p>
        """

    else:

        summary_html += (
            apply_growth_style(
                df.head(10)
            )
        )


# =========================================================
# CATEGORY DASHBOARD
# =========================================================

summary_html += """
<hr>
<h3>📦 Category Dashboard</h3>
"""

for brand, df in category_dashboard.items():

    summary_html += f"""
    <h4>{brand}</h4>
    """

    if df.empty:

        summary_html += """
        <p>No Data Available</p>
        """

    else:

        summary_html += (
            apply_growth_style(
                df.head(10)
            )
        )


# =========================================================
# TOP ITEMS
# =========================================================

summary_html += """
<hr>
<h3>🏆 Top Items</h3>
"""

for brand, df in item_dashboard.items():

    summary_html += f"""
    <h4>{brand}</h4>
    """

    if df.empty:

        summary_html += """
        <p>No Data Available</p>
        """

    else:

        summary_html += (
            apply_growth_style(
                df.head(10)
            )
        )


# =========================================================
# SWIGGY DISCOUNT DASHBOARD
# =========================================================

summary_html += create_discount_html(
    swiggy_discount_dashboard,
    "🟠 Swiggy Discount Dashboard"
)


# =========================================================
# ZOMATO DISCOUNT DASHBOARD
# =========================================================

summary_html += create_discount_html(
    zomato_discount_dashboard,
    "🔴 Zomato Discount Dashboard"
)


# =========================================================
# HTML CLOSE
# =========================================================

summary_html += """
</body>
</html>
"""

print("✅ Summary HTML Created")


#=========================================================
# HOURLY PRODUCT DASHBOARD MAIL
#=========================================================

import smtplib
import os

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import make_msgid

print("📧 Preparing Hourly Mail...")

EMAIL_USER = os.environ["EMAIL_USER"]
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]

# =========================================================
# RECEIVERS
# =========================================================

to_mails = [
    "mis2@frozenbottle.in"
]


all_recipients = (
    to_mails
)

# =========================================================
# SUBJECT
# =========================================================

mail_subject = (
    f"Hourly Product Level Sales Dashboard _ "
    f"{formatted_date}"
)

# =====================================================
# SUBJECT
# =====================================================

mail_subject = (
    f"📊 FTD & MTD "
    f"Product Dashboard "
    f"| {business_date}"
)
# =====================================================
# CREATE HTML
# =====================================================

ftd_mtd_summary_html = f"""

<html>

<head>

<style>

body {{
    font-family: Arial;
}}

table {{
    border-collapse: collapse;
    width: 100%;
    font-size: 13px;
}}

th {{
    background-color: #000000;
    color: white;
    border: 1px solid #dddddd;
    padding: 8px;
    text-align: center;
}}

td {{
    border: 1px solid #dddddd;
    padding: 6px;
    text-align: center;
}}

h2 {{
    color: #333333;
}}

</style>

</head>

<body>

<h2>
📊 FTD Product Dashboard
</h2>

📊 MTD Product Dashboard
</h2>


</body>

</html>

"""

# =========================================================
# CREATE MAIL
# =========================================================

msg = MIMEMultipart()

msg["From"] = EMAIL_USER
msg["To"] = ", ".join(to_mails)
msg["CC"] = ", ".join(cc_mails)
msg["Subject"] = mail_subject

# =========================================================
# SAME MAIL THREAD FOR ENTIRE DAY
# =========================================================

thread_id = (
    f"<hourly-dashboard-"
    f"{business_date}"
    f"@frozenbottle.in>"
)

msg["Message-ID"] = make_msgid()

msg["In-Reply-To"] = thread_id
msg["References"] = thread_id

# =========================================================
# ATTACH HTML
# =========================================================

msg.attach(
    MIMEText(
        summary_html,
        "html"
    )
)


# =========================================================
# SEND MAIL
# =========================================================

try:

    server = smtplib.SMTP(
        "smtp.gmail.com",
        587
    )

    server.starttls()

    server.login(
        EMAIL_USER,
        EMAIL_PASSWORD
    )

    server.sendmail(
        EMAIL_USER,
        all_recipients,
        msg.as_string()
    )

    server.quit()

    print(
        "✅ Hourly Mail Sent Successfully"
    )

except Exception as e:

    print(
        "❌ Hourly Mail Error:",
        str(e)
    )

# =====================================================
# FALLBACK HTML VARIABLES
# =====================================================

if "ftd_summary_html" not in locals():

    ftd_summary_html = summary_html

if "mtd_summary_html" not in locals():

    mtd_summary_html = summary_html

if "ftd_mtd_summary_html" not in locals():

    ftd_mtd_summary_html = summary_html

print(
    "✅ FTD/MTD HTML Variables Ready"
)
ftd_mtd_summary_html = f"""
<h3>
📊 FTD Product Dashboard
</h3>

{ftd_summary_df.to_html(
    index=False,
    escape=False
)}
"""


# =========================================================
# FTD / MTD MAIL
# RUN ONLY AT 10:00 AM
# =========================================================

import smtplib
from datetime import datetime
from zoneinfo import ZoneInfo

ist_now = datetime.now(
    ZoneInfo("Asia/Kolkata")
)

current_hour = ist_now.hour

# =========================================================
# SEND ONLY AT 10 AM
# =========================================================

if current_hour in [7, 8, 10]:

    print("📧 Preparing FTD/MTD Mail...")

    EMAIL_USER = os.environ[
        "EMAIL_USER"
    ]

    EMAIL_PASSWORD = os.environ[
        "EMAIL_PASSWORD"
    ]

    # =====================================================
    # RECEIVERS
    # =====================================================

    to_mails = [
        "mis2@frozenbottle.in"
    ]


    all_recipients = (
        to_mails
    )

    # =====================================================
    # SUBJECT
    # =====================================================

    mail_subject = (
        f"📊 FTD & MTD "
        f"Product Dashboard "
        f"| {business_date}"
    )

    # Example:
    # 📊 FTD & MTD Product Dashboard
    # | 2026-05-27

    # =====================================================
    # CREATE MAIL
    # =====================================================

    msg = MIMEMultipart()

    msg["From"] = EMAIL_USER

    msg["To"] = ", ".join(
        to_mails
    )


    msg["Subject"] = (
        mail_subject
    )
    
    # =====================================================
    # HTML CHECK
    # =====================================================
    
    if "ftd_mtd_summary_html" not in locals():
    
        print(
            "⚠ FTD/MTD HTML Missing"
        )
    
        ftd_mtd_summary_html = summary_html
    
        print(
            "✅ Using Summary HTML"
        )
    
    # =====================================================
    # ATTACH HTML
    # =====================================================
    
    msg.attach(
        MIMEText(
            ftd_mtd_summary_html,
            "html"
        )
    )
    # =====================================================
    # SEND MAIL
    # =====================================================

    try:

        server = smtplib.SMTP(
            "smtp.gmail.com",
            587
        )

        server.starttls()

        server.login(
            EMAIL_USER,
            EMAIL_PASSWORD
        )

        server.sendmail(
            EMAIL_USER,
            all_recipients,
            msg.as_string()
        )

        server.quit()

        print(
            "✅ FTD/MTD Mail Sent Successfully"
        )

    except Exception as e:

        print(
            "❌ FTD/MTD Mail Error:",
            str(e)
        )

else:

    print(
        "⏭ Skipping FTD/MTD Mail "
        "(Not 10 AM)"
    )

