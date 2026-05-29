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
    business_day,
    tag="CURRENT"
):

    all_sales = []

    print(
        f"\n📦 Fetching {tag}"
    )

    print(
    "Business Day:",
    business_day
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
        
            "day": business_day.strftime(
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
# 9 AM → PREVIOUS COMPLETED HOUR
# =========================================================

current_window_start = datetime.combine(
    business_date,
    datetime.min.time()
).replace(
    hour=9,
    minute=0,
    second=0,
    tzinfo=ZoneInfo("Asia/Kolkata")
)

# previous completed hour
end_hour = ist_now.hour - 1

if end_hour < 0:
    end_hour = 23

current_window_end = datetime.combine(
    ist_now.date(),
    datetime.min.time()
).replace(
    hour=end_hour,
    minute=59,
    second=59,
    tzinfo=ZoneInfo("Asia/Kolkata")
)

# =========================================================
# LAST WEEK SAME WINDOW
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
    business_date,
    "CURRENT"
)

lw_df = fetch_sales_window(
    business_date - timedelta(days=7),
    "LW"
)

sales_df = pd.concat(
    [current_df, lw_df],
    ignore_index=True
)

print(
    "✅ Total Rows:",
    len(sales_df)
)

print("📋 API Columns:")
print(sales_df.columns.tolist())

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
    closed_df["invoiceDate"],
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

print("Before Product Mix Function")
print("Current Columns:")
print(current_sales.columns.tolist())

print("LW Columns:")
print(lw_sales.columns.tolist())

# =========================================================
# FIX PRODUCT MIX COLUMN
# =========================================================

if "Product Mix_x" in current_sales.columns:

    current_sales["Product Mix"] = (
        current_sales["Product Mix_x"]
    )

elif "Product Mix_y" in current_sales.columns:

    current_sales["Product Mix"] = (
        current_sales["Product Mix_y"]
    )


if "Product Mix_x" in lw_sales.columns:

    lw_sales["Product Mix"] = (
        lw_sales["Product Mix_x"]
    )

elif "Product Mix_y" in lw_sales.columns:

    lw_sales["Product Mix"] = (
        lw_sales["Product Mix_y"]
    )

print("✅ Product Mix Fixed")

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

# =========================================================
# SAFE PRODUCT MIX
# =========================================================

for df in [current_df, lw_sales]:

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
# HOURLY COMPARISON DASHBOARD
# CURRENT VS LAST WEEK
# =========================================================

def create_hourly_dashboard(
    current_df,
    lw_sales
):

    brands = [
        "Frozen Bottle",
        "Boba Bar",
        "Madno",
        "Lubov"
    ]

    final_output = []

    for brand in brands:

        # =================================================
        # FILTER BRAND
        # =================================================

        curr = current_sales[
            current_sales["brandName"]
            .astype(str)
            .str.strip()
            .str.upper()
            ==
            brand.upper()
        ].copy()

        lw = lw_sales[
            lw_sales["brandName"]
            .astype(str)
            .str.strip()
            .str.upper()
            ==
            brand.upper()
        ].copy()

        print("Brand:", brand)
        print("Curr Columns:")
        print(curr.columns.tolist())
    
        print("LW Columns:")
        print(lw.columns.tolist())
    

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

hourly_dashboard = create_hourly_dashboard(
    current_sales,
    lw_sales
)

print("✅ Hourly Dashboard Created")
print(hourly_dashboard.head())


# =========================================================
# PRODUCT MIX DASHBOARD
# =========================================================

def create_product_mix_dashboard(
    current_sales,
    lw_sales
):

    print("Function Current Columns")
    print(current_sales.columns.tolist())

    print("Function LW Columns")
    print(lw_sales.columns.tolist())

    brands = [
        "Frozen Bottle",
        "Boba Bar",
        "Madno",
        "Lubov"
    ]

    product_mix_dashboard = {}

    for brand in brands:

        curr = current_df[
            current_df["brandName"]
            .astype(str)
            .str.upper()
            ==
            brand.upper()
        ].copy()
    
        lw = lw_sales[
            lw_sales["brandName"]
            .astype(str)
            .str.upper()
            ==
            brand.upper()
        ].copy()
    
        required_cols = [
            "item_quantity",
            "item_baseNetAmount",
            "item_baseNetDiscountAmount"
        ]
    
        for col in required_cols:
    
            if col not in curr.columns:
                curr[col] = 0
    
            if col not in lw.columns:
                lw[col] = 0

        # =============================================
        # CURRENT
        # =============================================

        curr_mix = (
            curr.groupby(
                curr["Product Mix"]
                .fillna("Others")
            )
            .agg(
                **{
                    "Net Rev": (
                        "item_baseNetAmount",
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

        curr_mix["Dis %"] = (
            curr_mix["Discount"]
            /
            curr_mix["Net Rev"]
            * 100
        ).round(1)

        curr_mix = curr_mix.drop(
            columns="Discount"
        )

        # =============================================
        # LAST WEEK
        # =============================================

        lw_mix = (
            lw.groupby(
                lw["Product Mix"]
                .fillna("Others")
            )
            .agg(
                **{
                    "LW Net Rev": (
                        "item_baseNetAmount",
                        "sum"
                    ),

                    "LW Qty": (
                        "item_quantity",
                        "sum"
                    ),

                    "LW Orders": (
                        "invoiceNumber",
                        "nunique"
                    )
                }
            )
            .reset_index()
        )

        # =============================================
        # MERGE
        # =============================================

        final_mix = curr_mix.merge(
            lw_mix,
            on="Product Mix",
            how="left"
        )

        final_mix = final_mix.fillna(0)

        final_mix = (
            final_mix
            .sort_values(
                "Net Rev",
                ascending=False
            )
        )

        product_mix_dashboard[
            brand
        ] = final_mix

    return product_mix_dashboard


# =========================================================
# CREATE PRODUCT MIX DASHBOARD
# =========================================================

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

# =========================================================
# CATEGORY DASHBOARD
# =========================================================

def create_category_dashboard(
    current_sales,
    lw_sales
):

    brands = [
        "Frozen Bottle",
        "Boba Bar",
        "Madno",
        "Lubov"
    ]

    category_dashboard = {}

    for brand in brands:

        # =============================================
        # BRAND FILTER
        # =============================================

        curr = current_df[
            current_sales["brandName"]
            .astype(str)
            .str.strip()
            .str.upper()
            ==
            brand.upper()
        ].copy()
        
        
        lw = lw_sales[
            lw_sales["brandName"]
            .astype(str)
            .str.strip()
            .str.upper()
            ==
            brand.upper()
        ].copy()

        # =============================================
        # CURRENT CATEGORY
        # =============================================

        curr_cat = (
            curr.groupby(
                "Category Group"
            )
            .agg(
                **{
                    "Net Rev": (
                        "item_baseNetAmount",
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

        curr_cat["Dis %"] = (
            curr_cat["Discount"]
            /
            curr_cat["Net Rev"]
            * 100
        ).round(1)

        curr_cat = curr_cat.drop(
            columns="Discount"
        )

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

                    "LW Qty": (
                        "item_quantity",
                        "sum"
                    ),

                    "LW Orders": (
                        "invoiceNumber",
                        "nunique"
                    )
                }
            )
            .reset_index()
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
            "LW Net Rev",
            "LW Qty",
            "LW Orders"
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

    brands = [
        "Frozen Bottle",
        "Boba Bar",
        "Madno",
        "Lubov"
    ]

    item_dashboard = {}

    for brand in brands:

        # =============================================
        # FILTER BRAND
        # =============================================

        curr = current_df[
            current_df["brandName"]
            .astype(str)
            .str.upper()
            ==
            brand.upper()
        ].copy()

        lw = lw_sales[
            lw_sales["brandName"]
            .astype(str)
            .str.upper()
            ==
            brand.upper()
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

        curr_item["Dis %"] = (
            curr_item["Discount"]
            /
            curr_item["Net Rev"]
            * 100
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

                    "LW Qty": (
                        "item_quantity",
                        "sum"
                    ),

                    "LW Orders": (
                        "invoiceNumber",
                        "nunique"
                    )
                }
            )
            .reset_index()
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
            "LW Net Rev",
            "LW Qty",
            "LW Orders"
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
# CREATE CODE COLUMNS
# =========================================================

current_df["Swiggy Code"] = (
    current_df["item_discounts"]
    .apply(
        extract_swiggy_code
    )
)

current_df["Zomato Code"] = (
    current_df["discounts"]
    .apply(
        extract_zomato_code
    )
)

print("✅ Discount Codes Extracted")

# =========================================================
# DASHBOARD FUNCTION
# =========================================================

def create_discount_dashboard(
    df,
    code_col,
    channel_name
):

    brands = [
        "Frozen Bottle",
        "Boba Bar",
        "Madno",
        "Lubov"
    ]

    dashboard = {}

    for brand in brands:

        temp = df[
            (
                df["brandName"]
                .astype(str)
                .str.upper()
                ==
                brand.upper()
            )
            &
            (
                df["channel"]
                .astype(str)
                .str.upper()
                .str.contains(
                    channel_name.upper(),
                    na=False
                )
            )
        ].copy()

        code_df = (
            temp.groupby(
                code_col
            )
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

                    "Discount Given": (
                        "item_baseNetDiscountAmount",
                        lambda x:
                        abs(x.sum())
                    )
                }
            )
            .reset_index()
        )

        code_df["AOV"] = (
            code_df["Net Rev"]
            /
            code_df["Orders"]
        ).round(1)

        code_df = (
            code_df
            .sort_values(
                "Orders",
                ascending=False
            )
            .head(15)
        )

        dashboard[
            brand
        ] = code_df

    return dashboard


# =========================================================
# SWIGGY DASHBOARD
# =========================================================

swiggy_discount_dashboard = (
    create_discount_dashboard(
        current_sales,
        "Swiggy Code",
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
        "Zomato Code",
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
            start_cell,
            [["No Data"]]
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

# =========================================================
# SEND EMAIL
# =========================================================

import smtplib

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

print("📧 Preparing Mail...")

EMAIL_USER = os.environ["EMAIL_USER"]
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]

# =========================================================
# RECEIVERS
# =========================================================

to_mails = [
    "ops.all@frozenbottle.in"
]

cc_mails = [
    "vivek@frozenbottle.in"
]

all_recipients = to_mails + cc_mails

# =========================================================
# MAIL SUBJECT
# =========================================================

mail_subject = (
    f"📊 Product Level Sales Dashboard "
    f"- {business_date}"
)

# Example for hourly
# 📊 Product Level Sales Dashboard - 2026-05-27

# =========================================================
# CREATE MESSAGE
# =========================================================

msg = MIMEMultipart()

msg["From"] = EMAIL_USER
msg["To"] = ", ".join(to_mails)
msg["CC"] = ", ".join(cc_mails)

msg["Subject"] = mail_subject

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

    print("✅ Mail Sent Successfully")

except Exception as e:

    print(
        "❌ Mail Error:",
        str(e)
    )

print("✅ Script Completed")

# =========================================================
# HOURLY PRODUCT DASHBOARD MAIL
# =========================================================

import smtplib

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

print("📧 Preparing Hourly Mail...")

EMAIL_USER = os.environ["EMAIL_USER"]
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]

# =========================================================
# RECEIVERS
# =========================================================

to_mails = [
    "ops.all@frozenbottle.in"
]

cc_mails = [
    "vivek@frozenbottle.in"
]

all_recipients = (
    to_mails +
    cc_mails
)

# =========================================================
# TIME WINDOW
# =========================================================

mail_subject = (
    f"📊 Hourly Product Dashboard "
    f"({start_hour} - {end_hour}) "
    f"| {business_date}"
)

# Example:
# 📊 Hourly Product Dashboard
# (09:00 AM - 01:59 PM)
# | 2026-05-27

# =========================================================
# CREATE MAIL
# =========================================================

msg = MIMEMultipart()

msg["From"] = EMAIL_USER
msg["To"] = ", ".join(to_mails)
msg["CC"] = ", ".join(cc_mails)

msg["Subject"] = mail_subject

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

if current_hour == 10:

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
        "ops.all@frozenbottle.in"
    ]

    cc_mails = [
        "vivek@frozenbottle.in"
    ]

    all_recipients = (
        to_mails +
        cc_mails
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

    msg["CC"] = ", ".join(
        cc_mails
    )

    msg["Subject"] = (
        mail_subject
    )

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

