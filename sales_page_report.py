# =========================================================
# IMPORT
# =========================================================

import os
import json
import time
import jwt
import requests
import pandas as pd

from datetime import (
    datetime,
    timedelta
)

import gspread

from google.oauth2.service_account import (
    Credentials
)

print("🚀 Sales Script Started")


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
        os.environ["GOOGLE_CREDENTIALS"]
    ),

    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)


# =========================================================
# GOOGLE SHEET
# =========================================================

spreadsheet = client.open_by_key(
    "19z6KkVBFoLC33_wcNqVhDLyQEC2dDQ8YQE0gE38BhVg"
)

print("✅ Connected Google Sheet")


# =========================================================
# DATE
# =========================================================

today_date = datetime.now().strftime(
    "%Y-%m-%d"
)

lw_date = (
    datetime.now()
    - timedelta(days=7)
).strftime("%Y-%m-%d")

print(
    "📅 Today Data:",
    today_date
)

print(
    "📅 LW Data:",
    lw_date
)

# =========================================================
# REFRESH SHEET
# =========================================================

def refresh_sheet(sheet_name, df):

    ws = spreadsheet.worksheet(
        sheet_name
    )

    ws.clear()

    if df.empty:

        ws.update(
            [["No Data"]]
        )

        return

    ws.update(
        [df.columns.tolist()]
        + df.values.tolist(),
        value_input_option=
        "USER_ENTERED"
    )

    print(
        f"✅ Refreshed: "
        f"{sheet_name}"
    )

# =========================================================
# HELP SHEET
# =========================================================

help_ws = spreadsheet.worksheet(
    "Help Sheet"
)

help_df = pd.DataFrame(
    help_ws.get_all_records()
)

# =========================================================
# REQUIRED COLUMNS
# =========================================================

required_cols = [
    "branchCode",
    "Ownership"
]

for c in required_cols:

    if c not in help_df.columns:

        help_df[c] = ""


# =========================================================
# FILTER COCO ONLY
# =========================================================

help_df = help_df[
    help_df["Ownership"]
    .astype(str)
    .str.upper()
    == "COCO"
]

# =========================================================
# BRANCH LIST FROM HELP SHEET
# =========================================================

branches = (
    help_df["branchCode"]
    .dropna()
    .astype(str)
    .str.strip()
)

branches = branches[
    branches != ""
]

branches = branches.unique().tolist()

print(
    "🏪 COCO Branch Count:",
    len(branches)
)


# =========================================================
# SALES API
# =========================================================

sales_url = (
    "https://api.ristaapps.com/v1/sales/page"
)


# =========================================================
# FETCH FUNCTION
# =========================================================

def fetch_sales_data(fetch_date):

    all_sales = []

    print(
        f"\n📦 Fetching Data: {fetch_date}"
    )

    for branch in branches:

        print(
            f"Fetching: {branch}"
        )

        params = {
            "branch": branch,
            "day": fetch_date
        }

        try:

            response = requests.get(
                sales_url,
                headers=headers(),
                params=params,
                timeout=120
            )

            if response.status_code != 200:

                print(
                    f"❌ Failed: {branch}"
                )

                continue

            js = response.json()

            data = js.get(
                "data",
                []
            )

            if not data:
                continue

            df = pd.json_normalize(
                data
            )

            all_sales.append(df)

            print(
                f"✅ Rows: {len(df)}"
            )

        except Exception as e:

            print(
                f"❌ Error {branch}: "
                f"{str(e)}"
            )

    if len(all_sales) == 0:

        return pd.DataFrame()

    final_sales = pd.concat(
        all_sales,
        ignore_index=True
    )

    print(
        f"✅ Total Rows "
        f"({fetch_date}):",
        len(final_sales)
    )

    return final_sales


# =========================================================
# FETCH TODAY DATA
# =========================================================

today_raw = fetch_sales_data(
    today_date
)


# =========================================================
# FETCH LW DATA
# =========================================================

lw_raw = fetch_sales_data(
    lw_date
)

# =========================================================
# PROCESS SALES DATA
# =========================================================

def process_sales_data(df):

    if df.empty:
        return pd.DataFrame()

    # =====================================================
    # EXPLODE ITEMS
    # =====================================================

    if "items" in df.columns:

        exploded_df = df.explode(
            "items"
        )

        item_df = pd.json_normalize(
            exploded_df["items"]
        ).add_prefix("item_")

        exploded_df = exploded_df.drop(
            columns=["items"]
        )

        final_df = pd.concat(
            [
                exploded_df.reset_index(
                    drop=True
                ),
                item_df.reset_index(
                    drop=True
                )
            ],
            axis=1
        )

    else:

        final_df = df.copy()

    print(
        "✅ Exploded Rows:",
        len(final_df)
    )

    # =====================================================
    # CHANNEL FILTER
    # =====================================================

    allowed_channels = [

        "Zomato Boba Bar",
        "Zomato Frozen Bottle",
        "Zomato Madno",
        "Zomato Lubov",

        "Swiggy Frozen Bottle",
        "Swiggy Boba Bar",
        "Swiggy Madno",
        "Swiggy Lubov"
    ]

    final_df = final_df[
        final_df["channel"]
        .astype(str)
        .isin(allowed_channels)
    ].copy()

    print(
        "✅ Channel Filter Rows:",
        len(final_df)
    )

# =====================================================
# TIME FORMAT
# =====================================================

def get_time(x):

    try:

        if pd.isna(x) or x == "":
            return ""

        return pd.to_datetime(
            x
        ).strftime("%H:%M:%S")

    except:

        return ""

# =====================================================
# CREATE TIME COLUMNS
# =====================================================

# Order Time = invoiceDate
final_df["Order Time"] = (
    final_df["invoiceDate"]
    .apply(get_time)
)

# Delivery Time = orderReadyTimestamp
final_df["Delivery Time"] = (
    final_df["orderReadyTimestamp"]
    .apply(get_time)
)

# Order Ready Time = delivery.deliveryDate
final_df["Order Ready Time"] = (
    final_df[
        "delivery.deliveryDate"
    ].apply(get_time)
)

# =====================================================
# KPT & O2D
# =====================================================

def calculate_minutes(start, end):

    try:

        if (
            pd.isna(start)
            or pd.isna(end)
            or start == ""
            or end == ""
        ):
            return ""

        start_dt = pd.to_datetime(
            start
        )

        end_dt = pd.to_datetime(
            end
        )

        return int(
            round(
                (
                    end_dt - start_dt
                ).total_seconds() / 60
            )
        )

    except:

        return ""

# KPT = Order → Delivery
final_df["KPT (Mins)"] = final_df.apply(

    lambda x: calculate_minutes(
        x["invoiceDate"],
        x["orderReadyTimestamp"]
    ),

    axis=1
)

# O2D = Order → Ready
final_df["O2D (Mins)"] = final_df.apply(

    lambda x: calculate_minutes(
        x["invoiceDate"],
        x[
            "delivery.deliveryDate"
        ]
    ),

    axis=1
)


# =====================================================
# REQUIRED OUTPUT COLUMNS
# =====================================================

required_columns = [

    "branchName",
    "branchCode",
    "brandName",
    "invoiceNumber",
    "sourceInfo.invoiceNumber",
    "invoiceDate",
    "Order Time",
    "Order Ready Time",
    "Delivery Time",
    "KPT (Mins)",
    "O2D (Mins)",
    "invoiceDay",
    "createdDate",
    "fulfillmentStatus",
    "channel",
    "item_baseGrossAmount",
    "item_baseNetDiscountAmount",
    "item_baseNetAmount",
    "totalMaterialCost",
    "discounts",
    "status",
    "item_skuCode",
    "item_shortName",
    "item_categoryName",
    "item_quantity",
    "item_unitPrice",
    "item_discounts"
]

for col in required_columns:

    if col not in final_df.columns:

        final_df[col] = ""

final_df = final_df[
    required_columns
].copy()

print(
    "✅ Final Columns:",
    len(final_df.columns)
)

return final_df

    # =====================================================
    # REQUIRED OUTPUT COLUMNS
    # =====================================================

required_columns = {

        "branchName": "Store Name",
        "branchCode": "Store Code",
        "brandName": "Brand Name",
        "invoiceNumber": "Inv. No",
        "sourceInfo.invoiceNumber": "Online Inv. No",
        "invoiceDate": "Order Date",
        "Order Time": "Order Time",
        "Order Ready Time": "Order Ready Time",
        "Delivery Time": "Delivery Time",
        "KPT (Mins)": "KPT (Mins)",
        "O2D (Mins)": "O2D (Mins)",
        "invoiceDay": "Business Date",
        "createdDate": "Created Date",
        "fulfillmentStatus": "Fulfillment Status",
        "channel": "Channel",
        "item_baseGrossAmount": "Gross Rev",
        "item_baseNetDiscountAmount": "Discount",
        "item_baseNetAmount": "Net Rev",
        "totalMaterialCost": "Material Cost",
        "discounts": "Zomato Discount Code",
        "status": "Status",
        "item_skuCode": "SKU Code",
        "item_shortName": "Item Name",
        "item_categoryName": "Category Name",
        "item_quantity": "Qty",
        "item_unitPrice": "Unit Price",
        "item_discounts": "Swiggy Discount Code"
}

    # add missing columns
    for col in required_columns.keys():

        if col not in final_df.columns:

            final_df[col] = ""

    # keep only required columns
    output_df = final_df[
        list(required_columns.keys())
    ].copy()

    # rename columns
    output_df.rename(
        columns=required_columns,
        inplace=True
    )

    output_df = (
        output_df.fillna("")
        .astype(str)
    )

    print(
        "✅ Final Output Rows:",
        len(output_df)
    )

    return output_df


# =========================================================
# PROCESS TODAY
# =========================================================

today_df = process_sales_data(
    today_raw
)

# =========================================================
# PROCESS LW
# =========================================================

lw_df = process_sales_data(
    lw_raw
)

# =========================================================
# PUSH TO GOOGLE SHEETS
# =========================================================

refresh_sheet(
    "Today_Data",
    today_df
)

refresh_sheet(
    "LW_Data",
    lw_df
)

# =========================================================
# COMPLETED
# =========================================================

print("🎉 SALES SCRIPT COMPLETED")

print(
    "✅ Today_Data Refreshed"
)

print(
    "✅ LW_Data Refreshed"
)
