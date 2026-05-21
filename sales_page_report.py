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

# Help Sheet

help_ws = spreadsheet.worksheet("Help Sheet")

help_data = help_ws.get("A:H")

if not help_data or len(help_data) < 2:
    help_df = pd.DataFrame()

else:
    headers = help_data[0]
    rows = help_data[1:]

    help_df = pd.DataFrame(rows, columns=headers)

    # 🔥 NORMALIZE ALL COLUMNS (IMPORTANT FIX)
    help_df.columns = (
        help_df.columns
        .astype(str)
        .str.strip()
        .str.lower()
    )

print("📌 Help Sheet Columns:", help_df.columns.tolist())

help_df.columns = (
    help_df.columns
    .str.strip()
    .str.replace(" ", "")
    .str.lower()
)
    
required_cols = [
    "branchcode",
    "ownership",
    "storename",
    "amemail",
    "rmemail",
    "ccmail",
    "region"
]

for c in required_cols:
    if c not in help_df.columns:
        help_df[c] = ""
        
# =========================================================
# FILTER COCO ONLY (FIXED)
# =========================================================

# normalize ownership column check
if "ownership" not in help_df.columns:
    print("❌ ownership column missing in Help Sheet")
    print(help_df.columns.tolist())
    exit()

# filter COCO first
help_df = help_df[
    help_df["ownership"].astype(str).str.upper() == "COCO"
].copy()

# rename AFTER filtering (clean mapping)
help_df.rename(columns={
    "branchcode": "branchCode",
    "storename": "Store Name",
    "amemail": "AM Email",
    "rmemail": "RM Email",
    "ccmail": "CC Mail",
    "region": "Region"
}, inplace=True)


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

    # Order Ready Time = orderReadyTimestamp
    final_df["Order Ready Time"] = (
        final_df[
            "orderReadyTimestamp"
        ].apply(get_time)
    )

    # Delivery Time = delivery.deliveryDate
    final_df["Delivery Time"] = (
        final_df[
            "modifiedDate"
        ].apply(get_time)
    )

    # =====================================================
    # KPT & O2D
    # =====================================================

    def calculate_minutes(
        start,
        end
    ):

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

            mins = int(
                round(
                    (
                        end_dt - start_dt
                    ).total_seconds() / 60
                )
            )

            return max(mins, 0)

        except:

            return ""

    # KPT = Order → Ready
    final_df["KPT (Mins)"] = (
        final_df.apply(

            lambda x:
            calculate_minutes(
                x["invoiceDate"],
                x["orderReadyTimestamp"]
            ),

            axis=1
        )
    )

    # O2D = Order → Delivery
    final_df["O2D (Mins)"] = (
        final_df.apply(

            lambda x:
            calculate_minutes(
                x["invoiceDate"],
                x[
                    "modifiedDate"
                ]
            ),

            axis=1
        )
    )

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
# MERGE HELP SHEET
# =========================================================

help_required_cols = [

    "branchCode",
    "Store Name",
    "AM Email",
    "RM Email",
    "AM Name",
    "CC Mail",
    "Region"
]

for col in help_required_cols:

    if col not in help_df.columns:

        help_df[col] = ""

help_merge = help_df[
    help_required_cols
].copy()

help_merge["branchCode"] = (
    help_merge["branchCode"]
    .astype(str)
    .str.strip()
)

# =========================================================
# MERGE TODAY DATA (FIXED)
# =========================================================

# standardize Rista column → branchCode
today_df.rename(columns={
    "Store Code": "branchCode"
}, inplace=True)

today_df["branchCode"] = (
    today_df["branchCode"]
    .astype(str)
    .str.strip()
)

today_df = today_df.merge(
    help_merge,
    on="branchCode",
    how="left"
)

print(
    "✅ Help Sheet Merged"
)

# =========================================================
# TODAY DATA ONLY
# =========================================================

today_business_date = (
    datetime.now()
    .strftime("%Y-%m-%d")
)

if "Business Date" in today_df.columns:

    today_df = today_df[
        today_df[
            "Business Date"
        ].astype(str)
        == today_business_date
    ].copy()

print(
    "✅ Today Orders:",
    len(today_df)
)

# =========================================================
# KPT CLEAN
# =========================================================

today_df["KPT (Mins)"] = pd.to_numeric(

    today_df["KPT (Mins)"],

    errors="coerce"
)

today_df = today_df[
    today_df["KPT (Mins)"]
    .notna()
].copy()

print(
    "✅ KPT Valid Rows:",
    len(today_df)
)

# =========================================================
# CHANNEL MASTER
# =========================================================

channel_order = [

    "Swiggy Frozen Bottle",
    "Swiggy Boba Bar",
    "Swiggy Madno",
    "Swiggy Lubov",

    "Zomato Frozen Bottle",
    "Zomato Boba Bar",
    "Zomato Madno",
    "Zomato Lubov"
]

# =========================================================
# TOP KPI
# =========================================================

swiggy_df = today_df[
    today_df["Channel"]
    .astype(str)
    .str.contains(
        "Swiggy",
        case=False,
        na=False
    )
].copy()

zomato_df = today_df[
    today_df["Channel"]
    .astype(str)
    .str.contains(
        "Zomato",
        case=False,
        na=False
    )
].copy()

avg_swiggy_kpt = round(

    swiggy_df[
        "KPT (Mins)"
    ].mean(),

    1
)

avg_zomato_kpt = round(

    zomato_df[
        "KPT (Mins)"
    ].mean(),

    1
)

overall_kpt = round(

    today_df[
        "KPT (Mins)"
    ].mean(),

    1
)

total_orders = len(today_df)

top_kpi = pd.DataFrame({

    "Metric": [

        "Avg Swiggy KPT",
        "Avg Zomato KPT",
        "Overall KPT",
        "Total Orders"
    ],

    "Value": [

        avg_swiggy_kpt,
        avg_zomato_kpt,
        overall_kpt,
        total_orders
    ]
})

print("✅ Top KPI Ready")

# =========================================================
# REGION LEVEL
# Channel in Rows | Region in Columns
# =========================================================

region_dashboard = pd.pivot_table(

    today_df,

    values="KPT (Mins)",

    index="Channel",

    columns="Region",

    aggfunc="mean"
)

region_dashboard = (
    region_dashboard
    .round(1)
    .reset_index()
)

# channel order
region_dashboard["sort_order"] = (

    region_dashboard["Channel"]
    .apply(

        lambda x:
        channel_order.index(x)

        if x in channel_order
        else 999
    )
)

region_dashboard = (

    region_dashboard
    .sort_values(
        "sort_order"
    )
    .drop(
        columns="sort_order"
    )
)

print(
    "✅ Region Dashboard Ready"
)

# =========================================================
# REGION STORE LEVEL
# Store in Row | Channel in Column
# =========================================================

store_dashboard = pd.pivot_table(

    today_df,

    values="KPT (Mins)",

    index=[
        "Region",
        "Store Name"
    ],

    columns="Channel",

    aggfunc="mean"
)

store_dashboard = (
    store_dashboard
    .round(1)
    .reset_index()
)

print(
    "✅ Store Dashboard Ready"
)

# =========================================================
# KPT DASHBOARD SHEET
# =========================================================

dashboard_sheet_name = (
    "KPT_Dashboard"
)

try:

    dashboard_ws = (
        spreadsheet.worksheet(
            dashboard_sheet_name
        )
    )

except:

    dashboard_ws = (
        spreadsheet.add_worksheet(
            title=
            dashboard_sheet_name,
            rows="5000",
            cols="50"
        )
    )

dashboard_ws.clear()

print(
    "✅ Dashboard Sheet Ready"
)

# =========================================================
# PUSH TOP KPI
# =========================================================

row_pointer = 1

dashboard_ws.update(

    f"A{row_pointer}",

    [["TOP KPI"]]
)

row_pointer += 1

dashboard_ws.update(

    f"A{row_pointer}",

    [top_kpi.columns.tolist()]
    +
    top_kpi.values.tolist(),

    value_input_option=
    "USER_ENTERED"
)

row_pointer += (
    len(top_kpi)
    + 4
)

print(
    "✅ KPI Uploaded"
)

# =========================================================
# PUSH REGION DASHBOARD
# =========================================================

dashboard_ws.update(

    f"A{row_pointer}",

    [["REGION LEVEL KPT"]]
)

row_pointer += 1

dashboard_ws.update(

    f"A{row_pointer}",

    [
        region_dashboard
        .columns.tolist()
    ]
    +
    region_dashboard
    .fillna("")
    .values.tolist(),

    value_input_option=
    "USER_ENTERED"
)

row_pointer += (
    len(region_dashboard)
    + 4
)

print(
    "✅ Region Dashboard Uploaded"
)

# =========================================================
# PUSH STORE DASHBOARD
# =========================================================

dashboard_ws.update(

    f"A{row_pointer}",

    [["REGION STORE LEVEL KPT"]]
)

row_pointer += 1

dashboard_ws.update(

    f"A{row_pointer}",

    [
        store_dashboard
        .columns.tolist()
    ]
    +
    store_dashboard
    .fillna("")
    .values.tolist(),

    value_input_option=
    "USER_ENTERED"
)

print(
    "✅ Store Dashboard Uploaded"
)

print(
    "✅ KPT Dashboard Refreshed"
)

# =========================================================
# HTML STYLE FUNCTION
# =========================================================

def get_kpt_color(val):

    try:

        val = float(val)

        if val < 10:

            return (
                "background-color:"
                "#d9ead3;"
                "color:#006100;"
            )

        elif val <= 15:

            return (
                "background-color:"
                "#fff2cc;"
                "color:#9c6500;"
            )

        else:

            return (
                "background-color:"
                "#f4cccc;"
                "color:#9c0006;"
            )

    except:

        return ""


def style_dashboard_table(df):

    if df.empty:

        return (
            "<p>No Data</p>"
        )

    html = """
    <table style="
        border-collapse:collapse;
        width:100%;
        font-family:Arial;
        font-size:13px;
    ">
    """

    # HEADER
    html += "<tr>"

    for col in df.columns:

        html += f"""
        <th style="
        background:#1F4E78;
        color:white;
        border:1px solid #d9d9d9;
        padding:8px;
        text-align:center;
        ">
        {col}
        </th>
        """

    html += "</tr>"

    # BODY
    for _, row in df.iterrows():

        html += "<tr>"

        for col in df.columns:

            val = row[col]

            style = ""

            if (
                isinstance(
                    val,
                    (int, float)
                )
                or str(val)
                .replace(".", "")
                .isdigit()
            ):

                style = get_kpt_color(
                    val
                )

            html += f"""
            <td style="
            border:1px solid #d9d9d9;
            padding:8px;
            text-align:center;
            {style}
            ">
            {val}
            </td>
            """

        html += "</tr>"

    html += "</table>"

    return html

print(
    "✅ HTML Style Ready"
)

# =========================================================
# PART 5 : OVERALL SUMMARY MAIL
# =========================================================

import smtplib

from email.mime.text import (
    MIMEText
)

from email.mime.multipart import (
    MIMEMultipart
)

EMAIL_USER = os.environ[
    "EMAIL_USER"
]

EMAIL_PASSWORD = os.environ[
    "EMAIL_PASSWORD"
]

# =========================================================
# TOP SUMMARY
# =========================================================

swiggy_avg = round(
    today_df[
        today_df["Channel"]
        .str.contains(
            "Swiggy",
            case=False,
            na=False
        )
    ]["KPT (Mins)"]
    .astype(float)
    .mean(),
    1
)

zomato_avg = round(
    today_df[
        today_df["Channel"]
        .str.contains(
            "Zomato",
            case=False,
            na=False
        )
    ]["KPT (Mins)"]
    .astype(float)
    .mean(),
    1
)

overall_avg = round(
    today_df[
        "KPT (Mins)"
    ].astype(float).mean(),
    1
)

# =========================================================
# REGION CHANNEL TABLE
# =========================================================

region_channel = pd.pivot_table(

    today_df,

    index="Channel",

    columns="Region",

    values="KPT (Mins)",

    aggfunc="mean"
)

region_channel = (
    region_channel
    .round(1)
    .reset_index()
)

# =========================================================
# REGION STORE TABLE
# =========================================================

region_store_html = ""

for region in sorted(

    today_df["Region"]
    .dropna()
    .unique()

):

    temp = today_df[
        today_df["Region"]
        == region
    ]

    store_table = pd.pivot_table(

        temp,

        index="Store Name",

        columns="Channel",

        values="KPT (Mins)",

        aggfunc="mean"
    )

    store_table = (
        store_table
        .round(1)
        .reset_index()
    )

    region_store_html += f"""

    <h3 style="
    background:#1F4E78;
    color:white;
    padding:8px;
    ">
    {region}
    </h3>

    {style_dashboard_table(store_table)}

    <br>
    """

# =========================================================
# HTML BODY
# =========================================================

summary_html = f"""

<html>

<body style="
font-family:Arial;
">

<h2>
📊 KPT Dashboard
({today_date})
</h2>

<table style="
width:100%;
margin-bottom:20px;
">

<tr>

<td style="
background:#d9ead3;
padding:15px;
border-radius:8px;
text-align:center;
font-size:18px;
font-weight:bold;
">

Swiggy Avg KPT
<br>
{swiggy_avg} mins

</td>

<td style="
background:#cfe2f3;
padding:15px;
border-radius:8px;
text-align:center;
font-size:18px;
font-weight:bold;
">

Zomato Avg KPT
<br>
{zomato_avg} mins

</td>

<td style="
background:#f4cccc;
padding:15px;
border-radius:8px;
text-align:center;
font-size:18px;
font-weight:bold;
">

Overall Avg KPT
<br>
{overall_avg} mins

</td>

</tr>

</table>

<h3>
📍 Region Level
</h3>

{style_dashboard_table(region_channel)}

<br>

<h3>
🏪 Region Store Level
</h3>

{region_store_html}

<br>

<p>
Regards,
<br>
MIS Team
</p>

</body>
</html>
"""

# =========================================================
# PART 6 : CC SUMMARY DASHBOARD MAIL
# =========================================================

cc_mails = []

for x in help_df["CC Mail"].dropna():

    for m in str(x).split(","):

        m = m.strip()

        if m:

            cc_mails.append(m)

cc_mails = list(set(cc_mails))


# =========================================================
# AVG KPT (CHANNEL LEVEL)
# =========================================================

channel_kpt = final_df.groupby(
    "channel"
)["KPT (Mins)"].mean().reset_index()

channel_kpt.columns = [
    "Channel",
    "Avg KPT"
]


# =========================================================
# REGION LEVEL
# =========================================================

region_kpt = final_df.groupby(
    "Region"
)["KPT (Mins)"].mean().reset_index()

region_kpt.columns = [
    "Region",
    "Avg KPT"
]


# =========================================================
# STORE LEVEL
# =========================================================

store_kpt = final_df.groupby(
    ["Region", "branchName"]
)["KPT (Mins)"].mean().reset_index()

store_kpt.columns = [
    "Region",
    "Store",
    "Avg KPT"
]


# =========================================================
# HTML BUILD
# =========================================================

summary_html = f"""
<html>
<body style="font-family:Arial;">

<h2>📊 SALES DASHBOARD SUMMARY</h2>

<h3>🔥 Channel Wise Avg KPT</h3>
{style_dashboard_table(channel_kpt)}

<br>

<h3>🌍 Region Wise Avg KPT</h3>
{style_dashboard_table(region_kpt)}

<br>

<h3>🏪 Store Wise Avg KPT</h3>
{style_dashboard_table(store_kpt)}

<br>

<p>Regards,<br>Sales Automation</p>

</body>
</html>
"""


# =========================================================
# SEND MAIL
# =========================================================

try:

    send_mail(
        cc_mails,
        f"📊 Sales Dashboard Summary - {business_day}",
        summary_html
    )

    print("✅ CC Summary Mail Sent")

except Exception as e:

    print(
        "❌ CC Summary Failed:",
        str(e)
    )
