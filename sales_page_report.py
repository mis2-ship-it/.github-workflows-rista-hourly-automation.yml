# =========================================================
# IMPORTS
# =========================================================

import os
import json
import time
import jwt
import requests
import pandas as pd
import smtplib

from datetime import datetime, timedelta

import gspread
from google.oauth2.service_account import Credentials

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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
    json.loads(os.environ["GOOGLE_CREDENTIALS"]),
    scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
)

client = gspread.authorize(creds)

spreadsheet = client.open_by_key(
    "19z6KkVBFoLC33_wcNqVhDLyQEC2dDQ8YQE0gE38BhVg"
)

print("✅ Connected Google Sheet")

# =========================================================
# DATE
# =========================================================

yesterday_date = (
    datetime.now() - timedelta(days=1)
).strftime("%Y-%m-%d")

print("📅 Fetching Yesterday Data:", yesterday_date)

# =========================================================
# HELP SHEET
# =========================================================

help_ws = spreadsheet.worksheet("Help Sheet")

# GET RAW DATA
help_data = help_ws.get()

if not help_data:
    print("❌ Help Sheet Empty")
    exit()

# =========================================================
# HEADER
# =========================================================

raw_headers = help_data[0]

safe_headers = []

for i, h in enumerate(raw_headers):

    h = str(h).strip()

    # blank header fix
    if h == "":
        h = f"blank_col_{i}"

    # duplicate header fix
    if h in safe_headers:
        h = f"{h}_{i}"

    safe_headers.append(h)

header_len = len(safe_headers)

print("✅ Header Count:", header_len)

# =========================================================
# NORMALIZE ROWS
# =========================================================

normalized_rows = []

for row in help_data[1:]:

    row = list(row)

    # ADD MISSING COLUMNS
    if len(row) < header_len:
        row.extend([""] * (header_len - len(row)))

    # REMOVE EXTRA COLUMNS
    elif len(row) > header_len:
        row = row[:header_len]

    normalized_rows.append(row)

# =========================================================
# CREATE DATAFRAME
# =========================================================

help_df = pd.DataFrame(
    normalized_rows,
    columns=safe_headers
)

# =========================================================
# CLEAN COLUMN NAMES
# =========================================================

help_df.columns = (
    help_df.columns
    .astype(str)
    .str.strip()
    .str.lower()
    .str.replace(" ", "")
)

print("✅ Help Sheet Loaded")
print("📋 Columns:", help_df.columns.tolist())

# =========================================================
# CHECK ownership COLUMN
# =========================================================

if "ownership" not in help_df.columns:

    print("❌ ownership column missing")
    print(help_df.columns.tolist())
    exit()

# =========================================================
# FILTER COCO
# =========================================================

help_df = help_df[
    help_df["ownership"]
    .astype(str)
    .str.upper()
    .str.strip() == "COCO"
].copy()

print("✅ COCO Rows:", len(help_df))

# =========================================================
# RENAME COLUMNS
# =========================================================

help_df = help_df.rename(columns={
    "branchcode": "branchCode",
    "storename": "Store Name",
    "amemail": "AM Email",
    "rmemail": "RM Email",
    "amname": "AM Name",
    "ccmail": "CC Mail",
    "region": "Region"
})

# =========================================================
# REQUIRED COLUMNS CHECK
# =========================================================

required_cols = [
    "branchCode",
    "Store Name",
    "AM Email",
    "RM Email",
    "AM Name",
    "CC Mail",
    "Region"
]

missing_cols = [
    c for c in required_cols
    if c not in help_df.columns
]

if missing_cols:

    print("❌ Missing Columns:", missing_cols)
    print("📋 Available Columns:")
    print(help_df.columns.tolist())

    exit()

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
    .astype(str)
    .str.strip()
    .unique()
    .tolist()
)

print("🏪 COCO Branch Count:", len(branches))

# =========================================================
# SALES API
# =========================================================

sales_url = "https://api.ristaapps.com/v1/sales/page"

def fetch_sales_data(fetch_date):

    all_sales = []

    print(f"\n📦 Fetching Data: {fetch_date}")

    for branch in branches:

        print("Fetching:", branch)

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

            print("Status:", response.status_code)

            if response.status_code != 200:
                continue

            response_json = response.json()

            data = response_json.get("data", [])

            if not data:
                print("No Data:", branch)
                continue

            df = pd.json_normalize(data)

            # branchCode safe creation
            if "branchCode" not in df.columns:

                if "branch" in df.columns:
                    df["branchCode"] = df["branch"]

                elif "storeCode" in df.columns:
                    df["branchCode"] = df["storeCode"]

                elif "Store Code" in df.columns:
                    df["branchCode"] = df["Store Code"]

            all_sales.append(df)

        except Exception as e:
            print(f"❌ Error {branch}: {e}")

    if not all_sales:
        print("❌ No Sales Data Found")
        return pd.DataFrame()

    final_df = pd.concat(
        all_sales,
        ignore_index=True
    )

    print("✅ Total Rows Fetched:", len(final_df))

    return final_df

# =========================================================
# FETCH DATA
# =========================================================

raw_df = fetch_sales_data(yesterday_date)

# =========================================================
# PROCESS DATA
# =========================================================

def process_sales_data(df):

    if df.empty:
        print("❌ Empty DataFrame")
        return pd.DataFrame()

    final_df = df.copy()

    # =====================================================
    # SAFE branchCode
    # =====================================================

    if "branchCode" not in final_df.columns:

        print("❌ branchCode Missing")
        print(final_df.columns.tolist())

        return pd.DataFrame()

    final_df["branchCode"] = (
        final_df["branchCode"]
        .astype(str)
        .str.strip()
    )

    # =====================================================
    # FIX CHANNEL COLUMN
    # =====================================================

    if "Channel" not in final_df.columns:

        if "channel" in final_df.columns:
            final_df["Channel"] = final_df["channel"]

    # =====================================================
    # FILTER CHANNELS
    # =====================================================

    allowed_channels = [
        "Zomato Frozen Bottle",
        "Zomato Boba Bar",
        "Zomato Madno",
        "Swiggy Frozen Bottle",
        "Swiggy Boba Bar",
        "Swiggy Madno",
    ]

    final_df = final_df[
        final_df["Channel"]
        .astype(str)
        .isin(allowed_channels)
    ].copy()

    return final_df

# =========================================================
# PROCESS DATASET
# =========================================================

sales_df = process_sales_data(raw_df)

if sales_df.empty:
    print("❌ No Processed Data Available")
    exit()

# =========================================================
# MERGE HELP SHEET
# =========================================================

help_merge = help_df[
    [
        "branchCode",
        "Store Name",
        "AM Email",
        "RM Email",
        "AM Name",
        "CC Mail",
        "Region"
    ]
].copy()

sales_df = sales_df.merge(
    help_merge,
    on="branchCode",
    how="left"
)

print("✅ Help Sheet Merged")

# =========================================================
# DATE FILTER
# =========================================================

if "invoiceDay" in sales_df.columns:

    sales_df = sales_df[
        sales_df["invoiceDay"]
        .astype(str) == yesterday_date
    ].copy()

print("✅ Orders:", len(sales_df))

# =========================================================
# COLUMN STANDARDIZATION
# =========================================================

print("📋 API Columns:")
print(sales_df.columns.tolist())

# =========================================================
# CREATE REQUIRED TIMESTAMP COLUMNS
# =========================================================

# ORDER TIME
if "createdDate" in sales_df.columns:

    sales_df["Order Time"] = pd.to_datetime(
        sales_df["createdDate"],
        errors="coerce"
    )

else:

    sales_df["Order Time"] = pd.NaT

# READY TIME
if "orderReadyTimestamp" in sales_df.columns:

    sales_df["Order Ready Time"] = pd.to_datetime(
        sales_df["orderReadyTimestamp"],
        errors="coerce"
    )

else:

    sales_df["Order Ready Time"] = pd.NaT

# DELIVERY TIME
if "modifiedDate" in sales_df.columns:

    sales_df["Delivery Time"] = pd.to_datetime(
        sales_df["modifiedDate"],
        errors="coerce"
    )

else:

    sales_df["Delivery Time"] = pd.NaT

# =========================================================
# CREATE KPT COLUMN IF MISSING
# =========================================================

if "KPT (Mins)" not in sales_df.columns:

    print("⚠️ KPT (Mins) Missing")
    print("✅ Calculating KPT")

    sales_df["KPT (Mins)"] = (
        (
            sales_df["Order Ready Time"]
            - sales_df["Order Time"]
        ).dt.total_seconds() / 60
    )

# =========================================================
# CREATE O2D COLUMN
# =========================================================

if "O2D (Mins)" not in sales_df.columns:

    print("✅ Calculating O2D")

    sales_df["O2D (Mins)"] = (
        (
            sales_df["Delivery Time"]
            - sales_df["Order Time"]
        ).dt.total_seconds() / 60
    )

# =========================================================
# CLEAN NUMERIC VALUES
# =========================================================

sales_df["KPT (Mins)"] = pd.to_numeric(
    sales_df["KPT (Mins)"],
    errors="coerce"
)

sales_df["O2D (Mins)"] = pd.to_numeric(
    sales_df["O2D (Mins)"],
    errors="coerce"
)

# =========================================================
# REMOVE INVALID ROWS
# =========================================================

sales_df = sales_df[
    sales_df["KPT (Mins)"].notna()
].copy()

sales_df = sales_df[
    sales_df["KPT (Mins)"] >= 0
].copy()

print("✅ Valid KPT Rows:", len(sales_df))

# =========================================================
# KPI
# =========================================================

swiggy_avg = round(
    sales_df[
        sales_df["Channel"]
        .str.contains("Swiggy", na=False)
    ]["KPT (Mins)"].mean(),
    1
)

zomato_avg = round(
    sales_df[
        sales_df["Channel"]
        .str.contains("Zomato", na=False)
    ]["KPT (Mins)"].mean(),
    1
)

overall_avg = round(
    sales_df["KPT (Mins)"].mean(),
    1
)

overall_o2d = round(
    sales_df["O2D (Mins)"].mean(),
    1
)

total_orders = len(sales_df)

top_kpi = pd.DataFrame({

    "Metric": [
        "Swiggy Avg KPT",
        "Zomato Avg KPT",
        "Overall Avg KPT",
        "Overall Avg O2D",
        "Total Orders"
    ],

    "Value": [
        swiggy_avg,
        zomato_avg,
        overall_avg,
        overall_o2d,
        total_orders
    ]
})

print("✅ KPI Created")

# =========================================================
# BRAND LEVEL BREACH DASHBOARD
# =========================================================

# SLA DEFINITIONS
KPT_SLA = 15
O2D_SLA = 40

# REQUIRED CHANNELS
brand_channels = [
    "Swiggy Frozen Bottle",
    "Swiggy Boba Bar",
    "Swiggy Madno",
    "Zomato Boba Bar",
    "Zomato Frozen Bottle",
    "Zomato Madno",
]

# FILTER CHANNELS
brand_df = sales_df[
    sales_df["Channel"]
    .astype(str)
    .isin(brand_channels)
].copy()

print("✅ Brand Rows:", len(brand_df))

# =========================================================
# CREATE BREACH FLAGS
# =========================================================

brand_df["KPT Breach"] = (
    brand_df["KPT (Mins)"] > KPT_SLA
)

brand_df["O2D Breach"] = (
    brand_df["O2D (Mins)"] > O2D_SLA
)

# =========================================================
# BRAND LEVEL DASHBOARD
# =========================================================

brand_dashboard = brand_df.groupby("Channel").agg({

    "KPT (Mins)": "mean",
    "O2D (Mins)": "mean",
    "KPT Breach": "sum",
    "O2D Breach": "sum"

}).reset_index().fillna("-")

# =========================================================
# TOTAL ORDERS
# =========================================================

order_counts = (
    brand_df.groupby("Channel")
    .size()
    .reset_index(name="Total Orders")
)

brand_dashboard = brand_dashboard.merge(
    order_counts,
    on="Channel",
    how="left"
)

# =========================================================
# ROUND VALUES
# =========================================================

brand_dashboard["KPT (Mins)"] = (
    brand_dashboard["KPT (Mins)"]
    .round(1)
)

brand_dashboard["O2D (Mins)"] = (
    brand_dashboard["O2D (Mins)"]
    .round(1)
)

# =========================================================
# BREACH %
# =========================================================

brand_dashboard["KPT Breach %"] = round(
    (
        brand_dashboard["KPT Breach"]
        / brand_dashboard["Total Orders"]
    ) * 100,
    1
)

brand_dashboard["O2D Breach %"] = round(
    (
        brand_dashboard["O2D Breach"]
        / brand_dashboard["Total Orders"]
    ) * 100,
    1
)

# =========================================================
# SLA STATUS
# =========================================================

brand_dashboard["KPT SLA Status"] = brand_dashboard[
    "KPT (Mins)"
].apply(
    lambda x: "✅ Within SLA"
    if x <= KPT_SLA
    else "❌ Breach"
)

brand_dashboard["O2D SLA Status"] = brand_dashboard[
    "O2D (Mins)"
].apply(
    lambda x: "✅ Within SLA"
    if x <= O2D_SLA
    else "❌ Breach"
)

# =========================================================
# FINAL COLUMN ORDER
# =========================================================

brand_dashboard = brand_dashboard[[
    "Channel",
    "Total Orders",
    "KPT (Mins)",
    "KPT Breach",
    "KPT Breach %",
    "KPT SLA Status",
    "O2D (Mins)",
    "O2D Breach",
    "O2D Breach %",
    "O2D SLA Status"
]]

print("✅ Brand Breach Dashboard Ready")
print(brand_dashboard.head())

# =========================================================
# KPI
# =========================================================

swiggy_avg = round(
    sales_df[
        sales_df["Channel"]
        .str.contains("Swiggy", na=False)
    ]["KPT (Mins)"].mean(),
    1
)

zomato_avg = round(
    sales_df[
        sales_df["Channel"]
        .str.contains("Zomato", na=False)
    ]["KPT (Mins)"].mean(),
    1
)

overall_avg = round(
    sales_df["KPT (Mins)"].mean(),
    1
)

total_orders = len(sales_df)

top_kpi = pd.DataFrame({
    "Metric": [
        "Swiggy Avg",
        "Zomato Avg",
        "Overall Avg",
        "Total Orders"
    ],
    "Value": [
        swiggy_avg,
        zomato_avg,
        overall_avg,
        total_orders
    ]
})

# =========================================================
# REGION DASHBOARD
# =========================================================

region_dashboard = (
    sales_df
    .groupby(
        ["Region", "Channel"]
    )
    .agg(

        Orders=(
            "invoiceNumber",
            "count"
        ),

        Avg_KPT=(
            "KPT (Mins)",
            "mean"
        ),

        Avg_O2D=(
            "O2D (Mins)",
            "mean"
        )

    )
    .round(1)
    .reset_index()
)

# pivot for clean format
region_dashboard = pd.pivot_table(

    region_dashboard,

    index="Region",

    columns="Channel",

    values=[
        "Orders",
        "Avg_KPT",
        "Avg_O2D"
    ],

    aggfunc="first"

)

region_dashboard = (
    region_dashboard
    .round(1)
    .fillna("-")
)

region_dashboard.columns = [
    f"{a}_{b}"
    for a, b
    in region_dashboard.columns
]

region_dashboard.reset_index(
    inplace=True
)

print(
    "✅ Region Dashboard Ready"
)

# =========================================================
# STORE DASHBOARD
# =========================================================

store_dashboard = (
    sales_df
    .groupby(
        [
            "Region",
            "Store Name",
            "Channel"
        ]
    )
    .agg(

        Orders=(
            "invoiceNumber",
            "count"
        ),

        Avg_KPT=(
            "KPT (Mins)",
            "mean"
        ),

        Avg_O2D=(
            "O2D (Mins)",
            "mean"
        )

    )
    .round(1)
    .reset_index()
)

store_dashboard = pd.pivot_table(

    store_dashboard,

    index=[
        "Region",
        "Store Name"
    ],

    columns="Channel",

    values=[
        "Orders",
        "Avg_KPT",
        "Avg_O2D"
    ],

    aggfunc="first"
)

store_dashboard = (
    store_dashboard
    .round(1)
    .fillna("-")
)

store_dashboard.columns = [
    f"{a}_{b}"
    for a, b
    in store_dashboard.columns
]

store_dashboard.reset_index(
    inplace=True
)

print(
    "✅ Store Dashboard Ready"
)

# =========================================================
# O2D REGION DASHBOARD
# =========================================================

o2d_region_dashboard = pd.pivot_table(

    sales_df,

    values="O2D (Mins)",

    index="Channel",

    columns="Region",

    aggfunc="mean"

).round(1).reset_index().fillna("-")


# =========================================================
# O2D STORE DASHBOARD
# =========================================================

o2d_store_dashboard = pd.pivot_table(

    sales_df,

    values="O2D (Mins)",

    index=[
        "Region",
        "Store Name"
    ],

    columns="Channel",

    aggfunc="mean"

).round(1).reset_index().fillna("-")

# =========================================================
# WRITE TO GOOGLE SHEET
# =========================================================

try:

    report_ws = spreadsheet.worksheet("Sales Dashboard")

    report_ws.clear()

    report_ws.update(
        [top_kpi.columns.values.tolist()] +
        top_kpi.values.tolist(),
        "A1"
    )

    print("✅ Dashboard Updated in GSheet")

except Exception as e:
    print("❌ Sheet Update Error:", e)

# =========================================================
# EMAIL LIST
# =========================================================

cc_mails = []

for x in help_df["CC Mail"].dropna():

    for m in str(x).split(","):

        if m.strip():
            cc_mails.append(m.strip())

cc_mails = list(set(cc_mails))

# =========================================================
# HTML STYLE FUNCTION
# =========================================================

def get_cell_color(val, metric="KPT"):

    try:
        val = float(val)

        

        # =====================================================
        # KPT LOGIC
        # Green <12
        # Yellow 12-15
        # Red >15
        # =====================================================
        if metric == "KPT":

            if val < 12:
                return (
                    "background-color:#d9ead3;"
                    "color:#006100;"
                    "font-weight:bold;"
                )

            elif val <= 15:
                return (
                    "background-color:#fff2cc;"
                    "color:#9c6500;"
                    "font-weight:bold;"
                )

            else:
                return (
                    "background-color:#f4cccc;"
                    "color:#9c0006;"
                    "font-weight:bold;"
                )

        # =====================================================
        # O2D LOGIC
        # Green <30
        # Yellow 30-35
        # Red >35
        # =====================================================
        elif metric == "O2D":

            if val < 30:
                return (
                    "background-color:#d9ead3;"
                    "color:#006100;"
                    "font-weight:bold;"
                )

            elif val <= 35:
                return (
                    "background-color:#fff2cc;"
                    "color:#9c6500;"
                    "font-weight:bold;"
                )

            else:
                return (
                    "background-color:#f4cccc;"
                    "color:#9c0006;"
                    "font-weight:bold;"
                )

    except:
        return ""

    return ""


def style_dashboard_table(df, metric="KPT"):

    if df.empty:
        return "<p>No Data</p>"

    # replace nan with "-"
    df = df.fillna("-")

    html = """
    <table style="
        border-collapse:collapse;
        width:100%;
        font-family:Arial;
        font-size:13px;
    ">
    """

    # =====================================================
    # HEADER
    # =====================================================

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

    # =====================================================
    # BODY
    # =====================================================

    for _, row in df.iterrows():

        html += "<tr>"

        for col in df.columns:

            val = row[col]

            style = ""

            # only highlight metric columns
            if col not in [
                "Region",
                "Store Name",
                "Channel",
                "Total Orders"
            ]:

                try:
                    float(val)
                    style = get_cell_color(
                        val,
                        metric
                    )
                except:
                    pass

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

# =========================================================
# SUMMARY HTML
# =========================================================

summary_html = f"""
<html>

<body style="font-family:Arial;">

<h2>
📊 Sales Performance Dashboard
({today_date})
</h2>

<br>

<!-- ================================================= -->
<!-- KPT DASHBOARD -->
<!-- ================================================= -->

<h2>
🔥 KPT Dashboard
</h2>

<h3>
📍 Region Level KPT
</h3>

{
style_dashboard_table(
    region_dashboard,
    metric="KPT"
)
}

<br>

<h3>
🏪 Store Level KPT
</h3>

{
style_dashboard_table(
    store_dashboard,
    metric="KPT"
)
}

<br><br>

<!-- ================================================= -->
<!-- O2D DASHBOARD -->
<!-- ================================================= -->

<h2>
🚚 O2D Dashboard
</h2>

<h3>
📍 Region Level O2D
</h3>

{
style_dashboard_table(
    o2d_region_dashboard,
    metric="O2D"
)
}

<br>

<h3>
🏪 Store Level O2D
</h3>

{
style_dashboard_table(
    o2d_store_dashboard,
    metric="O2D"
)
}

<br><br>

<p>
Regards,
<br>
MIS Team
</p>

</body>
</html>
"""

# =========================================================
# SEND EMAIL
# =========================================================

EMAIL_USER = os.environ["EMAIL_USER"]
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]

msg = MIMEMultipart()

msg["From"] = EMAIL_USER
msg["To"] = "ops.all@frozenbottle.in"
msg["Cc"] = ",".join(cc_mails)

msg["Subject"] = f"📊 KPT & O2D Dashboard - {yesterday_date}"

msg.attach(
    MIMEText(summary_html, "html")
)

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

    recipients = [EMAIL_USER] + cc_mails

    server.sendmail(
        EMAIL_USER,
        recipients,
        msg.as_string()
    )

    server.quit()

    print("✅ Mail Sent Successfully")

except Exception as e:
    print("❌ Mail Error:", e)

print("✅ Script Completed")
