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
        "Zomato Lubov",
        "Swiggy Frozen Bottle",
        "Swiggy Boba Bar",
        "Swiggy Madno",
        "Swiggy Lubov"
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
# KPT CLEAN
# =========================================================

if "KPT (Mins)" in sales_df.columns:

    sales_df["KPT (Mins)"] = pd.to_numeric(
        sales_df["KPT (Mins)"],
        errors="coerce"
    )

    sales_df = sales_df[
        sales_df["KPT (Mins)"].notna()
    ]

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

region_dashboard = pd.pivot_table(
    sales_df,
    values="KPT (Mins)",
    index="Channel",
    columns="Region",
    aggfunc="mean"
).round(1).reset_index()

# =========================================================
# STORE DASHBOARD
# =========================================================

store_dashboard = pd.pivot_table(
    sales_df,
    values="KPT (Mins)",
    index=["Region", "Store Name"],
    columns="Channel",
    aggfunc="mean"
).round(1).reset_index()

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
# HTML FUNCTION
# =========================================================

def style_table(df):

    if df.empty:
        return "<p>No Data</p>"

    html = """
    <table border='1'
    style='border-collapse:collapse;width:100%'>
    """

    # header
    html += "<tr>"

    for c in df.columns:

        html += f"""
        <th style='background:#1F4E78;
        color:white;padding:6px'>
        {c}
        </th>
        """

    html += "</tr>"

    # rows
    for _, row in df.iterrows():

        html += "<tr>"

        for c in df.columns:

            html += f"""
            <td style='padding:6px;text-align:center'>
            {row[c]}
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

<body>

<h2>📊 Sales Dashboard - {yesterday_date}</h2>

<h3>KPI</h3>
{style_table(top_kpi)}

<br>

<h3>Region Dashboard</h3>
{style_table(region_dashboard)}

<br>

<h3>Store Dashboard</h3>
{style_table(store_dashboard)}

<br>

<p>
Regards,<br>
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
msg["To"] = EMAIL_USER
msg["Cc"] = ",".join(cc_mails)

msg["Subject"] = f"📊 Sales Dashboard - {yesterday_date}"

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
