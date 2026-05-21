# =========================================================
# IMPORT
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
    return jwt.encode(payload, SECRET_KEY, algorithm="HS256")


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

today_date = datetime.now().strftime("%Y-%m-%d")
lw_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

print("📅 Today Data:", today_date)
print("📅 LW Data:", lw_date)

# =========================================================
# HELP SHEET
# =========================================================

help_ws = spreadsheet.worksheet("Help Sheet")
help_data = help_ws.get("A:H")

headers_row = help_data[0]
rows = help_data[1:]

help_df = pd.DataFrame(rows, columns=headers_row)

# normalize columns
help_df.columns = (
    help_df.columns
    .astype(str)
    .str.strip()
    .str.lower()
    .str.replace(" ", "")
)

print("📌 Help Sheet Columns:", help_df.columns.tolist())

# required columns
required_cols = [
    "branchcode",
    "ownership",
    "storename",
    "amemail",
    "rmemail",
    "amname",
    "ccmail",
    "region"
]

for c in required_cols:
    if c not in help_df.columns:
        help_df[c] = ""

# filter COCO
help_df = help_df[
    help_df["ownership"].astype(str).str.upper() == "COCO"
].copy()

# rename for final use
help_df.rename(columns={
    "branchcode": "branchCode",
    "storename": "Store Name",
    "amemail": "AM Email",
    "rmemail": "RM Email",
    "amname": "AM Name",
    "ccmail": "CC Mail",
    "region": "Region"
}, inplace=True)

# =========================================================
# BRANCH LIST
# =========================================================

branches = (
    help_df["branchCode"]
    .astype(str)
    .str.strip()
)

branches = branches[branches != ""].unique().tolist()

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

            if response.status_code != 200:
                continue

            data = response.json().get("data", [])

            if not data:
                continue

            df = pd.json_normalize(data)
            all_sales.append(df)

        except Exception as e:
            print(f"❌ Error {branch}: {e}")

    if not all_sales:
        return pd.DataFrame()

    return pd.concat(all_sales, ignore_index=True)

# =========================================================
# FETCH DATA
# =========================================================

today_raw = fetch_sales_data(today_date)
lw_raw = fetch_sales_data(lw_date)

# =========================================================
# PROCESS DATA
# =========================================================

def process_sales_data(df):

    if df.empty:
        return pd.DataFrame()

    final_df = df.copy()

    # =========================================================
    # SAFE branchCode FIX (IMPORTANT)
    # =========================================================
    
    if "branchCode" not in final_df.columns:
    
        if "Store Code" in final_df.columns:
            final_df["branchCode"] = final_df["Store Code"]
    
        elif "storeCode" in final_df.columns:
            final_df["branchCode"] = final_df["storeCode"]
    
        elif "branch" in final_df.columns:
            final_df["branchCode"] = final_df["branch"]
    
        else:
            print("❌ NO branchCode FOUND IN API RESPONSE")
            print(final_df.columns.tolist())
            return pd.DataFrame()

    # channel filter
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

    if "channel" in final_df.columns:
        final_df = final_df[
            final_df["channel"].astype(str).isin(allowed_channels)
        ].copy()

    return final_df

# =========================================================
# PROCESS DATASETS
# =========================================================

today_df = process_sales_data(today_raw)
lw_df = process_sales_data(lw_raw)

# =========================================================
# MERGE HELP SHEET
# =========================================================

help_merge = help_df[
    ["branchCode", "Store Name", "AM Email", "RM Email", "AM Name", "CC Mail", "Region"]
].copy()

today_df = today_df.merge(help_merge, on="branchCode", how="left")

print("✅ Help Sheet Merged")

# =========================================================
# TODAY FILTER
# =========================================================

if "invoiceDay" in today_df.columns:
    today_df = today_df[
        today_df["invoiceDay"].astype(str) == today_date
    ].copy()

print("✅ Today Orders:", len(today_df))

# =========================================================
# KPT CLEAN
# =========================================================

if "KPT (Mins)" in today_df.columns:
    today_df["KPT (Mins)"] = pd.to_numeric(today_df["KPT (Mins)"], errors="coerce")
    today_df = today_df[today_df["KPT (Mins)"].notna()]

print("✅ KPT Valid Rows:", len(today_df))

# =========================================================
# KPI
# =========================================================

swiggy_avg = round(
    today_df[today_df["Channel"].str.contains("Swiggy", na=False)]["KPT (Mins)"].mean(), 1
)

zomato_avg = round(
    today_df[today_df["Channel"].str.contains("Zomato", na=False)]["KPT (Mins)"].mean(), 1
)

overall_avg = round(today_df["KPT (Mins)"].mean(), 1)

total_orders = len(today_df)

top_kpi = pd.DataFrame({
    "Metric": ["Swiggy Avg", "Zomato Avg", "Overall Avg", "Total Orders"],
    "Value": [swiggy_avg, zomato_avg, overall_avg, total_orders]
})

# =========================================================
# REGION DASHBOARD
# =========================================================

region_dashboard = pd.pivot_table(
    today_df,
    values="KPT (Mins)",
    index="Channel",
    columns="Region",
    aggfunc="mean"
).round(1).reset_index()

# =========================================================
# STORE DASHBOARD
# =========================================================

store_dashboard = pd.pivot_table(
    today_df,
    values="KPT (Mins)",
    index=["Region", "Store Name"],
    columns="Channel",
    aggfunc="mean"
).round(1).reset_index()

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

    html = "<table border='1' style='border-collapse:collapse;width:100%'>"

    html += "<tr>"
    for c in df.columns:
        html += f"<th style='background:#1F4E78;color:white'>{c}</th>"
    html += "</tr>"

    for _, row in df.iterrows():
        html += "<tr>"
        for c in df.columns:
            html += f"<td>{row[c]}</td>"
        html += "</tr>"

    html += "</table>"

    return html

# =========================================================
# SUMMARY HTML
# =========================================================

summary_html = f"""
<html>
<body>

<h2>📊 SALES DASHBOARD</h2>

<h3>KPI</h3>
{style_table(top_kpi)}

<h3>Region Dashboard</h3>
{style_table(region_dashboard)}

<h3>Store Dashboard</h3>
{style_table(store_dashboard)}

<p>Regards,<br>MIS Team</p>

</body>
</html>
"""

# =========================================================
# SEND MAIL
# =========================================================

EMAIL_USER = os.environ["EMAIL_USER"]
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]

msg = MIMEMultipart()
msg["From"] = EMAIL_USER
msg["To"] = EMAIL_USER
msg["Cc"] = ",".join(cc_mails)
msg["Subject"] = f"📊 Sales Dashboard - {today_date}"

msg.attach(MIMEText(summary_html, "html"))

server = smtplib.SMTP("smtp.gmail.com", 587)
server.starttls()
server.login(EMAIL_USER, EMAIL_PASSWORD)

server.sendmail(
    EMAIL_USER,
    cc_mails,
    msg.as_string()
)

server.quit()

print("✅ Mail Sent Successfully")
