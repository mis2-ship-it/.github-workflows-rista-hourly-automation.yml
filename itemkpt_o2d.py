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

fetch_date = (
    datetime.now() - timedelta(days=1)
).strftime("%Y-%m-%d")

print("📅 Fetching Yesterday Data:", fetch_date)



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

raw_df = fetch_sales_data(fetch_date)

# =========================================================
# FETCH MTD DATA
# =========================================================

mtd_frames = []

start_date = datetime.strptime(
    fetch_date,
    "%Y-%m-%d"
).replace(day=1)

end_date = datetime.strptime(
    fetch_date,
    "%Y-%m-%d"
)

current_day = start_date

while current_day <= end_date:

    day_str = current_day.strftime("%Y-%m-%d")

    print(f"📅 Fetching MTD : {day_str}")

    temp_df = fetch_sales_data(day_str)

    if not temp_df.empty:
        mtd_frames.append(temp_df)

    current_day += timedelta(days=1)

if len(mtd_frames) > 0:

    raw_mtd_df = pd.concat(
        mtd_frames,
        ignore_index=True
    )

else:

    raw_mtd_df = pd.DataFrame()

print(
    "✅ MTD Rows:",
    len(raw_mtd_df)
)

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

mtd_df = process_sales_data(raw_mtd_df)

if mtd_df.empty:
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

mtd_df = mtd_df.merge(
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
        .astype(str) == fetch_date
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

# ORDER TIME (Correct = invoiceDate)
if "invoiceDate" in sales_df.columns:

    sales_df["Order Time"] = pd.to_datetime(
        sales_df["invoiceDate"],
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


# DELIVERY TIME (Correct = delivery.deliveryDate)
if "modifiedDate" in sales_df.columns:

    sales_df["Delivery Time"] = pd.to_datetime(
        sales_df["modifiedDate"],
        errors="coerce"
    )

else:

    sales_df["Delivery Time"] = pd.NaT


# =========================================================
# CREATE KPT COLUMN
# =========================================================

print("✅ Calculating KPT")

sales_df["KPT (Mins)"] = (
    (
        sales_df["Order Ready Time"]
        -
        sales_df["Order Time"]
    ).dt.total_seconds() / 60
)


# remove negatives
sales_df["KPT (Mins)"] = (
    sales_df["KPT (Mins)"]
    .clip(lower=0)
    .round(1)
)


# =========================================================
# CREATE O2D COLUMN
# =========================================================

print("✅ Calculating O2D")

sales_df["O2D (Mins)"] = (
    (
        sales_df["Delivery Time"]
        -
        sales_df["Order Time"]
    ).dt.total_seconds() / 60
)

sales_df["O2D (Mins)"] = (
    sales_df["O2D (Mins)"]
    .clip(lower=0)
    .round(1)
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

print(
    "✅ Valid KPT Rows:",
    len(sales_df)
)



# =====================================================
# MTD KPT / O2D CALCULATION
# =====================================================

if not mtd_df.empty:

    # ORDER TIME
    mtd_df["Order Time"] = pd.to_datetime(
        mtd_df["invoiceDate"],
        errors="coerce"
    )

    # READY TIME
    mtd_df["Order Ready Time"] = pd.to_datetime(
        mtd_df["orderReadyTimestamp"],
        errors="coerce"
    )

    # DELIVERY TIME
    mtd_df["Delivery Time"] = pd.to_datetime(
        mtd_df["modifiedDate"],
        errors="coerce"
    )

    # KPT
    mtd_df["KPT (Mins)"] = (
        (
            mtd_df["Order Ready Time"]
            -
            mtd_df["Order Time"]
        ).dt.total_seconds() / 60
    )

    mtd_df["KPT (Mins)"] = (
        mtd_df["KPT (Mins)"]
        .clip(lower=0)
        .round(1)
    )

    # O2D
    mtd_df["O2D (Mins)"] = (
        (
            mtd_df["Delivery Time"]
            -
            mtd_df["Order Time"]
        ).dt.total_seconds() / 60
    )

    mtd_df["O2D (Mins)"] = (
        mtd_df["O2D (Mins)"]
        .clip(lower=0)
        .round(1)
    )

    mtd_df = mtd_df[
        mtd_df["KPT (Mins)"].notna()
    ].copy()

    print(
        "✅ MTD KPT Rows:",
        len(mtd_df)
    )
    print(
    mtd_df[
        [
            "KPT (Mins)",
            "O2D (Mins)"
        ]
    ].head()
    )



def sla_metrics(df, group_col, metric, sla_limit):

    g = df.groupby(group_col).agg(
        Orders=(metric, "count"),
        Avg=(metric, "mean"),
        Median=(metric, "median"),
        P80=(metric, lambda x: x.quantile(0.80)),
        Breach=(metric, lambda x: (x > sla_limit).sum())
    ).reset_index()

    g["Breach %"] = (g["Breach"] / g["Orders"] * 100).round(1)

    g["Avg"] = g["Avg"].round(1)
    g["Median"] = g["Median"].round(1)
    g["P80"] = g["P80"].round(1)

    return g



# =========================================================
# CATEGORY / ITEM DASHBOARD BUILDER
# =========================================================

def build_item_dashboard(
    ftd_df,
    mtd_df,
    group_col,
    order_method="nunique"
):

    def agg_data(df):

        if order_method == "count":

            orders = (
                df.groupby(group_col)
                .size()
                .rename("Orders")
            )

        else:

            orders = (
                df.groupby(group_col)["invoiceNumber"]
                .nunique()
                .rename("Orders")
            )

        metrics = (
            df.groupby(group_col)
            .agg(
                KPT=("KPT (Mins)", "mean"),
                KPT_P80=(
                    "KPT (Mins)",
                    lambda x: x.quantile(0.80)
                ),
                KPT_Median=(
                    "KPT (Mins)",
                    "median"
                ),
                O2D=("O2D (Mins)", "mean"),
                O2D_P80=(
                    "O2D (Mins)",
                    lambda x: x.quantile(0.80)
                ),
                O2D_Median=(
                    "O2D (Mins)",
                    "median"
                )
            )
        )

        final = (
            pd.concat(
                [orders, metrics],
                axis=1
            )
            .reset_index()
            .round(2)
        )

        return final

    ftd = agg_data(ftd_df)
    mtd = agg_data(mtd_df)

    dashboard = ftd.merge(
        mtd,
        on=group_col,
        how="outer",
        suffixes=("_FTD", "_MTD")
    )

    return dashboard.fillna(0)



# =========================================================
# CATEGORY DASHBOARD
# =========================================================

category_dashboard = build_item_dashboard(
    sales_df,
    mtd_df,
    "item_categoryName",
    order_method="nunique"
)

# =========================================================
# CATEGORY BY SOURCE DASHBOARD
# =========================================================

category_source_dashboards = {}

for source in ["Swiggy", "Zomato"]:

    ftd_source = sales_df[
        sales_df["Channel"]
        .str.contains(source, na=False)
    ].copy()

    mtd_source = mtd_df[
        mtd_df["Channel"]
        .str.contains(source, na=False)
    ].copy()

    category_source_dashboards[source] = (
        build_item_dashboard(
            ftd_source,
            mtd_source,
            "item_categoryName",
            order_method="nunique"
        )
    )

# =========================================================
# ITEM DASHBOARD
# =========================================================

item_dashboard = build_item_dashboard(
    sales_df,
    mtd_df,
    "item_shortName",
    order_method="count"
)

# =========================================================
# ITEM BY SOURCE DASHBOARD
# =========================================================

item_source_dashboards = {}

for source in ["Swiggy", "Zomato"]:

    ftd_source = sales_df[
        sales_df["Channel"]
        .str.contains(source, na=False)
    ].copy()

    mtd_source = mtd_df[
        mtd_df["Channel"]
        .str.contains(source, na=False)
    ].copy()

    item_source_dashboards[source] = (
        build_item_dashboard(
            ftd_source,
            mtd_source,
            "item_shortName",
            order_method="count"
        )
    )

# =========================================================
# REGION ITEM DASHBOARDS
# =========================================================

region_item_dashboards = {}

for region in sorted(
    sales_df["Region"]
    .dropna()
    .unique()
):

    ftd_region = sales_df[
        sales_df["Region"] == region
    ]

    mtd_region = mtd_df[
        mtd_df["Region"] == region
    ]

    region_item_dashboards[region] = (
        build_item_dashboard(
            ftd_region,
            mtd_region,
            "item_shortName",
            order_method="count"
        )
    )

# =========================================================
# REGION + ITEM BY SOURCE DASHBOARD
# =========================================================

region_item_source_dashboards = {}

for region in sorted(
    sales_df["Region"]
    .dropna()
    .unique()
):

    region_item_source_dashboards[region] = {}

    for source in ["Swiggy", "Zomato"]:

        ftd_temp = sales_df[
            (sales_df["Region"] == region)
            &
            (
                sales_df["Channel"]
                .str.contains(source, na=False)
            )
        ].copy()

        mtd_temp = mtd_df[
            (mtd_df["Region"] == region)
            &
            (
                mtd_df["Channel"]
                .str.contains(source, na=False)
            )
        ].copy()

        region_item_source_dashboards[region][source] = (
            build_item_dashboard(
                ftd_temp,
                mtd_temp,
                "item_shortName",
                order_method="count"
            )
        )

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

    print(
        "❌ Sheet Update Error:",
        str(e)
    )

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

    df = df.fillna("-")

    html = """
    <table style="
        border-collapse:collapse;
        width:70%;
        font-family:Arial;
        font-size:10px;
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

            highlight_cols = [
                "KPT",
                "O2D",
                "Avg",
                "P80",
                "Median"
            ]

            if any(x in str(col) for x in highlight_cols):

                try:

                    float(val)

                    if "O2D" in str(col):
                        style = get_cell_color(
                            val,
                            "O2D"
                        )

                    else:
                        style = get_cell_color(
                            val,
                            "KPT"
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
# CATEGORY HTML
# =========================================================

category_html = f"""
<h2>Category Dashboard</h2>
{style_dashboard_table(category_dashboard)}
"""

# =========================================================
# CATEGORY SOURCE HTML
# =========================================================

category_source_html = ""

for source, df in category_source_dashboards.items():

    category_source_html += f"""
    <h3>{source}</h3>
    {style_dashboard_table(df)}
    <br><br>
    """

# =========================================================
# ITEM HTML
# =========================================================

item_html = f"""
<h2>Item Dashboard</h2>
{style_dashboard_table(item_dashboard)}
"""

# =========================================================
# ITEM SOURCE HTML
# =========================================================

item_source_html = ""

for source, df in item_source_dashboards.items():

    item_source_html += f"""
    <h3>{source}</h3>
    {style_dashboard_table(df)}
    <br><br>
    """

# =========================================================
# REGION ITEM HTML
# =========================================================

region_item_html = ""

for region, df in region_item_dashboards.items():

    region_item_html += f"""
    <h2>{region}</h2>
    {style_dashboard_table(df)}
    <br><br>
    """

# =========================================================
# REGION ITEM SOURCE HTML
# =========================================================

region_item_source_html = ""

for region, source_data in region_item_source_dashboards.items():

    region_item_source_html += f"""
    <h2>{region}</h2>
    """

    for source, df in source_data.items():

        region_item_source_html += f"""
        <h3>{source}</h3>
        {style_dashboard_table(df)}
        <br>
        """

    region_item_source_html += "<br><br>"


# =========================================================
# DEBUG
# =========================================================

print("FTD Rows:", len(sales_df))
print("MTD Rows:", len(mtd_df))

print("Category Dashboard Rows:", len(category_dashboard))
print("Item Dashboard Rows:", len(item_dashboard))
print("Region Item Dashboards:", len(region_item_dashboards))
print("Region Item Source Dashboards:", len(region_item_source_dashboards))

# =========================================================
# SUMMARY HTML
# =========================================================

summary_html = f"""
<html>

<body style="font-family:Arial;">

<h2>
📊 Item Level _ KPT and O2D Performance Dashboard
({fetch_date})
</h2>

<br>

<h2>Category Dashboards</h2>
{category_html}

<br>

<h2>Category by Source Dashboard</h2>
{category_source_html}

<br>

<h2>Item Level Dashboards</h2>
{item_html}

<br>

<h2>Item by Source Dashboard</h2>
{item_source_html}

<br>

<h2>Region + Item Dashboard</h2>
{region_item_html}

<br>

<h2>Region + Item by Source Dashboard</h2>
{region_item_source_html}

<br><br>

<p>
Regards,
<br>
MIS Team
</p>

</body>
</html>
"""

print("✅ Category Dashboards Ready")

print(
    "✅ Item Level Dashboards:",
    len(region_dashboards)
)

print("✅ Region Store Dashboard Ready")
# =========================================================
# SEND EMAIL
# =========================================================

EMAIL_USER = os.environ["EMAIL_USER"]
EMAIL_PASSWORD = os.environ["EMAIL_PASSWORD"]

msg = MIMEMultipart()

msg["From"] = EMAIL_USER
msg["To"] = "mis2@frozenbottle.in"
msg["CC"] = "mis2@frozenbottle.in"


msg["Subject"] = f"📊 Item Level KPT & O2D Dashboard - {fetch_date}"

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

    recipients = [
        "mis2@frozenbottle.in"
        
    ]
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
