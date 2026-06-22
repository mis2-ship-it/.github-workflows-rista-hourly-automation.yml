# =========================================================
# 🔥 IMPORTS
# =========================================================

import pandas as pd
import requests
import json
import os
import jwt
import time
import gspread

from google.oauth2.service_account import Credentials
from datetime import datetime
import pytz

# =========================================================
# 🔐 RISTA AUTH
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

def api_headers():

    return {
        "x-api-key": API_KEY,
        "x-api-token": get_token(),
        "content-type": "application/json"
    }

# =========================================================
# 🔐 GOOGLE AUTH
# =========================================================

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

creds_dict = json.loads(
    os.environ["GOOGLE_CREDENTIALS"]
)

creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=scope
)

client = gspread.authorize(creds)

# =========================================================
# 📄 SHEET
# =========================================================

SPREADSHEET_ID = "1ZZUcI844US1ZFqnqkG4e5P1eTXq-s2gyVqLiK_KdRyI"

spreadsheet = client.open_by_key(
    SPREADSHEET_ID
)

SHEET_NAME = "Availability_Raw"

try:
    ws = spreadsheet.worksheet(SHEET_NAME)
except:
    ws = spreadsheet.add_worksheet(
        title=SHEET_NAME,
        rows=5000,
        cols=100
    )

# =========================================================
# ⏰ TIME
# =========================================================

ist = pytz.timezone("Asia/Kolkata")

print(
    "⏰ Run Time:",
    datetime.now(ist)
)

# =========================================================
# 🏪 TEST BRANCH
# =========================================================

BRANCH_CODE = "FZBMAA023"

# =========================================================
# 📡 CALL SOLDOUT API
# =========================================================

url = "https://api.ristaapps.com/v1/items/soldout"

response = requests.get(
    url,
    headers=api_headers(),
    params={
        "branch": BRANCH_CODE
    },
    timeout=60
)

print(
    "API Status:",
    response.status_code
)

print(response.text[:3000])
# =========================================================
# 🚨 API FAILED
# =========================================================

if response.status_code != 200:

    print(response.text)
    raise Exception("API Failed")

# =========================================================
# 📦 JSON
# =========================================================

data = response.json()

print(
    "Response Type:",
    type(data)
)

# =========================================================
# 📊 CONVERT TO DF
# =========================================================

if isinstance(data, dict):

    if "data" in data:

        records = data["data"]

    else:

        records = [data]

elif isinstance(data, list):

    records = data

else:

    records = []

df = pd.json_normalize(records)

print(
    "Rows:",
    len(df)
)

print(
    "Columns:"
)

print(
    df.columns.tolist()
)

# =========================================================
# 📤 PUSH TO GSHEET
# =========================================================

ws.clear()

if len(df):

    ws.update(
        [df.columns.tolist()] +
        df.astype(str).values.tolist()
    )

else:

    ws.update(
        [["NO DATA RETURNED"]]
    )

print(
    "✅ Sheet Updated"
)

# =========================================================
# DONE
# =========================================================

print(
    "🚀 Availability Raw Pull Complete"
)
