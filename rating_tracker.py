import requests
import gspread
import re
from bs4 import BeautifulSoup
from oauth2client.service_account import ServiceAccountCredentials

# =========================================
# GOOGLE AUTH
# =========================================

scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive"
]

creds = ServiceAccountCredentials.from_json_keyfile_name(
    "service_account.json",
    scope
)

client = gspread.authorize(creds)

spreadsheet = client.open_by_key(
    "179JtVxWo1jEBwy0DXtV6uf22WQV-z6DiZ6qvTqxeh64"
)

mapping_sheet = spreadsheet.worksheet("RID_Mapping")
output_sheet = spreadsheet.worksheet("Rating_Output")

# =========================================
# LOAD RID DATA
# =========================================

raw_data = mapping_sheet.get("A:F")

headers = raw_data[0]
rows = raw_data[1:]

mapping_data = []

for row in rows:

    if len(row) == 0:
        continue

    while len(row) < 6:
        row.append("")

    row_dict = dict(zip(headers, row))

    # FILTER ONLY COCO
    if str(
        row_dict.get("Store Type", "")
    ).strip().upper() == "COCO":

        mapping_data.append(row_dict)

print(f"✅ COCO Stores Found: {len(mapping_data)}")

# =========================================
# REQUEST HEADERS
# =========================================

headers_req = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-IN,en;q=0.9",
    "Referer": "https://www.swiggy.com/",
    "Origin": "https://www.swiggy.com"
}

final_rows = []

print("🚀 Script Started")

# =========================================
# LOOP STORES
# =========================================

for row in mapping_data:

    s_rid = str(row.get("S_RID", "")).strip()
    z_rid = str(row.get("Z_RID", "")).strip()

    brand = row.get("Brand Name", "")
    store = row.get("Store Name", "")
    region = row.get("Region", "")

    s_rating = ""
    s_reviews = ""
    s_status = ""

    z_rating = ""
    z_reviews = ""

    # =====================================
    # SWIGGY FETCH
    # =====================================

    try:

        swiggy_url = (
            f"https://www.swiggy.com/dapi/restaurants/details/v2?"
            f"lat=12.9716"
            f"&lng=77.5946"
            f"&restaurantId={s_rid}"
        )

        response = requests.get(
            swiggy_url,
            headers=headers_req,
            timeout=20
        )

        if response.status_code == 200:

            data = response.json()

            info = (
                data.get("data", {})
                .get("cards", [{}])[0]
                .get("card", {})
                .get("card", {})
                .get("info", {})
            )

            s_rating = info.get(
                "avgRating", ""
            )

            s_reviews = info.get(
                "totalRatingsString", ""
            )

            availability = info.get(
                "availability", {}
            )

            opened = availability.get(
                "opened", False
            )

            s_status = (
                "Online"
                if opened
                else "Offline"
            )

            print(f"✅ Swiggy Done - {store}")

        else:
            print(
                f"❌ Swiggy Failed - "
                f"{store} - "
                f"{response.status_code}"
            )

    except Exception as e:
        print(
            f"❌ Swiggy Error - "
            f"{store} - {e}"
        )

# =====================================
# ZOMATO FETCH
# =====================================

z_status = ""

try:

    zomato_url = (
        f"https://www.zomato.com/"
        f"restaurant/{z_rid}"
    )

    response = requests.get(
        zomato_url,
        headers=headers_req,
        timeout=20
    )

    html = response.text

    # Rating
    rating_match = re.search(
        r'"ratingValue":"([\d.]+)"',
        html
    )

    if rating_match:
        z_rating = rating_match.group(1)

    # Reviews
    review_match = re.search(
        r'"reviewCount":"([\d,]+)"',
        html
    )

    if review_match:
        z_reviews = review_match.group(1)

    # Online / Offline
    lower_html = html.lower()

    if (
        "temporarily closed" in lower_html
        or "currently not accepting orders" in lower_html
        or "restaurant closed" in lower_html
    ):
        z_status = "Offline"
    else:
        z_status = "Online"

    print(f"✅ Zomato Done - {store}")

except Exception as e:
    print(
        f"❌ Zomato Error - "
        f"{store} - {e}"
    )

    # =====================================
    # FINAL OUTPUT
    # =====================================

    final_rows.append([
        brand,
        store,
        region,
        s_rating,
        z_rating,
        s_reviews,
        z_reviews,
        s_status,
        Z_status
    ])

# =========================================
# OUTPUT TO GSHEET
# =========================================

headers_output = [[
    "Brand Name",
    "Store Name",
    "Region",
    "S_Rating",
    "Z_Rating",
    "S_Reviews",
    "Z_Reviews",
    "Swiggy_Status"
    "Zomato_Status"
]]

# Clear old data
output_sheet.clear()

# Write fresh output
output_sheet.update(
    values=headers_output + final_rows,
    range_name="A1:I"
)

print("✅ Google Sheet Updated")
print("🏁 Script Completed")
