import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials

# --- Configuration ---
# GitHub Secrets will securely feed these variables
BASE_URL = os.environ.get('RISTA_BASE_URL', 'https://api.rista.com/v1') 
AUTH_TOKEN = os.environ.get('RISTA_AUTH_TOKEN')
SPREADSHEET_ID = '1umqb0k_G0F-cAzMbrmqSYnEz06-NjmCANWtWEa_NS9w'
SHEET_NAME = 'Help_Sheet'

def main():
    # 1. Authenticate with Google Sheets using GitHub Secrets
    # We store the entire Google Service Account JSON in a single secret
    google_creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if not google_creds_json:
        print("Error: GOOGLE_CREDENTIALS environment variable is missing.")
        return
        
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    creds_dict = json.loads(google_creds_json)
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    gc = gspread.authorize(creds)
    
    # Open the spreadsheet and tab
    try:
        sh = gc.open_by_key(SPREADSHEET_ID)
        sheet = sh.worksheet(SHEET_NAME)
    except Exception as e:
        print(f"Error opening Google Sheet: {e}")
        return

    # 2. Fetch data from Rista API
    endpoint = f"{BASE_URL}/inventory/store/items"
    headers = {
        'Authorization': f'Bearer {AUTH_TOKEN}',
        'Accept': 'application/json'
    }
    
    print("Fetching data from Rista API...")
    try:
        response = requests.get(endpoint, headers=headers)
        if response.status_code != 200:
            print(f"API Error: Status {response.status_code}. Response: {response.text}")
            return
            
        response_data = response.json()
    except Exception as e:
        print(f"HTTP Request failed: {e}")
        return

    # 3. Parse and filter available stores
    # Adjust payload mapping ('stores' or 'data') depending on Rista's actual payload shape
    stores = response_data.get('stores', response_data.get('data', []))
    
    # Prepare rows with headers
    rows = [['Store Name', 'Ownership', 'Region']]
    
    for store in stores:
        # Filter logic for available stores
        if store.get('available') is True or store.get('status') == 'available' or store.get('isActive') != False:
            rows.append([
                store.get('storeName', store.get('name', 'N/A')),
                store.get('ownership', 'N/A'),
                store.get('region', 'N/A')
            ])
            
    # 4. Update the Google Sheet
    print(f"Writing {len(rows) - 1} store records to Google Sheet...")
    sheet.clear()
    sheet.update('A1', rows)
    
    # Bold the header row
    sheet.format('A1:C1', {'textFormat': {'bold': True}})
    print("Successfully updated Help_Sheet!")

if __name__ == '__main__':
    main()
