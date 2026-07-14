import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials

# --- Configuration ---
# Update this string to the exact Base URL domain provided by your Rista team
RISTA_BASE_URL = 'https://api.ristaapps.com/v1' 

# Read the keys you set up in your GitHub Workflow env block
API_KEY = os.environ.get('API_KEY')
SECRET_KEY = os.environ.get('SECRET_KEY')

SPREADSHEET_ID = '1umqb0k_G0F-cAzMbrmqSYnEz06-NjmCANWtWEa_NS9w'
SHEET_NAME = 'Help_Sheet'

def main():
    # 1. Authenticate with Google Sheets using GitHub Secrets
    google_creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if not google_creds_json:
        print("Error: GOOGLE_CREDENTIALS environment variable is missing.")
        return
        
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    
    try:
        creds_dict = json.loads(google_creds_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SPREADSHEET_ID)
        sheet = sh.worksheet(SHEET_NAME)
    except Exception as e:
        print(f"Error authenticating or opening Google Sheet: {e}")
        return

    # 2. Setup Rista API Request Headers
    # Adjust this layout if your Rista team gave you specific header keys 
    # (e.g., 'x-api-key': API_KEY)
    endpoint = f"https://api.ristaapps.com/v1/inventory/store/items"
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'Accept': 'application/json'
    }
    
    print("Fetching data from Rista API...")
    if "https://api.ristaapps.com/v1" in RISTA_BASE_URL:
        print("Warning: You still need to replace RISTA_BASE_URL with the real URL domain.")
        return

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
    stores = response_data.get('stores', response_data.get('data', []))
    
    # Prepare rows with headers
    rows = [['Store Name', 'Ownership', 'Region']]
    
    for store in stores:
        # Check if the store is marked as available/active
        if store.get('available') is True or store.get('status') == 'available' or store.get('isActive') != False:
            rows.append([
                store.get('storeName', store.get('name', 'N/A')),
                store.get('ownership', 'N/A'),
                store.get('region', 'N/A')
            ])
            
    # 4. Update the Google Sheet
    print(f"Writing {len(rows) - 1} available store records to Google Sheet...")
    sheet.clear()
    sheet.update('A1', rows)
    
    # Format the header row to bold
    sheet.format('A1:C1', {'textFormat': {'bold': True}})
    print("Successfully updated Help_Sheet!")

if __name__ == '__main__':
    main()
