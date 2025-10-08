import csv
from datetime import datetime
import logging
import openpyxl
import os

from dotenv import load_dotenv
from sys import exit
from typing import Any, Dict, List, Optional

import requests

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variable
load_dotenv(dotenv_path=".env.uat")

# Get environment variable
BASE_URL = os.getenv("BaseURL")

if not BASE_URL:
    logging.error("Environment variable not complete!")
    exit(1)
    
# Output report file
OUTPUT_DIR = "./logs"
os.makedirs(OUTPUT_DIR, exist_ok=True)
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
REPORT_FILE = f"./{OUTPUT_DIR}/member_import_{TIMESTAMP}.csv"

def create_user(data: Dict[str, str]) -> tuple[str, dict, str]:
    """
    Returns: (status: 'created'|'updated'|'failed', response, user_id)
    """
    url = f"{BASE_URL}/api/User/CreateUser"
    try:
        clean_data = {k: v for k, v in data.items() if v is not None}
        response = requests.post(url, data=clean_data, timeout=30)
        if response.status_code == 200:
            resp_json = response.json()
            if resp_json.get("success"):
                user_id = resp_json.get("value", {}).get("id", "")
                return "created", resp_json, user_id
            elif resp_json.get("errorMessage") == "email_duplicate":
                # Handle duplicate → update
                email = data["email"]
                user_id = get_user_id_by_email(email)
                if user_id:
                    update_resp = update_user_profile(user_id, data)
                    if update_resp == 200:
                        return "updated", update_resp, user_id
                    else:
                        return "failed", update_resp, user_id
                else:
                    return "failed", {"error": "User not found for update"}, ""
            else:
                return "failed", resp_json, ""
        else:
            return "failed", {"error": response.text}, ""
    except Exception as e:
        return "failed", {"error": str(e)}, ""

def get_user_id_by_email(email: str) -> Optional[str]:
    """GET /api/Lookup/SearchUser?types=email&username={{email}}"""
    url = f"{BASE_URL}/api/Lookup/SearchUser"
    params = {"types": "email", "username": email}
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            users = response.json()
            if isinstance(users, list) and len(users) > 0:
                return users[0].get("id")
    except Exception as e:
        logging.error(f"Error fetching user by email {email}: {e}")
    return None

def update_user_profile(user_id: str, data: Dict[str, str]) -> int:
    """POST /api/User/UpdateUserProfile (form-data)"""
    url = f"{BASE_URL}/api/User/UpdateUserProfile"
    payload = {
        "userId": user_id,
        "personalTitle": data.get("title", ""),
        "firstName": data.get("firstName", ""),
        "lastName": data.get("lastName", ""),
    }
    try:
        response = requests.post(url, data=payload, timeout=30)
        status_code = response.status_code
        return status_code
    except Exception as e:
        return {"success": False, "error": str(e)}, 500

def prepare_data(row: List[Any], headers: List[str]) -> Dict[str, str]:
    def get_str(key: str) -> str | None:
        if key in headers:
            value = row[headers.index(key)]
            return str(value).strip() if value not in (None, "") else None
        return None

    first_name = get_str("First Name")
    last_name = get_str("Last Name")
    title = get_str("Title")
    password = get_str("password")
    email = get_str("Email")

    if not first_name or not email:
        raise ValueError("Missing required fields: First Name or Email")

    return {
        "title": title or "-",
        "firstName": first_name,
        "lastName": last_name or "-",
        "email": email,
        "password": password or "@Default1",
        "isFactorForceActivated": "true"
    }

def main():
    try:
        workbook = openpyxl.load_workbook("import-members.xlsx")
        sheet = workbook.active
        headers = [str(cell.value).strip() for cell in sheet[1]]

        with open(REPORT_FILE, mode='w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['row_number', 'email', 'status', 'user_id', 'error']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for row_num in range(2, sheet.max_row + 1):
                try:
                    excel_row = [cell.value for cell in sheet[row_num]]
                    data = prepare_data(excel_row, headers)

                    status, response, user_id = create_user(data)

                    error_msg = ""
                    if status == "failed":
                        error_msg = response.get("errorMessage") or response.get("error", str(response))

                    writer.writerow({
                        "row_number": row_num,
                        "email": data["email"],
                        "status": status,
                        "user_id": user_id,
                        "error": error_msg
                    })

                    if status == "created":
                        logging.info(f"Created: Row {row_num} → {data['email']} (ID: {user_id})")
                    elif status == "updated":
                        logging.info(f"Updated: Row {row_num} → {data['email']} (ID: {user_id})")
                    else:
                        logging.error(f"Failed: Row {row_num} → {data['email']} → {error_msg}")

                except ValueError as ve:
                    logging.warning(f"Skip Row {row_num}: {ve}")
                    writer.writerow({
                        "row_number": row_num,
                        "email": data["email"],
                        "status": "skipped",
                        "user_id": "",
                        "error": str(ve)
                    })
                except Exception as e:
                    logging.error(f"Unexpected error at Row {row_num}: {e}")
                    writer.writerow({
                        "row_number": row_num,
                        "email": data["email"],
                        "status": "error",
                        "user_id": "",
                        "error": str(e)
                    })

        logging.info(f"Report saved to: {REPORT_FILE}")

    except Exception as e:
        logging.error(f"Fatal error: {e}")
        exit(1)
        
if __name__ == "__main__":
    main()