import csv
from datetime import datetime
import logging
import openpyxl
import os

from dotenv import load_dotenv
from sys import exit
from typing import Any, Dict, List, Tuple

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

def insert_data(data: Dict[str, str]) -> tuple[int, dict]:
    url = f"{BASE_URL}/api/User/CreateUser"
    try:
        data = {k: v for k, v in data.items() if v is not None}
        response = requests.post(url, data=data, timeout=30)
        response.raise_for_status()

        return response.status_code, response.json()
    except requests.exceptions.RequestException as e:
        return response.status_code if 'response' in locals() else 500, {"error": response.text if 'response' in locals() else str(e)}

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
        raise ValueError("Missing required fields: First Name, Email")

    return {
        "title": title if title else "-",
        "firstName": first_name if first_name else "-",
        "lastName": last_name if last_name else "-",
        "email": email,
        "password": password,
        "isFactorForceActivated": "true"
    }

def main():
    try:
        workbook = openpyxl.load_workbook("import-members.xlsx")
        sheet = workbook.active

        headers = [str(cell.value).strip() for cell in sheet[1]]  # Row 1 = headers

        with open(REPORT_FILE, mode='w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['row_number', 'email', 'status_code', 'response', 'user_id', 'error']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for row_num in range(2, sheet.max_row + 1):
                try:
                    excel_row = [cell.value for cell in sheet[row_num]]
                    data = prepare_data(excel_row, headers)

                    status, response = insert_data(data)

                    user_id = response.get("userId", "") if status == 200 else ""
                    error_msg = response.get("error", "") if status != 200 else ""

                    writer.writerow({
                        "row_number": row_num,
                        "email": data["email"],
                        "status_code": status,
                        "response": response,
                        "user_id": user_id,
                        "error": error_msg
                    })

                    if status == 200 and response.get("success", False):
                        logging.info(f"Success: Row {row_num} → User ID: {user_id}")
                    else:
                        logging.error(f"Failed: Row {row_num} → Status: {status}, Error: {error_msg}, Detail: {response}")

                except ValueError as ve:
                    logging.warning(f"Skip Row {row_num}: {ve}")
                    writer.writerow({
                        "row_number": row_num,
                        "email": "",
                        "status_code": 0,
                        "response": response,
                        "user_id": "",
                        "error": str(ve)
                    })
                except Exception as e:
                    logging.error(f"Unexpected error at Row {row_num}: {e}")
                    writer.writerow({
                        "row_number": row_num,
                        "email": "",
                        "status_code": 0,
                        "response": response,
                        "user_id": "",
                        "error": str(e)
                    })

        logging.info(f"Report saved to: {REPORT_FILE}")

    except Exception as e:
        logging.error(f"Fatal error: {e}")
        exit(1)
        
if __name__ == "__main__":
    main()