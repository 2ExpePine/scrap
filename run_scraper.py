from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException
from bs4 import BeautifulSoup
import gspread
from datetime import date
import os
import time
import json
import pandas as pd
import requests
from io import BytesIO
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- SHARDING & SEQUENCE SETUP ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "0")) # Set to 0 to start from first row
END_INDEX = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", "checkpoint_new_1.txt")

# Read last successful index
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        last_i = int(f.read().strip())
else:
    last_i = START_INDEX

# ---------------- BROWSER SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    gc = gspread.service_account("credentials.json")
except Exception as e:
    print(f"Error loading credentials.json: {e}")
    exit(1)

# Target sheet for WRITING
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

# ---------------- READ STOCK LIST FROM GOOGLE SHEETS ---------------- #
print("📥 Fetching stock list from Google Sheet: 'Stock List'...")

try:
    list_workbook = gc.open('Stock List')
    list_sheet = list_workbook.worksheet('Sheet1')
    
    # Get all values (returns list of lists)
    all_rows = list_sheet.get_all_values()
    
    # We keep the sequence by slicing from the header downwards
    # Column A (0): Name | Column E (4): URL
    data_rows = all_rows[1:] 
    
    # Create clean lists preserving the exact order of the sheet
    name_list = [row[0] if len(row) > 0 else "" for row in data_rows]
    company_list = [row[4] if len(row) > 4 else "" for row in data_rows]

    print(f"✅ Loaded {len(company_list)} companies in sequence.")
except Exception as e:
    print(f"❌ Error reading Google Sheet: {e}")
    exit(1)

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER FUNCTION ---------------- #
def scrape_tradingview(company_url):
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_window_size(1920, 1080)
    try:
        if os.path.exists("cookies.json"):
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for cookie in cookies:
                try:
                    cookie_to_add = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
                    driver.add_cookie(cookie_to_add)
                except: pass
            driver.refresh()
            time.sleep(2)

        driver.get(company_url)
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((By.XPATH, '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
        )

        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', '').strip()
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values
    except Exception as e:
        print(f"Error at {company_url}: {e}")
        return []
    finally:
        driver.quit()

# ---------------- MAIN LOOP (SEQUENTIAL) ---------------- #
# We enumerate starting from 0 to match our data_rows list index
for i in range(len(company_list)):
    # 1. Skip if before our checkpoint or outside our range
    if i < last_i or i > END_INDEX:
        continue
        
    # 2. Handle Sharding (Parallel instances)
    if i % SHARD_STEP != SHARD_INDEX:
        continue

    url = company_list[i]
    name = name_list[i]

    # Skip empty rows
    if not url or not url.startswith("http"):
        print(f"⏩ Index {i}: Skipping empty/invalid URL.")
        continue

    print(f"🚀 Processing {i}: {name}")

    scraped_values = scrape_tradingview(url)
    
    if scraped_values:
        # Construct row: [Name, Date, Val1, Val2...]
        row_to_upload = [name, current_date] + scraped_values
        try:
            sheet_data.append_row(row_to_upload, table_range='A1')
            print(f"✅ Saved data for {name}.")
        except Exception as e:
            print(f"⚠️ Append failed for {name}: {e}")
    else:
        print(f"⚠️ No data found for {name}.")

    # Update checkpoint after every attempted row
    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1)) # Save i + 1 so it starts at the NEXT row on restart

    time.sleep(1)
