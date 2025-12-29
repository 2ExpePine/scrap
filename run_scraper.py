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
START_INDEX = int(os.getenv("START_INDEX", "0")) 
END_INDEX = int(os.getenv("END_INDEX", "2500"))

# IMPORTANT: Every shard MUST have its own checkpoint file name
# Your YAMLs are already doing this by passing UNIQUE filenames
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_shard_{SHARD_INDEX}.txt")

if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        last_i = int(f.read().strip())
else:
    last_i = START_INDEX

# ---------------- BROWSER SETUP (UNCHANGED) ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--remote-debugging-port=9222")

# ---------------- GOOGLE SHEETS AUTH (UNCHANGED) ---------------- #
try:
    gc = gspread.service_account("credentials.json")
except Exception as e:
    print(f"Error loading credentials.json: {e}")
    exit(1)

sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

# ---------------- READ STOCK LIST FROM GOOGLE SHEETS ---------------- #
print(f"📥 [Shard {SHARD_INDEX}] Fetching stock list...")

try:
    list_workbook = gc.open('Stock List')
    list_sheet = list_workbook.worksheet('Sheet1')
    all_rows = list_sheet.get_all_values()
    
    # Slice from header downwards
    data_rows = all_rows[1:] 
    
    name_list = [row[0] if len(row) > 0 else "" for row in data_rows]
    company_list = [row[4] if len(row) > 4 else "" for row in data_rows]

    print(f"✅ Loaded {len(company_list)} companies.")
except Exception as e:
    print(f"❌ Error reading Google Sheet: {e}")
    exit(1)

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER FUNCTION (UNCHANGED) ---------------- #
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

# ---------------- MAIN LOOP (FIXED FOR SEQUENTIAL SHARDING) ---------------- #
for i in range(len(company_list)):
    # 1. Sequence Bounds
    if i < last_i:
        continue
    if i > END_INDEX:
        break
        
    # 2. SHARDING LOGIC
    # This ensures Shard 0 takes row 0, 20, 40...
    # Shard 1 takes row 1, 21, 41...
    # They stay in their own "lane" but always move forward in order.
    if i % SHARD_STEP != SHARD_INDEX:
        continue

    url = company_list[i]
    name = name_list[i]

    if not url or not url.startswith("http"):
        print(f"⏩ Row {i}: Skipping empty/invalid URL.")
        # Even if skipped, update checkpoint so we don't check this row again
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))
        continue

    print(f"🚀 [Shard {SHARD_INDEX}] Processing Row {i}: {name}")

    scraped_values = scrape_tradingview(url)
    
    if scraped_values:
        row_to_upload = [name, current_date] + scraped_values
        try:
            sheet_data.append_row(row_to_upload, table_range='A1')
            print(f"✅ Saved data for {name}.")
        except Exception as e:
            print(f"⚠️ Append failed for {name}: {e}")
    else:
        print(f"⚠️ No data found for {name}.")

    # 3. UPDATE CHECKPOINT (Unique per Shard)
    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1)) 

    time.sleep(1)
