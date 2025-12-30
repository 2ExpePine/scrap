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

# ---------------- SHARDING & SEQUENCE SETUP (UNCHANGED) ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
START_INDEX = int(os.getenv("START_INDEX", "0")) 
END_INDEX = int(os.getenv("END_INDEX", "2500"))
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

# ---------------- READ STOCK LIST (UNCHANGED) ---------------- #
try:
    list_workbook = gc.open('Stock List')
    list_sheet = list_workbook.worksheet('Sheet1')
    all_rows = list_sheet.get_all_values()
    data_rows = all_rows[1:] 
    
    name_list = [row[0] if len(row) > 0 else "" for row in data_rows]
    company_list = [row[4] if len(row) > 4 else "" for row in data_rows]
except Exception as e:
    print(f"❌ Error reading Google Sheet: {e}")
    exit(1)

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER FUNCTION (UNCHANGED LOGIC - REFACTORED FOR PERSISTENCE) ---------------- #
def scrape_tradingview(driver, company_url):
    """
    Maintains your EXACT scraping logic and XPATH.
    Accepts an existing driver to prevent memory crashes over 2400 rows.
    """
    try:
        # Re-use existing session for cookies if not already set
        if os.path.exists("cookies.json") and driver.current_url == "data:,":
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
        
        # YOUR ORIGINAL XPATH
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
        print(f"   ⚠️ Scrape Error for {company_url}: {e}")
        return []

# ---------------- MAIN LOOP (FIXED FOR FULL COVERAGE) ---------------- #
# Start the browser ONCE outside the loop to ensure it doesn't crash mid-run
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
driver.set_window_size(1920, 1080)

try:
    for i in range(len(company_list)):
        if i < last_i or i > END_INDEX:
            continue
            
        if i % SHARD_STEP != SHARD_INDEX:
            continue

        url = company_list[i]
        name = name_list[i]

        if not url or not url.startswith("http"):
            continue

        print(f"🚀 [Shard {SHARD_INDEX}] Working on Row {i+2}: {name}")

        # RETRY LOGIC: Added 1 retry attempt if the first load fails
        scraped_values = []
        for attempt in range(2):
            scraped_values = scrape_tradingview(driver, url)
            if scraped_values:
                break
            print(f"   🔄 Retry {attempt + 1} for {name}...")
            time.sleep(3)

        if scraped_values:
            target_row_number = i + 2 
            row_to_upload = [name, current_date] + scraped_values
            
            try:
                # Update specific row (UNCHANGED LOGIC)
                range_label = f"A{target_row_number}"
                sheet_data.update(range_name=range_label, values=[row_to_upload])
                print(f"✅ Placed {name} in Row {target_row_number}")
            except Exception as e:
                print(f"⚠️ GSheet Update failed: {e}")
                time.sleep(5) # Delay to recover from Google API rate limits
        else:
            print(f"❌ Failed to scrape {name} after all attempts.")
        
        # Update checkpoint (UNCHANGED)
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        # Respectful delay between stocks
        time.sleep(0.5)

finally:
    # Always close the browser when finished
    driver.quit()
