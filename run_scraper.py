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

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    gc = gspread.service_account("credentials.json")
except Exception as e:
    print(f"Error loading credentials.json: {e}")
    exit(1)

# Target sheet for WRITING
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

# ---------------- READ STOCK LIST ---------------- #
try:
    list_workbook = gc.open('Stock List')
    list_sheet = list_workbook.worksheet('Sheet1')
    all_rows = list_sheet.get_all_values()
    data_rows = all_rows[1:] # Skip header
    
    name_list = [row[0] if len(row) > 0 else "" for row in data_rows]
    company_list = [row[4] if len(row) > 4 else "" for row in data_rows]
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
        return []
    finally:
        driver.quit()

# ---------------- MAIN LOOP (FIXED FOR EXACT ORDER) ---------------- #
for i in range(len(company_list)):
    if i < last_i or i > END_INDEX:
        continue
        
    # Sharding logic
    if i % SHARD_STEP != SHARD_INDEX:
        continue

    url = company_list[i]
    name = name_list[i]

    if not url or not url.startswith("http"):
        continue

    print(f"🚀 [Shard {SHARD_INDEX}] Working on Row {i+2}: {name}")

    scraped_values = scrape_tradingview(url)
    
    if scraped_values:
        row_to_upload = [name, current_date] + scraped_values
        
        # --- THE FIX FOR ORDER ---
        # Instead of appending, we calculate the EXACT row in Sheet5.
        # Since 'i' is the list index (starting at 0), Row 1 is header, 
        # so Row 2 corresponds to i=0.
        target_row_number = i + 2 
        
        try:
            # We update the range (e.g., 'A10:Z10') so it fills its specific slot
            # This ensures even if Shard 10 finishes before Shard 1, 
            # Shard 10's data goes to Row 12 and Shard 1's data goes to Row 3.
            range_label = f"A{target_row_number}"
            sheet_data.update(range_name=range_label, values=[row_to_upload])
            print(f"✅ Placed {name} in Row {target_row_number}")
        except Exception as e:
            print(f"⚠️ Update failed for {name}: {e}")
    
    # Update checkpoint
    with open(checkpoint_file, "w") as f:
        f.write(str(i + 1))

    time.sleep(1)
