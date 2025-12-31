import sys
import os
import time
import json
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import gspread
from webdriver_manager.chrome import ChromeDriverManager

# Force immediate log output
def log(msg):
    print(msg, flush=True)

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 1

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log("🌐 Initializing Hardened Chrome Instance...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage") # Fixes the 'unknown' stacktrace crash
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false") # Saves ~30% RAM
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.set_page_load_timeout(30)
    
    # Apply Cookies
    if os.path.exists("cookies.json"):
        try:
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                driver.add_cookie({k: v for k, v in c.items() if k in ('name', 'value', 'domain', 'path')})
            driver.refresh()
            time.sleep(2)
        except Exception as e:
            log(f"   ⚠️ Cookie error: {e}")
    return driver

# ---------------- SCRAPER LOGIC ---------------- #
def scrape_tradingview(driver, url):
    try:
        driver.get(url)
        # Wait for the data container to appear
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CLASS_NAME, "valueValue-l31H9iuA"))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        return [el.get_text().replace('−', '-').replace('∅', 'None') 
                for el in soup.find_all("div", class_="valueValue-l31H9iuA")]
    except (TimeoutException, NoSuchElementException):
        return []
    except WebDriverException as e:
        log(f"   🛑 Browser Crash Detected: {str(e)[:50]}")
        return "RESTART"

# ---------------- INITIAL SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open('Stock List').worksheet('Sheet1')
    sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
    company_list = sheet_main.col_values(5)
    name_list = sheet_main.col_values(1)
    current_date = date.today().strftime("%m/%d/%Y")
except Exception as e:
    log(f"❌ Setup Error: {e}"); sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = create_driver()
batch_list = []
BATCH_SIZE = 8 # Balanced for speed and API quota

try:
    for i, url in enumerate(company_list[last_i:], last_i):
        if i % SHARD_STEP != SHARD_INDEX: continue
        if i > 2500: break

        name = name_list[i] if i < len(name_list) else f"Row {i}"
        log(f"🔍 [{i}] Scraping: {name}")

        values = scrape_tradingview(driver, url)

        # Auto-Restart Logic if Chrome dies
        if values == "RESTART":
            log("♻️ Restarting Browser...")
            driver.quit()
            driver = create_driver()
            values = scrape_tradingview(driver, url) # Retry once
            if values == "RESTART": values = [] # Skip if still failing

        if isinstance(values, list) and values:
            batch_list.append({'range': f'A{i+1}', 'values': [[name, current_date] + values]})
            log(f"   📦 Buffered ({len(batch_list)}/{BATCH_SIZE})")
        else:
            log(f"   ⏭️ Skipped {name}")

        # Batch Write to Google Sheets
        if len(batch_list) >= BATCH_SIZE:
            try:
                sheet_data.batch_update(batch_list)
                log(f"🚀 Batch Saved (Row {i+1})")
                batch_list = []
            except Exception as e:
                log(f"⚠️ Sheets Error: {e}")
                if "429" in str(e): time.sleep(20)

        # Update Checkpoint
        with open(checkpoint_file, "w") as f: f.write(str(i))
        time.sleep(1)

finally:
    if batch_list:
        try: sheet_data.batch_update(batch_list); log("✅ Final data saved.")
        except: pass
    driver.quit()
    log("🏁 All done.")
