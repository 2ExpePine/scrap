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

# Force immediate log output for GitHub Actions
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
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false") # Block images to save RAM
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.set_page_load_timeout(35)
    
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
        # ORIGINAL XPATH MAINTAINED AS REQUESTED
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values
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
    log(f"✅ Setup complete. Shard {SHARD_INDEX} starting at index {last_i}")
except Exception as e:
    log(f"❌ Setup Error: {e}"); sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = create_driver()
batch_list = []
BATCH_SIZE = 50  # Maximum efficiency for large lists

try:
    for i, url in enumerate(company_list[last_i:], last_i):
        if i % SHARD_STEP != SHARD_INDEX: continue
        if i > 2500: break

        name = name_list[i] if i < len(name_list) else f"Row {i}"
        log(f"🔍 [{i}] Scraping: {name}")

        values = scrape_tradingview(driver, url)

        # Chrome Crash Recovery
        if values == "RESTART":
            log("♻️ Restarting Browser Process...")
            driver.quit()
            driver = create_driver()
            values = scrape_tradingview(driver, url)
            if values == "RESTART": values = []

        if isinstance(values, list) and values:
            target_row = i + 1
            batch_list.append({
                'range': f'A{target_row}', 
                'values': [[name, current_date] + values]
            })
            log(f"   📦 Buffered ({len(batch_list)}/{BATCH_SIZE})")
        else:
            log(f"   ⏭️ Skipped {name}")

        # Execute Batch Write when 50 items are ready
        if len(batch_list) >= BATCH_SIZE:
            try:
                sheet_data.batch_update(batch_list)
                log(f"🚀 SUCCESS: Mass Batch Write (Size: {len(batch_list)})")
                batch_list = [] # Clear only after success
            except Exception as e:
                log(f"⚠️ Sheets Quota Hit or Error: {e}")
                if "429" in str(e): 
                    log("   ⏳ Quota exceeded. Retrying batch in 45s...")
                    time.sleep(45)

        # Update Checkpoint
        with open(checkpoint_file, "w") as f: f.write(str(i))
        
        # Reduced sleep because batching protects us from the API limit
        time.sleep(0.3) 

finally:
    # FINAL FLUSH: Saves any remaining items (even if less than 50)
    if batch_list:
        try: 
            sheet_data.batch_update(batch_list)
            log(f"✅ FINAL SAVE: Wrote remaining {len(batch_list)} items.")
        except Exception as e:
            log(f"❌ Could not save final buffer: {e}")
    driver.quit()
    log("🏁 Shard processing complete.")
