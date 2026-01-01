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
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

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
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.set_page_load_timeout(45)
    
    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/") 
            time.sleep(3)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    new_cookie = {k: v for k, v in c.items() if k in ('name', 'value', 'path', 'secure', 'expiry')}
                    driver.add_cookie(new_cookie)
                except: continue
            driver.refresh()
            time.sleep(2)
            log("✅ Cookies applied successfully")
        except Exception as e:
            log(f"   ⚠️ Cookie error: {str(e)[:50]}")
    return driver

# ---------------- SCRAPER LOGIC ---------------- #
def scrape_tradingview(driver, url):
    try:
        driver.get(url)
        
        # Target the specific value containers (CSS is more stable than XPATH)
        wait = WebDriverWait(driver, 35)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "valueValue-l31H9iuA")))
        
        # Calculation Buffer: Let the technicals finish calculating
        time.sleep(3)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        val_elements = soup.find_all("div", class_="valueValue-l31H9iuA")
        
        # Scrape raw values without modification
        values = [el.get_text().replace('−', '-').replace('∅', 'None').strip() for el in val_elements]
        
        if not values or all(v == '' for v in values):
            return None 

        return values
    except (TimeoutException, NoSuchElementException):
        return []
    except WebDriverException:
        return "RESTART"

# ---------------- INITIAL SETUP & BATCH READ ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open('Stock List').worksheet('Sheet1')
    sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
    
    # --- BATCH READ IMPLEMENTATION ---
    # Fetch all data at once (Columns A to E) to avoid multiple col_values() calls
    all_rows = sheet_main.get_all_values() 
    
    # all_rows[i][0] is Name, all_rows[i][4] is URL (index 0 and 4)
    current_date = date.today().strftime("%m/%d/%Y")
    log(f"✅ Batch Read Complete. Total Rows: {len(all_rows)}")
except Exception as e:
    log(f"❌ Setup Error: {e}"); sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = create_driver()
batch_list = []
BATCH_SIZE = 50 

try:
    # We start from last_i. all_rows[0] is usually headers.
    for i in range(last_i, len(all_rows)):
        if i % SHARD_STEP != SHARD_INDEX: continue
        if i > 2500: break

        name = all_rows[i][0]  # Column A
        url = all_rows[i][4]   # Column E
        
        if not url or "tradingview.com" not in url:
            log(f"⏭️ Skipping Row {i}: Invalid URL")
            continue

        log(f"🔍 [{i}] Scraping: {name}")

        values = scrape_tradingview(driver, url)
        
        # Retry once if data is empty
        if values is None:
            log("   ⚠️ Retrying...")
            time.sleep(2)
            values = scrape_tradingview(driver, url)

        # Handle Crash Recovery
        if values == "RESTART":
            try: driver.quit()
            except: pass
            driver = create_driver()
            values = scrape_tradingview(driver, url)
            if values == "RESTART": values = []

        if isinstance(values, list) and values:
            target_row = i + 1 # Sheets are 1-indexed
            batch_list.append({
                'range': f'A{target_row}', 
                'values': [[name, current_date] + values]
            })
            log(f"   📦 Buffered ({len(batch_list)}/{BATCH_SIZE})")
        else:
            log(f"   ❌ No data for {name}")

        # Batch Write
        if len(batch_list) >= BATCH_SIZE:
            try:
                sheet_data.batch_update(batch_list)
                log(f"🚀 SUCCESS: Uploaded {len(batch_list)} items.")
                batch_list = [] 
            except Exception as e:
                log(f"⚠️ API Error: {e}")
                if "429" in str(e): 
                    log("⏳ Quota Hit! Waiting 60s...")
                    time.sleep(60)

        with open(checkpoint_file, "w") as f: f.write(str(i))

finally:
    if batch_list:
        try: sheet_data.batch_update(batch_list); log(f"✅ FINAL SAVE: {len(batch_list)} items.")
        except: pass
    driver.quit()
