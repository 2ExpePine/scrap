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
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import gspread
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIG ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
MAX_RETRIES = 2  # How many times to re-try a single stock if it fails
BATCH_SIZE = 30  # Smaller batches are safer for Google Sheets API

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log("🌐 Initializing Stealth Chrome Instance...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    
    # --- STEALTH ARGUMENTS ---
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.exclude_switches = ["enable-automation"]
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.set_page_load_timeout(30)
    
    # --- COOKIES ---
    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(2)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    cookie = {k: v for k, v in c.items() if k in ('name', 'value', 'path', 'secure', 'expiry')}
                    driver.add_cookie(cookie)
                except: continue
            driver.refresh()
            log("✅ Cookies applied")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:40]}")
    return driver

# ---------------- SCRAPER LOGIC ---------------- #
def scrape_tradingview(driver, url):
    """Returns a list of values or None if failed."""
    try:
        driver.get(url)
        
        # 1. Wait for the technical analysis container using CSS (More stable than XPATH)
        # We look for the class that contains the indicator values
        wait = WebDriverWait(driver, 25)
        wait.until(EC.presence_of_element_located((By.CLASS_NAME, "valueValue-l31H9iuA")))
        
        # 2. Lazy-load buffer: TradingView often loads the UI but calculates the math a second later
        time.sleep(2)
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Find all divs that hold values (matches classes starting with 'valueValue-')
        val_elements = soup.find_all("div", class_=lambda x: x and "valueValue-" in x)
        
        values = [el.get_text().replace('−', '-').replace('∅', 'None').strip() for el in val_elements]
        
        # Validate: If we got 0 items, something is wrong
        if not values:
            return None
            
        return values
    except (TimeoutException, WebDriverException) as e:
        log(f"   ❌ Load Error: {type(e).__name__}")
        return "RESTART" if isinstance(e, WebDriverException) else None

# ---------------- SETUP ---------------- #
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open('Stock List').worksheet('Sheet1')
    sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
    company_list = sheet_main.col_values(5)
    name_list = sheet_main.col_values(1)
    current_date = date.today().strftime("%m/%d/%Y")
except Exception as e:
    log(f"❌ Critical Setup Error: {e}"); sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = create_driver()
batch_list = []
checkpoint_file = f"checkpoint_{SHARD_INDEX}.txt"
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 1

try:
    for i, url in enumerate(company_list[last_i:], last_i):
        if i % SHARD_STEP != SHARD_INDEX: continue
        if i > 2500: break

        name = name_list[i] if i < len(name_list) else f"Row {i}"
        
        # --- RETRY WRAPPER ---
        data_found = False
        for attempt in range(MAX_RETRIES + 1):
            log(f"🔍 [{i}] {name} (Attempt {attempt+1})")
            
            values = scrape_tradingview(driver, url)

            if values == "RESTART":
                driver.quit()
                driver = create_driver()
                continue
            
            if values and isinstance(values, list):
                target_row = i + 1
                batch_list.append({
                    'range': f'A{target_row}', 
                    'values': [[name, current_date] + values]
                })
                data_found = True
                break # Exit retry loop on success
            
            log(f"   ⚠️ No data for {name}, retrying in 3s...")
            time.sleep(3)

        if not data_found:
            log(f"   🛑 FAILED PERMANENTLY: {name}")

        # --- BATCH UPLOAD ---
        if len(batch_list) >= BATCH_SIZE:
            try:
                sheet_data.batch_update(batch_list)
                log(f"🚀 SUCCESS: Uploaded batch of {len(batch_list)}")
                batch_list = []
            except Exception as e:
                log(f"⚠️ Sheet API Error: {e}")
                time.sleep(60) # Wait for quota reset

        # Update checkpoint every row
        with open(checkpoint_file, "w") as f: f.write(str(i))

finally:
    if batch_list:
        try: sheet_data.batch_update(batch_list)
        except: pass
    driver.quit()
