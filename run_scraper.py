import sys # Added for flushing
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from bs4 import BeautifulSoup
import gspread
from datetime import date
import os
import time
import json
from webdriver_manager.chrome import ChromeDriverManager

# Force print statements to show up in GitHub Actions immediately
def log(msg):
    print(msg, flush=True)

# ---------------- SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 1

log(f"🚀 STARTING: Shard {SHARD_INDEX} (Step: {SHARD_STEP}) from Index {last_i}")

# ---------------- GOOGLE SHEETS AUTH ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open('Stock List').worksheet('Sheet1')
    sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
    
    log("📥 Downloading company lists...")
    company_list = sheet_main.col_values(5)
    name_list = sheet_main.col_values(1)
    current_date = date.today().strftime("%m/%d/%Y")
    log(f"✅ Data fetched. Total companies in list: {len(company_list)}")
except Exception as e:
    log(f"❌ GOOGLE SHEETS ERROR: {e}")
    sys.exit(1)

# ---------------- CHROME SETUP ---------------- #
log("🌐 Setting up headless Chrome...")
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--window-size=1920,1080")
# Set a user agent to prevent being stuck on a blank page
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")

try:
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    log("✅ Chrome Driver initialized.")
except Exception as e:
    log(f"❌ CHROME INIT ERROR: {e}")
    sys.exit(1)

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, company_url):
    try:
        driver.get(company_url)
        # Use a slightly shorter timeout for the wait
        WebDriverWait(driver, 20).until(
            EC.visibility_of_element_located((By.CLASS_NAME, "valueValue-l31H9iuA"))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values
    except TimeoutException:
        return []
    except Exception as e:
        log(f"   ⚠️ Scrape Exception: {str(e)[:50]}")
        return []

# ---------------- MAIN LOOP ---------------- #
# Load cookies
if os.path.exists("cookies.json"):
    log("🍪 Applying cookies...")
    try:
        driver.get("https://www.tradingview.com/")
        with open("cookies.json", "r", encoding="utf-8") as f:
            cookies = json.load(f)
        for cookie in cookies:
            cookie_to_add = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
            driver.add_cookie(cookie_to_add)
        driver.refresh()
        time.sleep(3)
    except Exception as e:
        log(f"   ⚠️ Cookie warning: {e}")

batch_list = []
BATCH_SIZE = 5 

log("▶️ Beginning Loop...")

for i, company_url in enumerate(company_list[last_i:], last_i):
    if i % SHARD_STEP != SHARD_INDEX:
        continue
    if i > 2500:
        log("🏁 Limit reached.")
        break

    name = name_list[i] if i < len(name_list) else f"Row {i}"
    log(f"🔍 [{i}] Scraping: {name}")

    values = scrape_tradingview(driver, company_url)
    
    if values:
        row_data = [name, current_date] + values
        target_row = i + 1 
        batch_list.append({'range': f'A{target_row}', 'values': [row_data]})
        log(f"   📦 Buffered ({len(batch_list)}/{BATCH_SIZE})")
    else:
        log(f"   ⏭️ No data for {name}")

    if len(batch_list) >= BATCH_SIZE:
        try:
            log("📤 Writing batch to Sheets...")
            sheet_data.batch_update(batch_list)
            batch_list = []
            log("✅ Batch complete.")
        except Exception as e:
            log(f"❌ WRITE ERROR: {e}")
            time.sleep(10)

    # Checkpoint
    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    time.sleep(1)

# Final write
if batch_list:
    sheet_data.batch_update(batch_list)
    log("✅ Final data saved.")

driver.quit()
log("All done ✅")
