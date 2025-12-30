from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import gspread
from datetime import date
import os
import time
import json
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

# ---------------- BROWSER SETUP ---------------- #
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# ---------------- GOOGLE SHEETS AUTH ---------------- #
try:
    gc = gspread.service_account("credentials.json")
    sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
    
    list_workbook = gc.open('Stock List')
    list_sheet = list_workbook.worksheet('Sheet1')
    all_rows = list_sheet.get_all_values()
    data_rows = all_rows[1:]
    
    name_list = [row[0] if len(row) > 0 else "" for row in data_rows]
    company_list = [row[4] if len(row) > 4 else "" for row in data_rows]
except Exception as e:
    print(f"❌ Initialization Error: {e}")
    exit(1)

current_date = date.today().strftime("%m/%d/%Y")

# ---------------- IMPROVED SCRAPER LOGIC ---------------- #
def get_driver():
    """Initializes driver once to prevent memory leaks from 2400 restarts."""
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_window_size(1920, 1080)
    
    # Load cookies once at startup if they exist
    if os.path.exists("cookies.json"):
        try:
            driver.get("https://www.tradingview.com/")
            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for cookie in cookies:
                cookie_to_add = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
                driver.add_cookie(cookie_to_add)
            driver.refresh()
            time.sleep(2)
        except: pass
    return driver

def scrape_tradingview(driver, company_url):
    """Refactored to use an existing driver and handle retries."""
    try:
        driver.get(company_url)
        # Robust selector: TradingView uses this class for the 'Values' in the details pane
        val_class = "valueValue-l31H9iuA"
        
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CLASS_NAME, val_class))
        )
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        # Find elements with the specific class; removed apply-common-tooltip to be broader
        values = [
            el.get_text().replace('−', '-').replace('∅', '').strip()
            for el in soup.find_all("div", class_=lambda x: x and val_class in x)
        ]
        return values
    except Exception as e:
        print(f"   ⚠️ Scrape attempt failed: {e}")
        return []

# ---------------- MAIN LOOP ---------------- #
driver = get_driver()

try:
    for i in range(len(company_list)):
        if i < last_i or i > END_INDEX:
            continue
            
        if i % SHARD_STEP != SHARD_INDEX:
            continue

        url = company_list[i]
        name = name_list[i]
        target_row_number = i + 2 

        if not url or not url.startswith("http"):
            continue

        print(f"🚀 [Shard {SHARD_INDEX}] Row {target_row_number}: {name}")

        # RETRY MECHANISM: Try up to 2 times before giving up on a stock
        scraped_values = []
        for attempt in range(2):
            scraped_values = scrape_tradingview(driver, url)
            if scraped_values:
                break
            print(f"   🔄 Retry {attempt + 1} for {name}...")
            time.sleep(2)

        if scraped_values:
            row_to_upload = [name, current_date] + scraped_values
            try:
                range_label = f"A{target_row_number}"
                sheet_data.update(range_name=range_label, values=[row_to_upload])
                print(f"   ✅ Placed in Row {target_row_number}")
            except Exception as e:
                print(f"   ❌ GSheet Error (API Quota?): {e}")
                time.sleep(5) # Cooldown if API is throttled
        else:
            print(f"   ❌ Failed to scrape {name} after retries.")

        # Update checkpoint
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        # Small delay to respect Google Sheets API limits
        time.sleep(0.5)

finally:
    print("🧹 Cleaning up driver...")
    driver.quit()
