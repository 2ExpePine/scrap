import os
import time
import json
import gspread
from datetime import date
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- CONFIGURATION (MATCHING YOUR 4 YAMLs) ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "20")) 
START_INDEX = int(os.getenv("START_INDEX", "0")) 
END_INDEX = int(os.getenv("END_INDEX", "2500"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_shard_{SHARD_INDEX}.txt")

def get_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    driver.set_window_size(1920, 1080)
    return driver

# ---------------- GOOGLE SHEETS SETUP ---------------- #
gc = gspread.service_account("credentials.json")
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')
list_sheet = gc.open('Stock List').worksheet('Sheet1')
all_rows = list_sheet.get_all_values()[1:] 

name_list = [row[0] for row in all_rows]
company_list = [row[4] for row in all_rows]
current_date = date.today().strftime("%m/%d/%Y")

# ---------------- YOUR EXACT EXTRACTION LOGIC ---------------- #
def scrape_tradingview(driver, url):
    try:
        driver.get(url)
        
        # Load cookies ONLY if we are on the TradingView domain
        if os.path.exists("cookies.json"):
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for cookie in cookies:
                try:
                    driver.add_cookie({k: v for k, v in cookie.items() if k in ['name', 'value', 'domain', 'path']})
                except: pass
        
        # We wait for the specific class you used in your old code
        # This ensures we are scraping the exact same data points
        wait = WebDriverWait(driver, 30)
        wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "valueValue-l31H9iuA")))
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # EXACT SAME LOGIC AS YOUR ORIGINAL CODE
        values = [
            el.get_text().replace('−', '-').replace('∅', '').strip()
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values
    except Exception as e:
        print(f"      ⚠️ Error: {e}")
        return []

# ---------------- MAIN EXECUTION ---------------- #
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        last_i = int(f.read().strip())
else:
    last_i = START_INDEX

driver = get_driver()
session_count = 0

try:
    for i in range(len(company_list)):
        if i < last_i or i > END_INDEX: continue
        if i % SHARD_STEP != SHARD_INDEX: continue

        # REFRESH BROWSER EVERY 25 ROWS
        # This solves the "Missing Rows" issue caused by memory crashes
        session_count += 1
        if session_count % 25 == 0:
            driver.quit()
            driver = get_driver()

        url = company_list[i]
        name = name_list[i]
        if not url.startswith("http"): continue

        print(f"🚀 [Shard {SHARD_INDEX}] Row {i+2}: {name}")
        
        scraped_values = scrape_tradingview(driver, url)
        
        if scraped_values:
            target_row = i + 2
            row_to_upload = [name, current_date] + scraped_values
            
            # Update specific row with retry logic for API limits
            for attempt in range(3):
                try:
                    sheet_data.update(range_name=f"A{target_row}", values=[row_to_upload])
                    break
                except:
                    time.sleep(5)
        
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))
        
        time.sleep(1) 

finally:
    driver.quit()
