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
from webdriver_manager.chrome import ChromeDriverManager

# ---------------- SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP = int(os.getenv("SHARD_STEP", "1"))
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 1

# ---------------- CHROME SETUP ---------------- #
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

sheet_main = gc.open('Stock List').worksheet('Sheet1')
sheet_data = gc.open('Tradingview Data Reel Experimental May').worksheet('Sheet5')

# Batch read once
company_list = sheet_main.col_values(5)
name_list = sheet_main.col_values(1)
current_date = date.today().strftime("%m/%d/%Y")

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, company_url):
    try:
        driver.get(company_url)
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
    except NoSuchElementException:
        return []
    except Exception as e:
        print(f"Error scraping {company_url}: {e}")
        return []

# ---------------- MAIN LOOP ---------------- #
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)

# Load cookies (once per shard)
if os.path.exists("cookies.json"):
    driver.get("https://www.tradingview.com/")
    with open("cookies.json", "r", encoding="utf-8") as f:
        cookies = json.load(f)
    for cookie in cookies:
        try:
            cookie_to_add = {k: cookie[k] for k in ('name', 'value', 'domain', 'path') if k in cookie}
            cookie_to_add['secure'] = cookie.get('secure', False)
            cookie_to_add['httpOnly'] = cookie.get('httpOnly', False)
            driver.add_cookie(cookie_to_add)
        except Exception:
            pass
    driver.refresh()
    time.sleep(2)
else:
    print("⚠️ cookies.json not found, scraping without login")

buffer = []
BATCH_SIZE = 50

for i, company_url in enumerate(company_list[last_i:], last_i):
    if i % SHARD_STEP != SHARD_INDEX:
        continue
    if i > 2500:
        print("Reached scraping limit.")
        break

    name = name_list[i] if i < len(name_list) else f"Row {i}"
    print(f"Scraping {i}: {name}")

    values = scrape_tradingview(driver, company_url)
    if values:
        buffer.append([name, current_date] + values)
    else:
        print(f"Skipping {name}: no data")

    # Write checkpoint
    with open(checkpoint_file, "w") as f:
        f.write(str(i))

    # Write every 50 rows
    if len(buffer) >= BATCH_SIZE:
        try:
            sheet_data.append_rows(buffer, table_range='A1')
            print(f"✅ Wrote batch of {len(buffer)} rows.")
            buffer.clear()
        except Exception as e:
            print(f"⚠️ Batch write failed: {e}")

    time.sleep(1)

# Final flush
if buffer:
    try:
        sheet_data.append_rows(buffer, table_range='A1')
        print(f"✅ Final batch of {len(buffer)} rows written.")
    except Exception as e:
        print(f"⚠️ Final write failed: {e}")

driver.quit()
print("All done ✅")
