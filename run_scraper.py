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
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 0

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
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=opts
    )
    driver.set_page_load_timeout(40)

    # ---- COOKIE LOGIC (UNCHANGED) ----
    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(3)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    driver.add_cookie({
                        k: v for k, v in c.items()
                        if k in ("name", "value", "path", "secure", "expiry")
                    })
                except:
                    continue
            driver.refresh()
            time.sleep(2)
            log("✅ Cookies applied successfully")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:60]}")

    return driver

# ---------------- SCRAPER LOGIC ---------------- #
TV_WAIT_XPATH = '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'

def scrape_tradingview(driver, url):
    if not url or not str(url).strip():
        return []

    try:
        driver.get(url)
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((By.XPATH, TV_WAIT_XPATH))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace("−", "-").replace("∅", "None")
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values
    except (TimeoutException, NoSuchElementException):
        return []
    except WebDriverException:
        log("🛑 Browser Crash Detected")
        return "RESTART"

def scrape_with_restart(driver, url):
    values = scrape_tradingview(driver, url)
    if values != "RESTART":
        return driver, values

    # restart once
    try:
        driver.quit()
    except:
        pass
    driver = create_driver()
    values = scrape_tradingview(driver, url)
    if values == "RESTART":
        values = []
    return driver, values

# ---------------- INITIAL SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("Tradingview Data Reel Experimental May").worksheet("Sheet5")

    # ✅ Names in column A
    name_list = sheet_main.col_values(1)

    # ✅ Two URL columns
    url_list_c = sheet_main.col_values(3)  # Column C
    url_list_e = sheet_main.col_values(4)  # Column E

    current_date = date.today().strftime("%m/%d/%Y")
    max_len = max(len(name_list), len(url_list_c), len(url_list_e))

    log(f"✅ Setup complete | Shard {SHARD_INDEX} | Resume index {last_i} | Total rows {max_len}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = create_driver()
batch_list = []
BATCH_SIZE = 50

try:
    for i in range(last_i, max_len):
        if i % SHARD_STEP != SHARD_INDEX:
            continue
        if i >= 2500:
            break

        name = name_list[i] if i < len(name_list) and name_list[i].strip() else f"Row {i+1}"
        url_c = url_list_c[i].strip() if i < len(url_list_c) and url_list_c[i] else ""
        url_e = url_list_e[i].strip() if i < len(url_list_e) and url_list_e[i] else ""

        if not url_c and not url_e:
            log(f"⏭️ [{i}] Skipped {name} (no URL in C or E)")
            with open(checkpoint_file, "w") as f:
                f.write(str(i + 1))
            continue

        log(f"🔍 [{i}] Scraping: {name}")

        # Scrape both URLs (if present)
        values_c = []
        values_e = []

        if url_c:
            log(f"   • URL(C): {url_c[:70]}")
            driver, values_c = scrape_with_restart(driver, url_c)

        if url_e:
            log(f"   • URL(E): {url_e[:70]}")
            driver, values_e = scrape_with_restart(driver, url_e)

        # ✅ Build ONE combined row
        # Layout:
        # A: Name
        # B: Date
        # C: URL(C)
        # D.. : values from URL(C)
        # then marker + URL(E) + values from URL(E)
        combined_row = (
            [name, current_date]
            + ["URL_C", url_c] + values_c
            + ["URL_E", url_e] + values_e
        )

        target_row = i + 1
        batch_list.append({
            "range": f"A{target_row}",
            "values": [combined_row]
        })
        log(f"📦 Buffered ({len(batch_list)}/{BATCH_SIZE}) | C_vals={len(values_c)} | E_vals={len(values_e)}")

        # Flush batch
        if len(batch_list) >= BATCH_SIZE:
            try:
                sheet_data.batch_update(batch_list)
                log(f"🚀 Saved {len(batch_list)} rows")
                batch_list = []
            except Exception as e:
                log(f"⚠️ API Error: {e}")
                if "429" in str(e):
                    log("⏳ Quota hit, sleeping 60s...")
                    time.sleep(60)

        # Save checkpoint
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        time.sleep(0.5)

finally:
    if batch_list:
        try:
            sheet_data.batch_update(batch_list)
            log(f"✅ Final save: {len(batch_list)} rows")
        except Exception as e:
            log(f"⚠️ Final save failed: {e}")

    try:
        driver.quit()
    except:
        pass

    log("🏁 Scraping completed successfully")
