import time
import csv
import json
from datetime import datetime

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    StaleElementReferenceException
)

# ================== BASE SCRAPER ==================

class BaseScraper:
    def __init__(self, headless=True, timeout=30):
        self.timeout = timeout
        self.driver = self._init_driver(headless)
        self.wait = WebDriverWait(self.driver, self.timeout)

    def _init_driver(self, headless):
        options = uc.ChromeOptions()

        if headless:
            options.add_argument("--headless=new")

        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")

        return uc.Chrome(options=options)

    def open(self, url):
        self.driver.get(url)

    def wait_for(self, by, value):
        return self.wait.until(
            EC.presence_of_element_located((by, value))
        )

    def find_all_safe(self, by, value):
        try:
            return self.driver.find_elements(by, value)
        except Exception:
            return []

    def quit(self):
        try:
            self.driver.quit()
        except Exception:
            pass


# ================== IHG PET LINKS SCRAPER ==================

class IHGPetFriendlyLinksScraper(BaseScraper):

    START_URL = "https://www.ihg.com/explore/pet-friendly-hotels"

    def scrape_links(self):
        self.open(self.START_URL)

        try:
            self.wait_for(
                By.CSS_SELECTOR,
                "ul.cmp-list a.cmp-list__item-link"
            )
        except TimeoutException:
            print("❌ Page did not load lists")
            return []

        time.sleep(2)  # allow all lists to render

        elements = self.find_all_safe(
            By.CSS_SELECTOR,
            "ul.cmp-list a.cmp-list__item-link"
        )

        results = []
        seen = set()

        for el in elements:
            try:
                href = el.get_attribute("href")
                title = el.text.strip()

                if not href or href in seen:
                    continue

                seen.add(href)

                results.append({
                    "title": title,
                    "url": href,
                    "source": "IHG Pet Friendly",
                    "scraped_at": datetime.utcnow().isoformat()
                })

            except StaleElementReferenceException:
                continue

        return results


# ================== DATA WRITER ==================

class DataWriter:

    @staticmethod
    def save_csv(path, rows):
        if not rows:
            return

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=rows[0].keys()
            )
            writer.writeheader()
            writer.writerows(rows)

    @staticmethod
    def save_json(path, data):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)


# ================== MAIN ==================

if __name__ == "__main__":

    OUTPUT_CSV = "ihg_pet_friendly_links.csv"
    OUTPUT_JSON = "ihg_pet_friendly_links.json"

    scraper = IHGPetFriendlyLinksScraper(
        headless=False,
        timeout=30
    )

    try:
        links = scraper.scrape_links()
        print(f"✅ Scraped {len(links)} unique links")

        DataWriter.save_csv(OUTPUT_CSV, links)
        DataWriter.save_json(OUTPUT_JSON, links)

        print("📁 Files saved:")
        print(f" - {OUTPUT_CSV}")
        print(f" - {OUTPUT_JSON}")

    finally:
        scraper.quit()
