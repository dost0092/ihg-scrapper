# ihg_pet_hotels_scraper.py
import os
import re
import csv
import json
import time
from datetime import datetime
from urllib.parse import urlparse

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
    ElementNotInteractableException,
    WebDriverException,
)

# =====================================================
# CONFIG
# =====================================================

START_URL = "https://www.ihg.com/explore/pet-friendly-hotels"

CITY_CSV = "ihg_city_urls.csv"             # stores city/category listing URLs
HOTEL_JSON = "ihg_hotels_output.json"      # full output JSON
HOTEL_CSV = "ihg_hotels_output.csv"        # full output CSV

HEADLESS = False                            # set False to watch the browser
DRIVER_TIMEOUT = 30
SLEEP_AFTER_LOAD = 1.5

RUN_ONLY_ONE_CITY = True                   # run only first city for now
OVERWRITE = False                          # if True, ignores existing outputs

# Fields schema (order)
FIELDS = [
    "hotel_code",
    "hotel_name",
    "address",
    "phone",
    "rating",
    "description",
    "card_price",
    "overview_table_json",
    "pets_json",
    "parking_json",
    "amenities_json",
    "nearby_json",
    "airport_json",
    "is_pet_friendly",
    "last_updated"
]


# =====================================================
# UTILS
# =====================================================

def safe_text(el):
    try:
        return el.text.strip()
    except Exception:
        return None

def now_iso():
    return datetime.utcnow().isoformat()

def cleanup_price(text):
    if not text:
        return None
    return text.strip()

def extract_currency_from_card(card):
    try:
        cur = card.find_element(By.CSS_SELECTOR, ".cmp-card__hotel-price-currency")
        return safe_text(cur)
    except NoSuchElementException:
        return None

def get_hotel_code_from_url(url):
    # Try to pull the 5-char hotel code segment frequently present, e.g., /miaep/
    # Fallback to last path segment heuristic
    if not url:
        return None
    try:
        path = urlparse(url).path.rstrip("/")
        parts = path.split("/")
        # common pattern .../{code}/hoteldetail
        # or .../{city-hotel-name}/{code}/hoteldetail
        for i, p in enumerate(parts):
            if len(p) == 5 and re.match(r"^[a-z0-9]{5}$", p, re.I):
                return p.lower()
        # fallback: last non-hoteldetail segment
        for p in reversed(parts):
            if p and p.lower() not in ("hoteldetail", "amenities"):
                return p.lower()
    except Exception:
        pass
    return None

def click_if_present(driver, wait, locator, extra_sleep=0.5):
    try:
        el = wait.until(EC.element_to_be_clickable(locator))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.2)
        el.click()
        time.sleep(extra_sleep)
        return True
    except (TimeoutException, ElementClickInterceptedException, ElementNotInteractableException):
        return False

def wait_presence(driver, wait, locator, timeout=None):
    try:
        if timeout:
            return WebDriverWait(driver, timeout).until(EC.presence_of_element_located(locator))
        return wait.until(EC.presence_of_element_located(locator))
    except TimeoutException:
        return None

def wait_all_presence(driver, wait, locator, timeout=None):
    try:
        if timeout:
            return WebDriverWait(driver, timeout).until(EC.presence_of_all_elements_located(locator))
        return wait.until(EC.presence_of_all_elements_located(locator))
    except TimeoutException:
        return []

def ensure_dir(path):
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)


# =====================================================
# BASE DRIVER
# =====================================================

class BaseScraper:
    def __init__(self, headless=True, timeout=30):
        self.driver = self._init_driver(headless)
        self.wait = WebDriverWait(self.driver, timeout)

    def _init_driver(self, headless):
        options = uc.ChromeOptions()
        if headless:
            options.add_argument("--headless=new")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--lang=en-US,en;q=0.9")
        options.add_argument("--start-maximized")
        driver = uc.Chrome(options=options)
        driver.set_page_load_timeout(60)
        return driver

    def open(self, url):
        self.driver.get(url)

    def quit(self):
        try:
            self.driver.quit()
        except Exception:
            pass


# =====================================================
# STEP 1 — CITY URL SCRAPER (from main Explore page)
# =====================================================

class IHGCitiesScraper(BaseScraper):
    def scrape_city_urls(self):
        self.open(START_URL)

        # The Explore page lists internal sections/links.
        # We will target links to "Pet-friendly Hotels in <city>" blocks.
        # Using a broad selector that matches the provided content structure:
        # ul.cmp-list a.cmp-list__item-link
        el = wait_presence(
            self.driver, self.wait,
            (By.CSS_SELECTOR, "ul.cmp-list a.cmp-list__item-link")
        )
        time.sleep(SLEEP_AFTER_LOAD)

        links = self.driver.find_elements(By.CSS_SELECTOR, "ul.cmp-list a.cmp-list__item-link")
        seen = set()
        cities = []
        for el in links:
            url = el.get_attribute("href")
            name = el.text.strip()
            if not url or url in seen:
                continue
            # We only want city listing pages (not brand deep links), but the Explore page is mixed.
            # Heuristic: keep links that look like collections/search pages within ihg.com and not brand root pages.
            if "ihg.com" in url and ("hotels" in url or "/explore/" in url or "/destinations" in url or "pet" in url.lower()):
                seen.add(url)
                cities.append({"city_name": name, "city_url": url})

        return cities


# =====================================================
# STEP 2 — HOTEL SCRAPER
# =====================================================

class IHGHotelScraper(BaseScraper):

    def _accept_cookies_if_present(self):
        # Try a few common cookie banners
        possible_selectors = [
            'button#onetrust-accept-btn-handler',
            'button[aria-label="Accept all"]',
            'button:contains("Accept")',
            'button:contains("I Agree")',
        ]
        for sel in possible_selectors:
            try:
                btns = self.driver.find_elements(By.CSS_SELECTOR, sel)
                if btns:
                    btn = btns[0]
                    self.driver.execute_script("arguments[0].click();", btn)
                    time.sleep(0.5)
                    return True
            except Exception:
                pass
        return False

    def scrape_city(self, city):
        self.open(city["city_url"])
        self._accept_cookies_if_present()

        # Wait for hotel list container
        wait_presence(self.driver, self.wait, (By.ID, "hotelList"))
        time.sleep(SLEEP_AFTER_LOAD)

        # Ensure list items present
        cards = self.driver.find_elements(By.CSS_SELECTOR, "#hotelList > div > ul > li")
        hotels = []

        for idx, card in enumerate(cards, start=1):
            try:
                # NAME + URL
                name_el = card.find_element(By.CSS_SELECTOR, "a.cmp-card__title-link")
                hotel_name = safe_text(name_el)
                hotel_url = name_el.get_attribute("href") or ""

                # ADDRESS
                address = None
                try:
                    addr_el = card.find_element(By.CSS_SELECTOR, "address")
                    address = safe_text(addr_el)
                except NoSuchElementException:
                    address = None

                # CARD AMENITIES (list of labels)
                card_amenities = []
                for li in card.find_elements(By.CSS_SELECTOR, ".cmp-amenity-list .cmp-amenity-list__item .cmp-image__title"):
                    label = safe_text(li)
                    if label:
                        card_amenities.append(label)

                # PRICE + CURRENCY
                price_value = None
                try:
                    pv = card.find_element(By.CSS_SELECTOR, ".cmp-card__hotel-price-value")
                    price_value = cleanup_price(safe_text(pv))
                except NoSuchElementException:
                    pass

                currency = extract_currency_from_card(card)

                # RATING (from card, if available)
                rating = None
                try:
                    r_el = card.find_element(By.CSS_SELECTOR, ".cmp-card__guest-reviews .cmp-card__rating-count")
                    rating = safe_text(r_el)
                except NoSuchElementException:
                    pass

                # Prepare base record; detail fields will be filled after visiting detail page
                hotel_record = {
                    "hotel_code": get_hotel_code_from_url(hotel_url),
                    "hotel_name": hotel_name,
                    "address": address,
                    "phone": None,
                    "rating": rating,
                    "description": None,
                    "card_price": f"{price_value} {currency}".strip() if price_value else None,
                    "overview_table_json": None,
                    "pets_json": None,
                    "parking_json": None,
                    "amenities_json": json.dumps(card_amenities, ensure_ascii=False) if card_amenities else None,
                    "nearby_json": None,
                    "airport_json": None,
                    "is_pet_friendly": None,
                    "last_updated": now_iso(),
                    "_detail_url": hotel_url,             # internal helper
                    "_city": city.get("city_name"),       # internal helper
                }

                # Visit detail page in new tab to extract deeper data
                detail = self.scrape_hotel_detail(hotel_url)
                hotel_record.update(detail)

                # Normalize to required JSON strings for *_json fields if dict/list
                for key in ["overview_table_json", "pets_json", "parking_json", "amenities_json", "nearby_json", "airport_json"]:
                    val = hotel_record.get(key)
                    if val is not None and not isinstance(val, str):
                        hotel_record[key] = json.dumps(val, ensure_ascii=False)

                # Coerce is_pet_friendly to string "true"/"false"
                if isinstance(hotel_record.get("is_pet_friendly"), bool):
                    hotel_record["is_pet_friendly"] = "true" if hotel_record["is_pet_friendly"] else "false"

                # Remove helper keys
                hotel_record.pop("_detail_url", None)
                hotel_record.pop("_city", None)

                hotels.append(hotel_record)

            except (NoSuchElementException, StaleElementReferenceException, WebDriverException):
                continue

        return hotels

    def scrape_hotel_detail(self, url):
        out = {
            "phone": None,
            "description": None,
            "overview_table_json": None,
            "pets_json": None,
            "parking_json": None,
            "amenities_json": None,  # if we find a more complete list than card level
            "nearby_json": None,
            "airport_json": None,
            "is_pet_friendly": None,
        }
        if not url:
            return out

        main = self.driver.current_window_handle
        # Open in new tab
        self.driver.switch_to.new_window('tab')
        try:
            self.open(url)
        except WebDriverException:
            # If navigation fails, close tab and return partial
            self.driver.close()
            self.driver.switch_to.window(main)
            return out

        try:
            # If description container present, click "Read more"
            self._expand_description_if_present()

            # Extract description (page general description text)
            out["description"] = self._extract_description_text()

            # Extract highlight amenity icons section (as on-page JSON list of titles)
            highlights = self._extract_highlights_section()
            # We'll store these as part of amenities_json if we don't later overwrite
            if highlights:
                out["amenities_json"] = highlights

            # Extract phone if present on the page
            phone = self._extract_phone()
            if phone:
                out["phone"] = phone

            # Click "View all amenities" to go to amenities page (if available)
            amenities_detail_data = self._open_amenities_page_and_scrape()
            if amenities_detail_data:
                # Merge these into structured fields if present
                # Expect keys: amenities, parking, overview, nearby, airport, pets, phone
                if amenities_detail_data.get("amenities"):
                    out["amenities_json"] = amenities_detail_data["amenities"]
                if amenities_detail_data.get("parking"):
                    out["parking_json"] = amenities_detail_data["parking"]
                if amenities_detail_data.get("overview"):
                    out["overview_table_json"] = amenities_detail_data["overview"]
                if amenities_detail_data.get("nearby"):
                    out["nearby_json"] = amenities_detail_data["nearby"]
                if amenities_detail_data.get("airport"):
                    out["airport_json"] = amenities_detail_data["airport"]
                if amenities_detail_data.get("phone") and not out.get("phone"):
                    out["phone"] = amenities_detail_data["phone"]

            # Click "View pet policy" (if exists) and extract pet policy details
            pets_data = self._open_pet_policy_if_available()
            if pets_data:
                out["pets_json"] = pets_data
                out["is_pet_friendly"] = True
            else:
                # If no explicit policy link, infer pet-friendly from page description/highlights
                out["is_pet_friendly"] = self._infer_pet_friendly(out)

            return out

        finally:
            # Close detail tab and return to main
            try:
                self.driver.close()
                self.driver.switch_to.window(main)
            except Exception:
                pass

    def _expand_description_if_present(self):
        # The user provided an example "Read more" link with class .morelink
        try:
            # Scroll to description container region
            self.driver.execute_script("window.scrollTo(0, 400);")
            time.sleep(0.4)
            more_links = self.driver.find_elements(By.CSS_SELECTOR, "a.morelink, a.moreLink, a.read-more, a.readmore")
            for ml in more_links:
                txt = (ml.text or "").strip().lower()
                if "read more" in txt or "show more" in txt or "see more" in txt:
                    self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", ml)
                    time.sleep(0.2)
                    try:
                        ml.click()
                    except Exception:
                        self.driver.execute_script("arguments[0].click();", ml)
                    time.sleep(0.5)
                    break
        except Exception:
            pass

    def _extract_description_text(self):
        # Heuristics: description blocks often appear near page header or about sections
        # Try known containers first, then fall back to longest paragraph block
        candidates = [
            "div.hotel-description, div.description, .hotel-overview, .vx-description, .property-description",
            "section#overview, section.overview",
            ".cmp-text, .ihg-copy, .content-copy",
        ]
        for sel in candidates:
            try:
                blocks = self.driver.find_elements(By.CSS_SELECTOR, sel)
                texts = []
                for b in blocks:
                    t = safe_text(b)
                    if t and len(t) > 120:
                        texts.append(t)
                if texts:
                    return max(texts, key=len)
            except Exception:
                continue

        # Fallback: longest visible paragraph
        try:
            ps = self.driver.find_elements(By.CSS_SELECTOR, "p")
            best = ""
            for p in ps:
                t = safe_text(p) or ""
                if len(t) > len(best):
                    best = t
            return best if best else None
        except Exception:
            return None

    def _extract_highlights_section(self):
        # The user shared a "vx-highlight-items" with .amenity-title divs
        try:
            container = wait_presence(self.driver, self.wait, (By.CSS_SELECTOR, ".vx-highlight-items"))
            if not container:
                return None
            items = container.find_elements(By.CSS_SELECTOR, ".vx-highlight-item .amenity-title")
            titles = [safe_text(i) for i in items if safe_text(i)]
            return titles if titles else None
        except Exception:
            return None

    def _extract_phone(self):
        # Phone may be in page header, footer, or contact blocks.
        # Scan for tel: links first
        try:
            tel_links = self.driver.find_elements(By.CSS_SELECTOR, 'a[href^="tel:"]')
            for a in tel_links:
                href = a.get_attribute("href") or ""
                num = href.replace("tel:", "").strip()
                if num:
                    return num
        except Exception:
            pass

        # Otherwise regex scan common phone formats from visible text blocks
        try:
            body_text = self.driver.find_element(By.TAG_NAME, "body").text
            m = re.search(r"(\+?\d[\d\-\(\) \.]{7,}\d)", body_text)
            if m:
                return m.group(1).strip()
        except Exception:
            pass

        return None

    def _open_amenities_page_and_scrape(self):
        # Click the "View all amenities" button if present, scrape structured info, then go back
        # As per user example:
        # <a class="cmp-button" href=".../hoteldetail/amenities">View all amenities</a>
        try:
            btn = None
            buttons = self.driver.find_elements(By.CSS_SELECTOR, 'a.cmp-button, a.cmp-teaser__action-link')
            for b in buttons:
                txt = (b.text or "").strip().lower()
                if "view all amenities" in txt:
                    btn = b
                    break

            if not btn:
                return None

            href = btn.get_attribute("href")
            if not href:
                return None

            # Open amenities page in same tab
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
            time.sleep(0.2)
            try:
                btn.click()
            except Exception:
                self.driver.execute_script("arguments[0].click();", btn)

            time.sleep(0.8)
            # Wait for any amenities content
            wait_presence(self.driver, self.wait, (By.TAG_NAME, "body"))
            time.sleep(0.8)

            data = {
                "amenities": self._scrape_amenities_list_from_page(),
                "parking": self._scrape_parking_from_page(),
                "overview": self._scrape_overview_table_from_page(),
                "nearby": self._scrape_nearby_from_page(),
                "airport": self._scrape_airport_from_page(),
                "phone": self._extract_phone()
            }

            # Navigate back
            self.driver.back()
            time.sleep(0.8)

            # If no amenities found, return None to avoid overwriting card amenities
            if not any([data.get("amenities"), data.get("parking"), data.get("overview"), data.get("nearby"), data.get("airport")]):
                return None

            return data

        except Exception:
            # Attempt to go back if something failed
            try:
                self.driver.back()
            except Exception:
                pass
            return None

    def _scrape_amenities_list_from_page(self):
        # Collect bullet lists of amenities; look for common containers:
        selectors = [
            ".amenities-list li",
            ".cmp-amenity-list .cmp-image__title",
            ".cmp-amenity-list__item .cmp-image__title",
            ".amenities .amenity, .amenities li",
            '[data-component="amenities"] li'
        ]
        items = []
        for sel in selectors:
            try:
                els = self.driver.find_elements(By.CSS_SELECTOR, sel)
                for e in els:
                    t = safe_text(e)
                    if t and t not in items:
                        items.append(t)
            except Exception:
                continue
        return items if items else None

    def _scrape_parking_from_page(self):
        # Search for parking-related blocks
        # Heuristic: look for sections containing "Parking" header and collect sibling text
        text = self._collect_section_text(["parking", "valet", "self-parking"])
        if text:
            return {"parking_info": text}
        return None

    def _scrape_overview_table_from_page(self):
        # Try pairs like Key: Value tables
        # Heuristic: find dl/dt/dd or two-column rows
        data = {}
        try:
            # dt/dd
            dts = self.driver.find_elements(By.CSS_SELECTOR, "dl dt")
            dds = self.driver.find_elements(By.CSS_SELECTOR, "dl dd")
            if dts and dds and len(dts) == len(dds):
                for dt, dd in zip(dts, dds):
                    k = safe_text(dt)
                    v = safe_text(dd)
                    if k and v:
                        data[k] = v

            # Fallback: generic key-value rows
            if not data:
                rows = self.driver.find_elements(By.CSS_SELECTOR, ".table, .overview, .kv, .grid")
                for r in rows:
                    labels = r.find_elements(By.CSS_SELECTOR, ".label, .key, th")
                    vals = r.find_elements(By.CSS_SELECTOR, ".value, td")
                    if labels and vals and len(labels) == len(vals):
                        for k_el, v_el in zip(labels, vals):
                            k = safe_text(k_el)
                            v = safe_text(v_el)
                            if k and v:
                                data[k] = v
        except Exception:
            pass
        return data if data else None

    def _scrape_nearby_from_page(self):
        # Try to gather a list of nearby attractions with distances
        # Heuristic: look for lists with "Nearby" or "Attractions"
        text = self._collect_section_text(["nearby", "attractions", "points of interest"])
        if text:
            # If list-like lines, split
            lines = [ln.strip(" -•\t") for ln in text.splitlines() if ln.strip()]
            return lines if lines else None
        return None

    def _scrape_airport_from_page(self):
        # Heuristic: find "Airport" blocks
        text = self._collect_section_text(["airport", "airports", "shuttle"])
        if text:
            lines = [ln.strip(" -•\t") for ln in text.splitlines() if ln.strip()]
            return lines if lines else None
        return None

    def _collect_section_text(self, keywords):
        # Scan headings and their following sibling content for matches
        try:
            body = self.driver.find_element(By.TAG_NAME, "body")
            sections = self.driver.find_elements(By.CSS_SELECTOR, "section, .section, .cmp-section, .content-section, .accordion, .accordion-item")
            texts = []
            for sec in sections:
                sec_text = sec.text.lower()
                if any(k in sec_text for k in keywords):
                    txt = safe_text(sec)
                    if txt and txt not in texts:
                        texts.append(txt)
            # Fallback: whole page search, but return a concise chunk
            if not texts:
                full = body.text
                low = full.lower()
                for k in keywords:
                    if k in low:
                        idx = low.find(k)
                        # capture a window around keyword
                        snippet = full[max(0, idx-500): idx+700]
                        texts.append(snippet)
                        break
            if texts:
                # Return the longest chunk
                return max(texts, key=len)
        except Exception:
            pass
        return None

    def _open_pet_policy_if_available(self):
        # As per user example:
        # <a class="cmp-teaser__action-link cmp-button" ...> View pet policy </a>
        try:
            links = self.driver.find_elements(By.CSS_SELECTOR, 'a.cmp-teaser__action-link.cmp-button, a.cmp-button')
            pet_link = None
            for a in links:
                txt = (a.text or "").strip().lower()
                if "view pet policy" in txt or "pet policy" in txt:
                    pet_link = a
                    break
            if not pet_link:
                return None

            href = pet_link.get_attribute("href")
            if not href:
                return None

            # Open in same tab, scrape, back
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", pet_link)
            time.sleep(0.2)
            try:
                pet_link.click()
            except Exception:
                self.driver.execute_script("arguments[0].click();", pet_link)

            time.sleep(0.8)
            wait_presence(self.driver, self.wait, (By.TAG_NAME, "body"))
            time.sleep(0.6)

            # Scrape pet policy text blocks
            policy = self._collect_section_text(["pet", "pets", "pet policy", "dog", "cat"])
            # Navigate back
            self.driver.back()
            time.sleep(0.8)

            if policy:
                return {"policy": policy}
            return None

        except Exception:
            try:
                self.driver.back()
            except Exception:
                pass
            return None

    def _infer_pet_friendly(self, detail_dict):
        # If pets_json exists or description/highlights mention pets allowed
        if detail_dict.get("pets_json"):
            return True
        desc = (detail_dict.get("description") or "").lower()
        if any(w in desc for w in ["pet-friendly", "pets allowed", "pet friendly", "pet-friendly hotel", "pet policy"]):
            return True
        # highlights
        ams = detail_dict.get("amenities_json")
        if isinstance(ams, list):
            hay = " ".join(ams).lower()
            if any(w in hay for w in ["pet", "pets allowed", "pet-friendly"]):
                return True
        elif isinstance(ams, str):
            try:
                lst = json.loads(ams)
                if isinstance(lst, list):
                    hay = " ".join(lst).lower()
                    if any(w in hay for w in ["pet", "pets allowed", "pet-friendly"]):
                        return True
            except Exception:
                pass
        return False


# =====================================================
# STORAGE HELPERS (Resume support)
# =====================================================

def load_or_create_city_csv():
    if os.path.exists(CITY_CSV):
        with open(CITY_CSV, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
            if rows:
                return rows

    print("City CSV not found or empty — scraping cities...")
    scraper = IHGCitiesScraper(headless=HEADLESS, timeout=DRIVER_TIMEOUT)
    try:
        cities = scraper.scrape_city_urls()
    finally:
        scraper.quit()

    # Persist
    ensure_dir(CITY_CSV)
    with open(CITY_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["city_name", "city_url"])
        writer.writeheader()
        writer.writerows(cities)

    return cities


def load_existing_output():
    existing = {}
    if os.path.exists(HOTEL_JSON) and not OVERWRITE:
        try:
            with open(HOTEL_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
                for d in data:
                    key = (d.get("hotel_code"), d.get("hotel_name"))
                    existing[key] = d
        except Exception:
            pass
    return existing


def append_or_merge(hotels, existing_map):
    result_map = dict(existing_map)
    for h in hotels:
        key = (h.get("hotel_code"), h.get("hotel_name"))
        if key in result_map and not OVERWRITE:
            # already have it — skip
            continue
        result_map[key] = h
    return list(result_map.values())


def save_outputs(records):
    if not records:
        return
    # JSON
    ensure_dir(HOTEL_JSON)
    with open(HOTEL_JSON, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    # CSV
    ensure_dir(HOTEL_CSV)
    with open(HOTEL_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        for r in records:
            row = {k: r.get(k) for k in FIELDS}
            writer.writerow(row)


# =====================================================
# MAIN
# =====================================================

def main():
    cities = load_or_create_city_csv()

    if RUN_ONLY_ONE_CITY:
        cities = cities[:1]

    existing_map = load_existing_output()
    all_hotels = list(existing_map.values())

    hotel_scraper = IHGHotelScraper(headless=HEADLESS, timeout=DRIVER_TIMEOUT)

    try:
        for city in cities:
            print(f"Scraping city: {city.get('city_name')} -> {city.get('city_url')}")
            try:
                hotels = hotel_scraper.scrape_city(city)
            except Exception as e:
                print(f"Error scraping city {city.get('city_name')}: {e}")
                hotels = []

            merged = append_or_merge(hotels, {(h.get('hotel_code'), h.get('hotel_name')): h for h in all_hotels})
            all_hotels = merged
            save_outputs(all_hotels)  # incremental save for crash-resilience
            print(f"Saved total hotels so far: {len(all_hotels)}")

    finally:
        hotel_scraper.quit()

    print(f"Done. Total hotels: {len(all_hotels)}")
    print(f"JSON: {os.path.abspath(HOTEL_JSON)}")
    print(f"CSV:  {os.path.abspath(HOTEL_CSV)}")


if __name__ == "__main__":
    main()