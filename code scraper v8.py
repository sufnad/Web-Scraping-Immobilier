"""
SeLoger Scraper - TRULY PARALLEL VERSION v10
============================================

KEY CHANGE: Uses MULTIPLE BROWSER INSTANCES for TRUE parallelism.

Flow:
1. Opens ALL browser windows at once
2. Waits for you to accept cookies in EACH window
3. Once all cookies accepted, starts TRUE parallel scraping
4. Each browser scrapes independently - no locks, no waiting!


- Each worker has its OWN browser
- No shared locks during scraping
- True simultaneous scraping
- 3-5x faster than v9

FEATURES FROM v9:
- Debug mode (saves HTML/screenshots of failed pages)
- Retry system with multiple rounds
- Human-like scrolling (hesitations, scroll-back)
- Viewport randomization
- Random breaks to simulate human behavior
- Detailed logging and progress tracking
"""

import time
import csv
import random
import logging
import argparse
import os
import sys
import traceback
from datetime import datetime
from typing import Optional, List, Dict, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from queue import Queue
import re

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.common.exceptions import (
    NoSuchElementException,
    StaleElementReferenceException,
    TimeoutException,
    WebDriverException
)

try:
    import undetected_chromedriver as uc
    UNDETECTED_AVAILABLE = True
except ImportError:
    UNDETECTED_AVAILABLE = False

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import warnings
warnings.filterwarnings('ignore', message='Connection pool is full')

# Fix Windows console encoding
if sys.platform.startswith('win'):
    try:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    except:
        pass

# ---------------------------------------------------------
# CONFIG
# ---------------------------------------------------------
BASE_URL = "https://www.seloger.com/classified-search?distributionTypes=Buy&estateTypes=House,Apartment&locations=eyJwbGFjZUlkIjoiQUQwOEZSMzEwOTYiLCJyYWRpdXMiOjMwLCJwb2x5bGluZSI6Im1wempIZWhsTXBfQGJ1TmhfQm5hTmp7Q2J7TGpxRW5jS2JfR3h8SHxiSGJqRmx7SGxuQ3ZnSW5tQGxnSXlyQGx6SHtyQ25hSGFtRmp9Rn19SHBvRWtiS3x5Q2N4TGp-QWF9TWRfQHtvTmVfQHlvTmt-QWF9TX15Q2N4THFvRW1iS2t9Rnt9SG9hSGFtRm16SH1yQ21nSXlyQHdnSXBtQG17SGxuQ31iSGBqRmNfR3p8SGtxRWxjS2t7Q2J7TGlfQm5hTnFfQGJ1TiIsImNvb3JkaW5hdGVzIjp7ImxhdCI6NDguODU5Njk0NDg0NjY4NTE2LCJsbmciOjIuMzYxNzg2NTQ3MDMwNTU5fX0"

OUTPUT_DIR = "output"
MAX_RETRIES = 3
HEADLESS = False
PARALLEL_WORKERS = 3
MAX_WORKERS = 10
DEBUG_MODE = False
MISSING_DATA_INDICATOR = "N/A"

# Delays - balanced for speed + anti-bot
DELAY_BETWEEN_LISTINGS = (0.05, 0.15)
PAGE_LOAD_WAIT = (2, 4)
SCROLL_DELAY = (0.2, 0.5)
LAZY_SCROLL_WAIT = (0.8, 1.5)
FINAL_WAIT_AFTER_SCROLL = (1.0, 1.8)

# Retry delays
RETRY_DELAY = (5, 10)
MAX_RETRY_ROUNDS = 2

# Quality thresholds
MIN_LISTINGS_PER_PAGE = 15
MIN_COMPLETE_DATA_RATIO = 0.5

# Human behavior simulation
BREAK_EVERY_N_PAGES = (8, 15)  # Take a break every 8-15 pages
BREAK_DURATION = (5, 15)  # Break for 5-15 seconds

# Thread-safe locks (only for shared data, NOT for drivers!)
csv_lock = Lock()
scraped_urls_lock = Lock()
scraped_urls: Set[str] = set()
stats_lock = Lock()
retry_queue_lock = Lock()

# Retry queue for failed pages
retry_queue: Queue = Queue()

global_stats = {
    'total_listings': 0,
    'complete_listings': 0,
    'failed_pages': set(),
    'successful_pages': set(),
    'pages_by_worker': {}  # worker_id -> list of pages scraped
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# Common viewport sizes for randomization
VIEWPORT_SIZES = [
    (1920, 1080),  # Full HD
    (1366, 768),   # Common laptop
    (1536, 864),   # HD+
    (1440, 900),   # MacBook
    (1600, 900),   # HD+
    (1280, 720),   # HD
    (1680, 1050),  # WSXGA+
    (1920, 1200),  # WUXGA
]

# ---------------------------------------------------------
# Logging Setup
# ---------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [Worker-%(thread)d] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('seloger_scraper_v10.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)
logging.getLogger("selenium").setLevel(logging.WARNING)

# ---------------------------------------------------------
# Helper Functions
# ---------------------------------------------------------
def safe_get(element, selector: str, attribute: Optional[str] = None, fallback_selectors: Optional[List[str]] = None) -> Optional[str]:
    selectors_to_try = [selector]
    if fallback_selectors:
        selectors_to_try.extend(fallback_selectors)
    
    for sel in selectors_to_try:
        try:
            elem = element.find_element(By.CSS_SELECTOR, sel)
            if attribute:
                result = elem.get_attribute(attribute)
            else:
                result = elem.text.strip()
            if result:
                return result
        except (NoSuchElementException, StaleElementReferenceException):
            continue
    return None


def extract_with_regex_from_text(card_text: str, field: str) -> Optional[str]:
    if not card_text:
        return None
    
    patterns = {
        'type': [r'(Appartement|Maison|Studio|Loft|Duplex|Triplex|Villa|Terrain|Local|Bureau)'],
        'price': [r'([\d\s]{4,})\s*€(?!\s*/\s*m)', r'(\d{1,3}(?:\s?\d{3})+)\s*€'],
        'price_per_m2': [r'(\d[\d\s,]*\s*€\s*/\s*m[²2])'],
        'surface': [r'(\d+(?:[,\.]\d+)?)\s*m[²2]'],
        'rooms': [r'(\d+)\s*(?:pièces?|pcs?|p\b)'],
        'bedrooms': [r'(\d+)\s*(?:chambres?|ch\b)'],
        'delivery_date': [r'(dès\s+le\s+\d{2}/\d{2}/\d{4})', r'(Livraison\s+\d{4})'],
        'postal_code': [r'\((\d{5})\)', r'\b(\d{5})\b'],
    }
    
    if field not in patterns:
        return None
    
    for pattern in patterns[field]:
        match = re.search(pattern, card_text, re.IGNORECASE)
        if match:
            result = match.group(1).strip()
            if field == 'price':
                result = result.replace('\xa0', ' ').replace('\u202f', ' ') + ' €'
            elif field == 'surface':
                result = result + ' m²'
            elif field == 'rooms':
                result = result + ' pièce(s)'
            elif field == 'bedrooms':
                result = result + ' chambre(s)'
            return result
    return None


def extract_with_regex_from_html(card_html: str, field: str) -> Optional[str]:
    if not card_html:
        return None
    
    clean_html = re.sub(r'<[^>]+>', ' ', card_html)
    clean_html = re.sub(r'\s+', ' ', clean_html)
    
    patterns = {
        'type': [r'(Appartement|Maison|Studio|Loft|Duplex|Triplex|Villa|Terrain|Local|Bureau|Parking|Cave)'],
        'price': [r'(\d{1,3}(?:[\s\u00a0\u202f]\d{3})+)\s*€(?!\s*/)'],
        'price_per_m2': [r'(\d[\d\s,\.]*\s*€\s*/\s*m[²2])'],
        'surface': [r'(\d+(?:[,\.]\d+)?)\s*m[²2]'],
        'rooms': [r'(\d+)\s*(?:pièces?|pcs?|pieces?)'],
        'bedrooms': [r'(\d+)\s*(?:chambres?|bedrooms?)'],
        'delivery_date': [r'(dès\s+(?:le\s+)?\d{1,2}/\d{1,2}/\d{4})'],
        'postal_code': [r'\((\d{5})\)'],
        'url': [r'href="(/annonces/achat/[^"]+)"', r'href="(https://www\.seloger\.com/annonces/[^"]+)"'],
    }
    
    if field not in patterns:
        return None
    
    for pattern in patterns[field]:
        match = re.search(pattern, clean_html if field != 'url' else card_html, re.IGNORECASE)
        if match:
            result = match.group(1).strip()
            if field == 'price' and not result.endswith('€'):
                result = result.replace('\xa0', ' ').replace('\u202f', ' ') + ' €'
            elif field == 'surface' and 'm' not in result.lower():
                result = result + ' m²'
            elif field == 'url' and not result.startswith('http'):
                result = 'https://www.seloger.com' + result
            return result
    return None


def parse_listing(card, page_num: int, worker_id: int) -> Dict[str, Optional[str]]:
    """Extract all information from a listing card with triple-layer fallback"""
    data = {
        'page_num': page_num,
        'type': None, 'price': None, 'price_per_m2': None,
        'surface': None, 'rooms': None, 'bedrooms': None,
        'delivery_date': None, 'address': None, 'city': None,
        'postal_code': None, 'department': None, 'program_name': None,
        'url': None, 'raw_card_text': None, 'confidence_score': 10,
    }
    
    css_extracted = set()
    regex_text_extracted = set()
    regex_html_extracted = set()
    
    try:
        try:
            all_text = card.text
            data['raw_card_text'] = all_text
        except StaleElementReferenceException:
            return data
        
        try:
            card_html = card.get_attribute('outerHTML')
        except StaleElementReferenceException:
            card_html = None
        
        # LAYER 1: CSS SELECTORS
        data['type'] = safe_get(card, "div.css-1n0wsen", fallback_selectors=[
            "div[class*='property-type']", "span[class*='type']"
        ])
        if data['type']:
            css_extracted.add('type')
        
        price_elem = safe_get(card, "div[data-testid='cardmfe-price-testid']", fallback_selectors=[
            "div[class*='price']", "span[class*='price']"
        ])
        if price_elem:
            price_text = price_elem.replace('\xa0', ' ').replace('\u202f', ' ')
            if '€' in price_text:
                price_parts = re.split(r'\(.*?\)', price_text)
                main_price = price_parts[0].strip()
                if '€' in main_price:
                    data['price'] = main_price
                    css_extracted.add('price')
        
        try:
            price_per_m2_elem = card.find_element(By.CSS_SELECTOR, "span.css-xsih6f")
            data['price_per_m2'] = price_per_m2_elem.text.strip().replace('\xa0', ' ')
            if data['price_per_m2']:
                css_extracted.add('price_per_m2')
        except (NoSuchElementException, StaleElementReferenceException):
            pass
        
        try:
            keyfacts = card.find_element(By.CSS_SELECTOR, "div[data-testid='cardmfe-keyfacts-testid']")
            facts_elements = keyfacts.find_elements(By.CSS_SELECTOR, "div.css-9u48bm")
            for elem in facts_elements:
                try:
                    txt = elem.text.strip()
                    if txt and txt != '·':
                        txt_lower = txt.lower()
                        if 'pièce' in txt_lower:
                            data['rooms'] = txt
                            css_extracted.add('rooms')
                        elif 'chambre' in txt_lower:
                            data['bedrooms'] = txt
                            css_extracted.add('bedrooms')
                        elif 'm²' in txt_lower or 'm2' in txt_lower:
                            data['surface'] = txt
                            css_extracted.add('surface')
                        elif 'dès' in txt_lower or '/' in txt:
                            data['delivery_date'] = txt
                            css_extracted.add('delivery_date')
                except StaleElementReferenceException:
                    continue
        except (NoSuchElementException, StaleElementReferenceException):
            pass
        
        address_raw = safe_get(card, "div[data-testid='cardmfe-description-box-address']", fallback_selectors=[
            "div[class*='address']", "span[class*='location']"
        ])
        if address_raw:
            data['address'] = address_raw
            css_extracted.add('address')
            postal_match = re.search(r'\((\d{5})\)', address_raw)
            if postal_match:
                data['postal_code'] = postal_match.group(1)
                data['department'] = postal_match.group(1)[:2]
                css_extracted.add('postal_code')
            city_match = re.search(r',\s*([^,]+?)\s*\(', address_raw)
            if city_match:
                data['city'] = city_match.group(1).strip()
                css_extracted.add('city')
            if ',' in address_raw:
                parts = address_raw.split(',')
                if len(parts) >= 2:
                    potential_program = parts[0].strip()
                    if not re.match(r'^\d', potential_program):
                        data['program_name'] = potential_program
        
        url = safe_get(card, "a[data-testid='card-mfe-covering-link-testid']", attribute="href", fallback_selectors=[
            "a[href*='seloger']", "a[href*='/annonces/']"
        ])
        if url:
            if not url.startswith("http"):
                url = f"https://www.seloger.com{url}"
            data['url'] = url
            css_extracted.add('url')
        
        # LAYER 2: TEXT REGEX
        for field in ['type', 'price', 'price_per_m2', 'surface', 'rooms', 'bedrooms', 'delivery_date', 'postal_code']:
            if not data.get(field) and all_text:
                extracted = extract_with_regex_from_text(all_text, field)
                if extracted:
                    data[field] = extracted
                    regex_text_extracted.add(field)
                    if field == 'postal_code' and not data.get('department'):
                        data['department'] = extracted[:2]
        
        # LAYER 3: HTML REGEX
        for field in ['type', 'price', 'price_per_m2', 'surface', 'rooms', 'bedrooms', 'delivery_date', 'postal_code', 'url']:
            if not data.get(field) and card_html:
                extracted = extract_with_regex_from_html(card_html, field)
                if extracted:
                    data[field] = extracted
                    regex_html_extracted.add(field)
                    if field == 'postal_code' and not data.get('department'):
                        data['department'] = extracted[:2]
        
        # CONFIDENCE SCORE
        critical_fields = {'url', 'price', 'type', 'surface'}
        css_critical = len(critical_fields & css_extracted)
        regex_text_critical = len(critical_fields & regex_text_extracted)
        total_critical = len(critical_fields & (css_extracted | regex_text_extracted | regex_html_extracted))
        
        if css_critical >= 3:
            data['confidence_score'] = 10
        elif css_critical >= 2 or (css_critical >= 1 and regex_text_critical >= 2):
            data['confidence_score'] = 8
        elif regex_text_critical >= 2:
            data['confidence_score'] = 7
        elif total_critical >= 2:
            data['confidence_score'] = 5
        else:
            data['confidence_score'] = 3
            
    except Exception as e:
        logger.error(f"Worker {worker_id}: Error parsing listing: {e}")
        data['confidence_score'] = 0
    
    return data


def validate_listing(data: Dict) -> bool:
    has_url = bool(data.get('url'))
    has_price = bool(data.get('price'))
    has_type = bool(data.get('type'))
    return has_url and (has_price or has_type)


def format_for_csv(value: Optional[str]) -> str:
    if value is None or (isinstance(value, str) and value.strip() == ''):
        return MISSING_DATA_INDICATOR
    return value


# ---------------------------------------------------------
# Browser Setup
# ---------------------------------------------------------
def setup_chrome_driver(worker_id: int, headless: bool = False) -> webdriver:
    """Setup Chrome driver with unique settings per worker"""
    user_agent = USER_AGENTS[worker_id % len(USER_AGENTS)]
    
    if UNDETECTED_AVAILABLE:
        options = uc.ChromeOptions()
        options.add_argument(f"--user-agent={user_agent}")
        if headless:
            options.add_argument("--headless=new")
        driver = uc.Chrome(options=options)
    else:
        options = ChromeOptions()
        options.add_argument(f"--user-agent={user_agent}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--no-sandbox")
        if headless:
            options.add_argument("--headless=new")
        driver = webdriver.Chrome(options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    # Randomize viewport size (different for each worker)
    width, height = random.choice(VIEWPORT_SIZES)
    driver.set_window_size(width, height)
    
    # Position windows in a grid pattern so they don't overlap
    x_offset = (worker_id % 3) * 650
    y_offset = (worker_id // 3) * 450
    driver.set_window_position(x_offset, y_offset)
    
    logger.debug(f"Worker {worker_id}: Browser configured - {width}x{height} at ({x_offset}, {y_offset})")
    
    return driver


def randomize_viewport(driver, worker_id: int):
    """Randomly resize viewport to simulate different user environments"""
    width, height = random.choice(VIEWPORT_SIZES)
    driver.set_window_size(width, height)
    logger.debug(f"Worker {worker_id}: Viewport randomized to {width}x{height}")


def check_cookie_popup_gone(driver) -> bool:
    """Check if cookie popup has been dismissed"""
    try:
        shadow_script = """
            const root = document.querySelector('#usercentrics-root');
            if (!root || !root.shadowRoot) return true;
            const button = root.shadowRoot.querySelector('[data-testid="uc-accept-all-button"]');
            return button === null || button.offsetParent === null;
        """
        return driver.execute_script(shadow_script)
    except:
        return True


# ---------------------------------------------------------
# Scrolling (no locks needed - each worker has own driver!)
# ---------------------------------------------------------
def scroll_to_load_all_cards(driver, worker_id: int, page_num: int) -> int:
    """
    Scroll to trigger lazy loading with HUMAN-LIKE behavior
    Includes hesitations, scroll-back, and variable speeds
    """
    logger.debug(f"Worker {worker_id}: Scrolling page {page_num} with human-like behavior...")
    
    scroll_steps = 5
    last_card_count = 0
    stable_count = 0
    
    for step in range(scroll_steps):
        # Calculate target scroll position
        scroll_position = (step + 1) * (1.0 / scroll_steps)
        target_y = int(driver.execute_script("return document.body.scrollHeight") * scroll_position)
        current_y = driver.execute_script("return window.pageYOffset")
        
        # HUMAN-LIKE SCROLLING: Smooth scroll with random speed variations
        distance = target_y - current_y
        num_increments = random.randint(8, 15)
        
        for i in range(num_increments):
            progress = (i + 1) / num_increments
            # Use easing function for more natural deceleration
            eased_progress = 1 - pow(1 - progress, 3)  # Ease-out cubic
            next_y = int(current_y + (distance * eased_progress))
            
            # Add small random variation to simulate human imprecision
            variation = random.randint(-10, 10)
            next_y += variation
            
            driver.execute_script(f"window.scrollTo({{top: {next_y}, behavior: 'smooth'}});")
            
            # Variable micro-pauses (human doesn't scroll at constant speed)
            micro_pause = random.uniform(0.03, 0.12)
            time.sleep(micro_pause)
        
        # Occasional "hesitation" - humans sometimes pause mid-scroll
        if random.random() < 0.3:  # 30% chance
            hesitation = random.uniform(0.3, 0.8)
            logger.debug(f"Worker {worker_id}: Human-like hesitation ({hesitation:.1f}s)")
            time.sleep(hesitation)
        
        # Wait for new cards to load
        time.sleep(random.uniform(*LAZY_SCROLL_WAIT))
        
        # Occasionally scroll up slightly (humans do this when reviewing content)
        if random.random() < 0.25:  # 25% chance
            scroll_back = random.randint(50, 150)
            current = driver.execute_script("return window.pageYOffset")
            driver.execute_script(f"window.scrollTo({{top: {current - scroll_back}, behavior: 'smooth'}});")
            time.sleep(random.uniform(0.2, 0.5))
            # Then scroll back down
            driver.execute_script(f"window.scrollTo({{top: {current}, behavior: 'smooth'}});")
            time.sleep(random.uniform(0.3, 0.6))
        
        # Check card count
        try:
            cards = driver.find_elements(By.CSS_SELECTOR, "div[data-testid='serp-core-classified-card-testid']")
            current_count = len(cards)
            logger.debug(f"Worker {worker_id}: Found {current_count} cards after scroll step {step+1}")
            
            if current_count == last_card_count:
                stable_count += 1
                if stable_count >= 2:
                    logger.debug(f"Worker {worker_id}: Card count stable at {current_count}")
                    break
            else:
                stable_count = 0
            last_card_count = current_count
        except Exception as e:
            logger.debug(f"Worker {worker_id}: Error checking cards during scroll: {e}")
    
    # Scroll back to top with human-like behavior
    current_y = driver.execute_script("return window.pageYOffset")
    num_increments = random.randint(10, 18)
    
    for i in range(num_increments):
        progress = (i + 1) / num_increments
        eased_progress = 1 - pow(1 - progress, 2)  # Ease-out quadratic
        next_y = int(current_y * (1 - eased_progress))
        
        driver.execute_script(f"window.scrollTo({{top: {next_y}, behavior: 'smooth'}});")
        time.sleep(random.uniform(0.04, 0.12))
    
    # Final scroll to exact top
    driver.execute_script("window.scrollTo({top: 0, behavior: 'smooth'});")
    time.sleep(random.uniform(*FINAL_WAIT_AFTER_SCROLL))
    
    try:
        cards = driver.find_elements(By.CSS_SELECTOR, "div[data-testid='serp-core-classified-card-testid']")
        final_count = len(cards)
        logger.info(f"Worker {worker_id}: ✓ Page {page_num} loaded {final_count} cards")
        return final_count
    except:
        return 0


def save_debug_info(driver, worker_id: int, page_num: int, reason: str):
    """Save HTML and screenshot for debugging failed pages"""
    if not DEBUG_MODE:
        return
    
    debug_dir = os.path.join(OUTPUT_DIR, "debug", f"worker{worker_id}")
    os.makedirs(debug_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%H%M%S')
    
    # Save HTML
    try:
        html_file = os.path.join(debug_dir, f"page{page_num}_{reason}_{timestamp}.html")
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(driver.page_source)
        logger.debug(f"Worker {worker_id}: Saved HTML to {html_file}")
    except Exception as e:
        logger.debug(f"Worker {worker_id}: Could not save HTML: {e}")
    
    # Save screenshot
    try:
        screenshot_file = os.path.join(debug_dir, f"page{page_num}_{reason}_{timestamp}.png")
        driver.save_screenshot(screenshot_file)
        logger.debug(f"Worker {worker_id}: Saved screenshot to {screenshot_file}")
    except Exception as e:
        logger.debug(f"Worker {worker_id}: Could not save screenshot: {e}")


# ---------------------------------------------------------
# CSV Handling
# ---------------------------------------------------------
def initialize_csv(filename: str):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow([
            "Page_Number", "Type", "Price", "Price_Per_M2", "Surface_m2",
            "Rooms", "Bedrooms", "Delivery_Date", "Address", "City",
            "PostalCode", "Department", "Program_Name", "URL",
            "Confidence_Score", "Raw_Card_Text"
        ])
    logger.info(f"Initialized CSV: {filepath}")


def write_listings_to_csv(listings: List[Dict], output_file: str):
    """Thread-safe CSV writing"""
    with csv_lock:
        filepath = os.path.join(OUTPUT_DIR, output_file)
        with open(filepath, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            for listing in listings:
                raw_text = listing.get('raw_card_text', '')
                if raw_text and len(raw_text) > 500:
                    raw_text = raw_text[:500] + '...'
                
                writer.writerow([
                    format_for_csv(str(listing['page_num'])),
                    format_for_csv(listing['type']),
                    format_for_csv(listing['price']),
                    format_for_csv(listing['price_per_m2']),
                    format_for_csv(listing['surface']),
                    format_for_csv(listing['rooms']),
                    format_for_csv(listing['bedrooms']),
                    format_for_csv(listing['delivery_date']),
                    format_for_csv(listing['address']),
                    format_for_csv(listing['city']),
                    format_for_csv(listing['postal_code']),
                    format_for_csv(listing['department']),
                    format_for_csv(listing['program_name']),
                    format_for_csv(listing['url']),
                    listing['confidence_score'],
                    format_for_csv(raw_text) if listing['confidence_score'] <= 5 else MISSING_DATA_INDICATOR
                ])


def is_duplicate_url(url: Optional[str]) -> bool:
    if not url:
        return False
    with scraped_urls_lock:
        if url in scraped_urls:
            return True
        scraped_urls.add(url)
        return False


# ---------------------------------------------------------
# Worker Function (each has its own browser!)
# ---------------------------------------------------------
def worker_scrape_pages(worker_id: int, driver: webdriver, pages: List[int], output_file: str) -> Dict:
    """
    Worker function - scrapes assigned pages using its OWN browser.
    No driver locks needed!
    
    Includes:
    - Debug mode (saves HTML/screenshots on failure)
    - Random breaks to simulate human behavior
    - Detailed error logging with tracebacks
    - Per-worker page tracking
    """
    results = {
        'listings': 0, 
        'complete': 0, 
        'failed_pages': [], 
        'successful_pages': [],
        'pages_scraped': 0
    }
    
    # Track when to take a break
    pages_since_break = 0
    next_break_at = random.randint(*BREAK_EVERY_N_PAGES)
    
    for page_idx, page_num in enumerate(pages):
        try:
            # Random break every N pages (human behavior)
            pages_since_break += 1
            if pages_since_break >= next_break_at:
                break_time = random.uniform(*BREAK_DURATION)
                logger.info(f"Worker {worker_id}: 📊 Taking a {break_time:.1f}s break (human behavior)...")
                time.sleep(break_time)
                pages_since_break = 0
                next_break_at = random.randint(*BREAK_EVERY_N_PAGES)
                
                # Occasionally randomize viewport during breaks
                if random.random() < 0.3:
                    randomize_viewport(driver, worker_id)
            
            url = f"{BASE_URL}&page={page_num}"
            logger.info(f"Worker {worker_id}: Loading page {page_num} ({page_idx+1}/{len(pages)})")
            
            driver.get(url)
            time.sleep(random.uniform(*PAGE_LOAD_WAIT))
            
            # Scroll to load lazy content
            card_count = scroll_to_load_all_cards(driver, worker_id, page_num)
            
            if card_count == 0:
                logger.warning(f"Worker {worker_id}: No cards on page {page_num}")
                save_debug_info(driver, worker_id, page_num, "no_cards")
                results['failed_pages'].append(page_num)
                
                # Add to retry queue
                with retry_queue_lock:
                    retry_queue.put((page_num, worker_id, 1))
                continue
            
            cards = driver.find_elements(By.CSS_SELECTOR, "div[data-testid='serp-core-classified-card-testid']")
            logger.info(f"Worker {worker_id}: Page {page_num} has {len(cards)} cards")
            
            listings = []
            complete_count = 0
            duplicate_count = 0
            
            for card_idx, card in enumerate(cards):
                time.sleep(random.uniform(*DELAY_BETWEEN_LISTINGS))
                data = parse_listing(card, page_num, worker_id)
                
                if is_duplicate_url(data.get('url')):
                    duplicate_count += 1
                    continue
                
                if validate_listing(data):
                    complete_count += 1
                else:
                    # Save debug info for cards with missing data
                    if DEBUG_MODE and data.get('confidence_score', 10) <= 5:
                        try:
                            debug_dir = os.path.join(OUTPUT_DIR, "debug", f"worker{worker_id}", "cards")
                            os.makedirs(debug_dir, exist_ok=True)
                            card_html = card.get_attribute('outerHTML')
                            debug_file = os.path.join(debug_dir, f"page{page_num}_card{card_idx+1}.html")
                            with open(debug_file, 'w', encoding='utf-8') as f:
                                f.write(card_html)
                        except:
                            pass
                
                listings.append(data)
            
            # Write to CSV
            write_listings_to_csv(listings, output_file)
            
            results['listings'] += len(listings)
            results['complete'] += complete_count
            results['successful_pages'].append(page_num)
            results['pages_scraped'] += 1
            
            # Update global stats
            with stats_lock:
                global_stats['total_listings'] += len(listings)
                global_stats['complete_listings'] += complete_count
                global_stats['successful_pages'].add(page_num)
                if worker_id not in global_stats['pages_by_worker']:
                    global_stats['pages_by_worker'][worker_id] = []
                global_stats['pages_by_worker'][worker_id].append(page_num)
            
            # Determine success status
            success = len(listings) >= MIN_LISTINGS_PER_PAGE
            if listings and complete_count < len(listings) * MIN_COMPLETE_DATA_RATIO:
                success = False
            
            status = "✓" if success else "⚠"
            logger.info(f"Worker {worker_id}: Page {page_num} {status} - {len(listings)} listings ({complete_count} complete, {duplicate_count} dupes)")
            
            if not success:
                save_debug_info(driver, worker_id, page_num, "low_quality")
                with retry_queue_lock:
                    retry_queue.put((page_num, worker_id, 1))
            
            # Small delay between pages
            time.sleep(random.uniform(0.5, 1.5))
            
        except TimeoutException as e:
            logger.error(f"Worker {worker_id}: Timeout on page {page_num}: {e}")
            save_debug_info(driver, worker_id, page_num, "timeout")
            results['failed_pages'].append(page_num)
            with retry_queue_lock:
                retry_queue.put((page_num, worker_id, 1))
                
        except WebDriverException as e:
            logger.error(f"Worker {worker_id}: WebDriver error on page {page_num}: {e}")
            logger.debug(f"Worker {worker_id}: Traceback: {traceback.format_exc()}")
            save_debug_info(driver, worker_id, page_num, "webdriver_error")
            results['failed_pages'].append(page_num)
            with retry_queue_lock:
                retry_queue.put((page_num, worker_id, 1))
                
        except Exception as e:
            logger.error(f"Worker {worker_id}: Error on page {page_num}: {e}")
            logger.debug(f"Worker {worker_id}: Traceback: {traceback.format_exc()}")
            save_debug_info(driver, worker_id, page_num, "error")
            results['failed_pages'].append(page_num)
            with retry_queue_lock:
                retry_queue.put((page_num, worker_id, 1))
    
    return results


def retry_failed_pages(drivers: List[Tuple[int, webdriver]], output_file: str) -> Dict:
    """
    Retry pages that failed in the initial scrape.
    Uses round-robin assignment across available workers.
    """
    results = {'retried': 0, 'succeeded': 0, 'failed': []}
    
    if retry_queue.empty():
        return results
    
    # Collect all pages to retry
    pages_to_retry = []
    while not retry_queue.empty():
        try:
            page_num, original_worker, attempt = retry_queue.get_nowait()
            if attempt <= MAX_RETRIES:
                pages_to_retry.append((page_num, attempt))
        except:
            break
    
    if not pages_to_retry:
        return results
    
    logger.info(f"\n{'='*70}")
    logger.info(f"RETRY PHASE: {len(pages_to_retry)} pages to retry")
    logger.info(f"{'='*70}")
    
    # Wait before retrying
    retry_delay = random.uniform(*RETRY_DELAY)
    logger.info(f"Waiting {retry_delay:.1f}s before retry...")
    time.sleep(retry_delay)
    
    for page_num, attempt in pages_to_retry:
        # Pick a random worker for retry
        worker_id, driver = random.choice(drivers)
        
        logger.info(f"Retrying page {page_num} (attempt {attempt + 1}/{MAX_RETRIES}) on Worker {worker_id}")
        
        try:
            url = f"{BASE_URL}&page={page_num}"
            driver.get(url)
            time.sleep(random.uniform(*PAGE_LOAD_WAIT))
            
            card_count = scroll_to_load_all_cards(driver, worker_id, page_num)
            
            if card_count == 0:
                logger.warning(f"Retry failed for page {page_num}: no cards")
                if attempt < MAX_RETRIES:
                    retry_queue.put((page_num, worker_id, attempt + 1))
                else:
                    results['failed'].append(page_num)
                    with stats_lock:
                        global_stats['failed_pages'].add(page_num)
                continue
            
            cards = driver.find_elements(By.CSS_SELECTOR, "div[data-testid='serp-core-classified-card-testid']")
            
            listings = []
            complete_count = 0
            
            for card in cards:
                data = parse_listing(card, page_num, worker_id)
                if not is_duplicate_url(data.get('url')):
                    if validate_listing(data):
                        complete_count += 1
                    listings.append(data)
            
            write_listings_to_csv(listings, output_file)
            
            results['retried'] += 1
            results['succeeded'] += 1
            
            with stats_lock:
                global_stats['total_listings'] += len(listings)
                global_stats['complete_listings'] += complete_count
                global_stats['successful_pages'].add(page_num)
            
            logger.info(f"✓ Page {page_num} RETRY SUCCESS: {len(listings)} listings")
            
        except Exception as e:
            logger.error(f"✗ Page {page_num} retry failed: {e}")
            if attempt < MAX_RETRIES:
                retry_queue.put((page_num, worker_id, attempt + 1))
            else:
                results['failed'].append(page_num)
                with stats_lock:
                    global_stats['failed_pages'].add(page_num)
    
    return results


# ---------------------------------------------------------
# Main Orchestrator
# ---------------------------------------------------------
def scrape_parallel(start_page: int, end_page: int, output_file: str, num_workers: int):
    """
    Main function - opens multiple browsers, waits for cookies, then scrapes in parallel.
    
    Includes:
    - Phase 1: Initial parallel scrape
    - Phase 2: Retry failed pages
    - Detailed statistics and reporting
    """
    global scraped_urls
    
    logger.info("=" * 70)
    logger.info("SeLoger TRULY PARALLEL Scraper v10")
    logger.info("=" * 70)
    logger.info(f"Pages: {start_page} to {end_page}")
    logger.info(f"Workers (browsers): {num_workers}")
    logger.info(f"Output: {output_file}")
    logger.info(f"Debug mode: {'ON' if DEBUG_MODE else 'OFF'}")
    logger.info("=" * 70)
    
    # Reset state
    with scraped_urls_lock:
        scraped_urls = set()
    
    with stats_lock:
        global_stats['total_listings'] = 0
        global_stats['complete_listings'] = 0
        global_stats['failed_pages'] = set()
        global_stats['successful_pages'] = set()
        global_stats['pages_by_worker'] = {}
    
    # Clear retry queue
    while not retry_queue.empty():
        try:
            retry_queue.get_nowait()
        except:
            break
    
    initialize_csv(output_file)
    
    # STEP 1: Open all browsers
    print("\n" + "=" * 70)
    print(f"🌐 OPENING {num_workers} BROWSER WINDOWS...")
    print("=" * 70)
    
    drivers = []
    for i in range(num_workers):
        print(f"   Opening browser {i+1}/{num_workers}...")
        try:
            driver = setup_chrome_driver(i)
            driver.get(BASE_URL)
            drivers.append((i, driver))
            time.sleep(1)
        except Exception as e:
            logger.error(f"Failed to open browser {i+1}: {e}")
    
    if not drivers:
        logger.error("No browsers could be opened!")
        return
    
    print(f"\n✅ Opened {len(drivers)} browsers")
    
    # STEP 2: Wait for cookie consent in ALL browsers
    print("\n" + "=" * 70)
    print("🍪 PLEASE ACCEPT COOKIES IN ALL BROWSER WINDOWS")
    print("=" * 70)
    print(f"   You need to click 'Tout accepter' in each of the {len(drivers)} windows.")
    print("   The script will wait until ALL windows have cookies accepted.")
    print("=" * 70)
    
    timeout = 180
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        all_accepted = True
        status = []
        
        for worker_id, driver in drivers:
            try:
                accepted = check_cookie_popup_gone(driver)
                status.append(f"B{worker_id+1}:{'✅' if accepted else '⏳'}")
                if not accepted:
                    all_accepted = False
            except:
                status.append(f"B{worker_id+1}:❓")
                all_accepted = False
        
        print(f"\r   Status: {' | '.join(status)}", end="", flush=True)
        
        if all_accepted:
            print("\n\n✅ ALL COOKIES ACCEPTED! Starting scrape...\n")
            break
        
        time.sleep(2)
    else:
        print("\n\n⚠️  Timeout waiting for cookies. Continuing anyway...")
    
    time.sleep(2)
    
    # STEP 3: Distribute pages among workers
    pages = list(range(start_page, end_page + 1))
    pages_per_worker = []
    
    for i in range(len(drivers)):
        worker_pages = [p for j, p in enumerate(pages) if j % len(drivers) == i]
        pages_per_worker.append(worker_pages)
    
    for i, wp in enumerate(pages_per_worker):
        if wp:
            logger.info(f"Worker {i}: assigned {len(wp)} pages ({wp[0]}-{wp[-1]})")
    
    # STEP 4: PHASE 1 - Initial parallel scrape
    print("\n" + "=" * 70)
    print("🚀 PHASE 1: PARALLEL SCRAPE")
    print("=" * 70)
    
    phase1_start = time.time()
    all_results = []
    
    with ThreadPoolExecutor(max_workers=len(drivers)) as executor:
        futures = []
        for (worker_id, driver), worker_pages in zip(drivers, pages_per_worker):
            if worker_pages:
                future = executor.submit(
                    worker_scrape_pages,
                    worker_id,
                    driver,
                    worker_pages,
                    output_file
                )
                futures.append((future, worker_id))
        
        for future, worker_id in futures:
            try:
                result = future.result()
                all_results.append(result)
                logger.info(f"Worker {worker_id} finished: {result['listings']} listings from {result['pages_scraped']} pages")
            except Exception as e:
                logger.error(f"Worker {worker_id} failed: {e}")
                logger.debug(f"Traceback: {traceback.format_exc()}")
    
    phase1_time = time.time() - phase1_start
    
    # STEP 5: PHASE 2 - Retry failed pages
    retry_results = {'retried': 0, 'succeeded': 0, 'failed': []}
    
    for retry_round in range(MAX_RETRY_ROUNDS):
        if retry_queue.empty():
            break
        
        print(f"\n{'='*70}")
        print(f"🔄 RETRY ROUND {retry_round + 1}/{MAX_RETRY_ROUNDS}")
        print(f"{'='*70}")
        
        round_results = retry_failed_pages(drivers, output_file)
        retry_results['retried'] += round_results['retried']
        retry_results['succeeded'] += round_results['succeeded']
        retry_results['failed'].extend(round_results['failed'])
    
    total_time = time.time() - phase1_start
    
    # STEP 6: Summary
    with stats_lock:
        total_listings = global_stats['total_listings']
        complete_listings = global_stats['complete_listings']
        failed_pages = global_stats['failed_pages']
        successful_pages = global_stats['successful_pages']
    
    with scraped_urls_lock:
        unique_urls = len(scraped_urls)
    
    print("\n" + "=" * 70)
    print("📊 SCRAPING COMPLETE")
    print("=" * 70)
    print(f"   Total listings scraped: {total_listings}")
    print(f"   Complete data: {complete_listings} ({complete_listings/total_listings*100:.1f}%)" if total_listings else "   Complete data: 0")
    print(f"   Unique URLs tracked: {unique_urls}")
    print(f"   Pages successful: {len(successful_pages)}/{end_page - start_page + 1}")
    print(f"   ")
    print(f"   Phase 1 time: {phase1_time:.1f}s ({phase1_time/60:.1f} min)")
    print(f"   Total time: {total_time:.1f}s ({total_time/60:.1f} min)")
    print(f"   Speed: {len(successful_pages)/total_time*60:.1f} pages/min")
    
    if failed_pages:
        print(f"   ")
        print(f"   ⚠️  Failed pages: {sorted(failed_pages)}")
    else:
        print(f"   ✅ All pages scraped successfully!")
    
    if retry_results['retried'] > 0:
        print(f"   ")
        print(f"   Retries: {retry_results['succeeded']}/{retry_results['retried']} succeeded")
    
    # Per-worker stats
    print(f"\n   📈 Per-worker statistics:")
    with stats_lock:
        for worker_id, pages_list in sorted(global_stats['pages_by_worker'].items()):
            print(f"      Worker {worker_id}: {len(pages_list)} pages")
    
    print(f"   ")
    print(f"   Output: {OUTPUT_DIR}/{output_file}")
    if DEBUG_MODE:
        print(f"   Debug files: {OUTPUT_DIR}/debug/")
    print("=" * 70)
    
    # Cleanup - close browsers
    print("\nClosing browsers...")
    for worker_id, driver in drivers:
        try:
            driver.quit()
        except:
            pass
    
    print("Done!")


# ---------------------------------------------------------
# CLI
# ---------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="SeLoger TRULY PARALLEL Scraper v10",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape pages 1-50 with 5 browsers
  python seloger_scraper_v10.py --start 1 --end 50 --workers 5
  
  # With debug mode (saves HTML/screenshots of failed pages)
  python seloger_scraper_v10.py --start 1 --end 20 --workers 3 --debug
  
  # Interactive mode
  python seloger_scraper_v10.py

KEY FEATURES:
  ✨ TRUE PARALLELISM - Each browser works independently (3-5x faster)
  ✨ Human-like scrolling with hesitations and scroll-back
  ✨ Random breaks to simulate human behavior
  ✨ Viewport randomization per browser
  ✨ Debug mode - saves HTML/screenshots of failed pages
  ✨ Retry system - failed pages are retried automatically
  ✨ Duplicate prevention using URL tracking
  ✨ Triple-layer extraction (CSS → Text Regex → HTML Regex)
  ✨ Confidence scoring (1-10)
  ✨ Detailed per-worker statistics

NOTE: You will need to manually accept cookies in EACH browser window
      when they first open. The script waits for all to be accepted.
        """
    )
    
    parser.add_argument("--start", type=int, help="Start page number")
    parser.add_argument("--end", type=int, help="End page number")
    parser.add_argument("--workers", type=int, default=PARALLEL_WORKERS, 
                        help=f"Number of browsers (default: {PARALLEL_WORKERS}, max: {MAX_WORKERS})")
    parser.add_argument("--output", type=str, help="Output CSV filename")
    parser.add_argument("--headless", action="store_true", 
                        help="Run in headless mode (not recommended - can't accept cookies)")
    parser.add_argument("--debug", action="store_true", 
                        help="Enable debug mode (saves HTML/screenshots of failed pages)")
    
    args = parser.parse_args()
    
    global DEBUG_MODE, HEADLESS
    DEBUG_MODE = args.debug
    HEADLESS = args.headless
    
    if DEBUG_MODE:
        logger.info("🐛 Debug mode enabled - will save HTML/screenshots of failed pages")
        logging.getLogger().setLevel(logging.DEBUG)
    
    if HEADLESS:
        print("\n" + "⚠️ " * 20)
        print("WARNING: Headless mode enabled!")
        print("You won't be able to manually accept cookies in headless mode.")
        print("This will likely cause the scraper to fail.")
        print("Consider running without --headless")
        print("⚠️ " * 20 + "\n")
    
    # Interactive mode
    if not args.start or not args.end:
        print("\n" + "=" * 70)
        print("SeLoger TRULY PARALLEL Scraper v10")
        print("=" * 70)
        print("\n✨ KEY IMPROVEMENT: Uses MULTIPLE BROWSERS for TRUE parallelism!")
        print("   Each browser works independently - no waiting, no locks!")
        print("   3-5x faster than tab-based approaches\n")
        
        print("🍪 Flow:")
        print("   1. Opens N browser windows")
        print("   2. Waits for you to accept cookies in ALL windows")
        print("   3. Starts truly parallel scraping")
        print("   4. Retries any failed pages\n")
        
        print("📋 Features:")
        print("   • Human-like scrolling (hesitations, scroll-back)")
        print("   • Random breaks to simulate human behavior")
        print("   • Viewport randomization")
        print("   • Debug mode (--debug) saves HTML/screenshots")
        print("   • Automatic retry of failed pages")
        print("   • Duplicate URL prevention")
        print("   • Triple-layer data extraction")
        print()
        
        try:
            start = int(input("Start page (e.g., 1): ").strip())
            end = int(input("End page (e.g., 50): ").strip())
            
            workers_input = input(f"Number of browsers (1-{MAX_WORKERS}, default {PARALLEL_WORKERS}): ").strip()
            workers = int(workers_input) if workers_input else PARALLEL_WORKERS
            workers = max(1, min(workers, MAX_WORKERS))
            
            debug_input = input("Enable debug mode? (y/n, default n): ").strip().lower()
            if debug_input == 'y':
                DEBUG_MODE = True
                logging.getLogger().setLevel(logging.DEBUG)
                print("   🐛 Debug mode enabled")
                
        except (ValueError, KeyboardInterrupt):
            print("\nCancelled.")
            return
    else:
        start = args.start
        end = args.end
        workers = max(1, min(args.workers, MAX_WORKERS))
    
    output_file = args.output or f"seloger_v10_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    total_pages = end - start + 1
    # Estimate: ~12 seconds per page with true parallelism
    est_time = (total_pages / workers) * 12
    
    print()
    print("=" * 70)
    print(f"Will scrape pages {start} to {end} ({total_pages} pages)")
    print(f"Using {workers} parallel browsers")
    print(f"Estimated time: ~{est_time/60:.1f} minutes")
    print(f"Output: {output_file}")
    print()
    print("🍪 IMPORTANT: When the browsers open, please accept cookies in EACH one!")
    print("   The script will wait until ALL browsers have cookies accepted.")
    print()
    print("📋 CSV COLUMNS:")
    print("   Page_Number, Type, Price, Price_Per_M2, Surface_m2, Rooms,")
    print("   Bedrooms, Delivery_Date, Address, City, PostalCode, Department,")
    print("   Program_Name, URL, Confidence_Score, Raw_Card_Text")
    print("=" * 70)
    print()
    
    confirm = input("Start scraping? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Cancelled.")
        return
    
    scrape_parallel(start, end, output_file, workers)


if __name__ == "__main__":
    main()
