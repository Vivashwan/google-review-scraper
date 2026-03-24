"""
Google Maps Reviews — Hybrid Scraper
======================================
Strategy: Playwright scrolls the reviews panel (just like your original scraper),
but instead of reading from the DOM, we intercept every `listugcposts` API
response the browser makes automatically as you scroll.

Result:
  - Zero DOM parsing, zero virtualization issues, zero truncation
  - Complete attributes (food/service/atmosphere ratings, dining mode, etc.)
  - Full review text always — no "More" button needed
  - Same anti-bot stealth as your original scraper

Usage:
    pip install playwright playwright-stealth
    playwright install chromium
    python gmaps_hybrid_scraper.py --url "https://www.google.com/maps/place/..." --output reviews.csv
    python gmaps_hybrid_scraper.py --url "..." --output reviews.csv --speed turbo
    python gmaps_hybrid_scraper.py --url "..." --output reviews.csv --headless
"""

import asyncio
import csv
import json
import logging
import os
import random
import re
import signal
import sys
import time
import argparse
from pathlib import Path
from typing import Optional

from playwright.async_api import (
    async_playwright, Page, BrowserContext,
    TimeoutError as PWTimeout,
)

# ── playwright-stealth ────────────────────────────────────────────────────────
HAS_STEALTH = False
try:
    import inspect as _inspect
    from playwright_stealth import Stealth as _Stealth
    _stealth_obj = _Stealth()

    def _find_method(obj, names):
        for n in names:
            m = getattr(obj, n, None)
            if m: return n, m
        return None, None

    _page_name, _page_method = _find_method(_stealth_obj, [
        "stealth_page", "apply_stealth", "apply_stealth_sync",
    ])
    _ctx_name, _ctx_method = _find_method(_stealth_obj, [
        "stealth_context", "apply_stealth_context",
    ])

    def _wrap(method):
        if _inspect.iscoroutinefunction(method):
            async def _f(t): await method(t)
        else:
            async def _f(t):
                r = method(t)
                if _inspect.isawaitable(r): await r
        return _f

    apply_stealth         = _wrap(_page_method) if _page_method else None
    apply_stealth_context = _wrap(_ctx_method)  if _ctx_method  else None
    HAS_STEALTH           = bool(_page_method)
    print(f"[INFO] playwright-stealth active | method={_page_name}")
except Exception as e:
    print(f"[WARN] playwright-stealth unavailable: {e}")
    async def apply_stealth(p): pass
    apply_stealth_context = None

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("hybrid_scraper.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("GMapsHybrid")

# ── Config ────────────────────────────────────────────────────────────────────
CFG = {
    "HEADLESS":    False,
    "VIEWPORT":    (1366, 768),
    "USER_AGENT":  (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "LOCALE":      "en-US",
    "TIMEZONE":    "America/New_York",
    "SESSION_DIR": "./gmaps_session_hybrid",

    "MAX_REVIEWS":          10_000_000,
    "MAX_RUNTIME_SECONDS":  5_184_000,
    "MAX_SCROLL_PLATEAU":   40,

    # Hybrid mode — scroll fast, API does the work
    # No need for slow DOM render waits. Only limit: don't scroll so fast
    # that Maps stops firing API calls (it batches ~10 reviews per request).
    "SCROLL_BATCH_SIZE":     10,
    "DELAY_BETWEEN_SCROLLS": (0.05, 0.15),   # very fast — no DOM to wait for
    "DELAY_AFTER_CLICK":     (0.8, 1.5),
    "DELAY_IDLE_PAUSE":      (2.0, 4.0),
    "IDLE_PAUSE_EVERY":      (400, 700),
    "REVERSE_SCROLL_EVERY":  (100, 200),
    "SCROLL_DISTANCE":       (1000, 1800),
    "SCROLL_JUMP":           True,
    "CONTENT_WAIT_TIMEOUT":  1.0,
    "CONTENT_WAIT_POLL":     0.05,
    "SLOW_MODE_DELAY":       (4.0, 8.0),
    "CAPTCHA_PAUSE":         120,
}

SPEED_PROFILES = {
    "turbo": {
        # Absolute max speed — scroll as fast as Maps can fire API calls
        # Maps fires one listugcposts per ~10 reviews loaded
        # Too fast = Maps skips firing calls; this is the sweet spot
        "DELAY_BETWEEN_SCROLLS": (0.0, 0.03),
        "DELAY_AFTER_CLICK":     (0.3, 0.6),
        "DELAY_IDLE_PAUSE":      (0.5, 1.0),
        "IDLE_PAUSE_EVERY":      (1000, 2000),
        "REVERSE_SCROLL_EVERY":  (500, 1000),
        "SCROLL_BATCH_SIZE":     30,
        "SCROLL_DISTANCE":       (2000, 3000),
        "MAX_SCROLL_PLATEAU":    60,
    },
    "fast": {
        "DELAY_BETWEEN_SCROLLS": (0.02, 0.06),
        "DELAY_AFTER_CLICK":     (0.5, 1.0),
        "DELAY_IDLE_PAUSE":      (1.0, 2.0),
        "IDLE_PAUSE_EVERY":      (600, 1000),
        "REVERSE_SCROLL_EVERY":  (200, 400),
        "SCROLL_BATCH_SIZE":     20,
        "SCROLL_DISTANCE":       (1500, 2500),
        "MAX_SCROLL_PLATEAU":    50,
    },
    "safe": {
        "DELAY_BETWEEN_SCROLLS": (0.1, 0.3),
        "DELAY_AFTER_CLICK":     (1.0, 2.0),
        "DELAY_IDLE_PAUSE":      (3.0, 6.0),
        "IDLE_PAUSE_EVERY":      (200, 400),
        "REVERSE_SCROLL_EVERY":  (60, 120),
        "SCROLL_BATCH_SIZE":     8,
        "SCROLL_DISTANCE":       (800, 1400),
        "MAX_SCROLL_PLATEAU":    40,
    },
}

SELECTORS = {
    "reviews_tab": [
        'button[aria-label*="Reviews"]',
        'button[jsaction*="reviews"]',
        'button:has-text("Reviews")',
    ],
    "review_sort_button": [
        'button[aria-label*="Sort reviews"]',
        'button:has-text("Sort")',
    ],
    "sort_newest": [
        '[data-index="1"]',
        'li[aria-label*="Newest"]',
        'li:has-text("Newest")',
    ],
    "scroll_container": [
        'div[aria-label*="Reviews"][role="feed"]',
        '.m6QErb.DxyBCb.kA9KIf.dS8AEf',
        'div[jstcache][style*="overflow"]',
    ],
    "captcha_signals": [
        '#captcha', 'iframe[src*="recaptcha"]',
        'form[action*="challenge"]',
    ],
}

# ── Attribute key map ─────────────────────────────────────────────────────────
# Keys confirmed from live API response inspection
ATTR_KEY_MAP = {
    # Standard structured attrs — value at attr[2] (single-select)
    "GUIDED_DINING_MODE":                          "dining_mode",
    "GUIDED_DINING_MEAL_TYPE":                     "meal_type",
    "GUIDED_DINING_PRICE_RANGE":                   "price_per_person",
    "GUIDED_DINING_PARKING_SPACE_AVAILABILITY":    "parking_space",
    "GUIDED_DINING_RECOMMEND_TO_VEGETARIANS":      "vegetarian_recommendation",

    # Numeric sub-ratings — value at attr[11]
    "GUIDED_DINING_FOOD_ASPECT":                   "food",
    "GUIDED_DINING_SERVICE_ASPECT":                "service_rating",
    "GUIDED_DINING_ATMOSPHERE_ASPECT":             "atmosphere",

    # Multi-select attrs — value(s) at attr[3]
    "GUIDED_DINING_SEATING_TYPE":                  "seating_type",
    "GUIDED_DINING_WAIT_TIME":                     "wait_time",
    "GUIDED_DINING_PARKING_OPTIONS":               "parking_options",
    "GUIDED_DINING_GROUP_SIZE":                    "group_size",
    "GUIDED_DINING_SPECIAL_EVENTS":                "special_events",
    "GUIDED_DINING_VEGETARIAN_OFFERINGS_INFO":     "vegetarian_offerings",
    "GUIDED_DINING_NOISE_LEVEL":                   "noise_level",
    "GUIDED_DINING_RESERVATIONS":                  "reservations",

    # Dish recommendations — display names at attr[3]
    "GUIDED_DINING_DISH_RECOMMENDATION":           "recommended_dishes",
}

# Freetext tip keys — value at attr[10][0] (the actual text the user wrote)
TIPS_VALUE_KEY_MAP = {
    "GUIDED_DINING_KID_FRIENDLINESS_TIPS":         "kid_friendliness",
    "GUIDED_DINING_ACCESSIBILITY_TIPS":            "wheelchair_accessibility",
    "GUIDED_DINING_VEGETARIAN_OPTIONS_TIPS":       "vegetarian_options",
    "GUIDED_DINING_DIETARY_RESTRICTIONS_TIPS":           "dietary_restrictions",
    "GUIDED_DINING_OTHER_DIETARY_RESTRICTIONS_TIPS":   "dietary_restrictions",
    "GUIDED_DINING_PARKING_TIPS":                  "parking_notes",
}

# ── Utilities ─────────────────────────────────────────────────────────────────
def rand_delay(band): return random.uniform(*band)
async def human_sleep(band): await asyncio.sleep(rand_delay(band))

async def micro_mouse_move(page: Page) -> bool:
    try:
        for _ in range(random.randint(2, 4)):
            await page.mouse.move(
                random.randint(200, 900), random.randint(150, 600),
                steps=random.randint(3, 7)
            )
            await asyncio.sleep(random.uniform(0.04, 0.15))
        return True
    except Exception:
        return False

async def try_selector(page, selectors, timeout=5000):
    for sel in selectors:
        try:
            el = await page.wait_for_selector(sel, timeout=timeout, state="visible")
            if el: return el
        except Exception:
            continue
    return None

# ── CSV Writer ────────────────────────────────────────────────────────────────
class ReviewCSVWriter:
    FIELDS = [
        "review_id", "place_name", "reviewer_name", "reviewer_id", "local_guide",
        "rating", "review_text", "likes", "date", "attributes",
    ]

    def __init__(self, filepath: str):
        self.filepath  = filepath
        self.seen_ids: set = set()
        self._file     = None
        self._writer   = None
        self._load_existing()

    def _load_existing(self):
        if not Path(self.filepath).exists():
            return
        with open(self.filepath, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                # Use review_id as the unique key (one person can have multiple reviews)
                # Fall back to reviewer_id+date if review_id column doesn't exist
                rid = row.get("review_id") or (row.get("reviewer_id","") + "_" + row.get("date",""))
                if rid:
                    self.seen_ids.add(rid)
        log.info(f"Resumed — {len(self.seen_ids)} existing reviews from {self.filepath}")

    def open(self):
        is_new = not Path(self.filepath).exists()
        self._file   = open(self.filepath, "a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._file, fieldnames=self.FIELDS)
        if is_new:
            self._writer.writeheader()

    def write(self, review: dict) -> bool:
        rid = review.get("review_id") or (
            review.get("reviewer_id","") + "_" + review.get("date","")
        )
        if not rid or rid in self.seen_ids:
            return False
        self.seen_ids.add(rid)
        row = {f: review.get(f, "") for f in self.FIELDS}
        self._writer.writerow(row)
        self._file.flush()
        return True

    def close(self):
        if self._file:
            self._file.close()

    @property
    def total_seen(self): return len(self.seen_ids)

# ── API Response Parser ───────────────────────────────────────────────────────
class APIResponseParser:
    """
    Parses listugcposts API responses intercepted from the browser.

    Confirmed positions from live debug (data[2][N][0]):
      r[0]              = review_id
      r[1][4][5][0]     = reviewer name (string)
      r[1][4][2][0]     = profile URL → extract reviewer_id
      r[1][4][5][10][0] = "Local Guide · N reviews" (if local guide)
      r[1][6]           = date "3 months ago"
      r[2][0][0]        = star rating integer 1-5
      r[2][6]           = attributes array
      r[2][6][N][0][0]  = attribute key "GUIDED_DINING_MODE"
      r[2][6][N][2]     = selected options (category attrs)
      r[2][6][N][11]    = [rating] (food/service/atmosphere numeric)
      r[2][15][0][0]    = full review text (never truncated)
    """

    XSS_PREFIX = ")]}'"

    @staticmethod
    def safe_get(obj, *keys, default=None):
        cur = obj
        for k in keys:
            try:
                cur = cur[k]
            except (IndexError, KeyError, TypeError):
                return default
        return cur

    def parse_response(self, text: str) -> list[dict]:
        text = text.strip()
        if text.startswith(self.XSS_PREFIX):
            text = text[len(self.XSS_PREFIX):].strip()
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return []

        reviews_raw = self.safe_get(data, 2, default=[])
        if not isinstance(reviews_raw, list):
            return []

        reviews = []
        for entry in reviews_raw:
            if not isinstance(entry, list) or not entry:
                continue
            # entry = [review_array, None, "CAESY0_token"]
            r = entry[0] if isinstance(entry[0], list) else entry
            parsed = self._parse_review(r)
            if parsed:
                reviews.append(parsed)

        return reviews

    def _parse_review(self, r: list) -> Optional[dict]:
        g = self.safe_get

        # review_id
        review_id = g(r, 0, default=None)
        if not isinstance(review_id, str) or len(review_id) < 10:
            return None

        # date
        date = str(g(r, 1, 6, default="") or "")

        # reviewer name — r[1][4][5][0] is a plain string
        reviewer_name = str(g(r, 1, 4, 5, 0, default="") or "")

        # reviewer_id — extract number from profile URL at r[1][4][2][0]
        reviewer_id = ""
        profile_url = str(g(r, 1, 4, 2, 0, default="") or "")
        m = re.search(r'/contrib/(\d+)', profile_url)
        if m:
            reviewer_id = m.group(1)

        # local guide
        lg_text = str(g(r, 1, 4, 5, 10, 0, default="") or "")
        local_guide = "local guide" in lg_text.lower()

        # star rating
        rating = g(r, 2, 0, 0, default=None)

        # review text — full, never truncated
        review_text = str(g(r, 2, 15, 0, 0, default="") or "")
        review_text = review_text.replace("\n", " ").strip()

        # attributes
        attrs = {}
        for attr in (g(r, 2, 6, default=[]) or []):
            key_raw = g(attr, 0, 0, default=None)
            if key_raw == "GUIDED_DINING_TIPS_TOPICS":
                # Skip — topics already covered by dedicated keys below
                continue
            elif key_raw in TIPS_VALUE_KEY_MAP:
                # Freetext tips attrs — value at attr[10] as list of strings
                field_name = TIPS_VALUE_KEY_MAP[key_raw]
                val_list = g(attr, 10, default=None)
                if isinstance(val_list, list) and val_list:
                    val = ", ".join(str(v) for v in val_list if v)
                    if val and field_name not in attrs:
                        attrs[field_name] = val
            else:
                result = self._parse_attribute(attr)
                if result:
                    key, val = result
                    if key not in attrs:
                        attrs[key] = val

        return {
            "review_id":     review_id,
            "reviewer_name": reviewer_name,
            "reviewer_id":   reviewer_id,
            "local_guide":   local_guide,
            "rating":        rating,
            "review_text":   review_text,
            "likes":         0,
            "date":          date,
            "attributes":    json.dumps(attrs, ensure_ascii=False) if attrs else "",
        }

    def _parse_attribute(self, attr: list) -> Optional[tuple]:
        g = self.safe_get

        key_raw = g(attr, 0, 0, default=None)
        if not key_raw:
            return None

        # Pattern 0: freetext tip keys — value at attr[10][0]
        # e.g. GUIDED_DINING_KID_FRIENDLINESS_TIPS, GUIDED_DINING_ACCESSIBILITY_TIPS
        if key_raw in TIPS_VALUE_KEY_MAP:
            field_name = TIPS_VALUE_KEY_MAP[key_raw]
            tip_val = g(attr, 10, 0, default=None)
            if isinstance(tip_val, str) and tip_val.strip():
                return field_name, tip_val.strip()
            return None

        if key_raw not in ATTR_KEY_MAP:
            return None

        field_name = ATTR_KEY_MAP[key_raw]

        # Pattern A: numeric sub-ratings (food/service/atmosphere)
        # attr[11] = [value] e.g. [5]
        rating_container = g(attr, 11, default=None)
        if isinstance(rating_container, list) and rating_container:
            val = rating_container[0]
            if isinstance(val, (int, float)) and 1 <= val <= 5:
                return field_name, val

        # Pattern B: category/multi-select — try attr[2] then attr[3]
        def extract_options(options_outer):
            if not isinstance(options_outer, list) or not options_outer:
                return []
            values = []
            for group in options_outer:
                if not isinstance(group, list):
                    continue
                for option in group:
                    if isinstance(option, list) and len(option) > 1:
                        display_val = option[1]
                        if isinstance(display_val, str) and display_val:
                            values.append(display_val)
            return values

        for idx in (2, 3):
            options_outer = g(attr, idx, default=None)
            values = extract_options(options_outer)
            if values:
                return field_name, ", ".join(values)

        return None

    # Label → field_key mapping for TIPS_TOPICS topic labels
    TIPS_TOPICS_KEY_MAP = {
        "parking":                  "parking",
        "wheelchair accessibility": "wheelchair_accessibility",
        "vegetarian options":       "vegetarian_options",
        "vegetarian offerings":     "vegetarian_offerings",
        "kid-friendliness":         "kid_friendliness",
        "dietary restrictions":     "dietary_restrictions",
        "recommended dishes":       "recommended_dishes",
        "highlights":               "highlights",
        "offerings":                "offerings",
        "amenities":                "amenities",
        "crowd":                    "crowd",
        "planning":                 "planning",
        "getting there":            "getting_there",
    }

    def _parse_tips_topics(self, attr: list) -> dict:
        """
        Parse GUIDED_DINING_TIPS_TOPICS into individual boolean keys.
        Each selected topic becomes its own key with value True.
        e.g. ["Parking", "Wheelchair accessibility"] →
             {"parking": True, "wheelchair_accessibility": True}
        """
        g = self.safe_get
        result = {}

        def extract_options(options_outer):
            if not isinstance(options_outer, list):
                return []
            values = []
            for group in options_outer:
                if not isinstance(group, list):
                    continue
                for option in group:
                    if isinstance(option, list) and len(option) > 1:
                        v = option[1]
                        if isinstance(v, str) and v:
                            values.append(v)
            return values

        for idx in (2, 3):
            options_outer = g(attr, idx, default=None)
            values = extract_options(options_outer)
            if values:
                for label in values:
                    key = self.TIPS_TOPICS_KEY_MAP.get(label.lower().strip())
                    if key:
                        result[key] = True
                    else:
                        # Unknown topic — store with sanitized key
                        safe_key = label.lower().strip().replace(" ", "_").replace("-", "_")
                        result[safe_key] = True
                break

        return result

# ── Main Scraper ──────────────────────────────────────────────────────────────
class GoogleMapsHybridScraper:
    """
    Scrolls the Maps reviews panel with Playwright,
    intercepts every listugcposts API response,
    parses complete review data from the response (no DOM reading).
    """

    def __init__(self, url: str, output_csv: str, place_name: str = "", expected_total: int = 0):
        self.url        = url
        self.place_name = place_name or "Unknown Place"
        self.expected_total = int(expected_total or 0)
        self.csv        = ReviewCSVWriter(output_csv)
        self.parser     = APIResponseParser()
        self._shutdown  = False
        self._start_time = None
        # Queue: intercepted responses waiting to be processed
        self._response_queue: asyncio.Queue = asyncio.Queue()

    def _register_signals(self):
        def _h(sig, frame):
            log.info("🛑  Shutdown — finishing...")
            self._shutdown = True
        signal.signal(signal.SIGINT, _h)
        signal.signal(signal.SIGTERM, _h)

    def _runtime_exceeded(self):
        return (time.time() - self._start_time) > CFG["MAX_RUNTIME_SECONDS"]

    # ── Browser setup ─────────────────────────────────────────────────────────

    async def _start_browser(self):
        self._playwright = await async_playwright().start()
        session_dir = CFG["SESSION_DIR"]
        os.makedirs(session_dir, exist_ok=True)

        self.context = await self._playwright.chromium.launch_persistent_context(
            user_data_dir=session_dir,
            headless=CFG["HEADLESS"],
            viewport={"width": CFG["VIEWPORT"][0], "height": CFG["VIEWPORT"][1]},
            user_agent=CFG["USER_AGENT"],
            locale=CFG["LOCALE"],
            timezone_id=CFG["TIMEZONE"],
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--ozone-platform=x11",
                "--disable-gpu",
                "--disable-software-rasterizer",
            ],
            ignore_default_args=["--enable-automation"],
        )

        await self.context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
            window.chrome = {runtime: {}};
        """)

        if HAS_STEALTH and apply_stealth_context:
            try: await apply_stealth_context(self.context)
            except Exception as e: log.warning(f"Context stealth failed: {e}")

        pages = self.context.pages
        self.page = pages[0] if pages else await self.context.new_page()

        if HAS_STEALTH and apply_stealth:
            try: await apply_stealth(self.page)
            except Exception as e: log.warning(f"Page stealth failed: {e}")

        log.info("Browser started")

    async def _stop_browser(self):
        if hasattr(self, 'context'): await self.context.close()
        if hasattr(self, '_playwright'): await self._playwright.stop()

    # ── Network interception ──────────────────────────────────────────────────

    def _setup_interception(self, page: Page):
        """Register response handler to capture listugcposts responses."""
        async def on_response(response):
            if "listugcposts" in response.url:
                try:
                    text = await response.text()
                    await self._response_queue.put(text)
                except Exception as e:
                    log.debug(f"Failed to read response: {e}")

        page.on("response", on_response)
        log.info("Network interception active — listening for listugcposts")

    async def _drain_queue(self, place_name: str) -> int:
        """Process all queued API responses. Returns number of new reviews written."""
        written = 0
        while not self._response_queue.empty():
            try:
                text = self._response_queue.get_nowait()
                reviews = self.parser.parse_response(text)
                for review in reviews:
                    review["place_name"] = place_name
                    if self.csv.write(review):
                        written += 1
            except asyncio.QueueEmpty:
                break
            except Exception as e:
                log.debug(f"Error processing queued response: {e}")
        return written

    # ── Navigation ────────────────────────────────────────────────────────────

    async def _dismiss_consent(self, page: Page):
        for sel in [
            'button[aria-label*="Accept all"]', 'button:has-text("Accept all")',
            'button:has-text("I agree")', '#L2AGLb',
        ]:
            try:
                btn = await page.wait_for_selector(sel, timeout=3000, state="visible")
                if btn:
                    await btn.click()
                    await asyncio.sleep(1.5)
                    return
            except Exception:
                continue

    async def _navigate(self, page: Page):
        url = self.url
        sep = "&" if "?" in url else "?"
        if "hl=" not in url: url += sep + "hl=en"; sep = "&"
        if "gl=" not in url: url += sep + "gl=in"

        for attempt in range(4):
            try:
                if attempt == 0:
                    try:
                        await page.goto("https://www.google.com/maps?hl=en&gl=in",
                                        wait_until="domcontentloaded", timeout=30000)
                        await asyncio.sleep(rand_delay((1.5, 3.0)))
                        await self._dismiss_consent(page)
                    except Exception:
                        pass
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                await self._dismiss_consent(page)
                await asyncio.sleep(rand_delay((2.0, 4.0)))
                return
            except Exception as e:
                log.warning(f"Navigation attempt {attempt+1}/4: {e}")
                await asyncio.sleep(5 * (attempt + 1))
        raise RuntimeError("Failed to navigate after 4 attempts")

    async def _open_reviews_tab(self, page: Page):
        log.info("Opening Reviews tab...")
        tab = await try_selector(page, SELECTORS["reviews_tab"], timeout=10000)
        if not tab:
            raise RuntimeError("Reviews tab not found")
        await micro_mouse_move(page)
        await tab.click()
        await human_sleep(CFG["DELAY_AFTER_CLICK"])

        sort_btn = await try_selector(page, SELECTORS["review_sort_button"], timeout=6000)
        if sort_btn:
            await sort_btn.click()
            await asyncio.sleep(rand_delay((0.8, 1.5)))
            newest = await try_selector(page, SELECTORS["sort_newest"], timeout=4000)
            if newest:
                await newest.click()
                await asyncio.sleep(rand_delay((1.5, 3.0)))
                log.info("Sorted by Newest")

    async def _get_place_name(self, page: Page) -> str:
        try:
            name = await page.evaluate(r"""
            () => {
                const h1 = document.querySelector('h1.DUwDvf, h1[class*="fontHeadlineLarge"]');
                if (h1) return h1.innerText.trim();
                return document.title.replace(/\s*[-\u2013].*$/, '').trim();
            }
            """)
            return name or self.place_name
        except Exception:
            return self.place_name

    async def _get_total_review_count(self, page: Page) -> int:
        try:
            count = await page.evaluate(r"""
            () => {
                const parseCount = (raw) => {
                    if (!raw) return 0;
                    const txt = String(raw).replace(/,/g, '').trim().toLowerCase();
                    const m = txt.match(/(\d+(?:\.\d+)?)\s*([km])?\s+reviews?/i);
                    if (!m) return 0;
                    const num = parseFloat(m[1]);
                    const suffix = (m[2] || '').toLowerCase();
                    if (!Number.isFinite(num)) return 0;
                    if (suffix === 'k') return Math.round(num * 1000);
                    if (suffix === 'm') return Math.round(num * 1000000);
                    return Math.round(num);
                };

                // 1) Prefer dedicated review-tab/button labels.
                let best = 0;
                const btns = document.querySelectorAll('button[jsaction*="reviews"], button[aria-label*="reviews"]');
                for (const btn of btns) {
                    const c = parseCount(btn.getAttribute('aria-label'));
                    if (c > best) best = c;
                }

                // 2) Also scan a few high-signal containers only (avoid entire body to
                // prevent picking star-breakdown rows like "1,267 reviews").
                const scoped = document.querySelectorAll(
                    'h1, h2, div[role="main"], div[role="region"], div[aria-label*="reviews"]'
                );
                for (const el of scoped) {
                    const c = parseCount(el.getAttribute('aria-label'));
                    if (c > best) best = c;
                    const t = parseCount(el.innerText || '');
                    if (t > best) best = t;
                }

                return best || 0;
            }
            """)
            return int(count) if count else 0
        except Exception:
            return 0

    async def _resolve_target_review_count(self, listing_total: int) -> int:
        if self.expected_total > 0 and listing_total > 0 and listing_total != self.expected_total:
            log.warning(
                f"Review-count mismatch: input={self.expected_total} vs page={listing_total}. "
                f"Using input value."
            )
        if self.expected_total > 0:
            return self.expected_total
        return listing_total

    # ── Scroll loop ───────────────────────────────────────────────────────────

    async def _find_scroll_container(self, page: Page):
        for sel in SELECTORS["scroll_container"]:
            try:
                el = await page.query_selector(sel)
                if el: return el
            except Exception:
                continue
        return None

    async def _scroll_once(self, page: Page, container, distance: int):
        beh = "instant" if CFG["SCROLL_JUMP"] else "smooth"
        if container:
            await page.evaluate(
                "([el,d,b]) => el.scrollBy({top:d,behavior:b})",
                [container, distance, beh]
            )
        else:
            await page.evaluate(f"window.scrollBy({{top:{distance},behavior:'{beh}'}})")

    async def _detect_captcha(self, page: Page) -> bool:
        for sel in SELECTORS["captcha_signals"]:
            try:
                if await page.query_selector(sel): return True
            except Exception:
                pass
        title = await page.title()
        return "captcha" in title.lower() or "unusual traffic" in title.lower()

    async def _scroll_loop(self, page: Page, place_name: str):
        container       = await self._find_scroll_container(page)
        total_written   = 0
        scroll_count    = 0
        plateau_count   = 0
        next_idle_at    = random.randint(*CFG["IDLE_PAUSE_EVERY"])
        next_reverse_at = random.randint(*CFG["REVERSE_SCROLL_EVERY"])
        slow_mode_count = 0

        page_total = await self._get_total_review_count(page)
        total_on_listing = await self._resolve_target_review_count(page_total)
        log.info(f"Total reviews on listing: {page_total or '?'}")
        if self.expected_total > 0:
            log.info(f"Target reviews from input: {self.expected_total}")
        log.info("Starting scroll loop — intercepting API responses...")

        # Initial drain — first load may have already triggered API calls
        await asyncio.sleep(2.0)
        initial = await self._drain_queue(place_name)
        if initial:
            total_written += initial
            log.info(f"Initial load: {initial} reviews from first API response")

        while not self._shutdown:
            if self._runtime_exceeded():
                log.warning("⏱️  Max runtime reached")
                break
            if self.csv.total_seen >= CFG["MAX_REVIEWS"]:
                log.info(f"✅  Max reviews reached")
                break
            if plateau_count >= CFG["MAX_SCROLL_PLATEAU"]:
                log.info(f"📊  Plateau — {self.csv.total_seen} reviews. Done.")
                break

            # Smart stop
            if total_on_listing > 0:
                gap = total_on_listing - self.csv.total_seen
                if gap <= max(10, int(total_on_listing * 0.01)) and self.csv.total_seen > 0:
                    log.info(f"✅  Collected {self.csv.total_seen}/{total_on_listing}. Done.")
                    break

            # Captcha check
            if await self._detect_captcha(page):
                log.warning(f"⚠️  CAPTCHA — pausing {CFG['CAPTCHA_PAUSE']}s")
                await asyncio.sleep(CFG["CAPTCHA_PAUSE"])

            # Scroll batch
            for _ in range(CFG["SCROLL_BATCH_SIZE"]):
                lo, hi = CFG["SCROLL_DISTANCE"]
                dist = random.randint(lo, hi)
                await self._scroll_once(page, container, dist)
                await human_sleep(CFG["DELAY_BETWEEN_SCROLLS"])
                scroll_count += 1

                # Occasional reverse scroll
                if scroll_count >= next_reverse_at:
                    rev = random.randint(80, 220)
                    await self._scroll_once(page, container, -rev)
                    await asyncio.sleep(0.1)
                    await self._scroll_once(page, container, rev + random.randint(150, 350))
                    next_reverse_at = scroll_count + random.randint(*CFG["REVERSE_SCROLL_EVERY"])

                # Idle pause
                if scroll_count >= next_idle_at:
                    pause = rand_delay(CFG["DELAY_IDLE_PAUSE"])
                    log.info(f"  [idle pause] {pause:.1f}s")
                    await micro_mouse_move(page)
                    await asyncio.sleep(pause)
                    next_idle_at = scroll_count + random.randint(*CFG["IDLE_PAUSE_EVERY"])

            # Mouse move
            alive = await micro_mouse_move(page)
            if not alive:
                log.error("Browser closed")
                self._shutdown = True
                break

            # Wait briefly for any in-flight API responses
            await asyncio.sleep(0.3)

            # Drain queue — this is where all the magic happens
            written = await self._drain_queue(place_name)
            total_written += written

            if written > 0:
                plateau_count = 0
                pct = f"{self.csv.total_seen/total_on_listing*100:.1f}%" if total_on_listing else "?%"
                log.info(
                    f"  scrolls={scroll_count:5d} | "
                    f"+{written:3d} new | "
                    f"total={self.csv.total_seen:6,}/{total_on_listing or '?'} ({pct}) | "
                    f"runtime={int(time.time()-self._start_time)}s"
                )
            else:
                plateau_count += 1

            # Re-acquire container every 20 scroll batches
            if scroll_count % (CFG["SCROLL_BATCH_SIZE"] * 20) == 0:
                container = await self._find_scroll_container(page)

        # Final drain — catch any last responses
        final = await self._drain_queue(place_name)
        total_written += final
        if final:
            log.info(f"Final drain: {final} additional reviews")

        log.info(f"Scroll loop done. This run: {total_written} | CSV total: {self.csv.total_seen}")

    # ── Entry point ───────────────────────────────────────────────────────────

    async def run(self):
        self._register_signals()
        self._start_time = time.time()
        self.csv.open()
        status = "ok"

        try:
            await self._start_browser()
            page = self.page

            # Set up interception BEFORE navigation so we don't miss any responses
            self._setup_interception(page)

            await self._navigate(page)
            await self._open_reviews_tab(page)

            place_name = await self._get_place_name(page)
            if place_name and place_name != "Unknown Place":
                self.place_name = place_name
            log.info(f"Place: {self.place_name}")

            await self._scroll_loop(page, self.place_name)

        except RuntimeError as e:
            log.error(f"Fatal: {e}")
            status = "fatal"
        except Exception as e:
            err = str(e)
            if "TargetClosedError" in type(e).__name__ or "been closed" in err:
                log.warning(f"Browser closed. Saved: {self.csv.total_seen}")
                status = "browser_closed"
            else:
                log.exception(f"Unexpected error: {e}")
                status = "error"
        finally:
            self.csv.close()
            await self._stop_browser()
            elapsed = int(time.time() - self._start_time)
            log.info(
                f"\n{'='*60}\n"
                f"  DONE\n"
                f"  Reviews      : {self.csv.total_seen:,}\n"
                f"  Elapsed      : {elapsed}s ({elapsed//60}m {elapsed%60}s)\n"
                f"  Output       : {self.csv.filepath}\n"
                f"{'='*60}"
            )
        return {
            "status": status,
            "reviews": self.csv.total_seen,
            "output": self.csv.filepath,
        }

# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_runtime(value):
    if not value: return 5_184_000
    v = value.strip().lower()
    if v.endswith("d"): return int(v[:-1]) * 86400
    if v.endswith("h"): return int(v[:-1]) * 3600
    if v.endswith("m"): return int(v[:-1]) * 60
    return int(v)

def parse_args():
    p = argparse.ArgumentParser(
        description="Google Maps Reviews — Hybrid Scraper (scroll + network intercept)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python gmaps_hybrid_scraper.py --url "https://www.google.com/maps/place/..." --output reviews.csv
  python gmaps_hybrid_scraper.py --url "..." --output reviews.csv --speed turbo
  python gmaps_hybrid_scraper.py --url "..." --output reviews.csv --runtime 8h
        """
    )
    p.add_argument("--url",      required=True, help="Full Google Maps listing URL")
    p.add_argument("--output",   default="reviews.csv")
    p.add_argument("--place",    default="", help="Place name override")
    p.add_argument("--max",      type=int, default=0)
    p.add_argument("--headless", action="store_true")
    p.add_argument("--speed",    choices=["turbo","fast","safe"], default="fast")
    p.add_argument("--runtime",  default=None, help="e.g. 8h, 3d, 90m")
    p.add_argument("--expected-total", type=int, default=0,
                   help="Optional review count from your input file. Used as stop target.")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    CFG["MAX_REVIEWS"]         = args.max if args.max > 0 else 10_000_000
    CFG["MAX_RUNTIME_SECONDS"] = parse_runtime(args.runtime)
    CFG["HEADLESS"]            = args.headless
    CFG.update(SPEED_PROFILES.get(args.speed, SPEED_PROFILES["fast"]))

    log.info(f"Speed: {args.speed.upper()} | Max: {CFG['MAX_REVIEWS']:,} | Runtime: {args.runtime or 'unlimited'}")

    scraper = GoogleMapsHybridScraper(
        url=args.url,
        output_csv=args.output,
        place_name=args.place,
        expected_total=args.expected_total,
    )
    asyncio.run(scraper.run())
