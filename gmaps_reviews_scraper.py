"""
Google Maps Reviews Scraper v3 — Hybrid Architecture
=====================================================
WHAT CHANGED FROM v2:
  1. TRUE HYBRID MODE (default)
     - Phase 1: Network interception — paginate Google's RPC directly.
       Gets all review_id, name, rating, text, date, likes in minutes.
       Attributes left blank intentionally (not in RPC response).
     - Phase 2: Targeted DOM attribute fill — reloads reviews tab,
       scrolls only to cards in empty_attr_ids, expands them, patches CSV.
       Does NOT scroll through all 52k reviews — uses scroll-to-index trick.

  2. VIRTUAL SCROLLER STALL FIX (the 700-review cliff)
     Root cause: Maps' virtual DOM recycles nodes. After ~700 reviews the
     recycling lag exceeds the 0.5s post-expansion wait, so attribute chips
     are read in their pre-hydration state ("Dine in…" instead of "Dine in").
     Fix A: Dynamic wait — scales from 0.5s → 2.0s based on scroll depth.
     Fix B: Chip truncation detection — if any chip value ends with "…",
            the card is marked is_truncated=True, same as review text.
            This feeds the existing 3-attempt retry loop correctly.

  3. DEEP CLEAN NO LONGER NUKES empty_attr_ids CARDS
     Previous: deep clean every 500 reviews removed ALL cards except last 15,
               including cards still in empty_attr_ids, killing retry chances.
     Fixed:    deep clean now explicitly preserves empty_attr_ids cards.

  4. ATTRIBUTE FILL PASS IS TARGETED (not a full re-scroll)
     For 52k reviews, a naive re-scroll to fill ~2k missing attrs would take
     hours. Instead: after network phase, we sort empty_attr_ids by their
     position in the review stream (index), then jump the scroll container
     directly to that approximate pixel offset, wait for the card to appear,
     expand, extract, patch. Each card takes ~3-5 seconds. 2k cards ≈ 2 hours.

  5. CSV PATCHING (UPDATE vs APPEND)
     When DOM pass fills attributes for a previously-written row, we rewrite
     the CSV row in-place rather than appending a duplicate. This keeps the
     CSV clean for resumption.

  6. NETWORK MODE IMPROVEMENTS
     - Logs the full intercepted URL for debugging
     - Explicit fallback message if RPC pattern changes
     - Retries up to 3 times per page with exponential backoff
     - Handles both aiohttp and Playwright fetch fallback cleanly

Usage:
    pip install playwright playwright-stealth aiohttp
    playwright install chromium

    # Recommended: full hybrid (network bulk + DOM attribute fill)
    python gmaps_reviews_scraper_v3.py --url "https://maps.google.com/..." --output reviews.csv

    # Network only (fastest, no attributes)
    python gmaps_reviews_scraper_v3.py --url "..." --output reviews.csv --mode network

    # DOM only (slow, all attributes — use --speed safe for best quality)
    python gmaps_reviews_scraper_v3.py --url "..." --output reviews.csv --mode dom

    # Speed profiles: turbo / fast (default) / safe
    python gmaps_reviews_scraper_v3.py --url "..." --output reviews.csv --speed safe
"""

import asyncio
import csv
import io
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
from datetime import datetime

from playwright.async_api import (
    async_playwright, Browser, BrowserContext, Page,
    Request, Response,
    TimeoutError as PWTimeout,
)

# ── playwright-stealth integration ───────────────────────────────────────────
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
    HAS_STEALTH = bool(_page_method)
    print(f"[INFO] playwright-stealth active | method={_page_name}")
except Exception as e:
    print(f"[WARN] playwright-stealth unavailable: {e}")
    async def apply_stealth(p): pass
    apply_stealth_context = None

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scraper_v3.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("GMapsV3")

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

CFG = {
    "HEADLESS":    False,
    "VIEWPORT":    (1366, 768),
    "USER_AGENT":  (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "LOCALE":      "en-US",
    "TIMEZONE":    "America/New_York",
    "SESSION_DIR": "./gmaps_session_v3",

    "MAX_REVIEWS":          10_000_000,
    "MAX_RUNTIME_SECONDS":  5_184_000,
    "MAX_SCROLL_PLATEAU":   30,

    # DOM mode scroll settings
    "SCROLL_BATCH_SIZE":         4,
    "DELAY_BETWEEN_SCROLLS":     (0.25, 0.7),
    "DELAY_AFTER_CLICK":         (1.0, 2.2),
    "DELAY_IDLE_PAUSE":          (3.0, 7.0),
    "IDLE_PAUSE_EVERY":          (200, 350),
    "REVERSE_SCROLL_EVERY":      (40, 80),
    "SCROLL_DISTANCE":           (800, 1400),
    "SCROLL_JUMP":               True,
    "CONTENT_WAIT_POLL":         0.08,
    "CONTENT_WAIT_TIMEOUT":      2.0,

    # FIX: keep more anchors — prevents virtual scroller reset
    "DOM_PRUNE_KEEP_TAIL":       15,
    # FIX: deep clean far less often — previous 500 was thrashing
    "DOM_DEEP_CLEAN_EVERY":      2000,

    # FIX A — dynamic expansion wait scaling
    # wait_seconds = BASE + SCALE_PER_1K * (reviews_scraped / 1000)
    # e.g. at 0 reviews: 0.5s. At 1000: 0.8s. At 5000: 2.0s. Capped at MAX.
    "EXPAND_WAIT_BASE":          0.5,
    "EXPAND_WAIT_SCALE_PER_1K":  0.3,
    "EXPAND_WAIT_MAX":           2.5,

    # Anti-block
    "SLOW_MODE_DELAY":           (4.0, 8.0),
    "CAPTCHA_PAUSE":             120,

    # Network mode
    "NETWORK_PAGINATION_DELAY":  (0.3, 0.8),
    "NETWORK_MAX_RETRIES":       3,

    # Attribute fill pass (Phase 2)
    # How many pixels per review card (approximate, used for jump-scrolling)
    "APPROX_CARD_HEIGHT_PX":     200,
    # Max seconds to wait for a target card to appear after jump-scroll
    "ATTR_FILL_CARD_WAIT":       8.0,
    # Max scroll attempts to find a card before giving up on it
    "ATTR_FILL_MAX_SCROLL_ATTEMPTS": 15,
}

SPEED_PROFILES = {
    "turbo": {
        "DELAY_BETWEEN_SCROLLS": (0.05, 0.2),
        "DELAY_AFTER_CLICK":     (0.6, 1.2),
        "DELAY_IDLE_PAUSE":      (1.5, 3.0),
        "IDLE_PAUSE_EVERY":      (300, 500),
        "REVERSE_SCROLL_EVERY":  (150, 300),
        "SCROLL_BATCH_SIZE":     20,
        "SCROLL_DISTANCE":       (1200, 2000),
        "CONTENT_WAIT_TIMEOUT":  1.5,
    },
    "fast": {
        "DELAY_BETWEEN_SCROLLS": (0.25, 0.7),
        "DELAY_AFTER_CLICK":     (1.0, 2.2),
        "DELAY_IDLE_PAUSE":      (3.0, 7.0),
        "IDLE_PAUSE_EVERY":      (200, 350),
        "REVERSE_SCROLL_EVERY":  (40, 80),
        "SCROLL_BATCH_SIZE":     4,
        "SCROLL_DISTANCE":       (800, 1400),
        "CONTENT_WAIT_TIMEOUT":  2.0,
    },
    "safe": {
        "DELAY_BETWEEN_SCROLLS": (1.8, 4.2),
        "DELAY_AFTER_CLICK":     (2.0, 4.5),
        "DELAY_IDLE_PAUSE":      (8.0, 20.0),
        "IDLE_PAUSE_EVERY":      (40, 80),
        "REVERSE_SCROLL_EVERY":  (12, 25),
        "SCROLL_BATCH_SIZE":     5,
        "SCROLL_DISTANCE":       (300, 700),
        "CONTENT_WAIT_TIMEOUT":  8.0,
    },
}

SELECTORS = {
    "reviews_tab": [
        'button[aria-label*="Reviews"]',
        'button[jsaction*="reviews"]',
        '[data-tab-index="1"]',
        'button:has-text("Reviews")',
    ],
    "review_sort_button": [
        'button[aria-label*="Sort reviews"]',
        'button[data-value*="sort"]',
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
        '.review-dialog-list',
    ],
    "captcha_signals": [
        '#captcha', 'iframe[src*="recaptcha"]',
        'form[action*="challenge"]', 'div[id*="challenge"]',
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def rand_delay(band): return random.uniform(*band)
async def human_sleep(band): await asyncio.sleep(rand_delay(band))

def expansion_wait(reviews_scraped: int) -> float:
    """
    FIX A: Dynamic expansion wait.
    Scales up as DOM gets deeper and Maps' renderer gets slower.
    """
    base  = CFG["EXPAND_WAIT_BASE"]
    scale = CFG["EXPAND_WAIT_SCALE_PER_1K"] * (reviews_scraped / 1000)
    return min(base + scale, CFG["EXPAND_WAIT_MAX"])

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

async def detect_captcha(page: Page) -> bool:
    for sel in SELECTORS["captcha_signals"]:
        try:
            if await page.query_selector(sel): return True
        except Exception:
            pass
    title = await page.title()
    return "captcha" in title.lower() or "unusual traffic" in title.lower()

# ─────────────────────────────────────────────────────────────────────────────
# CSV WRITER  (append-only, resume-safe, supports in-place row patching)
# ─────────────────────────────────────────────────────────────────────────────

class ReviewCSVWriter:
    FIELDS = [
        "place_name", "reviewer_name", "reviewer_id", "local_guide",
        "rating", "review_text", "likes", "date", "attributes",
    ]

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.seen_ids: set = set()
        self.empty_attr_ids: set = set()
        # FIX: track write order so attribute fill pass knows card index
        self.id_to_index: dict = {}   # review_id → row index (0-based, excl header)
        self._file = None
        self._writer = None
        self._load_existing()

    def _load_existing(self):
        if not Path(self.filepath).exists():
            return
        with open(self.filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                rid = row.get("review_id") or (
                    row.get("reviewer_name", "") + "_" + row.get("date", "")
                )
                if rid:
                    self.seen_ids.add(rid)
                    self.id_to_index[rid] = i
                    if not row.get("attributes", "").strip():
                        self.empty_attr_ids.add(rid)
        log.info(f"Resumed — {len(self.seen_ids)} existing | {len(self.empty_attr_ids)} missing attrs")

    def open(self):
        is_new = not Path(self.filepath).exists()
        self._file = open(self.filepath, "a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._file, fieldnames=self.FIELDS)
        if is_new:
            self._writer.writeheader()

    def write(self, review: dict) -> bool:
        """Append a new review row. Returns True if written."""
        rid = review.get("review_id") or (
            review.get("reviewer_name", "") + "_" + review.get("date", "")
        )
        if not rid:
            return False
        has_attrs = bool(review.get("attributes", "").strip())
        if rid in self.seen_ids and rid not in self.empty_attr_ids:
            return False
        if rid in self.seen_ids and rid in self.empty_attr_ids and not has_attrs:
            return False

        if rid not in self.id_to_index:
            self.id_to_index[rid] = len(self.seen_ids)
        self.seen_ids.add(rid)
        row = {f: review.get(f, "") for f in self.FIELDS}
        self._writer.writerow(row)
        self._file.flush()
        if has_attrs:
            self.empty_attr_ids.discard(rid)
        else:
            self.empty_attr_ids.add(rid)
        return True

    def patch_attributes(self, review_id: str, attributes: str) -> bool:
        """
        FIX: In-place patch of a CSV row's attributes column.
        Rewrites the entire CSV — acceptable because this runs in Phase 2
        after all network writes are done, not during the hot write loop.
        Returns True if the row was found and patched.
        """
        if not attributes or review_id not in self.empty_attr_ids:
            return False

        self._file.close()
        self._file = None

        path = Path(self.filepath)
        tmp_path = path.with_suffix(".tmp")

        patched = False
        with open(path, newline="", encoding="utf-8") as fin, \
             open(tmp_path, "w", newline="", encoding="utf-8") as fout:
            reader = csv.DictReader(fin)
            writer = csv.DictWriter(fout, fieldnames=self.FIELDS)
            writer.writeheader()
            for row in reader:
                rid = row.get("review_id") or (
                    row.get("reviewer_name", "") + "_" + row.get("date", "")
                )
                if rid == review_id and not row.get("attributes", "").strip():
                    row["attributes"] = attributes
                    patched = True
                writer.writerow(row)

        tmp_path.replace(path)

        # Re-open for appending
        self._file = open(path, "a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._file, fieldnames=self.FIELDS)

        if patched:
            self.empty_attr_ids.discard(review_id)
        return patched

    def close(self):
        if self._file: self._file.close()

    @property
    def total_seen(self): return len(self.seen_ids)


# ─────────────────────────────────────────────────────────────────────────────
# NETWORK INTERCEPTOR
# ─────────────────────────────────────────────────────────────────────────────

class NetworkInterceptor:
    RPC_PATTERNS = [
        "listentitiesreviews",
        "maps/api/js/reviews",
        "GetReview",
        "preview/review",
    ]

    def __init__(self):
        self.captured_url: Optional[str] = None
        self.captured_headers: dict = {}
        self._lock = asyncio.Lock()

    def matches(self, url: str) -> bool:
        return any(p in url for p in self.RPC_PATTERNS)

    async def on_request(self, request: Request):
        if self.matches(request.url):
            async with self._lock:
                if not self.captured_url:
                    self.captured_url = request.url
                    self.captured_headers = dict(request.headers)
                    log.info(f"[NET] INTERCEPTED: {request.url[:150]}")

    def is_ready(self) -> bool:
        return bool(self.captured_url)

    def build_next_page_url(self, pagination_token: str) -> Optional[str]:
        if not self.captured_url:
            return None
        return re.sub(r'(pb=)[^&]*', r'\g<1>' + pagination_token, self.captured_url)


class NetworkReviewFetcher:
    XSS_PREFIX = ")]}'"

    def __init__(self, interceptor: NetworkInterceptor, session_cookies: str = ""):
        self.interceptor = interceptor
        self.session_cookies = session_cookies

    @staticmethod
    def _strip_xss(text: str) -> str:
        text = text.strip()
        if text.startswith(NetworkReviewFetcher.XSS_PREFIX):
            text = text[len(NetworkReviewFetcher.XSS_PREFIX):]
        return text.strip()

    @staticmethod
    def _deep_find_reviews(data, depth=0) -> list:
        if depth > 10 or not isinstance(data, (list, dict)):
            return []
        results = []
        if isinstance(data, list):
            if 15 < len(data) < 100:
                review = NetworkReviewFetcher._try_parse_review_array(data)
                if review:
                    results.append(review)
                    return results
            for item in data:
                results.extend(NetworkReviewFetcher._deep_find_reviews(item, depth + 1))
        return results

    @staticmethod
    def _try_parse_review_array(arr: list) -> Optional[dict]:
        try:
            strings, numbers = [], []

            def flatten(x):
                if isinstance(x, str) and len(x) > 0:
                    strings.append(x)
                elif isinstance(x, (int, float)):
                    numbers.append(x)
                elif isinstance(x, list):
                    for item in x: flatten(item)

            flatten(arr)
            if not strings or not numbers:
                return None

            ratings = [n for n in numbers if 1 <= n <= 5]
            if not ratings:
                return None

            review_id = reviewer_name = review_text = date_str = None
            for s in strings:
                if len(s) > 20 and re.match(r'^[A-Za-z0-9_\-]+$', s) and not review_id:
                    review_id = s
                elif len(s) > 3 and not reviewer_name and re.match(
                        r'^[A-Za-z\s\u00C0-\u024F\u0900-\u097F]+$', s):
                    reviewer_name = s
                elif len(s) > 15 and not review_text:
                    review_text = s

            for s in strings:
                if re.search(r'\d+\s+(day|week|month|year)s?\s+ago|just now|yesterday', s, re.I):
                    date_str = s
                    break

            if not review_id:
                return None

            return {
                "review_id":    review_id,
                "reviewer_name": reviewer_name or "",
                "reviewer_id":  "",
                "local_guide":  False,
                "rating":       ratings[0],
                "review_text":  review_text or "",
                "likes":        0,
                "date":         date_str or "",
                "attributes":   "",
                "source":       "network",
            }
        except Exception:
            return None

    async def fetch_page(self, page: Page, url: str) -> tuple[list, Optional[str]]:
        """Fetch one page of reviews. Retries up to NETWORK_MAX_RETRIES times."""
        for attempt in range(CFG["NETWORK_MAX_RETRIES"]):
            text = await self._fetch_raw(page, url)
            if text:
                break
            wait = 2 ** attempt
            log.warning(f"[NET] Retry {attempt+1}/{CFG['NETWORK_MAX_RETRIES']} in {wait}s")
            await asyncio.sleep(wait)
        else:
            return [], None

        text = self._strip_xss(text)
        if not text:
            return [], None

        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            log.debug(f"[NET] JSON parse failed: {text[:200]}")
            return [], None

        reviews = self._deep_find_reviews(data)

        next_token = None
        if isinstance(data, list) and len(data) > 0:
            last = data[-1]
            if isinstance(last, str) and len(last) > 10:
                next_token = last

        log.info(f"[NET] Fetched {len(reviews)} reviews")
        return reviews, next_token

    async def _fetch_raw(self, page: Page, url: str) -> Optional[str]:
        headers = dict(self.interceptor.captured_headers)
        if self.session_cookies:
            headers["cookie"] = self.session_cookies
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    url, headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as resp:
                    if resp.status != 200:
                        log.warning(f"[NET] HTTP {resp.status}")
                        return None
                    return await resp.text()
        except ImportError:
            try:
                result = await page.evaluate("""
                async (url) => {
                    const r = await fetch(url, {credentials: 'include'});
                    return {status: r.status, text: await r.text()};
                }
                """, url)
                if result["status"] != 200:
                    return None
                return result["text"]
            except Exception as e:
                log.warning(f"[NET] page.evaluate fetch failed: {e}")
                return None
        except Exception as e:
            log.warning(f"[NET] aiohttp fetch failed: {e}")
            return None


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER SESSION
# ─────────────────────────────────────────────────────────────────────────────

class BrowserSession:
    def __init__(self, proxy=None, session_dir=None, worker_id=0):
        self.proxy       = proxy
        self.worker_id   = worker_id
        self.session_dir = session_dir or CFG["SESSION_DIR"]
        os.makedirs(self.session_dir, exist_ok=True)
        self._playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

    async def start(self):
        self._playwright = await async_playwright().start()
        kwargs = dict(
            user_data_dir=self.session_dir,
            headless=CFG["HEADLESS"],
            viewport={"width": CFG["VIEWPORT"][0], "height": CFG["VIEWPORT"][1]},
            user_agent=CFG["USER_AGENT"],
            locale=CFG["LOCALE"],
            timezone_id=CFG["TIMEZONE"],
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--disable-web-security",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--ozone-platform=x11",
                "--disable-gpu",
                "--disable-software-rasterizer",
            ],
            ignore_default_args=["--enable-automation"],
        )
        if self.proxy:
            kwargs["proxy"] = self.proxy

        self.context = await self._playwright.chromium.launch_persistent_context(**kwargs)

        await self.context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
            Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
            window.chrome = {runtime: {}};
            const orig = navigator.permissions.query;
            navigator.permissions.query = (p) =>
                p.name === 'notifications'
                    ? Promise.resolve({state: Notification.permission})
                    : orig(p);
        """)

        if HAS_STEALTH and apply_stealth_context:
            try: await apply_stealth_context(self.context)
            except Exception as e: log.warning(f"Context stealth failed: {e}")

        pages = self.context.pages
        self.page = pages[0] if pages else await self.context.new_page()

        if HAS_STEALTH and apply_stealth:
            try: await apply_stealth(self.page)
            except Exception as e: log.warning(f"Page stealth failed: {e}")

        log.info("Browser session started")

    async def stop(self):
        if self.context: await self.context.close()
        if self._playwright: await self._playwright.stop()
        log.info("Browser session closed")

    async def get_cookies_str(self) -> str:
        try:
            cookies = await self.context.cookies()
            return "; ".join(f"{c['name']}={c['value']}" for c in cookies)
        except Exception:
            return ""


# ─────────────────────────────────────────────────────────────────────────────
# REVIEW EXTRACTOR  (DOM mode)
# ─────────────────────────────────────────────────────────────────────────────

# FIX B: The JS extractor now detects truncated attribute chips ("Dine in…")
# and sets is_truncated=True for those cards, not just for truncated review text.
# This feeds the existing 3-attempt retry correctly.

_EXTRACT_JS = r"""
(args) => {
    const seenSet    = new Set(args.seenIds);
    const emptySet   = new Set(args.emptyAttrIds);
    const targetIds  = args.targetIds ? new Set(args.targetIds) : null;
    const results    = [];

    const allEls = document.querySelectorAll('[data-review-id]');
    const blocks = [];
    allEls.forEach(el => {
        const rid = el.getAttribute('data-review-id') || '';
        // If we have a target set, only extract those cards
        if (targetIds && !targetIds.has(rid)) return;
        // Skip if already fully written (has attrs)
        if (rid && seenSet.has(rid) && !emptySet.has(rid)) return;
        const parent = el.parentElement
            ? el.parentElement.closest('[data-review-id]')
            : null;
        if (!parent) blocks.push(el);
    });

    if (blocks.length === 0 && !targetIds) {
        document.querySelectorAll('div.jftiEf').forEach(el => blocks.push(el));
    }

    blocks.forEach(block => {
        try {
            const rid = block.getAttribute('data-review-id') || '';

            const nameEl = block.querySelector('.d4r55, [class*="reviewer-name"]');
            const name   = nameEl ? nameEl.innerText.trim() : '';
            if (!rid && !name) return;

            let reviewer_id = '';
            const profileBtn = block.querySelector('button[data-href*="/maps/contrib/"]');
            if (profileBtn) {
                const m = (profileBtn.getAttribute('data-href') || '').match(/contrib[/](\d+)/);
                if (m) reviewer_id = m[1];
            }

            const rfnDt = block.querySelector('.RfnDt');
            const local_guide = rfnDt
                ? rfnDt.innerText.trim().startsWith('Local Guide')
                : false;

            const ratingEl = block.querySelector('span[aria-label*="star"], [role="img"][aria-label*="star"]');
            const ratingM  = (ratingEl ? ratingEl.getAttribute('aria-label') : '').match(/(\d+(\.\d+)?)/);
            const rating   = ratingM ? parseFloat(ratingM[1]) : null;

            const replyContainer = block.querySelector('.CDe7pd, .D1wvnb, div[jsname="BnxFpd"]');

            let review_text = '';
            const TEXT_SELS = ['span[jsname="bN97Pc"]', '.wiI7pd', '.MyEned span'];
            for (const sel of TEXT_SELS) {
                for (const el of block.querySelectorAll(sel)) {
                    if (replyContainer && replyContainer.contains(el)) continue;
                    const t = el.innerText.trim();
                    if (t) { review_text = t; break; }
                }
                if (review_text) break;
            }

            const dateEl = block.querySelector('.rsqaWe, .xRkPPb span');
            const date   = dateEl ? dateEl.innerText.trim() : '';

            const likesEl = block.querySelector('button[aria-label*="helpful"] span, .pkWtMe');
            let likes = 0;
            if (likesEl) {
                const lm = likesEl.innerText.trim().match(/(\d+)/);
                likes = lm ? parseInt(lm[1]) : 0;
            }

            // ── ATTRIBUTES ──────────────────────────────────────────────────
            const LABEL_MAP_A = {
                'service': 'dining_mode', 'meal type': 'meal_type',
                'price per person': 'price_per_person', 'wait time': 'wait_time',
                'cleanliness': 'cleanliness', 'noise level': 'noise_level',
                'parking space': 'parking_space', 'parking options': 'parking_options',
                'parking': 'parking_notes', 'wheelchair accessibility': 'wheelchair_accessibility',
                'group size': 'group_size', 'accessibility': 'accessibility',
                'kids menu': 'kids_menu', 'kid-friendliness': 'kid_friendliness',
                'children': 'children', 'reservations': 'reservations',
                'amenities': 'amenities', 'recommended dishes': 'recommended_dishes',
                'recommendation for vegetarians': 'vegetarian_recommendation',
                'vegetarian offerings': 'vegetarian_offerings',
                'vegetarian options': 'vegetarian_options',
                'getting there': 'getting_there', 'planning': 'planning',
                'service options': 'service_options', 'highlights': 'highlights',
                'popular for': 'popular_for', 'offerings': 'offerings',
                'dining options': 'dining_options', 'crowd': 'crowd',
                'payments': 'payments', 'dine in': 'dining_mode',
                'dine-in': 'dining_mode', 'seating type': 'seating_type',
                'dietary restrictions': 'dietary_restrictions',
                'special events': 'special_events', 'kid-friendly': 'kid_friendliness',
                'noise': 'noise_level', 'restroom': 'restroom',
                'dogs allowed': 'dogs_allowed', 'good for groups': 'good_for_groups',
                'good for watching sports': 'good_for_sports',
                'outdoor seating': 'outdoor_seating', 'live music': 'live_music',
                'gender-neutral restrooms': 'gender_neutral_restrooms',
                'accepts credit cards': 'accepts_credit_cards',
                'delivery': 'delivery', 'takeaway': 'takeaway',
            };
            const LABEL_MAP_B = {
                'food': 'food', 'service': 'service_rating', 'atmosphere': 'atmosphere',
            };

            const attrs = {};
            // FIX B: track whether any chip value is still truncated
            let any_chip_truncated = false;

            block.querySelectorAll('div[jslog^="126926"]').forEach(pbk => {
                if (replyContainer && replyContainer.contains(pbk)) return;

                const boldEl = pbk.querySelector('b');
                if (boldEl) {
                    const rawLabel = boldEl.innerText.trim().replace(/:$/, '').toLowerCase();
                    let val = '';
                    boldEl.parentElement.childNodes.forEach(node => {
                        if (node === boldEl) return;
                        val += node.nodeType === Node.TEXT_NODE
                            ? node.textContent
                            : (node.innerText || node.textContent || '');
                    });
                    val = val.replace(/^[\s:]+/, '').trim();

                    // FIX B: chip truncation detection
                    if (val.endsWith('…') || val.endsWith('...')) {
                        any_chip_truncated = true;
                    }

                    if (rawLabel && val && LABEL_MAP_B.hasOwnProperty(rawLabel)) {
                        const key = LABEL_MAP_B[rawLabel];
                        const num = parseFloat(val);
                        if (!attrs[key]) attrs[key] = isNaN(num) ? val : num;
                    }
                } else {
                    const divs = pbk.querySelectorAll(':scope > div');
                    if (divs.length < 2) return;

                    const boldSpan = divs[0].querySelector('span[style*="font-weight"]');
                    if (!boldSpan) return;
                    const rawLabel = boldSpan.innerText.trim().toLowerCase();

                    const valueSpan = divs[1].querySelector('span.RfDO5c > span:first-child');
                    if (!valueSpan) return;
                    const val = valueSpan.innerText.trim();

                    // FIX B: chip truncation detection
                    if (val.endsWith('…') || val.endsWith('...')) {
                        any_chip_truncated = true;
                    }

                    if (rawLabel && val && LABEL_MAP_A.hasOwnProperty(rawLabel)) {
                        const key = LABEL_MAP_A[rawLabel];
                        if (!attrs[key]) attrs[key] = val;
                    }
                }
            });

            const attributes = Object.keys(attrs).length > 0 ? JSON.stringify(attrs) : '';

            // Truncation: check review text button AND chip truncation
            const truncated_btn = block.querySelector('button.w8nwRe:not([aria-expanded="true"])');
            const is_truncated  = (
                truncated_btn !== null ||
                review_text.endsWith('…') ||
                review_text.endsWith('...') ||
                any_chip_truncated  // FIX B: this was the missing check
            );

            results.push({
                review_id:     rid || (name + '_' + date),
                reviewer_name: name,
                reviewer_id:   reviewer_id,
                local_guide:   local_guide,
                rating:        rating,
                review_text:   review_text.replace(/[\r\n]+/g, ' ').trim(),
                likes:         likes,
                date:          date,
                attributes:    attributes,
                is_truncated:  is_truncated,
            });

        } catch(e) {}
    });

    return results;
}
"""

_EXPAND_JS = """
(args) => {
    const alreadyClicked = new Set(args.alreadyClicked);
    const targetIds      = args.targetIds ? new Set(args.targetIds) : null;
    const clicked = [];

    const candidates = document.querySelectorAll(
        'button.w8nwRe, button[aria-label*="See more"], button[aria-label*="see more"]'
    );
    const BLOCK_WORDS = ['photo','flag','report','helpful','share','translate','like',
                         'contributor','profile'];

    candidates.forEach(btn => {
        if (btn.getAttribute('aria-expanded') === 'true') return;
        if (!btn.offsetParent) return;

        const lbl = (btn.getAttribute('aria-label') || '').toLowerCase();
        for (const w of BLOCK_WORDS) { if (lbl.includes(w)) return; }

        let el = btn.parentElement;
        let card_id = null;
        for (let i = 0; i < 20 && el; i++) {
            card_id = el.getAttribute('data-review-id');
            if (card_id) break;
            el = el.parentElement;
        }
        if (!card_id) return;
        if (targetIds && !targetIds.has(card_id)) return;

        const jsaction = btn.getAttribute('jsaction') || btn.className || 'btn';
        const key = card_id + '::' + jsaction;
        if (alreadyClicked.has(key)) return;

        try { btn.click(); clicked.push(key); } catch(e) {}
    });

    return clicked;
}
"""


class ReviewExtractor:
    def __init__(self):
        self._expanded_buttons: set = set()

    async def expand_truncated(self, page: Page,
                                reviews_scraped: int = 0,
                                target_ids: list = None) -> int:
        """
        Click all unexpanded 'More' buttons.
        FIX A: wait time scales with reviews_scraped depth.
        target_ids: if set, only expand buttons inside those specific cards.
        """
        total_clicked = 0
        for _pass in range(4):
            already = list(self._expanded_buttons)
            clicked_keys = await page.evaluate(_EXPAND_JS, {
                "alreadyClicked": already,
                "targetIds": target_ids,
            })
            if not clicked_keys:
                break
            for k in clicked_keys:
                self._expanded_buttons.add(k)
            total_clicked += len(clicked_keys)

            # FIX A: dynamic wait — deeper = slower Maps renderer = longer wait
            wait = expansion_wait(reviews_scraped) if _pass == 0 else 0.3
            await asyncio.sleep(wait)

        return total_clicked

    @staticmethod
    async def get_place_name(page: Page) -> str:
        try:
            name = await page.evaluate(r"""
            () => {
                const h1 = document.querySelector('h1.DUwDvf, h1[class*="fontHeadlineLarge"]');
                if (h1) return h1.innerText.trim();
                return document.title.replace(/\s*[-\u2013].*$/, '').trim();
            }
            """)
            return name or "Unknown Place"
        except Exception:
            return "Unknown Place"

    @staticmethod
    async def get_total_review_count(page: Page) -> int:
        try:
            count = await page.evaluate(r"""
            () => {
                const btns = document.querySelectorAll('button[jsaction*="reviews"], [aria-label*="reviews"]');
                for (const btn of btns) {
                    const lbl = (btn.getAttribute('aria-label') || '').replace(/,/g, '');
                    const m = lbl.match(/(\d+)\s+reviews?/i);
                    if (m) return parseInt(m[1]);
                }
                const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
                let node;
                while ((node = walker.nextNode())) {
                    const m = node.textContent.replace(/,/g,'').match(/(\d{2,})\s+reviews?/i);
                    if (m) return parseInt(m[1]);
                }
                return 0;
            }
            """)
            return int(count) if count else 0
        except Exception:
            return 0

    async def extract_visible(self, page: Page, seen_ids: set, place_name: str,
                               empty_attr_ids: set = None,
                               target_ids: list = None) -> list:
        empty_attr_ids = empty_attr_ids or set()
        raw = await page.evaluate(_EXTRACT_JS, {
            "seenIds": list(seen_ids),
            "emptyAttrIds": list(empty_attr_ids),
            "targetIds": target_ids,
        })
        new_reviews = []
        for r in raw:
            rid = r["review_id"]
            if rid not in seen_ids:
                r["place_name"] = place_name
                new_reviews.append(r)
            elif rid in empty_attr_ids and r.get("attributes", "").strip():
                r["place_name"] = place_name
                new_reviews.append(r)
        return new_reviews


# ─────────────────────────────────────────────────────────────────────────────
# SCROLL ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class ScrollEngine:
    def __init__(self, page: Page):
        self.page          = page
        self.scroll_count  = 0
        self.plateau_count = 0
        self.next_idle_at  = random.randint(*CFG["IDLE_PAUSE_EVERY"])
        self.next_rev_at   = random.randint(*CFG["REVERSE_SCROLL_EVERY"])

    async def find_scroll_container(self):
        for sel in SELECTORS["scroll_container"]:
            try:
                el = await self.page.query_selector(sel)
                if el:
                    log.debug(f"Scroll container: {sel}")
                    return el
            except Exception:
                continue
        log.warning("Scroll container not found — using window scroll")
        return None

    async def scroll_once(self, container) -> int:
        lo, hi = CFG["SCROLL_DISTANCE"]
        dist   = random.randint(lo, hi)
        beh    = "instant" if CFG.get("SCROLL_JUMP") else "smooth"
        if container:
            await self.page.evaluate(
                "([el,d,b]) => el.scrollBy({top:d,behavior:b})", [container, dist, beh]
            )
        else:
            await self.page.evaluate(f"window.scrollBy({{top:{dist},behavior:'{beh}'}})")
        self.scroll_count += 1
        return dist

    async def jump_to_index(self, container, card_index: int):
        """
        Jump the scroll container to approximately the position of card_index.
        Used in the attribute fill pass to avoid scrolling through all 52k reviews.
        """
        approx_px = card_index * CFG["APPROX_CARD_HEIGHT_PX"]
        if container:
            await self.page.evaluate(
                "([el, px]) => { el.scrollTop = px; }",
                [container, max(0, approx_px - 400)]  # slight overshoot-back
            )
        await asyncio.sleep(0.5)

    async def reverse_scroll(self, container):
        dist = random.randint(80, 220)
        beh  = "instant" if CFG.get("SCROLL_JUMP") else "smooth"
        if container:
            await self.page.evaluate(
                "([el,d,b]) => el.scrollBy({top:-d,behavior:b})", [container, dist, beh]
            )
        await asyncio.sleep(random.uniform(0.1, 0.3))
        fwd = dist + random.randint(150, 350)
        if container:
            await self.page.evaluate(
                "([el,d,b]) => el.scrollBy({top:d,behavior:b})", [container, fwd, beh]
            )

    async def wait_for_new_content(self, prev_count: int, timeout: float = None) -> int:
        timeout = timeout or CFG["CONTENT_WAIT_TIMEOUT"]
        poll    = CFG["CONTENT_WAIT_POLL"]
        deadline = time.time() + timeout
        while time.time() < deadline:
            count = await self.page.evaluate(
                "() => document.querySelectorAll('div[data-review-id],div.jftiEf').length"
            )
            if count > prev_count:
                return count - prev_count
            await asyncio.sleep(poll)
        return 0

    async def wait_for_card(self, rid: str, timeout: float = None) -> bool:
        """Wait until a specific review card (by review_id) appears in DOM."""
        timeout = timeout or CFG["ATTR_FILL_CARD_WAIT"]
        deadline = time.time() + timeout
        while time.time() < deadline:
            found = await self.page.evaluate(
                "(rid) => !!document.querySelector(`[data-review-id='${rid}']`)",
                rid
            )
            if found:
                return True
            await asyncio.sleep(0.15)
        return False

    async def maybe_idle_pause(self):
        if self.scroll_count >= self.next_idle_at:
            pause = rand_delay(CFG["DELAY_IDLE_PAUSE"])
            log.info(f"  [idle pause] {pause:.1f}s")
            await micro_mouse_move(self.page)
            await asyncio.sleep(pause)
            self.next_idle_at = self.scroll_count + random.randint(*CFG["IDLE_PAUSE_EVERY"])

    async def maybe_reverse_scroll(self, container):
        if self.scroll_count >= self.next_rev_at:
            await self.reverse_scroll(container)
            self.next_rev_at = self.scroll_count + random.randint(*CFG["REVERSE_SCROLL_EVERY"])


# ─────────────────────────────────────────────────────────────────────────────
# ANTI-BLOCK MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class AntiBlockManager:
    def __init__(self, page: Page):
        self.page            = page
        self.slow_mode       = False
        self.slow_mode_count = 0

    async def check(self) -> str:
        if await detect_captcha(self.page): return "captcha"
        url = self.page.url
        if any(p in url for p in ["sorry", "challenge", "consent"]): return "captcha"
        count = await self.page.evaluate(
            "() => document.querySelectorAll('div[data-review-id],div.jftiEf').length"
        )
        if count == 0 and self.slow_mode_count > 3: return "slow"
        return "ok"

    async def handle_captcha(self):
        log.warning(f"⚠️  CAPTCHA — pausing {CFG['CAPTCHA_PAUSE']}s for manual solve")
        await asyncio.sleep(CFG["CAPTCHA_PAUSE"])
        if await detect_captcha(self.page):
            raise RuntimeError("CAPTCHA unsolved after pause")

    async def apply_slow_mode(self):
        self.slow_mode_count += 1
        delay = rand_delay(CFG["SLOW_MODE_DELAY"])
        log.warning(f"⚠️  Soft-ban signal — slow mode {delay:.1f}s (#{self.slow_mode_count})")
        await asyncio.sleep(delay)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN SCRAPER ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

class GoogleMapsReviewScraper:
    def __init__(self, url: str, output_csv: str,
                 proxy=None, session_dir=None, worker_id=0, mode="hybrid"):
        self.url       = url
        self.mode      = mode
        self.csv       = ReviewCSVWriter(output_csv) if output_csv else None
        self.session   = BrowserSession(proxy=proxy, session_dir=session_dir, worker_id=worker_id)
        self.worker_id = worker_id
        self._shutdown = False
        self._start_time = None
        self._truncation_attempts: dict = {}

    def _register_signals(self):
        def _h(sig, frame):
            log.info("🛑  Shutdown signal — finishing batch...")
            self._shutdown = True
        signal.signal(signal.SIGINT, _h)
        signal.signal(signal.SIGTERM, _h)

    def _runtime_exceeded(self) -> bool:
        return (time.time() - self._start_time) > CFG["MAX_RUNTIME_SECONDS"]

    @staticmethod
    async def _dismiss_consent(page: Page):
        for sel in [
            'button[aria-label*="Accept all"]', 'button:has-text("Accept all")',
            'button:has-text("I agree")', 'button:has-text("Agree")',
            'form[action*="consent"] button', '#L2AGLb',
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
                if "about:blank" in page.url:
                    raise PWTimeout("Blank page loaded")
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

    def _is_valid_url(self, url: str) -> bool:
        for bad in ["/contrib/", "/search", "consent.google", "/sorry", "challenge"]:
            if bad in url: return False
        return "/maps/place/" in url

    async def _recover(self, page: Page) -> bool:
        log.warning(f"  URL drift → {page.url[:80]} — recovering...")
        try:
            await page.goto(self.url, wait_until="domcontentloaded", timeout=25000)
            await asyncio.sleep(rand_delay((2.0, 3.5)))
            await self._open_reviews_tab(page)
            return True
        except Exception as e:
            log.error(f"  Recovery failed: {e}")
            return False

    # ── PHASE 1: NETWORK LOOP ─────────────────────────────────────────────────

    async def _network_loop(self, page: Page) -> int:
        """
        Paginate Google Maps' internal RPC to get all reviews fast.
        Returns number of reviews written.
        Attributes will be empty — filled by Phase 2.
        """
        interceptor = NetworkInterceptor()
        page.on("request", interceptor.on_request)
        page.on("response", lambda r: None)  # keep listener count consistent

        extractor  = ReviewExtractor()
        place_name = await extractor.get_place_name(page)
        log.info(f"[NET] Place: {place_name}")
        log.info("[NET] Waiting for RPC endpoint interception...")

        await self._open_reviews_tab(page)

        # Wait up to 30s for interception
        wait_start = time.time()
        while not interceptor.is_ready() and (time.time() - wait_start) < 30:
            await asyncio.sleep(0.5)

        if not interceptor.is_ready():
            log.warning("[NET] RPC not intercepted after 30s — falling back to DOM mode")
            log.warning("[NET] Possible causes: Maps changed RPC pattern, or ad blocker interference")
            return 0  # caller will fall back to DOM

        log.info(f"[NET] Endpoint: {interceptor.captured_url[:120]}...")

        cookies = await self.session.get_cookies_str()
        fetcher = NetworkReviewFetcher(interceptor, cookies)

        total_written = 0
        current_url   = interceptor.captured_url
        page_num      = 0
        no_new_pages  = 0

        while not self._shutdown and current_url:
            if self._runtime_exceeded():
                log.warning("⏱️  Runtime exceeded")
                break
            if self.csv.total_seen >= CFG["MAX_REVIEWS"]:
                log.info("✅  Target reached")
                break

            page_num += 1
            reviews, next_token = await fetcher.fetch_page(page, current_url)

            if not reviews:
                no_new_pages += 1
                if no_new_pages >= 3:
                    log.info("[NET] No reviews in 3 consecutive pages — done")
                    break
                await asyncio.sleep(rand_delay(CFG["NETWORK_PAGINATION_DELAY"]))
                continue
            no_new_pages = 0

            written = 0
            for r in reviews:
                r["place_name"] = place_name
                if self.csv.write(r):
                    written += 1
                    total_written += 1

            log.info(
                f"[NET] Page {page_num:4d} | +{written:3d} | "
                f"total={total_written} | csv={self.csv.total_seen}"
            )

            if next_token:
                current_url = interceptor.build_next_page_url(next_token)
            else:
                log.info("[NET] No next pagination token — done")
                break

            await asyncio.sleep(rand_delay(CFG["NETWORK_PAGINATION_DELAY"]))

        log.info(f"[NET] Phase 1 done. Written: {total_written} | Empty attrs: {len(self.csv.empty_attr_ids)}")
        return total_written

    # ── PHASE 2: TARGETED ATTRIBUTE FILL ─────────────────────────────────────

    async def _attr_fill_loop(self, page: Page):
        """
        Phase 2 of hybrid mode: fill missing attributes for reviews written
        in Phase 1. Instead of scrolling through all reviews, we:
          1. Sort empty_attr_ids by their write index (stream order)
          2. Jump-scroll to each card's approximate position
          3. Scan ±20 cards around that position for the card
          4. Expand it, extract only its chips, patch the CSV row

        This is O(missing_attrs) not O(total_reviews).
        """
        if not self.csv.empty_attr_ids:
            log.info("[FILL] No empty attrs — skipping Phase 2")
            return

        total_missing = len(self.csv.empty_attr_ids)
        log.info(f"[FILL] Phase 2: filling attributes for {total_missing} reviews")

        # Sort by index so we scroll forward through the list (cache-friendly)
        sorted_ids = sorted(
            self.csv.empty_attr_ids,
            key=lambda rid: self.csv.id_to_index.get(rid, 999999)
        )

        extractor = ReviewExtractor()
        scroller  = ScrollEngine(page)
        container = await scroller.find_scroll_container()

        filled  = 0
        skipped = 0

        for i, rid in enumerate(sorted_ids):
            if self._shutdown or self._runtime_exceeded():
                break

            card_index = self.csv.id_to_index.get(rid, i)
            log.info(
                f"[FILL] {i+1}/{total_missing} | rid={rid[:20]}... | "
                f"est_index={card_index}"
            )

            # Jump to approximate position
            await scroller.jump_to_index(container, card_index)

            # Scroll a bit forward/backward to load the card into DOM
            found = await scroller.wait_for_card(rid, timeout=2.0)
            if not found:
                # Try scrolling a few times to load it
                for _ in range(CFG["ATTR_FILL_MAX_SCROLL_ATTEMPTS"]):
                    await scroller.scroll_once(container)
                    await asyncio.sleep(0.3)
                    found = await scroller.wait_for_card(rid, timeout=1.0)
                    if found:
                        break

            if not found:
                log.warning(f"[FILL] Card not found after scrolling: {rid[:20]}")
                skipped += 1
                continue

            # Expand this specific card (target_ids limits expansion to just this card)
            await extractor.expand_truncated(
                page,
                reviews_scraped=card_index,
                target_ids=[rid]
            )

            # Small wait for chip hydration — use dynamic wait based on depth
            await asyncio.sleep(expansion_wait(card_index))

            # Extract only this card
            reviews = await extractor.extract_visible(
                page,
                seen_ids=self.csv.seen_ids,
                place_name="",  # not needed for patch
                empty_attr_ids=self.csv.empty_attr_ids,
                target_ids=[rid],
            )

            patched = False
            for r in reviews:
                if r.get("review_id") == rid and r.get("attributes", "").strip():
                    if r.get("is_truncated"):
                        # Chip still truncated after expansion — skip, will retry next run
                        log.debug(f"[FILL] Still truncated: {rid[:20]}")
                        break
                    success = self.csv.patch_attributes(rid, r["attributes"])
                    if success:
                        filled += 1
                        patched = True
                        log.debug(f"[FILL] Patched: {rid[:20]} → {r['attributes'][:60]}")
                    break

            if not patched:
                log.debug(f"[FILL] No attrs extracted for {rid[:20]}")

            # Brief pause between cards to avoid hammering the renderer
            await asyncio.sleep(random.uniform(0.2, 0.5))

        remaining = len(self.csv.empty_attr_ids)
        log.info(
            f"[FILL] Phase 2 done. "
            f"Filled: {filled}/{total_missing} | "
            f"Skipped: {skipped} | "
            f"Still missing: {remaining}"
        )

    # ── DOM MODE LOOP (standalone, with all fixes) ────────────────────────────

    async def _dom_loop(self, page: Page):
        scroller   = ScrollEngine(page)
        extractor  = ReviewExtractor()
        antiblock  = AntiBlockManager(page)

        container       = await scroller.find_scroll_container()
        total_written   = 0
        scroll_cycle    = 0
        drift_count     = 0
        MAX_DRIFTS      = 5

        place_name       = await extractor.get_place_name(page)
        total_on_listing = await extractor.get_total_review_count(page)
        log.info(f"Place: {place_name}  |  Total on listing: {total_on_listing or '?'}")

        expanded = await extractor.expand_truncated(page, reviews_scraped=0)
        log.info(f"Pre-expanded {expanded} buttons. Starting scroll loop...")
        await asyncio.sleep(2.0)

        while not self._shutdown:
            if self._runtime_exceeded():
                log.warning("⏱️  Max runtime reached")
                break
            if self.csv.total_seen >= CFG["MAX_REVIEWS"]:
                log.info(f"✅  Target {CFG['MAX_REVIEWS']} reached")
                break

            if total_on_listing > 0:
                gap = total_on_listing - self.csv.total_seen
                if gap <= max(10, int(total_on_listing * 0.01)) and self.csv.total_seen > 0:
                    log.info(f"✅  Collected {self.csv.total_seen}/{total_on_listing} (within {gap}). Done.")
                    break

            if scroller.plateau_count >= CFG["MAX_SCROLL_PLATEAU"]:
                log.info(f"📊  Plateau reached. Collected {self.csv.total_seen}. Stopping.")
                break

            if not self._is_valid_url(page.url):
                drift_count += 1
                if drift_count > MAX_DRIFTS:
                    log.error("Too many drifts — aborting")
                    break
                if not await self._recover(page):
                    break
                container = await scroller.find_scroll_container()
                scroller.plateau_count = 0
                continue

            status = await antiblock.check()
            if status == "captcha":
                await antiblock.handle_captcha()
                container = await scroller.find_scroll_container()
            elif status == "slow":
                await antiblock.apply_slow_mode()

            prev_count = await page.evaluate(
                "() => document.querySelectorAll('div[data-review-id],div.jftiEf').length"
            )
            for _ in range(CFG["SCROLL_BATCH_SIZE"]):
                if not self._is_valid_url(page.url):
                    log.warning("Mid-batch URL drift")
                    break
                await scroller.scroll_once(container)
                await human_sleep(CFG["DELAY_BETWEEN_SCROLLS"])
                await scroller.maybe_reverse_scroll(container)
                await scroller.maybe_idle_pause()

            alive = await micro_mouse_move(page)
            if not alive:
                log.error("Browser closed — exiting")
                self._shutdown = True
                break

            scroll_cycle += 1

            new_nodes = await scroller.wait_for_new_content(prev_count)
            scroller.plateau_count = 0 if new_nodes > 0 else scroller.plateau_count + 1

            # FIX A: expand with dynamic wait based on depth
            await extractor.expand_truncated(page, reviews_scraped=total_written)

            new_reviews = await extractor.extract_visible(
                page, self.csv.seen_ids, place_name, self.csv.empty_attr_ids
            )

            written_this_batch = 0
            for review in new_reviews:
                if review.get("is_truncated"):
                    rid = review.get("review_id", "?")
                    attempts = self._truncation_attempts.get(rid, 0) + 1
                    self._truncation_attempts[rid] = attempts
                    if attempts < 3:
                        log.debug(f"Truncated (chip or text) {rid[:20]} — attempt {attempts}/3")
                        continue
                    log.debug(f"Truncated {rid[:20]} — gave up after 3, writing as-is")
                if self.csv.write(review):
                    written_this_batch += 1
                    total_written += 1

            # ── DOM PRUNING ────────────────────────────────────────────────────
            keep_tail    = CFG["DOM_PRUNE_KEEP_TAIL"]
            clean_every  = CFG["DOM_DEEP_CLEAN_EVERY"]
            is_deep_clean = (total_written > 0 and total_written % clean_every < written_this_batch)

            if is_deep_clean:
                log.info(f"  [deep clean] {total_written} — purging DOM")
                # FIX: preserve empty_attr_ids cards so retry loop can still reach them
                empty_list = list(self.csv.empty_attr_ids)
                await page.evaluate("""
                ([keepTail, emptyArr]) => {
                    const emptySet = new Set(emptyArr);
                    const all = Array.from(document.querySelectorAll('[data-review-id]'));
                    const anchors = new Set(all.slice(-keepTail).map(e => e.getAttribute('data-review-id')));
                    all.forEach(el => {
                        const rid = el.getAttribute('data-review-id');
                        if (!rid) return;
                        // FIX: never remove cards still needing attribute fill
                        if (emptySet.has(rid)) return;
                        if (anchors.has(rid)) return;
                        const parent = el.parentElement ? el.parentElement.closest('[data-review-id]') : null;
                        if (!parent) el.remove();
                    });
                }
                """, [keep_tail, empty_list])
                await asyncio.sleep(2.5)
                container = await scroller.find_scroll_container()
                log.info(f"  [deep clean] done")
            else:
                await page.evaluate("""
                ([seenArr, emptyArr, keepTail]) => {
                    const seenSet  = new Set(seenArr);
                    const emptySet = new Set(emptyArr);
                    const all = Array.from(document.querySelectorAll('[data-review-id]'));
                    const anchors = new Set(all.slice(-keepTail).map(e => e.getAttribute('data-review-id')));
                    all.forEach(el => {
                        const rid = el.getAttribute('data-review-id');
                        if (!rid || anchors.has(rid)) return;
                        if (emptySet.has(rid)) return;  // FIX: preserve retry targets
                        if (seenSet.has(rid)) {
                            const parent = el.parentElement ? el.parentElement.closest('[data-review-id]') : null;
                            if (!parent) el.remove();
                        }
                    });
                }
                """, [list(self.csv.seen_ids), list(self.csv.empty_attr_ids), keep_tail])

            if written_this_batch:
                pct = f"{total_written/total_on_listing*100:.1f}%" if total_on_listing else "?%"
                log.info(
                    f"  cycle={scroll_cycle:4d} | +{written_this_batch:3d} | "
                    f"total={total_written}/{total_on_listing or '?'} ({pct}) | "
                    f"scrolls={scroller.scroll_count} | "
                    f"empty_attrs={len(self.csv.empty_attr_ids)} | "
                    f"expand_wait={expansion_wait(total_written):.1f}s | "
                    f"runtime={int(time.time()-self._start_time)}s"
                )

        log.info(f"DOM loop done. This run: {total_written} | CSV total: {self.csv.total_seen}")

    # ── PUBLIC ENTRY POINT ────────────────────────────────────────────────────

    async def run(self):
        self._register_signals()
        self._start_time = time.time()
        self.csv.open()

        try:
            await self.session.start()
            page = self.session.page
            page.on("crash", lambda: log.error("Page crashed"))

            await self._navigate(page)

            if self.mode == "network":
                log.info("Mode: NETWORK ONLY (no attribute fill)")
                written = await self._network_loop(page)
                if written == 0:
                    log.info("Network interception failed — falling back to DOM mode")
                    await self._open_reviews_tab(page)
                    await self._dom_loop(page)

            elif self.mode == "hybrid":
                log.info("Mode: HYBRID (Phase 1: network bulk → Phase 2: DOM attribute fill)")
                written = await self._network_loop(page)
                if written == 0:
                    log.warning("Network phase yielded nothing — running full DOM mode instead")
                    await self._open_reviews_tab(page)
                    await self._dom_loop(page)
                else:
                    if self.csv.empty_attr_ids and not self._shutdown:
                        log.info(f"Phase 2: {len(self.csv.empty_attr_ids)} reviews need attribute fill")
                        # Fresh page navigation for Phase 2 DOM pass
                        await self._navigate(page)
                        await self._open_reviews_tab(page)
                        await asyncio.sleep(2.0)
                        await self._attr_fill_loop(page)
                    else:
                        log.info("Phase 2: no missing attrs or shutdown — skipping")

            else:  # dom
                log.info("Mode: DOM ONLY (all fixes applied)")
                await self._open_reviews_tab(page)
                await self._dom_loop(page)

        except RuntimeError as e:
            log.error(f"Fatal: {e}")
        except Exception as e:
            err = str(e)
            if "TargetClosedError" in type(e).__name__ or "been closed" in err:
                log.warning(f"Browser closed unexpectedly. Saved: {self.csv.total_seen}")
            else:
                log.exception(f"Unexpected error: {e}")
        finally:
            self.csv.close()
            await self.session.stop()
            elapsed = int(time.time() - self._start_time)
            log.info(
                f"\n{'='*60}\n"
                f"  DONE\n"
                f"  Reviews total   : {self.csv.total_seen}\n"
                f"  Missing attrs   : {len(self.csv.empty_attr_ids)}\n"
                f"  Elapsed         : {elapsed}s ({elapsed//60}m {elapsed%60}s)\n"
                f"  Output          : {self.csv.filepath}\n"
                f"{'='*60}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_runtime(value):
    if not value: return 5_184_000
    v = value.strip().lower()
    if v.endswith("d"): return int(v[:-1]) * 86400
    if v.endswith("h"): return int(v[:-1]) * 3600
    if v.endswith("m"): return int(v[:-1]) * 60
    return int(v)

def parse_args():
    p = argparse.ArgumentParser(description="Google Maps Reviews Scraper v3 — Hybrid")
    p.add_argument("--url",      required=True)
    p.add_argument("--output",   default="reviews_v3.csv")
    p.add_argument("--max",      type=int, default=0)
    p.add_argument("--headless", action="store_true")
    p.add_argument("--speed",    choices=["turbo","fast","safe"], default="fast")
    p.add_argument("--runtime",  default=None, help="e.g. 8h, 3d, 90m")
    p.add_argument("--mode",     choices=["dom","network","hybrid"], default="hybrid",
                   help=(
                       "hybrid  = network bulk + DOM attribute fill (DEFAULT, recommended)\n"
                       "network = RPC interception only, no attributes\n"
                       "dom     = DOM scroll only, all fixes applied, slower"
                   ))
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()

    CFG["MAX_REVIEWS"]         = args.max if args.max > 0 else 10_000_000
    CFG["MAX_RUNTIME_SECONDS"] = parse_runtime(args.runtime)
    CFG["HEADLESS"]            = args.headless

    profile = SPEED_PROFILES.get(args.speed, SPEED_PROFILES["fast"])
    CFG.update(profile)

    log.info(
        f"v3 | Speed: {args.speed.upper()} | Mode: {args.mode.upper()} | "
        f"Max: {CFG['MAX_REVIEWS']:,} | Runtime: {args.runtime or 'no limit'}"
    )

    scraper = GoogleMapsReviewScraper(
        url=args.url,
        output_csv=args.output,
        mode=args.mode,
    )
    asyncio.run(scraper.run())