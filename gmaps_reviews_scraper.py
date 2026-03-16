"""
Google Maps Reviews Scraper — Production Grade
=================================================
Author: Anti-Bot Scraping Engineer
Target: 5,000+ reviews per restaurant listing
Stack:  Python 3, Playwright (async), playwright-stealth
Usage:
    pip install playwright playwright-stealth asyncio aiofiles
    playwright install chromium
    python gmaps_reviews_scraper.py --url "https://maps.google.com/..." --output reviews.csv
"""

import asyncio
import csv
import hashlib
import json
import logging
import os
import random
import re
import signal
import sys
import time
import argparse
from datetime import datetime
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright, Browser, BrowserContext, Page, TimeoutError as PWTimeout

# ── playwright-stealth v2.x integration ──────────────────────────────────────
# v2 replaced stealth_async() with a Stealth class + context manager.
# Usage: async with stealth(context): ...  OR  Stealth().apply_stealth(page)
# We wrap it so the rest of the code just calls: await apply_stealth(page)
HAS_STEALTH = False

try:
    import asyncio as _asyncio
    import inspect as _inspect
    from playwright_stealth import Stealth as _Stealth
    _stealth_obj = _Stealth()

    # v2.x has both sync and async variants depending on the exact build.
    # We probe every known method name and wrap it correctly.
    def _find_method(obj, names):
        for n in names:
            m = getattr(obj, n, None)
            if m is not None:
                return n, m
        return None, None

    _page_name, _page_method = _find_method(_stealth_obj, [
        "stealth_page",        # v2.0.x async
        "apply_stealth",       # some builds async
        "apply_stealth_sync",  # some builds SYNC (misleading name)
    ])
    _ctx_name, _ctx_method = _find_method(_stealth_obj, [
        "stealth_context",          # v2.0.x async
        "apply_stealth_context",    # alt name
    ])

    if not _page_method:
        raise AttributeError(
            f"No known page method on Stealth(). "
            f"Available: {[m for m in dir(_stealth_obj) if not m.startswith('_')]}"
        )

    # Wrap correctly based on whether the method is sync or async
    def _wrap(method):
        if _inspect.iscoroutinefunction(method):
            async def _async_call(target):
                await method(target)
        else:
            async def _async_call(target):
                result = method(target)
                # Some sync methods return a coroutine anyway — await if so
                if _inspect.isawaitable(result):
                    await result
        return _async_call

    apply_stealth         = _wrap(_page_method)
    apply_stealth_context = _wrap(_ctx_method) if _ctx_method else None

    HAS_STEALTH = True
    is_async = _inspect.iscoroutinefunction(_page_method)
    print(f"[INFO] playwright-stealth v2 active | method={_page_name} | async={is_async}")

except ImportError:
    print("[WARN] playwright-stealth not found. Run: pip install playwright-stealth")
    async def apply_stealth(page): pass
    apply_stealth_context = None
except Exception as e:
    print(f"[WARN] playwright-stealth failed: {type(e).__name__}: {e}")
    async def apply_stealth(page): pass
    apply_stealth_context = None

if not HAS_STEALTH:
    print("[WARN] Running WITHOUT stealth — manual JS patches still active.")
    async def apply_stealth(page): pass
    apply_stealth_context = None

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

CFG = {
    # Browser
    "HEADLESS": False,              # Headful strongly recommended for Maps
    "VIEWPORT": (1366, 768),        # Common 1366×768 laptop resolution
    "USER_AGENT": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "LOCALE": "en-US",
    "TIMEZONE": "America/New_York",

    # Session persistence
    "SESSION_DIR": "./gmaps_session",

    # Scraping limits
    "MAX_REVIEWS": 10_000_000,      # effectively unlimited — use --max to cap
    "MAX_RUNTIME_SECONDS": 5_184_000,  # 60 days default — override with --runtime
    "MAX_SCROLL_PLATEAU": 25,
    "SCROLL_BATCH_SIZE": 12,        # 12 scrolls per extraction cycle (was 5)

    # ── SPEED PROFILES ────────────────────────────────────────────────────────
    # turbo : ~70-90 reviews/min  — residential IP, short sessions only
    # fast  : ~35-55 reviews/min  — good daily-use balance  ← DEFAULT
    # safe  : ~15-25 reviews/min  — original conservative mode
    # Switch by changing the tuple values below.
    # ─────────────────────────────────────────────────────────────────────────

    # fast profile
    "DELAY_BETWEEN_SCROLLS": (0.25, 0.7),   # was (1.8, 4.2)
    "DELAY_AFTER_CLICK":     (1.0, 2.2),    # was (2.0, 4.5)
    "DELAY_IDLE_PAUSE":      (3.0, 7.0),    # was (8.0, 20.0)
    "IDLE_PAUSE_EVERY":      (100, 180),    # less frequent pauses
    "REVERSE_SCROLL_EVERY":  (40, 80),      # less frequent reverse scrolls

    # scroll behaviour
    "SCROLL_JUMP": True,                    # instant scroll, no smooth animation
    "SCROLL_DISTANCE": (800, 1400),         # bigger jumps (was 300-700)

    # content-load wait
    "CONTENT_WAIT_POLL":    0.12,           # poll interval seconds (was 0.4)
    "CONTENT_WAIT_TIMEOUT": 3.0,           # max wait per cycle (was 8.0)

    # Anti-block
    "SLOW_MODE_DELAY":    (4.0, 8.0),
    "CAPTCHA_PAUSE":      120,
}

# ─────────────────────────────────────────────────────────────────────────────
# SELECTORS  (primary + fallbacks — Google Maps changes these regularly)
# ─────────────────────────────────────────────────────────────────────────────

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
        'div[jstcache][style*="overflow"]',
        '.m6QErb.DxyBCb.kA9KIf.dS8AEf',
        '.review-dialog-list',
    ],
    "review_block": [
        'div[data-review-id]',
        'div[jsaction*="review"]',
        '.jftiEf',
        '.MyEned',
    ],
    "reviewer_name": [
        '.d4r55',
        '[class*="reviewer-name"]',
        '.x3AX1-LfntMc-header-title-title span',
    ],
    "rating": [
        'span[aria-label*="star"]',
        '[role="img"][aria-label*="star"]',
    ],
    "review_text": [
        '.MyEned span',
        '.wiI7pd',
        'span[class*="review-full-text"]',
        '[jsaction*="expandReview"] span',
    ],
    "review_date": [
        '.rsqaWe',
        'span[class*="review-publish-date"]',
        '.xRkPPb span',
    ],
    "more_button": [
        'button[aria-label*="See more"]',
        'button.w8nwRe',
        'button:has-text("More")',
    ],
    "captcha_signals": [
        '#captcha',
        'iframe[src*="recaptcha"]',
        'form[action*="challenge"]',
        'div[id*="challenge"]',
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("GMapsReviews")


# ─────────────────────────────────────────────────────────────────────────────
# UTILITY HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def review_id_hash(review_id: str) -> str:
    return hashlib.sha1(review_id.encode()).hexdigest()


def rand_delay(band: tuple) -> float:
    lo, hi = band
    return random.uniform(lo, hi)


def jitter(base: float, pct: float = 0.2) -> float:
    return base * random.uniform(1 - pct, 1 + pct)


async def human_sleep(band: tuple):
    await asyncio.sleep(rand_delay(band))


async def micro_mouse_move(page: Page) -> bool:
    """
    Tiny random mouse movements to mimic human hand tremor.
    Returns False if the page/browser has been closed (Wayland crash etc.)
    so callers can react rather than crash.
    """
    try:
        for _ in range(random.randint(2, 5)):
            x = random.randint(200, 900)
            y = random.randint(150, 600)
            await page.mouse.move(x, y, steps=random.randint(3, 8))
            await asyncio.sleep(random.uniform(0.05, 0.18))
        return True
    except Exception as e:
        log.warning(f"micro_mouse_move failed (browser may have closed): {e}")
        return False


async def try_selector(page: Page, selectors: list, timeout: int = 5000) -> Optional[object]:
    """Return first matching element across a list of CSS selectors."""
    for sel in selectors:
        try:
            el = await page.wait_for_selector(sel, timeout=timeout, state="visible")
            if el:
                return el
        except Exception:
            continue
    return None


async def detect_captcha(page: Page) -> bool:
    for sel in SELECTORS["captcha_signals"]:
        try:
            el = await page.query_selector(sel)
            if el:
                return True
        except Exception:
            pass
    title = await page.title()
    if "captcha" in title.lower() or "unusual traffic" in title.lower():
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────────
# CSV STORAGE  (append-only, memory-safe)
# ─────────────────────────────────────────────────────────────────────────────

class ReviewCSVWriter:
    FIELDS = [
        "place_name", "reviewer_name", "rating",
        "review_text", "owner_reply",
        "likes", "date",
        "dining_mode", "meal_type", "price_range",
        "food_rating", "service_rating", "atmosphere_rating",
        "extra_attributes",
    ]

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.seen_ids: set = set()
        self._file = None
        self._writer = None
        self._load_existing()

    @staticmethod
    def _make_dedup_key(row: dict) -> str:
        """Build a dedup key from the fields we actually store in CSV."""
        return (row.get("reviewer_name", "") + "_" + row.get("date", "")).strip("_")

    def _load_existing(self):
        """Pre-load dedup keys from existing CSV to support resumption."""
        if not Path(self.filepath).exists():
            return
        with open(self.filepath, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = self._make_dedup_key(row)
                if key:
                    self.seen_ids.add(key)
        log.info(f"Resumed — {len(self.seen_ids)} existing reviews loaded from {self.filepath}")

    def open(self):
        is_new = not Path(self.filepath).exists()
        self._file = open(self.filepath, "a", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._file, fieldnames=self.FIELDS)
        if is_new:
            self._writer.writeheader()

    def write(self, review: dict) -> bool:
        """Returns True if written (new), False if duplicate."""
        rid = review.get("review_id") or self._make_dedup_key(review)
        if not rid or rid in self.seen_ids:
            return False
        self.seen_ids.add(rid)
        # Write only the defined fields, in order
        row = {f: review.get(f, "") for f in self.FIELDS}
        self._writer.writerow(row)
        self._file.flush()
        return True

    def close(self):
        if self._file:
            self._file.close()

    @property
    def total_seen(self) -> int:
        return len(self.seen_ids)


# ─────────────────────────────────────────────────────────────────────────────
# BROWSER SESSION MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

class BrowserSession:
    def __init__(self, proxy: dict = None, session_dir: str = None, worker_id: int = 0):
        """
        proxy     : dict with keys 'server', optionally 'username'+'password'
                    e.g. {"server": "http://host:port", "username": "u", "password": "p"}
        session_dir: path to persistent profile directory. Each worker should
                    have its own directory so cookies/identity don't collide.
        worker_id : integer label used in log messages to identify this worker.
        """
        self.proxy      = proxy
        self.worker_id  = worker_id
        self.session_dir = session_dir or CFG["SESSION_DIR"]
        os.makedirs(self.session_dir, exist_ok=True)
        self._playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

    async def start(self):
        self._playwright = await async_playwright().start()

        launch_kwargs = dict(
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

        # Inject proxy if provided
        if self.proxy:
            launch_kwargs["proxy"] = self.proxy
            log.info(f"[Worker {self.worker_id}] Using proxy: {self.proxy['server']}")

        # Persistent context reuses cookies/localStorage across runs
        self.context = await self._playwright.chromium.launch_persistent_context(
            **launch_kwargs
        )

        # Override JS fingerprint markers
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

        # Apply stealth at context level first (covers ALL pages automatically)
        if HAS_STEALTH and apply_stealth_context is not None:
            try:
                await apply_stealth_context(self.context)
                log.info("Stealth applied at context level")
            except Exception as e:
                log.warning(f"Context-level stealth failed, falling back to page-level: {e}")

        pages = self.context.pages
        self.page = pages[0] if pages else await self.context.new_page()

        # Apply at page level (belt-and-suspenders, also covers page if context failed)
        if HAS_STEALTH:
            try:
                await apply_stealth(self.page)
                log.info("Stealth applied at page level")
            except Exception as e:
                log.warning(f"Page-level stealth failed: {e}")

        log.info("Browser session started (persistent context)")

    async def stop(self):
        if self.context:
            await self.context.close()
        if self._playwright:
            await self._playwright.stop()
        log.info("Browser session closed")


# ─────────────────────────────────────────────────────────────────────────────
# REVIEW EXTRACTOR
# ─────────────────────────────────────────────────────────────────────────────

class ReviewExtractor:
    """Extracts review data from currently visible DOM nodes."""

    @staticmethod
    async def _is_safe_more_button(page: Page, btn) -> bool:
        """
        Returns True if this button is a review text expand button.
        Uses JS DOM walk to check for danger signals — same block-list logic
        as before, but now called per-button from Python so we can use
        Playwright's native .click() instead of JS btn.click().
        """
        try:
            return await page.evaluate(r"""
            (btn) => {
                const BLOCK_ARIA = [
                    'photo', 'flag', 'report', 'helpful',
                    'share', 'translate', 'like'
                ];
                // Block by the button's own aria-label
                const btnLabel = (btn.getAttribute('aria-label') || '').toLowerCase();
                for (const word of BLOCK_ARIA) {
                    if (btnLabel.includes(word)) return false;
                }
                // Walk up DOM looking for danger signals
                let el = btn.parentElement;
                let depth = 0;
                while (el && depth < 12) {
                    const label = (el.getAttribute('aria-label') || '').toLowerCase();
                    const cls   = (el.className || '').toLowerCase();
                    if (el.tagName === 'A')                    return false;
                    if (label.includes('contributor'))         return false;
                    if (label.includes('profile'))             return false;
                    if (cls.includes('contrib'))               return false;
                    if (el.hasAttribute('data-review-id'))     break;
                    el = el.parentElement;
                    depth++;
                }
                return true;
            }
            """, btn)
        except Exception:
            return False

    @staticmethod
    async def expand_truncated(page: Page):
        """
        Click all 'More' / 'See more' buttons using Playwright's native
        .click() method, then wait for each button to disappear before
        moving on — guaranteeing the full text is in the DOM before we
        extract.

        Why Playwright .click() instead of JS btn.click():
          JS btn.click() only fires the 'click' event.
          Google Maps buttons need mousedown → mouseup → click in sequence
          to trigger their React event handlers and load the full text.
          Playwright's .click() fires all three correctly.

        Why wait for button to disappear:
          After a successful expand, Google removes the 'More' button from
          the DOM and replaces it with the full text. Waiting for it to
          detach confirms the text is loaded before we extract.
        """
        try:
            # Query all candidate buttons from Python side
            buttons = await page.query_selector_all(
                'button[aria-label*="See more"], '
                'button[aria-label*="see more"], '
                'button.w8nwRe'
            )

            for btn in buttons:
                try:
                    # Safety check — skip profile links and action buttons
                    if not await ReviewExtractor._is_safe_more_button(page, btn):
                        continue

                    # Check button is still visible (virtualised list may
                    # have recycled it since we queried)
                    is_visible = await btn.is_visible()
                    if not is_visible:
                        continue

                    # Playwright native click — fires full mouse event sequence
                    await btn.click(timeout=3000)

                    # Wait for the button to disappear from DOM.
                    # This is the signal that Google has injected the full text.
                    # Timeout of 3s — if it doesn't disappear, text may already
                    # have been expanded or the click didn't register.
                    try:
                        await btn.wait_for_element_state("hidden", timeout=3000)
                    except Exception:
                        pass  # button may already be gone or text was short

                    # Small pause between clicks — avoid hammering the UI
                    await asyncio.sleep(random.uniform(0.1, 0.25))

                except Exception:
                    continue  # stale handle or invisible — skip silently

        except Exception:
            pass

    @staticmethod
    async def get_place_name(page: Page) -> str:
        """Extract the place/business name from the listing header."""
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
        """
        Read the total review count shown in the listing header (e.g. '915 reviews' -> 915).
        Used to detect when we are near the end and can stop early.
        Returns 0 if not parseable.
        """
        try:
            count = await page.evaluate(r"""
            () => {
                // Try the reviews tab button aria-label first: "1,234 reviews"
                const btns = document.querySelectorAll(
                    'button[jsaction*="reviews"], [aria-label*="reviews"]'
                );
                for (const btn of btns) {
                    const lbl = (btn.getAttribute('aria-label') || '').replace(/,/g, '');
                    const m = lbl.match(/(\d+)\s+reviews?/i);
                    if (m) return parseInt(m[1]);
                }
                // Rating block span
                const spans = document.querySelectorAll('span[aria-label]');
                for (const s of spans) {
                    const lbl = (s.getAttribute('aria-label') || '').replace(/,/g, '');
                    const m = lbl.match(/(\d+)\s+reviews?/i);
                    if (m) return parseInt(m[1]);
                }
                // Text node scan
                const walker = document.createTreeWalker(
                    document.body, NodeFilter.SHOW_TEXT
                );
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

    @staticmethod
    async def extract_visible(page: Page, seen_ids: set, place_name: str) -> list[dict]:
        """
        Extract all visible review blocks.
        
        Key design: we find the OUTERMOST data-review-id element for each review.
        Nested divs may also have data-review-id, so we filter to only top-level
        ones (those not contained inside another data-review-id). This guarantees
        block.innerText contains ALL content for that review — text, attributes,
        sub-ratings — with no cross-contamination between reviews.
        """
        await ReviewExtractor.expand_truncated(page)

        raw = await page.evaluate(r"""
        () => {
            // Get ALL elements with data-review-id
            const allReviewEls = document.querySelectorAll('[data-review-id]');

            // Keep only OUTERMOST ones — skip any that are nested inside another
            const blocks = [];
            allReviewEls.forEach(el => {
                const parent = el.parentElement
                    ? el.parentElement.closest('[data-review-id]')
                    : null;
                if (!parent) blocks.push(el);  // no ancestor with data-review-id = outermost
            });

            // Fallback: if no data-review-id elements found use class selectors
            if (blocks.length === 0) {
                document.querySelectorAll('div.jftiEf').forEach(el => blocks.push(el));
            }

            const results = [];

            blocks.forEach(block => {
                try {
                    // ── dedup key ──
                    const rid = block.getAttribute('data-review-id') || '';

                    // ── reviewer name ──
                    const nameEl = block.querySelector(
                        '.d4r55, [class*="reviewer-name"], .x3AX1-LfntMc-header-title-title span'
                    );
                    const name = nameEl ? nameEl.innerText.trim() : '';
                    if (!rid && !name) return;

                    // ── overall star rating ──
                    const ratingEl = block.querySelector(
                        'span[aria-label*="star"], [role="img"][aria-label*="star"]'
                    );
                    const ratingLabel = ratingEl ? ratingEl.getAttribute('aria-label') : '';
                    const ratingMatch = ratingLabel.match(/(\d+(\.\d+)?)/);
                    const rating = ratingMatch ? parseFloat(ratingMatch[1]) : null;

                    // ── customer review text ──
                    const textEl = block.querySelector(
                        '.MyEned span, .wiI7pd, span[jsname="bN97Pc"]'
                    );
                    const review_text = textEl ? textEl.innerText.trim() : '';

                    // ── owner reply ──
                    let owner_reply = '';
                    const replyEls = block.querySelectorAll(
                        '.CDe7pd, div[class*="owner-response"], div[aria-label*="response"]'
                    );
                    replyEls.forEach(el => {
                        if (!owner_reply) owner_reply = el.innerText.trim();
                    });

                    // ── date ──
                    const dateEl = block.querySelector(
                        '.rsqaWe, span[class*="review-publish-date"], .xRkPPb span'
                    );
                    const date = dateEl ? dateEl.innerText.trim() : '';

                    // ── likes ──
                    const likesEl = block.querySelector(
                        'button[aria-label*="helpful"] span, .pkWtMe, .GBkF3d span'
                    );
                    let likes = 0;
                    if (likesEl) {
                        const lm = likesEl.innerText.trim().match(/(\d+)/);
                        likes = lm ? parseInt(lm[1]) : 0;
                    }

                    // ── attributes + sub-ratings ──
                    // Read from block.innerText ONLY — never go above the block.
                    // The outermost data-review-id div contains everything for
                    // this one review. No sibling/parent walking needed.
                    let dining_mode = '';
                    let meal_type = '';
                    let price_range = '';
                    let food_rating = '';
                    let service_rating = '';
                    let atmosphere_rating = '';
                    const extra = {};

                    // Split into clean deduplicated lines
                    const seen_lines = new Set();
                    const lines = block.innerText
                        .split('\n')
                        .map(s => s.trim())
                        .filter(s => {
                            if (!s || seen_lines.has(s)) return false;
                            seen_lines.add(s);
                            return true;
                        });

                    // Pattern A — sub-ratings: "Food: 3", "Service: 4", "Atmosphere: 5"
                    // Format: Label + ": " + single digit 1-5 (nothing else on the line)
                    const subRatingRe = /^(.+?):\s*([1-5])$/;
                    const consumed = new Set();

                    lines.forEach((line, idx) => {
                        const m = line.match(subRatingRe);
                        if (!m) return;
                        const lbl = m[1].trim().toLowerCase();
                        const val = parseFloat(m[2]);
                        consumed.add(idx);
                        if      (lbl === 'food')        food_rating = val;
                        else if (lbl === 'service')     service_rating = val;
                        else if (lbl === 'atmosphere')  atmosphere_rating = val;
                        else                            extra[lbl] = val;
                    });

                    // Pattern B — categorical: label line followed by value line
                    // "Service" -> "Dine in"
                    // "Meal type" -> "Lunch"
                    // "Price per person" -> "₹200–400"
                    for (let i = 0; i < lines.length - 1; i++) {
                        if (consumed.has(i) || consumed.has(i + 1)) continue;
                        const lbl = lines[i].toLowerCase().trim();
                        const val = lines[i + 1].trim();

                        // reject long sentences (review text, not attribute values)
                        if (val.length > 60) continue;
                        // reject sub-rating lines as values
                        if (/:\s*[1-5]$/.test(val)) continue;

                        if (!meal_type && lbl === 'meal type') {
                            meal_type = val;
                            consumed.add(i); consumed.add(i + 1);
                        } else if (!dining_mode && lbl === 'service') {
                            // bare "Service" label = dining mode category
                            // (vs "Service: 1" which is a sub-rating, caught by Pattern A)
                            dining_mode = val;
                            consumed.add(i); consumed.add(i + 1);
                        } else if (!dining_mode && lbl.includes('dine')) {
                            dining_mode = val;
                            consumed.add(i); consumed.add(i + 1);
                        } else if (!price_range && lbl.includes('price per person')) {
                            price_range = val;
                            consumed.add(i); consumed.add(i + 1);
                        }
                    }

                    const extra_attributes = Object.keys(extra).length > 0
                        ? JSON.stringify(extra) : '';

                    results.push({
                        review_id:         rid || (name + '_' + date),
                        reviewer_name:     name,
                        rating:            rating,
                        review_text:       review_text,
                        owner_reply:       owner_reply,
                        likes:             likes,
                        date:              date,
                        dining_mode:       dining_mode,
                        meal_type:         meal_type,
                        price_range:       price_range,
                        food_rating:       food_rating,
                        service_rating:    service_rating,
                        atmosphere_rating: atmosphere_rating,
                        extra_attributes:  extra_attributes,
                    });

                } catch(e) {}
            });
            return results;
        }
        """)

        new_reviews = []
        for r in raw:
            if r["review_id"] not in seen_ids:
                r["place_name"] = place_name
                new_reviews.append(r)
        return new_reviews


# ─────────────────────────────────────────────────────────────────────────────
# SCROLL ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class ScrollEngine:
    """
    Infinite scroll manager for Google Maps review panel.
    Handles virtualized lists where old DOM nodes are recycled.
    """

    def __init__(self, page: Page):
        self.page = page
        self.scroll_count = 0
        self.plateau_count = 0
        self.next_idle_at = random.randint(*CFG["IDLE_PAUSE_EVERY"])
        self.next_reverse_at = random.randint(*CFG["REVERSE_SCROLL_EVERY"])

    async def find_scroll_container(self) -> Optional[object]:
        for sel in SELECTORS["scroll_container"]:
            try:
                el = await self.page.query_selector(sel)
                if el:
                    log.debug(f"Scroll container found via: {sel}")
                    return el
            except Exception:
                continue
        log.warning("Scroll container not found — falling back to window scroll")
        return None

    async def scroll_once(self, container) -> int:
        """
        Scroll down by a randomised distance.
        Returns pixel distance scrolled.
        """
        lo, hi = CFG["SCROLL_DISTANCE"]
        distance = random.randint(lo, hi)
        # instant scroll = no animation delay; 'smooth' wastes ~400ms per scroll
        behavior = "instant" if CFG.get("SCROLL_JUMP", True) else "smooth"

        if container:
            await self.page.evaluate(
                """([el, dist, beh]) => { el.scrollBy({top: dist, behavior: beh}); }""",
                [container, distance, behavior]
            )
        else:
            await self.page.evaluate(
                f"window.scrollBy({{top: {distance}, behavior: '{behavior}'}})"
            )

        self.scroll_count += 1
        return distance

    async def reverse_scroll(self, container):
        """Small back-scroll to mimic human re-reading behaviour."""
        dist = random.randint(80, 220)
        beh = "instant" if CFG.get("SCROLL_JUMP", True) else "smooth"
        if container:
            await self.page.evaluate(
                """([el, dist, beh]) => { el.scrollBy({top: -dist, behavior: beh}); }""",
                [container, dist, beh]
            )
        await asyncio.sleep(random.uniform(0.1, 0.3))
        fwd = dist + random.randint(150, 350)
        if container:
            await self.page.evaluate(
                """([el, dist, beh]) => { el.scrollBy({top: dist, behavior: beh}); }""",
                [container, fwd, beh]
            )

    async def wait_for_new_content(self, prev_count: int, timeout: float = None) -> int:
        """
        Poll DOM until new review nodes appear or timeout.
        Returns count of new nodes found.
        """
        if timeout is None:
            timeout = CFG["CONTENT_WAIT_TIMEOUT"]
        poll = CFG["CONTENT_WAIT_POLL"]
        deadline = time.time() + timeout
        while time.time() < deadline:
            count = await self.page.evaluate(
                "() => document.querySelectorAll('div[data-review-id], div.jftiEf').length"
            )
            if count > prev_count:
                return count - prev_count
            await asyncio.sleep(poll)
        return 0

    async def maybe_idle_pause(self):
        if self.scroll_count >= self.next_idle_at:
            pause = rand_delay(CFG["DELAY_IDLE_PAUSE"])
            log.info(f"  [Human simulation] Idle pause for {pause:.1f}s")
            await micro_mouse_move(self.page)
            await asyncio.sleep(pause)
            self.next_idle_at = self.scroll_count + random.randint(*CFG["IDLE_PAUSE_EVERY"])

    async def maybe_reverse_scroll(self, container):
        if self.scroll_count >= self.next_reverse_at:
            log.debug("  [Human simulation] Reverse scroll")
            await self.reverse_scroll(container)
            self.next_reverse_at = self.scroll_count + random.randint(*CFG["REVERSE_SCROLL_EVERY"])


# ─────────────────────────────────────────────────────────────────────────────
# ANTI-BLOCK MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class AntiBlockManager:
    def __init__(self, page: Page):
        self.page = page
        self.slow_mode = False
        self.slow_mode_count = 0

    async def check(self) -> str:
        """Returns 'ok', 'slow', or 'captcha'."""
        if await detect_captcha(self.page):
            return "captcha"

        # Soft-ban signals: empty content, redirect, error overlays
        url = self.page.url
        if "sorry" in url or "challenge" in url or "consent" in url:
            return "captcha"

        # Check if reviews panel went blank (content blocked)
        review_count = await self.page.evaluate("""
            () => document.querySelectorAll('div[data-review-id], div.jftiEf').length
        """)
        if review_count == 0 and self.slow_mode_count > 3:
            return "slow"

        return "ok"

    async def handle_captcha(self):
        log.warning("⚠️  CAPTCHA detected — pausing for manual solve")
        log.warning(f"   Solve the CAPTCHA in the browser window within {CFG['CAPTCHA_PAUSE']}s")
        await asyncio.sleep(CFG["CAPTCHA_PAUSE"])
        # After pause, check if resolved
        if await detect_captcha(self.page):
            log.error("CAPTCHA not solved — aborting")
            raise RuntimeError("CAPTCHA unsolved after pause")

    async def apply_slow_mode(self):
        self.slow_mode = True
        self.slow_mode_count += 1
        delay = rand_delay(CFG["SLOW_MODE_DELAY"])
        log.warning(f"⚠️  Soft-ban signal — slow mode delay {delay:.1f}s "
                    f"(incident #{self.slow_mode_count})")
        await asyncio.sleep(delay)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN SCRAPER ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

class GoogleMapsReviewScraper:
    def __init__(self, url: str, output_csv: str,
                 proxy: dict = None, session_dir: str = None, worker_id: int = 0):
        self.url = url
        # Only construct a CSV writer if a path is given.
        # When used inside a parallel worker, the shared ReviewCSVWriter
        # is injected after construction (scraper.csv = shared_writer),
        # so we must not crash trying to open an empty path here.
        self.csv = ReviewCSVWriter(output_csv) if output_csv else None
        self.session = BrowserSession(
            proxy=proxy,
            session_dir=session_dir,
            worker_id=worker_id,
        )
        self.worker_id   = worker_id
        self._shutdown   = False
        self._start_time = None
        self._last_written = 0  # reviews written in last scrape session

    def _register_signals(self):
        """Handle Ctrl+C / SIGTERM gracefully."""
        def _handler(sig, frame):
            log.info("\n🛑  Shutdown signal received — finishing current batch...")
            self._shutdown = True
        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

    def _runtime_exceeded(self) -> bool:
        return (time.time() - self._start_time) > CFG["MAX_RUNTIME_SECONDS"]

    # ── Phase 1: Navigate to listing ─────────────────────────────────────────

    @staticmethod
    async def _dismiss_consent(page: Page):
        """
        Dismiss Google consent / cookie wall that appears on first visit
        from a new IP or region. Must be handled before any Maps page loads.
        """
        try:
            consent_selectors = [
                'button[aria-label*="Accept all"]',
                'button:has-text("Accept all")',
                'button:has-text("I agree")',
                'button:has-text("Agree")',
                'form[action*="consent"] button',
                '#L2AGLb',   # Google's "I agree" button id
            ]
            for sel in consent_selectors:
                try:
                    btn = await page.wait_for_selector(sel, timeout=3000, state="visible")
                    if btn:
                        await btn.click()
                        log.info("  Consent wall dismissed")
                        await asyncio.sleep(1.5)
                        return
                except Exception:
                    continue
        except Exception:
            pass

    async def _navigate(self, page: Page):
        # Append language/region params to force English + India context
        # regardless of proxy IP location
        url = self.url
        sep = "&" if "?" in url else "?"
        if "hl=" not in url:
            url = url + sep + "hl=en"
            sep = "&"
        if "gl=" not in url:
            url = url + sep + "gl=in"

        log.info(f"Navigating to: {url}")

        for attempt in range(4):
            try:
                # Step 1: warm up with plain maps.google.com first on first attempt
                # This sets cookies and handles consent before the real page load
                if attempt == 0:
                    try:
                        await page.goto(
                            "https://www.google.com/maps?hl=en&gl=in",
                            wait_until="domcontentloaded",
                            timeout=30000
                        )
                        await asyncio.sleep(rand_delay((1.5, 3.0)))
                        await self._dismiss_consent(page)
                        await asyncio.sleep(rand_delay((1.0, 2.0)))
                    except Exception:
                        pass  # warm-up failure is non-fatal

                # Step 2: navigate to actual listing
                await page.goto(url, wait_until="domcontentloaded", timeout=60000)

                # Dismiss consent if it appeared on this page
                await self._dismiss_consent(page)

                # Verify page actually loaded
                current_url = page.url
                if "about:blank" in current_url or not current_url.startswith("http"):
                    raise PWTimeout("Page loaded blank — proxy may not be connected")

                # Verify it's actually a Maps page
                if "google.com/maps" not in current_url and "maps.google" not in current_url:
                    log.warning(f"  Unexpected redirect to: {current_url[:80]}")
                    # May have been redirected to consent or country page
                    await self._dismiss_consent(page)
                    # Try navigating again
                    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
                    await self._dismiss_consent(page)

                await asyncio.sleep(rand_delay((2.0, 4.0)))
                return

            except PWTimeout:
                wait = 5 * (attempt + 1)
                log.warning(f"Navigation timeout (attempt {attempt+1}/4) — waiting {wait}s")
                await asyncio.sleep(wait)
            except Exception as e:
                log.warning(f"Navigation error (attempt {attempt+1}/4): {e}")
                await asyncio.sleep(5)

        raise RuntimeError("Failed to load listing page after 4 attempts")

    # ── Phase 2: Open reviews tab ─────────────────────────────────────────────

    async def _open_reviews_tab(self, page: Page):
        log.info("Opening Reviews tab...")
        tab = await try_selector(page, SELECTORS["reviews_tab"], timeout=10000)
        if not tab:
            raise RuntimeError("Reviews tab not found — page structure may have changed")

        await micro_mouse_move(page)
        await tab.click()
        await human_sleep(CFG["DELAY_AFTER_CLICK"])

        # Sort by Newest for consistent ordering
        sort_btn = await try_selector(page, SELECTORS["review_sort_button"], timeout=6000)
        if sort_btn:
            await sort_btn.click()
            await asyncio.sleep(rand_delay((0.8, 1.5)))
            newest = await try_selector(page, SELECTORS["sort_newest"], timeout=4000)
            if newest:
                await newest.click()
                await asyncio.sleep(rand_delay((1.5, 3.0)))
                log.info("Sorted by Newest")
            else:
                log.warning("Newest sort option not found — proceeding with default order")
        else:
            log.warning("Sort button not found — proceeding with default order")

    # ── Phase 3: Core scroll + extract loop ──────────────────────────────────

    def _is_valid_listing_url(self, url: str) -> bool:
        """
        Returns True only if the current page URL is still our target listing.
        Rejects: contributor profiles, search pages, consent pages, other places.
        """
        BAD_PATTERNS = [
            "/contrib/",        # reviewer profile pages  ← the bug you hit
            "/search",          # search results page
            "consent.google",   # consent/cookie wall
            "/sorry",           # rate limit page
            "challenge",        # captcha challenge
        ]
        for pat in BAD_PATTERNS:
            if pat in url:
                return False
        # Must still contain the place path
        return "/maps/place/" in url

    async def _recover_to_listing(self, page: Page, original_url: str) -> bool:
        """Navigate back to original listing and re-open the reviews panel."""
        log.warning(f"  URL drift detected → {page.url[:80]}...")
        log.warning(f"  Recovering: navigating back to listing...")
        try:
            await page.goto(original_url, wait_until="domcontentloaded", timeout=25000)
            await asyncio.sleep(rand_delay((2.0, 3.5)))
            await self._open_reviews_tab(page)
            log.info("  Recovery successful — back on listing reviews panel")
            return True
        except Exception as e:
            log.error(f"  Recovery failed: {e}")
            return False

    async def _scroll_and_extract_loop(self, page: Page):
        scroller = ScrollEngine(page)
        extractor = ReviewExtractor()
        antiblock = AntiBlockManager(page)

        container = await scroller.find_scroll_container()
        total_written    = 0   # reviews written this session for this restaurant
        scroll_cycle     = 0
        drift_recoveries = 0
        MAX_DRIFT_RECOVERIES = 5

        # ── Fetch place name and total review count once at start ──
        place_name = await extractor.get_place_name(page)
        total_on_listing = await extractor.get_total_review_count(page)
        log.info(f"Place      : {place_name}")
        log.info(f"Total reviews on listing: {total_on_listing or 'unknown'}")
        log.info("Starting scroll-extract loop...")

        while not self._shutdown:
            # ── Hard guards ──
            if self._runtime_exceeded():
                log.warning("⏱️  Max runtime reached — stopping")
                break
            if self.csv.total_seen >= CFG["MAX_REVIEWS"]:
                log.info(f"✅  Target of {CFG['MAX_REVIEWS']} reviews reached")
                break

            # Smart completion: if we know the total, stop when we are within
            # the final ~1% or 10 reviews (whichever is larger).
            # Google withholds the last few reviews — no point hammering for them.
            if total_on_listing > 0:
                gap = total_on_listing - self.csv.total_seen
                threshold = max(10, int(total_on_listing * 0.01))
                if gap <= threshold and self.csv.total_seen > 0:
                    log.info(
                        f"✅  Collected {self.csv.total_seen}/{total_on_listing} reviews "
                        f"(within {gap} of total — Google withholds the last few). Stopping."
                    )
                    break

            if scroller.plateau_count >= CFG["MAX_SCROLL_PLATEAU"]:
                log.info(
                    f"📊 Plateau: {scroller.plateau_count} consecutive scroll cycles with "
                    f"no new reviews. Collected {self.csv.total_seen}"
                    + (f"/{total_on_listing}" if total_on_listing else "")
                    + ". Stopping."
                )
                break

            # ── URL drift check (catches reviewer profile navigation) ──
            current_url = page.url
            if not self._is_valid_listing_url(current_url):
                drift_recoveries += 1
                log.warning(f"⚠️  URL drift #{drift_recoveries}: not on listing page")
                if drift_recoveries > MAX_DRIFT_RECOVERIES:
                    log.error("Too many URL drifts — aborting to avoid scraping wrong data")
                    break
                recovered = await self._recover_to_listing(page, self.url)
                if not recovered:
                    break
                container = await scroller.find_scroll_container()
                scroller.plateau_count = 0
                continue

            # ── Anti-block check ──
            status = await antiblock.check()
            if status == "captcha":
                await antiblock.handle_captcha()
                container = await scroller.find_scroll_container()
            elif status == "slow":
                await antiblock.apply_slow_mode()

            # ── Scroll batch ──
            prev_dom_count = await page.evaluate(
                "() => document.querySelectorAll('div[data-review-id], div.jftiEf').length"
            )

            for _ in range(CFG["SCROLL_BATCH_SIZE"]):
                # Check URL hasn't drifted mid-batch (click may have fired)
                if not self._is_valid_listing_url(page.url):
                    log.warning("  Mid-batch URL drift detected — breaking scroll batch")
                    break
                await scroller.scroll_once(container)
                await human_sleep(CFG["DELAY_BETWEEN_SCROLLS"])
                await scroller.maybe_reverse_scroll(container)
                await scroller.maybe_idle_pause()
                alive = await micro_mouse_move(page)
                if not alive:
                    log.error("Browser appears closed — exiting scroll loop")
                    self._shutdown = True
                    break

            scroll_cycle += 1

            # ── Wait for new content ──
            new_nodes = await scroller.wait_for_new_content(prev_dom_count)
            if new_nodes == 0:
                scroller.plateau_count += 1
                log.debug(f"No new nodes (plateau {scroller.plateau_count}/{CFG['MAX_SCROLL_PLATEAU']})")
            else:
                scroller.plateau_count = 0

            # ── Extract batch ──
            new_reviews = await extractor.extract_visible(page, self.csv.seen_ids, place_name)
            written_this_batch = 0
            for review in new_reviews:
                if self.csv.write(review):
                    written_this_batch += 1
                    total_written += 1

            if written_this_batch:
                listing_pct = (
                    f"{total_written/total_on_listing*100:.1f}%"
                    if total_on_listing else "?%"
                )
                log.info(
                    f"  Scroll cycle {scroll_cycle:4d} | "
                    f"+{written_this_batch:3d} new | "
                    f"this_restaurant={total_written:5d}/{total_on_listing or '?'} ({listing_pct}) | "
                    f"csv_total={self.csv.total_seen:6d} | "
                    f"scrolls={scroller.scroll_count:5d} | "
                    f"runtime={int(time.time()-self._start_time)}s"
                )

            # ── Re-acquire scroll container periodically ──
            if scroll_cycle % 30 == 0:
                container = await scroller.find_scroll_container()

        self._last_written = total_written
        log.info(
            f"Scroll loop finished. "
            f"This restaurant: {total_written} reviews collected. "
            f"CSV total (all restaurants): {self.csv.total_seen}"
        )

    # ── Public entry point ────────────────────────────────────────────────────

    async def run(self):
        self._register_signals()
        self._start_time = time.time()
        self.csv.open()

        try:
            await self.session.start()
            page = self.session.page

            # Page-level crash recovery
            page.on("crash", lambda: log.error("Page crashed — attempting recovery"))

            await self._navigate(page)
            await self._open_reviews_tab(page)
            await self._scroll_and_extract_loop(page)

        except RuntimeError as e:
            log.error(f"Fatal scraper error: {e}")
        except Exception as e:
            err_str = str(e)
            # TargetClosedError = browser window killed externally (Wayland crash,
            # user closed window, OOM killer, etc.). Data already flushed to CSV.
            if "TargetClosedError" in type(e).__name__ or "Target page" in err_str or "been closed" in err_str:
                msg = (
                    "Browser was closed unexpectedly (possibly Wayland/display crash). "
                    f"Reviews saved so far: {self.csv.total_seen}. "
                    "Re-run with the same --output file to resume."
                )
                log.warning(msg)
            else:
                log.exception(f"Unexpected error: {e}")
        finally:
            self.csv.close()
            await self.session.stop()
            elapsed = int(time.time() - self._start_time)
            log.info(
                f"\n{'='*60}\n"
                f"  DONE\n"
                f"  Reviews extracted : {self.csv.total_seen}\n"
                f"  Elapsed time      : {elapsed}s ({elapsed//60}m {elapsed%60}s)\n"
                f"  Output file       : {self.csv.filepath}\n"
                f"{'='*60}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# CLI ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

SPEED_PROFILES = {
    "turbo": {
        "DELAY_BETWEEN_SCROLLS": (0.05, 0.2),
        "DELAY_AFTER_CLICK":     (0.6, 1.2),
        "DELAY_IDLE_PAUSE":      (1.5, 3.0),
        "IDLE_PAUSE_EVERY":      (200, 400),
        "REVERSE_SCROLL_EVERY":  (100, 200),
        "SCROLL_BATCH_SIZE":     20,
        "SCROLL_DISTANCE":       (1200, 2000),
        "CONTENT_WAIT_TIMEOUT":  2.0,
        "CONTENT_WAIT_POLL":     0.08,
    },
    "fast": {
        "DELAY_BETWEEN_SCROLLS": (0.25, 0.7),
        "DELAY_AFTER_CLICK":     (1.0, 2.2),
        "DELAY_IDLE_PAUSE":      (3.0, 7.0),
        "IDLE_PAUSE_EVERY":      (100, 180),
        "REVERSE_SCROLL_EVERY":  (40, 80),
        "SCROLL_BATCH_SIZE":     12,
        "SCROLL_DISTANCE":       (800, 1400),
        "CONTENT_WAIT_TIMEOUT":  3.0,
        "CONTENT_WAIT_POLL":     0.12,
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
        "CONTENT_WAIT_POLL":     0.4,
    },
}


def apply_speed_profile(profile: str):
    if profile not in SPEED_PROFILES:
        log.warning(f"Unknown speed profile '{profile}' — using 'fast'")
        profile = "fast"
    CFG.update(SPEED_PROFILES[profile])
    log.info(f"Speed profile: {profile.upper()}  |  "
             f"scroll_delay={CFG['DELAY_BETWEEN_SCROLLS']}  "
             f"batch={CFG['SCROLL_BATCH_SIZE']}  "
             f"jump_dist={CFG['SCROLL_DISTANCE']}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Google Maps Reviews Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape until all reviews collected (no time/count limit):
  python3 gmaps_reviews_scraper.py --url "..." --output out.csv

  # Stop after 5000 reviews:
  python3 gmaps_reviews_scraper.py --url "..." --output out.csv --max 5000

  # Run for exactly 8 hours then stop (resume later):
  python3 gmaps_reviews_scraper.py --url "..." --output out.csv --runtime 8h

  # Run for 3 days:
  python3 gmaps_reviews_scraper.py --url "..." --output out.csv --runtime 3d
        """
    )
    parser.add_argument("--url", required=True, help="Full Google Maps listing URL")
    parser.add_argument("--output", default="reviews.csv", help="Output CSV file path")
    parser.add_argument(
        "--max", type=int, default=0,
        help="Maximum reviews to collect (default: no limit, scrape everything)"
    )
    parser.add_argument("--headless", action="store_true",
                        help="Run headless (not recommended for Maps)")
    parser.add_argument(
        "--speed",
        choices=["turbo", "fast", "safe"],
        default="fast",
        help=(
            "Speed profile: "
            "turbo=~80 rev/min (risky), "
            "fast=~45 rev/min (default), "
            "safe=~20 rev/min (stealth)"
        ),
    )
    parser.add_argument(
        "--runtime",
        default=None,
        help=(
            "Max runtime before auto-stopping. Supports: "
            "30m (30 minutes), 8h (8 hours), 3d (3 days), 60d (60 days). "
            "Default: no limit (runs until all reviews collected)."
        ),
    )
    return parser.parse_args()


def parse_runtime(value: str) -> int:
    """Convert runtime string like '8h', '3d', '90m' to seconds."""
    if value is None:
        return 5_184_000  # 60 days fallback
    value = value.strip().lower()
    if value.endswith("d"):
        return int(value[:-1]) * 86400
    if value.endswith("h"):
        return int(value[:-1]) * 3600
    if value.endswith("m"):
        return int(value[:-1]) * 60
    if value.endswith("s"):
        return int(value[:-1])
    # bare number = seconds
    return int(value)


if __name__ == "__main__":
    args = parse_args()

    # Max reviews: 0 means no limit
    CFG["MAX_REVIEWS"] = args.max if args.max > 0 else 10_000_000

    # Runtime
    CFG["MAX_RUNTIME_SECONDS"] = parse_runtime(args.runtime)
    runtime_desc = args.runtime or "no limit (until all reviews done)"
    log.info(f"Runtime limit : {runtime_desc}  ({CFG['MAX_RUNTIME_SECONDS']}s)")
    log.info(f"Reviews limit : {CFG['MAX_REVIEWS']:,}")

    CFG["HEADLESS"] = args.headless
    apply_speed_profile(args.speed)

    scraper = GoogleMapsReviewScraper(url=args.url, output_csv=args.output)
    asyncio.run(scraper.run())