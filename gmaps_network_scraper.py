"""
Google Maps Reviews — Network API Scraper
==========================================
Uses the internal `listugcposts` RPC endpoint directly.
No DOM, no scrolling, no virtualization issues.
Complete attribute data — exactly as Google's server sends it.

How it works:
  1. Open Maps in browser, go to Reviews tab
  2. Open DevTools → Network → filter XHR → find `listugcposts`
  3. Copy the full URL and paste it as --url argument
  4. This script paginates automatically using the next-page token
     embedded in each response

Usage:
    pip install httpx
    python3 gmaps_network_scraper.py --url "https://www.google.com/maps/rpc/listugcposts?..." --output reviews.csv

Resume:
    Just re-run with the same --output file. Already-scraped reviews are skipped.
"""

import asyncio
import csv
import json
import re
import sys
import time
import logging
import argparse
import random
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, quote

import httpx

# ── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("network_scraper.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("GMapsNet")

# ── Config ────────────────────────────────────────────────────────────────────

CFG = {
    "DELAY_BETWEEN_PAGES": (0.8, 2.0),   # seconds between API calls
    "MAX_RETRIES":          4,
    "RETRY_DELAY":          5.0,
    "TIMEOUT":              20,
    "MAX_REVIEWS":          10_000_000,
}

# ── Attribute key → CSV field name ───────────────────────────────────────────

ATTR_KEY_MAP = {
    "GUIDED_DINING_MODE":                  "dining_mode",
    "GUIDED_DINING_MEAL_TYPE":             "meal_type",
    "GUIDED_DINING_PRICE_RANGE":           "price_per_person",
    "GUIDED_DINING_FOOD_ASPECT":           "food",
    "GUIDED_DINING_SERVICE_ASPECT":        "service_rating",
    "GUIDED_DINING_ATMOSPHERE_ASPECT":     "atmosphere",
    "GUIDED_DINING_SEATING_TYPE":          "seating_type",
    "GUIDED_DINING_WAIT_TIME":             "wait_time",
    "GUIDED_DINING_PARKING_SPACE_AVAILABILITY": "parking_space",
    "GUIDED_DINING_PARKING_OPTIONS":       "parking_options",
    # TIPS_TOPICS handled specially in _parse_attribute — not in simple map
    "GUIDED_DINING_GROUP_SIZE":            "group_size",
    "GUIDED_DINING_RECOMMENDED_DISHES":    "recommended_dishes",
    "GUIDED_DINING_SPECIAL_EVENTS":        "special_events",
    "GUIDED_DINING_VEGETARIAN_OFFERINGS":  "vegetarian_offerings",
    "GUIDED_DINING_VEGETARIAN_OPTIONS":    "vegetarian_options",
    "GUIDED_DINING_VEGETARIAN_RECOMMENDATION": "vegetarian_recommendation",
    "GUIDED_DINING_KID_FRIENDLINESS":      "kid_friendliness",
    "GUIDED_DINING_NOISE_LEVEL":           "noise_level",
    "GUIDED_DINING_DIETARY_RESTRICTIONS":  "dietary_restrictions",
    "GUIDED_DINING_ACCESSIBILITY":         "accessibility",
    "GUIDED_DINING_RESERVATIONS":          "reservations",
}

# ── CSV Writer ────────────────────────────────────────────────────────────────

class ReviewCSVWriter:
    FIELDS = [
        "place_name", "reviewer_name", "reviewer_id", "local_guide",
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
                rid = row.get("review_id") or (row.get("reviewer_name","") + "_" + row.get("date",""))
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
            review.get("reviewer_name","") + "_" + review.get("date","")
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
    def total_seen(self):
        return len(self.seen_ids)

# ── Response Parser ───────────────────────────────────────────────────────────

class ResponseParser:
    XSS_PREFIX = ")]}'"

    @staticmethod
    def strip_xss(text: str) -> str:
        text = text.strip()
        if text.startswith(ResponseParser.XSS_PREFIX):
            text = text[len(ResponseParser.XSS_PREFIX):]
        return text.strip()

    @staticmethod
    def safe_get(obj, *keys, default=None):
        """Safely traverse nested list/dict."""
        cur = obj
        for k in keys:
            try:
                cur = cur[k]
            except (IndexError, KeyError, TypeError):
                return default
        return cur

    def parse(self, text: str) -> tuple[list, Optional[str]]:
        """
        Returns (reviews, next_page_token).
        reviews is a list of dicts ready to write to CSV.
        """
        text = self.strip_xss(text)
        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            log.warning(f"JSON parse failed: {e} — snippet: {text[:200]}")
            return [], None

        # Top-level structure:
        # [null, current_page_token, [[review1], [review2], ...], null, ..., next_page_token]
        # Reviews array is at index 2
        # Next page token is the last string element of the top-level array

        reviews_raw = self.safe_get(data, 2, default=[])
        if not isinstance(reviews_raw, list):
            log.warning(f"Unexpected structure — data[2] is {type(reviews_raw)}")
            return [], None

        # Sanity check: if first element is not a list, try data[1] or other positions
        if reviews_raw and not isinstance(reviews_raw[0], list):
            log.debug(f"data[2][0] is {type(reviews_raw[0])}, scanning for reviews array")
            for i, item in enumerate(data):
                if isinstance(item, list) and item and isinstance(item[0], list):
                    reviews_raw = item
                    log.debug(f"Found reviews array at data[{i}]")
                    break

        log.debug(f"Reviews array length: {len(reviews_raw)}")

        # Next page token at data[2][N][2] for each entry.
        # Collect ALL unique CAESY0 tokens from entries, pick the one that
        # differs from the FIRST entry's token (first = current page pointer).
        # End of results = all entries have the same token, or no token found.
        next_token = None
        tokens_seen = []
        if isinstance(reviews_raw, list):
            for entry in reviews_raw:
                if isinstance(entry, list) and len(entry) > 2:
                    t = entry[2]
                    if isinstance(t, str) and t.startswith("CAESY0"):
                        tokens_seen.append(t)

        if tokens_seen:
            # The last unique token is the next page pointer
            # If all tokens are identical it means no more pages
            unique = list(dict.fromkeys(tokens_seen))  # preserve order, deduplicate
            if len(unique) == 1:
                # All same — this IS the next page token (use it)
                # End-of-results is detected by getting 0 new reviews instead
                next_token = unique[0]
            else:
                # Multiple tokens — last one points to next page
                next_token = unique[-1]

        log.info(f"Tokens found: {len(tokens_seen)}, unique: {len(set(tokens_seen))}, using: {next_token[:30] if next_token else None}")

        reviews = []
        for entry in reviews_raw:
            if not isinstance(entry, list) or not entry:
                continue
            review_array = entry[0] if isinstance(entry[0], list) else entry
            parsed = self._parse_review(review_array)
            if parsed:
                reviews.append(parsed)
            else:
                log.debug(f"_parse_review returned None for entry starting with: {str(review_array[0] if review_array else '?')[:40]}")

        log.info(f"Parsed {len(reviews)} reviews from {len(reviews_raw)} entries")
        return reviews, next_token

    def _parse_review(self, r: list) -> Optional[dict]:
        """
        All positions confirmed from live debug output:

          r[0]              = review_id "Ci9DQUI..."
          r[1][4][2][0]     = profile URL → extract reviewer_id number
          r[1][4][5][0]     = reviewer name (string)
          r[1][4][5][10][0] = "Local Guide · N reviews" (if local guide)
          r[1][6]           = date "3 months ago"
          r[2][0][0]        = star rating integer 1-5
          r[2][6]           = attributes array
          r[2][6][N][0][0]  = attribute key "GUIDED_DINING_MODE"
          r[2][6][N][2]     = selected options (category attrs)
          r[2][6][N][11]    = [rating] (numeric sub-ratings food/service/atmosphere)
          r[2][15][0][0]    = full review text (never truncated)
        """
        g = self.safe_get

        # ── review_id ──
        review_id = g(r, 0, default=None)
        if not isinstance(review_id, str) or len(review_id) < 10:
            return None

        # ── date ──
        date = str(g(r, 1, 6, default="") or "")

        # ── reviewer name — r[1][4][5][0] is a plain string ──
        reviewer_name = str(g(r, 1, 4, 5, 0, default="") or "")

        # ── reviewer_id — extract number from contrib URL at r[1][4][2][0] ──
        reviewer_id = ""
        profile_url = str(g(r, 1, 4, 2, 0, default="") or "")
        m = re.search(r'/contrib/(\d+)', profile_url)
        if m:
            reviewer_id = m.group(1)

        # ── local guide — r[1][4][5][10][0] = "Local Guide · N reviews" ──
        lg_text = str(g(r, 1, 4, 5, 10, 0, default="") or "")
        local_guide = "local guide" in lg_text.lower()

        # ── star rating — r[2][0][0] ──
        rating = g(r, 2, 0, 0, default=None)

        # ── review text — r[2][15][0][0] ──
        review_text = str(g(r, 2, 15, 0, 0, default="") or "")
        review_text = review_text.replace("\n", " ").strip()

        # ── attributes — r[2][6] ──
        attrs = {}
        for attr in (g(r, 2, 6, default=[]) or []):
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
        """
        Parse one attribute block.

        Confirmed structure from API:
          attr[0][0] = key e.g. "GUIDED_DINING_MODE"
          attr[4]    = display label e.g. "Service", "Food"
          attr[2]    = selected options array (for category attrs like dining_mode)
                       [[["E:DINE_IN"], "Dine in", 2, ...], ...]
          attr[11]   = [rating_value] for numeric aspects (food/service/atmosphere)
                       e.g. [5] means 5 stars
        """
        g = self.safe_get

        key_raw = g(attr, 0, 0, default=None)
        if not key_raw:
            return None

        # Special case: TIPS_TOPICS is a freeform multi-select
        # (Parking, Recommended dishes, etc.) — handle before ATTR_KEY_MAP check
        if key_raw == "GUIDED_DINING_TIPS_TOPICS":
            options = g(attr, 2, default=None)
            if isinstance(options, list) and options:
                values = []
                for option_group in options:
                    if isinstance(option_group, list):
                        for option in option_group:
                            if isinstance(option, list) and len(option) > 1:
                                v = option[1]
                                if isinstance(v, str) and v:
                                    values.append(v)
                if values:
                    return "tips_topics", ", ".join(values)
            return None

        if key_raw not in ATTR_KEY_MAP:
            return None

        field_name = ATTR_KEY_MAP[key_raw]

        # Pattern A: numeric rating aspects (food, service_rating, atmosphere)
        # attr[11] = [value] e.g. [5]
        rating_container = g(attr, 11, default=None)
        if isinstance(rating_container, list) and rating_container:
            val = rating_container[0]
            if isinstance(val, (int, float)) and 1 <= val <= 5:
                return field_name, val

        # Pattern B: category attributes (dining_mode, meal_type, etc.)
        # Confirmed structure from debug:
        # attr[2] = [[[['E:DINE_IN'], 'Dine in', 2, None, ...]], 1]
        # attr[2][0] = [[['E:DINE_IN'], 'Dine in', 2, ...]]  ← list of options
        # attr[2][0][N] = [['E:DINE_IN'], 'Dine in', 2, ...]
        # attr[2][0][N][1] = 'Dine in'  ← display value
        options_outer = g(attr, 2, default=None)
        if isinstance(options_outer, list) and options_outer:
            values = []
            options_inner = options_outer[0] if isinstance(options_outer[0], list) else options_outer
            for option in options_inner:
                if isinstance(option, list) and len(option) > 1:
                    display_val = option[1]
                    if isinstance(display_val, str) and display_val:
                        values.append(display_val)
            if values:
                return field_name, ", ".join(values)

        return None

# ── Pagination Token Builder ──────────────────────────────────────────────────

def build_next_url(base_url: str, next_token: str) -> str:
    """
    Swap the pagination token in the pb= parameter.
    The page token appears after !2s in the pb param.
    Format: ...!2m2!1i10!2s<CURRENT_TOKEN>!5m2...
    We replace CURRENT_TOKEN with next_token.
    """
    import urllib.parse

    try:
        parsed    = urllib.parse.urlparse(base_url)
        params    = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        pb        = params.get("pb", [""])[0]

        # The token sits between !2s and the next ! segment
        # Replace it with the new token
        new_pb = re.sub(r'(!2s)CAESY0[^!]*', r'\g<1>' + next_token, pb, count=1)

        if new_pb == pb:
            log.warning("build_next_url: token pattern not found in pb param — pagination may break")

        params["pb"]  = [new_pb]
        new_query     = urllib.parse.urlencode(params, doseq=True)
        return urllib.parse.urlunparse(parsed._replace(query=new_query))
    except Exception as e:
        log.warning(f"Failed to build next URL: {e}")
        return base_url

# ── HTTP Client ───────────────────────────────────────────────────────────────

class APIClient:
    def __init__(self, base_url: str, cookie: str = ""):
        self.base_url = base_url
        self.headers = {
            "accept":                        "*/*",
            "accept-language":               "en-US,en;q=0.9",
            "accept-encoding":               "gzip, deflate, br, zstd",
            "referer":                       "https://www.google.com/",
            "user-agent":                    (
                "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:148.0) "
                "Gecko/20100101 Firefox/148.0"
            ),
            "x-maps-diversion-context-bin":  "CAE=",
            "alt-used":                      "www.google.com",
            "connection":                    "keep-alive",
            "sec-fetch-dest":                "empty",
            "sec-fetch-mode":                "cors",
            "sec-fetch-site":                "same-origin",
        }
        if cookie:
            self.headers["cookie"] = cookie

    async def fetch(self, url: str) -> Optional[str]:
        for attempt in range(CFG["MAX_RETRIES"]):
            try:
                async with httpx.AsyncClient(
                    timeout=CFG["TIMEOUT"],
                    follow_redirects=True,
                ) as client:
                    resp = await client.get(url, headers=self.headers)
                    if resp.status_code == 200:
                        return resp.text
                    elif resp.status_code == 429:
                        wait = CFG["RETRY_DELAY"] * (attempt + 2)
                        log.warning(f"Rate limited (429) — waiting {wait:.0f}s")
                        await asyncio.sleep(wait)
                    else:
                        log.warning(f"HTTP {resp.status_code} for {url[:80]}")
                        await asyncio.sleep(CFG["RETRY_DELAY"])
            except httpx.TimeoutException:
                log.warning(f"Timeout on attempt {attempt+1}/{CFG['MAX_RETRIES']}")
                await asyncio.sleep(CFG["RETRY_DELAY"])
            except Exception as e:
                log.warning(f"Request error: {e}")
                await asyncio.sleep(CFG["RETRY_DELAY"])
        return None

# ── Main Scraper ──────────────────────────────────────────────────────────────

async def scrape(url: str, output_csv: str, place_name: str, max_reviews: int, cookie: str = ""):
    csv_writer = ReviewCSVWriter(output_csv)
    csv_writer.open()

    client = APIClient(url, cookie=cookie)
    parser = ResponseParser()

    current_url  = url
    total        = 0
    page         = 0
    start_time   = time.time()
    no_new_pages = 0

    log.info(f"Starting network scrape")
    log.info(f"Place     : {place_name}")
    log.info(f"Output    : {output_csv}")
    log.info(f"Max       : {max_reviews:,}")

    try:
        while current_url:
            if total >= max_reviews:
                log.info(f"✅  Max reviews ({max_reviews:,}) reached")
                break

            page += 1
            text = await client.fetch(current_url)

            if not text:
                no_new_pages += 1
                if no_new_pages >= 3:
                    log.info("3 consecutive failed pages — stopping")
                    break
                await asyncio.sleep(CFG["RETRY_DELAY"])
                continue

            reviews, next_token = parser.parse(text)

            if not reviews:
                no_new_pages += 1
                log.info(f"Page {page}: no reviews found (attempt {no_new_pages}/3)")
                if no_new_pages >= 3:
                    log.info("No more reviews — done")
                    break
                await asyncio.sleep(CFG["RETRY_DELAY"])
                continue

            no_new_pages = 0
            written = 0
            for review in reviews:
                review["place_name"] = place_name
                if csv_writer.write(review):
                    written += 1
                    total += 1

            elapsed = int(time.time() - start_time)
            rate    = total / elapsed * 60 if elapsed > 0 else 0
            log.info(
                f"Page {page:4d} | +{written:3d} reviews | "
                f"total={total:6,} | "
                f"rate={rate:.0f}/min | "
                f"elapsed={elapsed}s"
            )

            if not next_token:
                log.info("No next page token — reached end of reviews")
                break

            # Also stop if we wrote 0 new reviews for 3 consecutive pages
            # (all duplicates = we've looped back to start)

            # Build URL for next page
            current_url = build_next_url(url, next_token)

            # Polite delay
            await asyncio.sleep(random.uniform(*CFG["DELAY_BETWEEN_PAGES"]))

    finally:
        csv_writer.close()
        elapsed = int(time.time() - start_time)
        log.info(
            f"\n{'='*60}\n"
            f"  DONE\n"
            f"  Total reviews : {total:,}\n"
            f"  Pages fetched : {page}\n"
            f"  Elapsed       : {elapsed}s ({elapsed//60}m {elapsed%60}s)\n"
            f"  Output        : {output_csv}\n"
            f"{'='*60}"
        )


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Google Maps Reviews — Network API Scraper",
        epilog="""
How to get the URL:
  1. Open Google Maps → navigate to the restaurant
  2. Click Reviews tab
  3. Open DevTools (F12) → Network tab → filter by "listugcposts"
  4. Scroll a few reviews to trigger the API call
  5. Right-click the request → Copy → Copy URL
  6. Paste it as the --url argument
        """
    )
    p.add_argument("--url",   required=True, help="Full listugcposts API URL")
    p.add_argument("--output", default="reviews_network.csv", help="Output CSV file")
    p.add_argument("--place",  default="Unknown Place", help="Place name for CSV")
    p.add_argument("--max",    type=int, default=0, help="Max reviews (0 = unlimited)")
    p.add_argument("--cookie", default="",
                   help="Browser cookie string (from DevTools → Copy as cURL → cookie header). "
                        "Required if you get 403 errors.")
    return p.parse_args()


if __name__ == "__main__":
    args  = parse_args()
    limit = args.max if args.max > 0 else CFG["MAX_REVIEWS"]
    asyncio.run(scrape(args.url, args.output, args.place, limit, cookie=args.cookie))