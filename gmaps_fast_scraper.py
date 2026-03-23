"""
Google Maps Reviews — Fast Network Scraper
==========================================
Direct API calls, no browser, no DOM, no scrolling overhead.
Uses the fully-tested parser from the hybrid scraper.

Speed: ~500-2000 reviews/min (limited only by Google's API rate)

How to get the URL:
  1. Open Maps → Reviews tab → Sort by Newest
  2. DevTools → Network → filter "listugcposts"
  3. Scroll once → right-click request → Copy URL
  4. Change !1i10 to !1i50 in the URL for 50 reviews per page

Usage:
    pip install httpx
    python3 gmaps_fast_scraper.py --url "https://..." --cookie "NID=..." --output reviews.csv
"""

import asyncio, csv, json, re, sys, time, logging, argparse, random
from pathlib import Path
from typing import Optional
import urllib.parse
import httpx

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("fast_scraper.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("GMapsFast")

# ── Attribute maps (fully confirmed from live API) ────────────────────────────
ATTR_KEY_MAP = {
    "GUIDED_DINING_MODE":                        "dining_mode",
    "GUIDED_DINING_MEAL_TYPE":                   "meal_type",
    "GUIDED_DINING_PRICE_RANGE":                 "price_per_person",
    "GUIDED_DINING_FOOD_ASPECT":                 "food",
    "GUIDED_DINING_SERVICE_ASPECT":              "service_rating",
    "GUIDED_DINING_ATMOSPHERE_ASPECT":           "atmosphere",
    "GUIDED_DINING_SEATING_TYPE":                "seating_type",
    "GUIDED_DINING_WAIT_TIME":                   "wait_time",
    "GUIDED_DINING_PARKING_SPACE_AVAILABILITY":  "parking_space",
    "GUIDED_DINING_PARKING_OPTIONS":             "parking_options",
    "GUIDED_DINING_GROUP_SIZE":                  "group_size",
    "GUIDED_DINING_SPECIAL_EVENTS":              "special_events",
    "GUIDED_DINING_NOISE_LEVEL":                 "noise_level",
    "GUIDED_DINING_RESERVATIONS":                "reservations",
    "GUIDED_DINING_RECOMMEND_TO_VEGETARIANS":    "vegetarian_recommendation",
    "GUIDED_DINING_VEGETARIAN_OFFERINGS_INFO":   "vegetarian_offerings",
    "GUIDED_DINING_DISH_RECOMMENDATION":         "recommended_dishes",
}

TIPS_VALUE_KEY_MAP = {
    "GUIDED_DINING_KID_FRIENDLINESS_TIPS":              "kid_friendliness",
    "GUIDED_DINING_ACCESSIBILITY_TIPS":                 "wheelchair_accessibility",
    "GUIDED_DINING_VEGETARIAN_OPTIONS_TIPS":            "vegetarian_options",
    "GUIDED_DINING_DIETARY_RESTRICTIONS_TIPS":          "dietary_restrictions",
    "GUIDED_DINING_OTHER_DIETARY_RESTRICTIONS_TIPS":    "dietary_restrictions",
    "GUIDED_DINING_PARKING_TIPS":                       "parking_notes",
}

# ── Parser (confirmed positions from live debug) ──────────────────────────────
def safe_get(obj, *keys, default=None):
    cur = obj
    for k in keys:
        try: cur = cur[k]
        except: return default
    return cur

def parse_attribute(attr):
    g = safe_get
    key_raw = g(attr, 0, 0, default=None)
    if not key_raw:
        return None

    # Freetext tips: value at attr[10]
    if key_raw in TIPS_VALUE_KEY_MAP:
        field = TIPS_VALUE_KEY_MAP[key_raw]
        val_list = g(attr, 10, default=None)
        if isinstance(val_list, list) and val_list:
            val = ", ".join(str(v) for v in val_list if v)
            if val: return field, val
        return None

    # Skip TIPS_TOPICS (covered by dedicated keys above)
    if key_raw == "GUIDED_DINING_TIPS_TOPICS":
        return None

    if key_raw not in ATTR_KEY_MAP:
        return None

    field = ATTR_KEY_MAP[key_raw]

    # Numeric sub-ratings: attr[11] = [value]
    rc = g(attr, 11, default=None)
    if isinstance(rc, list) and rc:
        val = rc[0]
        if isinstance(val, (int, float)) and 1 <= val <= 5:
            return field, val

    # Category/multi-select: try attr[2] then attr[3]
    def extract(options_outer):
        if not isinstance(options_outer, list): return []
        vals = []
        for group in options_outer:
            if not isinstance(group, list): continue
            for opt in group:
                if isinstance(opt, list) and len(opt) > 1:
                    dv = opt[1]
                    if isinstance(dv, str) and dv:
                        vals.append(dv)
        return vals

    for idx in (2, 3):
        vals = extract(g(attr, idx, default=None))
        if vals: return field, ", ".join(vals)

    return None

def parse_review(r):
    """Parse single review array. All positions confirmed from live API."""
    g = safe_get

    review_id = g(r, 0, default=None)
    if not isinstance(review_id, str) or len(review_id) < 10:
        return None

    date          = str(g(r, 1, 6, default="") or "")
    reviewer_name = str(g(r, 1, 4, 5, 0, default="") or "")

    reviewer_id = ""
    profile_url = str(g(r, 1, 4, 2, 0, default="") or "")
    m = re.search(r'/contrib/(\d+)', profile_url)
    if m: reviewer_id = m.group(1)

    lg_text    = str(g(r, 1, 4, 5, 10, 0, default="") or "")
    local_guide = "local guide" in lg_text.lower()
    rating      = g(r, 2, 0, 0, default=None)
    review_text = str(g(r, 2, 15, 0, 0, default="") or "").replace("\n", " ").strip()

    attrs = {}
    for attr in (g(r, 2, 6, default=[]) or []):
        result = parse_attribute(attr)
        if result:
            k, v = result
            if k not in attrs:
                attrs[k] = v

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

def parse_response(text):
    """Returns (reviews, next_token)."""
    text = text.strip()
    if text.startswith(")]}'"): text = text[4:].strip()
    try:
        data = json.loads(text)
    except:
        return [], None

    reviews_raw = safe_get(data, 2, default=[])
    if not isinstance(reviews_raw, list):
        return [], None

    reviews = []
    for entry in reviews_raw:
        if not isinstance(entry, list) or not entry: continue
        r = entry[0] if isinstance(entry[0], list) else entry
        parsed = parse_review(r)
        if parsed:
            reviews.append(parsed)

    # Next page token at data[2][N][2]
    next_token = None
    for entry in reviews_raw:
        if isinstance(entry, list) and len(entry) > 2:
            t = entry[2]
            if isinstance(t, str) and t.startswith("CAESY"):
                next_token = t

    return reviews, next_token

def build_next_url(base_url, token):
    parsed = urllib.parse.urlparse(base_url)
    params = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    pb = params.get("pb", [""])[0]
    # Replace the token after !2s
    new_pb = re.sub(r'(!2s)CAESY[^!]*', r'\g<1>' + token, pb, count=1)
    if new_pb == pb:
        log.warning("Token not found in pb param — pagination may break")
    params["pb"] = [new_pb]
    return urllib.parse.urlunparse(parsed._replace(
        query=urllib.parse.urlencode(params, doseq=True)
    ))

# ── CSV Writer ────────────────────────────────────────────────────────────────
class CSVWriter:
    FIELDS = [
        "review_id", "place_name", "reviewer_name", "reviewer_id",
        "local_guide", "rating", "review_text", "likes", "date", "attributes",
    ]

    def __init__(self, path):
        self.path     = path
        self.seen_ids = set()
        self._load_existing()

    def _load_existing(self):
        if not Path(self.path).exists(): return
        with open(self.path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                rid = row.get("review_id") or (row.get("reviewer_id","") + "_" + row.get("date",""))
                if rid: self.seen_ids.add(rid)
        log.info(f"Resumed — {len(self.seen_ids)} existing reviews")

    def open(self):
        is_new = not Path(self.path).exists()
        self._f = open(self.path, "a", newline="", encoding="utf-8")
        self._w = csv.DictWriter(self._f, fieldnames=self.FIELDS)
        if is_new: self._w.writeheader()

    def write(self, review):
        rid = review.get("review_id") or (review.get("reviewer_id","") + "_" + review.get("date",""))
        if not rid or rid in self.seen_ids: return False
        self.seen_ids.add(rid)
        self._w.writerow({f: review.get(f, "") for f in self.FIELDS})
        self._f.flush()
        return True

    def close(self):
        if hasattr(self, '_f'): self._f.close()

# ── HTTP fetch ────────────────────────────────────────────────────────────────
async def fetch(client, url, headers):
    for attempt in range(4):
        try:
            r = await client.get(url, headers=headers)
            if r.status_code == 200: return r.text
            if r.status_code == 429:
                wait = 10 * (attempt + 1)
                log.warning(f"Rate limited — waiting {wait}s")
                await asyncio.sleep(wait)
            else:
                log.warning(f"HTTP {r.status_code}")
                await asyncio.sleep(3)
        except Exception as e:
            log.warning(f"Request error: {e}")
            await asyncio.sleep(3)
    return None

# ── Main ──────────────────────────────────────────────────────────────────────
async def scrape(url, output, place, max_reviews, cookie, delay):
    headers = {
        "accept":                       "*/*",
        "accept-language":              "en-US,en;q=0.9",
        "accept-encoding":              "gzip, deflate, br, zstd",
        "referer":                      "https://www.google.com/",
        "user-agent":                   "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:148.0) Gecko/20100101 Firefox/148.0",
        "x-maps-diversion-context-bin": "CAE=",
        "connection":                   "keep-alive",
    }
    if cookie: headers["cookie"] = cookie

    writer = CSVWriter(output)
    writer.open()

    current_url = url
    total = 0
    page  = 0
    fails = 0
    start = time.time()

    log.info(f"Place  : {place}")
    log.info(f"Output : {output}")
    log.info(f"Delay  : {delay}s between pages")

    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
        while current_url:
            if max_reviews and total >= max_reviews:
                log.info("Max reviews reached")
                break

            page += 1
            text = await fetch(client, current_url, headers)

            if not text:
                fails += 1
                if fails >= 3:
                    log.error("3 consecutive failures — stopping")
                    break
                await asyncio.sleep(5)
                continue

            reviews, next_token = parse_response(text)

            if not reviews:
                fails += 1
                log.warning(f"Page {page}: no reviews parsed (fail {fails}/3)")
                if fails >= 3:
                    log.info("No more reviews")
                    break
                await asyncio.sleep(3)
                continue

            fails = 0
            written = sum(1 for r in reviews if writer.write({**r, "place_name": place}))
            total += written

            elapsed = int(time.time() - start)
            rate    = total / elapsed * 60 if elapsed > 0 else 0
            log.info(f"Page {page:4d} | +{written:3d} | total={total:6,} | {rate:.0f}/min | {elapsed}s")

            if not next_token:
                log.info("No next token — done")
                break

            current_url = build_next_url(url, next_token)
            await asyncio.sleep(delay + random.uniform(0, delay * 0.3))

    writer.close()
    elapsed = int(time.time() - start)
    log.info(f"\n{'='*60}\n  DONE  |  {total:,} reviews  |  {elapsed}s  |  {output}\n{'='*60}")

# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Google Maps Fast Network Scraper")
    p.add_argument("--url",    required=True, help="listugcposts URL (sort by Newest, page size !1i50)")
    p.add_argument("--output", default="reviews.csv")
    p.add_argument("--place",  default="Unknown Place")
    p.add_argument("--cookie", default="", help="Browser cookie string")
    p.add_argument("--max",    type=int, default=0)
    p.add_argument("--delay",  type=float, default=0.2, help="Seconds between pages (default 0.2)")
    args = p.parse_args()

    asyncio.run(scrape(
        url=args.url,
        output=args.output,
        place=args.place,
        max_reviews=args.max if args.max > 0 else 0,
        cookie=args.cookie,
        delay=args.delay,
    ))