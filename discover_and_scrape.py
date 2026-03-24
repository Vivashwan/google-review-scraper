"""
Scrape multiple Google Maps places using gmaps_hybrid_scraper.py logic.

Expected input CSV columns:
  place_name,maps_url,review_count,scraped

Examples:
  python3 discover_and_scrape.py --query "restaurants in Mumbai" --top 20 --output mumbai_reviews.csv --speed fast
  python3 discover_and_scrape.py --places-file discovered_places.csv --top 10 --output all_reviews.csv
  python3 discover_and_scrape.py --places-file discovered_places.csv --top 50 --speed turbo --headless
"""

import argparse
import asyncio
import csv
import time
import random
import urllib.parse
import signal
import re
from pathlib import Path

from playwright.async_api import async_playwright

from gmaps_hybrid_scraper import (
    CFG,
    SPEED_PROFILES,
    GoogleMapsHybridScraper,
    parse_runtime,
    log,
)

PLACES_FIELDS = ["place_name", "maps_url", "rating", "review_count", "scraped"]
MAX_REVIEW_COUNT = 2200
STOP_REQUESTED = False


def _request_stop(_sig=None, _frame=None):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    log.warning("Kill switch triggered — stopping after current step...")


def load_places(filepath: str) -> list[dict]:
    if not Path(filepath).exists():
        raise FileNotFoundError(f"Places file not found: {filepath}")

    rows = []
    with open(filepath, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            url = (row.get("maps_url") or "").strip()
            if not url:
                continue
            rows.append({
                "place_name": (row.get("place_name") or "").strip() or "Unknown Place",
                "maps_url": url,
                "rating": (row.get("rating") or "").strip(),
                "review_count": int((row.get("review_count") or "0").replace(",", "") or 0),
                "scraped": (row.get("scraped") or "false").strip().lower() == "true",
            })
    return rows


def save_places(rows: list[dict], filepath: str):
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PLACES_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({
                "place_name": r.get("place_name", ""),
                "maps_url": r.get("maps_url", ""),
                "rating": r.get("rating", ""),
                "review_count": int(r.get("review_count", 0) or 0),
                "scraped": "true" if r.get("scraped") else "false",
            })


def select_targets(rows: list[dict], top: int, include_scraped: bool) -> list[dict]:
    if include_scraped:
        pool = rows
    else:
        pool = [r for r in rows if not r.get("scraped")]

    # Keep only restaurants with <= 3000 reviews
    pool = [r for r in pool if int(r.get("review_count", 0) or 0) <= MAX_REVIEW_COUNT]
    pool = sorted(pool, key=lambda r: int(r.get("review_count", 0) or 0), reverse=True)
    return pool[:top] if top > 0 else pool


async def discover_places(query: str, max_places: int) -> list[dict]:
    log.info(f"Discovery query: {query}")
    found: dict[str, dict] = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=CFG["HEADLESS"])
        context = await browser.new_context(
            viewport={"width": CFG["VIEWPORT"][0], "height": CFG["VIEWPORT"][1]},
            user_agent=CFG["USER_AGENT"],
            locale=CFG["LOCALE"],
            timezone_id=CFG["TIMEZONE"],
        )
        page = await context.new_page()

        q = urllib.parse.quote_plus(query)
        await page.goto(f"https://www.google.com/maps/search/{q}/", wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(random.uniform(2.0, 3.5))

        # consent (best-effort)
        for sel in ['button[aria-label*="Accept all"]', 'button:has-text("Accept all")', '#L2AGLb']:
            try:
                btn = await page.query_selector(sel)
                if btn:
                    await btn.click()
                    await asyncio.sleep(1.0)
                    break
            except Exception:
                continue

        async def extract_once():
            return await page.evaluate(
                r"""
                () => {
                    const rows = [];
                    const anchors = document.querySelectorAll('a[href*="/maps/place/"]');
                    const seen = new Set();
                    for (const a of anchors) {
                        const href = (a.href || '').split('?')[0];
                        if (!href || seen.has(href)) continue;
                        seen.add(href);

                        let card = a;
                        for (let i = 0; i < 8; i++) {
                            if (!card.parentElement) break;
                            card = card.parentElement;
                            if (card.getAttribute('role') === 'article' || card.classList.contains('Nv2PK')) break;
                        }

                        let name = (a.getAttribute('aria-label') || '').trim();
                        if (!name) {
                            const h = card.querySelector('.qBF1Pd,.NrDZNb,h3');
                            name = h ? (h.innerText || '').trim() : '';
                        }
                        if (!name) continue;

                        let rating = '';
                        let review_count = 0;

                        const scan = (txt) => {
                            if (!txt) return;
                            const t = String(txt).replace(/,/g, '');
                            const rm = t.match(/(\d+(?:\.\d+)?)\s*([km])?\s+reviews?/i);
                            if (rm) {
                                let n = parseFloat(rm[1]);
                                const s = (rm[2] || '').toLowerCase();
                                if (s === 'k') n *= 1000;
                                if (s === 'm') n *= 1000000;
                                review_count = Math.max(review_count, Math.round(n));
                            }
                            if (!rating) {
                                const r = t.match(/(\d+\.\d+)/);
                                if (r) rating = r[1];
                            }
                        };

                        scan(a.getAttribute('aria-label') || '');
                        for (const el of card.querySelectorAll('[aria-label]')) {
                            scan(el.getAttribute('aria-label') || '');
                        }

                        rows.push({ place_name: name, maps_url: href, rating, review_count });
                    }
                    return rows;
                }
                """
            )

        plateau = 0
        while len(found) < max_places and plateau < 10 and not STOP_REQUESTED:
            raw = await extract_once()
            new_in_this_round = 0
            for item in raw:
                url = item.get("maps_url", "")
                if not url:
                    continue
                prev = found.get(url)
                if prev is None:
                    found[url] = {
                        "place_name": item.get("place_name", "Unknown Place"),
                        "maps_url": url,
                        "rating": item.get("rating", ""),
                        "review_count": int(item.get("review_count", 0) or 0),
                        "scraped": False,
                    }
                    new_in_this_round += 1
                else:
                    prev["review_count"] = max(prev["review_count"], int(item.get("review_count", 0) or 0))

            if new_in_this_round == 0:
                plateau += 1
            else:
                plateau = 0

            await page.evaluate(
                """() => {
                    const feed = document.querySelector('div[role="feed"]') ||
                                 document.querySelector('div.m6QErb.DxyBCb.kA9KIf.dS8AEf');
                    if (feed) feed.scrollBy({ top: 1200, behavior: 'instant' });
                    else window.scrollBy({ top: 1200, behavior: 'instant' });
                }"""
            )
            await asyncio.sleep(random.uniform(1.0, 2.2))

        await context.close()
        await browser.close()

    rows = sorted(found.values(), key=lambda r: r["review_count"], reverse=True)
    log.info(f"Discovery complete: {len(rows)} places found")
    return rows


async def scrape_places(places_file: str, output: str, top: int, include_scraped: bool, query: str = "", discover_limit: int = 0):
    rows = load_places(places_file) if Path(places_file).exists() else []

    if query:
        limit = discover_limit if discover_limit > 0 else max(top, 50)
        discovered = await discover_places(query=query, max_places=limit)
        by_url = {r["maps_url"]: r for r in rows}
        added = 0
        for d in discovered:
            existing = by_url.get(d["maps_url"])
            if existing:
                existing["review_count"] = max(existing["review_count"], d["review_count"])
                if not existing.get("rating"):
                    existing["rating"] = d.get("rating", "")
                if not existing.get("place_name") or existing["place_name"] == "Unknown Place":
                    existing["place_name"] = d.get("place_name", existing["place_name"])
            else:
                rows.append(d)
                added += 1
        save_places(rows, places_file)
        log.info(f"Discovery merged: {added} new places (total now {len(rows)})")

    if not rows:
        raise FileNotFoundError(
            f"No places available. Provide --query or a valid --places-file: {places_file}"
        )

    targets = select_targets(rows, top, include_scraped)

    if not targets:
        log.info("Nothing to scrape.")
        return

    log.info(f"Loaded {len(rows)} places from {places_file}")
    log.info(f"Selected {len(targets)} target places (review_count <= {MAX_REVIEW_COUNT})")

    output_dir = Path(output)
    output_dir.mkdir(parents=True, exist_ok=True)

    def per_place_output(place_name: str) -> str:
        slug = re.sub(r"[^a-z0-9]+", "_", place_name.lower()).strip("_")
        slug = slug or "unknown_place"
        return str(output_dir / f"{slug}.csv")

    start = time.time()
    for idx, place in enumerate(targets, start=1):
        if STOP_REQUESTED:
            log.warning("Kill switch active — ending batch run.")
            break

        name = place["place_name"]
        expected_total = int(place.get("review_count", 0) or 0)
        output_file = per_place_output(name)
        log.info(
            f"\n[{idx}/{len(targets)}] Scraping: {name} "
            f"(expected reviews: {expected_total or 'unknown'}) "
            f"-> {output_file}"
        )

        scraper = GoogleMapsHybridScraper(
            url=place["maps_url"],
            output_csv=output_file,
            place_name=name,
            expected_total=expected_total,
        )
        result = await scraper.run()
        if result and result.get("status") == "browser_closed":
            log.warning("Detected browser close event — stopping batch run.")
            break

        place["scraped"] = True
        save_places(rows, places_file)

    elapsed = int(time.time() - start)
    log.info(
        f"\n{'='*60}\n"
        f"  DONE\n"
        f"  Places scraped : {len(targets)}\n"
        f"  Places file    : {places_file}\n"
        f"  Output reviews : {output}\n"
        f"  Elapsed        : {elapsed}s ({elapsed//60}m {elapsed%60}s)\n"
        f"{'='*60}"
    )


def parse_args():
    p = argparse.ArgumentParser(
        description="Batch scrape restaurants from a CSV input (with review_count)."
    )
    p.add_argument("--query", default="", help="Optional Google Maps search query, e.g. 'restaurants in Mumbai'")
    p.add_argument("--discover-limit", type=int, default=0,
                   help="Max places to discover when --query is used (default: max(top, 50)).")
    p.add_argument("--places-file", default="discovered_places.csv")
    p.add_argument("--output", default="reviews_by_place",
                   help="Directory for per-restaurant CSV files.")
    p.add_argument("--top", type=int, default=10, help="0 = scrape all selected rows")
    p.add_argument("--include-scraped", action="store_true",
                   help="Scrape rows even if scraped=true")
    p.add_argument("--speed", choices=["turbo", "fast", "safe"], default="fast")
    p.add_argument("--runtime", default=None, help="Per-place max runtime: 8h, 3d, 90m")
    p.add_argument("--headless", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    CFG["HEADLESS"] = args.headless
    CFG["MAX_RUNTIME_SECONDS"] = parse_runtime(args.runtime)
    CFG.update(SPEED_PROFILES.get(args.speed, SPEED_PROFILES["fast"]))

    log.info(
        f"Speed: {args.speed.upper()} | Runtime per place: "
        f"{args.runtime or 'unlimited'} | Headless: {args.headless}"
    )

    asyncio.run(
        scrape_places(
            places_file=args.places_file,
            output=args.output,
            top=args.top,
            include_scraped=args.include_scraped,
            query=args.query,
            discover_limit=args.discover_limit,
        )
    )
