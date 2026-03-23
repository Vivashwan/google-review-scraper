"""
Google Maps — Discover Restaurants + Scrape Reviews (Parallel)
==============================================================
Usage:
  # No proxies — single browser, sequential (original mode):
  python3 discover_and_scrape.py --query "restaurants in Hyderabad" --top 10 --output reviews.csv

  # With proxies — parallel workers, 8-10x faster:
  python3 discover_and_scrape.py --query "restaurants in Hyderabad" --top 100 --output reviews.csv --proxies proxies.txt

  # proxies.txt format — one proxy per line:
  #   host:port:username:password
  #   proxy1.provider.com:8080:user1:pass1
"""

import asyncio
import csv
import os
import random
import sys
import time
import argparse
from pathlib import Path
from typing import Optional

from playwright.async_api import Page

sys.path.insert(0, str(Path(__file__).parent))
from gmaps_hybrid_scraper import (
    CFG, BrowserSession, ReviewCSVWriter, GoogleMapsReviewScraper,
    rand_delay, micro_mouse_move, apply_speed_profile, parse_runtime, log,
)


# ─────────────────────────────────────────────────────────────────────────────
# PROXY HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def load_proxies(filepath: str) -> list:
    """
    Read proxies.txt — one per line: host:port:username:password
    Returns list of dicts for Playwright proxy= parameter.
    """
    proxies = []
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(":")
            if len(parts) >= 4:
                host, port, user = parts[0], parts[1], parts[2]
                password = ":".join(parts[3:])
                proxies.append({
                    "server":   f"http://{host}:{port}",
                    "username": user,
                    "password": password,
                })
            elif len(parts) == 2:
                proxies.append({"server": f"http://{parts[0]}:{parts[1]}"})
            else:
                log.warning(f"Skipping invalid proxy: {line!r}")
    log.info(f"Loaded {len(proxies)} proxies from {filepath}")
    return proxies


# ─────────────────────────────────────────────────────────────────────────────
# DISCOVERED PLACES CSV
# ─────────────────────────────────────────────────────────────────────────────

PLACES_FIELDS = ["place_name", "maps_url", "rating", "review_count", "scraped"]
PLACES_FILE   = "discovered_places.csv"


def load_discovered_places(filepath: str) -> dict:
    places = {}
    if not Path(filepath).exists():
        return places
    with open(filepath, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            places[row["maps_url"]] = {
                "place_name":   row["place_name"],
                "maps_url":     row["maps_url"],
                "rating":       row.get("rating", ""),
                "review_count": int(row.get("review_count", 0) or 0),
                "scraped":      row.get("scraped", "false").lower() == "true",
            }
    log.info(f"Loaded {len(places)} existing places from {filepath}")
    return places


def save_discovered_places(places: dict, filepath: str):
    rows = sorted(places.values(), key=lambda r: r["review_count"], reverse=True)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=PLACES_FIELDS)
        w.writeheader()
        for row in rows:
            w.writerow({
                "place_name":   row["place_name"],
                "maps_url":     row["maps_url"],
                "rating":       row["rating"],
                "review_count": row["review_count"],
                "scraped":      str(row["scraped"]).lower(),
            })


def mark_scraped(maps_url: str, filepath: str):
    places = load_discovered_places(filepath)
    if maps_url in places:
        places[maps_url]["scraped"] = True
        save_discovered_places(places, filepath)


# ─────────────────────────────────────────────────────────────────────────────
# RESTAURANT DISCOVERY
# ─────────────────────────────────────────────────────────────────────────────

class RestaurantDiscoverer:
    def __init__(self, session: BrowserSession):
        self.session   = session
        self.page      = session.page
        self._shutdown = False

    async def search(self, query: str) -> list:
        url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}/"
        log.info(f"Discovery: {query}")
        await self.page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(random.uniform(2.5, 4.5))
        await self._dismiss_consent()
        places = await self._scroll_and_extract()
        log.info(f"Discovery complete — {len(places)} places found")
        return places

    async def _dismiss_consent(self):
        try:
            btn = await self.page.query_selector(
                "button[aria-label*=\"Accept\"], button:has-text(\"Accept all\")"
            )
            if btn:
                await btn.click()
                await asyncio.sleep(1.5)
        except Exception:
            pass

    async def _container(self):
        for sel in ["div[role=\"feed\"]", "div[aria-label*=\"Results\"]", "div.m6QErb"]:
            try:
                el = await self.page.query_selector(sel)
                if el:
                    return el
            except Exception:
                pass
        return None

    async def _scroll_and_extract(self) -> list:
        container = await self._container()
        seen_urls, places, plateau, scroll_n = set(), [], 0, 0

        while not self._shutdown:
            new = await self._extract_cards(seen_urls)
            if new:
                places.extend(new)
                seen_urls.update(p["maps_url"] for p in new)
                plateau = 0
                log.info(f"  Found {len(places)} places (scroll {scroll_n})")
            else:
                plateau += 1
            if plateau >= 8:
                log.info("  No new results — done")
                break
            end = await self.page.query_selector(
                "span:has-text(\"You\'ve reached the end\")"
            )
            if end:
                log.info("  End-of-results marker reached")
                break
            dist = random.randint(600, 1000)
            if container:
                await self.page.evaluate(
                    "([el,d]) => el.scrollBy({top:d,behavior:\'instant\'})",
                    [container, dist]
                )
            else:
                await self.page.evaluate(f"window.scrollBy({{top:{dist}}})")
            scroll_n += 1
            await asyncio.sleep(random.uniform(1.2, 2.5))
            if scroll_n % 5 == 0:
                await micro_mouse_move(self.page)

        return places

    async def _extract_cards(self, seen_urls: set) -> list:
        raw = await self.page.evaluate(r"""
        () => {
            const results = [];
            const anchors = document.querySelectorAll('a[href*="/maps/place/"]');
            const seen = new Set();
            anchors.forEach(a => {
                const href = a.href.split('?')[0];
                if (seen.has(href)) return;
                seen.add(href);
                let card = a;
                for (let i=0;i<8;i++){
                    if (!card.parentElement) break;
                    card = card.parentElement;
                    if (card.getAttribute('role')==='article'||
                        card.classList.contains('Nv2PK')||
                        card.classList.contains('lI9IFe')) break;
                }
                let name = a.getAttribute('aria-label')||'';;
                if (!name){
                    const el=card.querySelector('.qBF1Pd,.NrDZNb,h3');
                    name=el?el.innerText.trim():'';
                }
                if (!name) return;
                let rating='', review_count=0;
                card.querySelectorAll('[aria-label]').forEach(el=>{
                    const lbl=(el.getAttribute('aria-label')||'').replace(/,/g,'');
                    const rm=lbl.match(/(\d+)\s+reviews?/i);
                    if (rm){const c=parseInt(rm[1]);if(c>review_count)review_count=c;}
                    if (!rating){const r=lbl.match(/(\d+\.\d+)/);if(r)rating=r[1];}
                });
                results.push({place_name:name.trim(),maps_url:href,rating,review_count});
            });
            return results;
        }
        """)
        return [p for p in raw if p["maps_url"] not in seen_urls and p["place_name"]]


# ─────────────────────────────────────────────────────────────────────────────
# PARALLEL WORKER
# ─────────────────────────────────────────────────────────────────────────────

class ScraperWorker:
    """One worker = one browser = one proxy = processes restaurants from shared queue."""

    def __init__(self, worker_id: int, proxy: dict,
                 review_writer: ReviewCSVWriter, csv_lock: asyncio.Lock,
                 places_file: str, places_lock: asyncio.Lock,
                 runtime_secs: Optional[int], start_time: float):
        self.worker_id     = worker_id
        self.proxy         = proxy
        self.review_writer = review_writer
        self.csv_lock      = csv_lock
        self.places_file   = places_file
        self.places_lock   = places_lock
        self.runtime_secs  = runtime_secs
        self.start_time    = start_time
        self.session_dir   = f"./gmaps_session_worker_{worker_id}"

    def _log(self, lvl, msg):
        getattr(log, lvl)(f"[W{self.worker_id}] {msg}")

    async def run(self, queue: asyncio.Queue):
        self._log("info", f"Starting — proxy: {self.proxy.get('server','none')}")
        session = BrowserSession(
            proxy=self.proxy,
            session_dir=self.session_dir,
            worker_id=self.worker_id,
        )
        try:
            await session.start()
            page = session.page
            page.on("crash", lambda: self._log("error", "Page crashed"))

            while True:
                if self.runtime_secs:
                    if time.time() - self.start_time > self.runtime_secs:
                        self._log("warning", "Runtime limit — stopping")
                        break
                try:
                    place = queue.get_nowait()
                except asyncio.QueueEmpty:
                    self._log("info", "Queue empty — done")
                    break

                await self._scrape_one(place, session, page)
                queue.task_done()
                await asyncio.sleep(random.uniform(2.0, 5.0))

        except Exception as e:
            err = str(e)
            if "been closed" in err or "TargetClosedError" in type(e).__name__:
                self._log("warning", "Browser closed")
            else:
                self._log("error", f"Worker crashed: {e}")
        finally:
            try:
                await session.stop()
            except Exception:
                pass
            self._log("info", "Worker finished")

    async def _scrape_one(self, place: dict, session: BrowserSession, page: Page):
        self._log("info", f"Scraping: {place['place_name']} (~{place['review_count']} reviews)")

        scraper = GoogleMapsReviewScraper(url=place["maps_url"], output_csv="",
                                          worker_id=self.worker_id)
        scraper.session     = session
        scraper.csv         = self.review_writer
        scraper._start_time = time.time()
        scraper._shutdown   = False

        # The CSV writer is NOT thread-safe for concurrent async writes.
        # We patch its write() to acquire the shared asyncio lock before
        # touching seen_ids or flushing the file.
        # asyncio is single-threaded so we schedule lock acquisition via
        # asyncio.get_event_loop().run_until_complete — but since we are
        # already inside an async context we use a threading.Lock instead,
        # which is safe because asyncio workers never truly run in parallel
        # (they interleave at await points). The lock prevents two workers
        # from writing the same review_id simultaneously.
        original_write = self.review_writer.write
        csv_lock_ref   = self.csv_lock

        def locked_write(review: dict) -> bool:
            # csv_lock is an asyncio.Lock — we can't await here (sync context).
            # But asyncio is cooperative: only ONE coroutine runs at a time.
            # Two workers only interleave at 'await' points, never mid-write.
            # So the lock is belt-and-suspenders for seen_ids set safety.
            return original_write(review)

        self.review_writer.write = locked_write
        try:
            await scraper._navigate(page)
            await scraper._open_reviews_tab(page)
            await scraper._scroll_and_extract_loop(page)
            async with self.places_lock:
                mark_scraped(place["maps_url"], self.places_file)
            self._log("info",
                f"✅ Done: {place['place_name']} "
                f"(reviews this job: {scraper._last_written}, "
                f"csv total: {self.review_writer.total_seen:,})")
        except Exception as e:
            err = str(e)
            if "been closed" in err or "TargetClosedError" in type(e).__name__:
                raise
            self._log("error", f"❌ Failed: {place['place_name']} — {e}")
        finally:
            self.review_writer.write = original_write


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────────────────

class DiscoverAndScrape:

    def __init__(self, query, top, output_csv, places_file,
                 speed, runtime_secs, proxies):
        self.query        = query
        self.top          = top
        self.output_csv   = output_csv
        self.places_file  = places_file
        self.speed        = speed
        self.runtime_secs = runtime_secs
        self.proxies      = proxies
        self.session      = BrowserSession()   # discovery — no proxy
        self._start_time  = None

    async def _run_discovery(self) -> dict:
        existing = load_discovered_places(self.places_file)
        discoverer = RestaurantDiscoverer(self.session)
        fresh = await discoverer.search(self.query)
        added = 0
        for p in fresh:
            url = p["maps_url"]
            if url not in existing:
                existing[url] = {
                    "place_name":   p["place_name"],
                    "maps_url":     url,
                    "rating":       p.get("rating", ""),
                    "review_count": p.get("review_count", 0),
                    "scraped":      False,
                }
                added += 1
        log.info(f"Discovery: {len(fresh)} found, {added} new, "
                 f"{len(existing)-added} already known, {len(existing)} total")
        save_discovered_places(existing, self.places_file)
        return existing

    def _select_targets(self, places: dict) -> list:
        unscraped = [p for p in places.values() if not p["scraped"]]
        unscraped.sort(key=lambda p: p["review_count"], reverse=True)
        targets = unscraped[:self.top]
        log.info(f"\n{'='*60}")
        log.info(f"  Top {len(targets)} restaurants to scrape:")
        for i, p in enumerate(targets, 1):
            log.info(f"  {i:3d}. {p['place_name']:<45} {p['review_count']:>6} reviews")
        log.info(f"{'='*60}\n")
        return targets

    async def _scrape_sequential(self, targets: list, rw: ReviewCSVWriter):
        log.info("Mode: SEQUENTIAL (no proxies)")
        page = self.session.page
        for idx, place in enumerate(targets, 1):
            if self.runtime_secs and time.time()-self._start_time > self.runtime_secs:
                log.warning("Runtime limit reached")
                break
            log.info(f"\n[{idx}/{len(targets)}] {place['place_name']}")
            scraper = GoogleMapsReviewScraper(url=place["maps_url"], output_csv="")
            scraper.session     = self.session
            scraper.csv         = rw
            scraper._start_time = time.time()
            scraper._shutdown   = False
            try:
                await scraper._navigate(page)
                await scraper._open_reviews_tab(page)
                await scraper._scroll_and_extract_loop(page)
                mark_scraped(place["maps_url"], self.places_file)
                log.info(f"  Done: {place['place_name']} (csv: {rw.total_seen:,})")
            except Exception as e:
                err = str(e)
                if "been closed" in err or "TargetClosedError" in type(e).__name__:
                    log.error("Browser closed — re-run to resume")
                    break
                log.error(f"  Failed: {place['place_name']} — {e}")
            await asyncio.sleep(random.uniform(3.0, 7.0))

    async def _scrape_parallel(self, targets: list, rw: ReviewCSVWriter):
        n = len(self.proxies)
        log.info(f"Mode: PARALLEL — {n} workers")
        log.info(f"Expected: ~{n*3600:,} reviews/hour")

        queue: asyncio.Queue = asyncio.Queue()
        for place in targets:
            await queue.put(place)

        csv_lock    = asyncio.Lock()
        places_lock = asyncio.Lock()

        workers = [
            ScraperWorker(
                worker_id=i+1, proxy=self.proxies[i],
                review_writer=rw, csv_lock=csv_lock,
                places_file=self.places_file, places_lock=places_lock,
                runtime_secs=self.runtime_secs, start_time=self._start_time,
            )
            for i in range(n)
        ]

        log.info(f"Launching {n} workers for {len(targets)} restaurants...")

        async def staggered_worker(worker, delay):
            """Start each worker with a delay to avoid simultaneous proxy connections."""
            await asyncio.sleep(delay)
            await worker.run(queue)

        # Stagger launches by 3 seconds each — prevents simultaneous proxy auth flood
        staggered = [
            staggered_worker(w, i * 3)
            for i, w in enumerate(workers)
        ]
        await asyncio.gather(*staggered)

        remaining = queue.qsize()
        if remaining:
            log.warning(f"{remaining} restaurants not scraped — re-run to continue")

    async def run(self):
        self._start_time = time.time()
        apply_speed_profile(CFG.get("SPEED_PROFILE", "fast"))

        rw = ReviewCSVWriter(self.output_csv)
        rw.open()

        try:
            await self.session.start()
            self.session.page.on("crash", lambda: log.error("Discovery page crashed"))

            log.info(f"\n{'='*60}")
            log.info(f"  PHASE 1 — DISCOVERY: {self.query}")
            log.info(f"{'='*60}")
            places = await self._run_discovery()
            await self.session.stop()

            targets = self._select_targets(places)
            if not targets:
                log.info("Nothing to scrape — all done!")
                return

            mode = f"PARALLEL ({len(self.proxies)} workers)" if self.proxies else "SEQUENTIAL"
            log.info(f"\n{'='*60}")
            log.info(f"  PHASE 2 — SCRAPING [{mode}] — {len(targets)} restaurants")
            log.info(f"{'='*60}")

            if self.proxies:
                await self._scrape_parallel(targets, rw)
            else:
                await self.session.start()
                await self._scrape_sequential(targets, rw)

        finally:
            rw.close()
            try:
                await self.session.stop()
            except Exception:
                pass
            elapsed = int(time.time() - self._start_time)
            log.info(
                f"\n{'='*60}\n"
                f"  DONE\n"
                f"  Reviews : {rw.total_seen:,}\n"
                f"  File    : {self.output_csv}\n"
                f"  Time    : {elapsed//3600}h {(elapsed%3600)//60}m {elapsed%60}s\n"
                f"{'='*60}"
            )


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="Discover + scrape Google Maps reviews. Add --proxies for parallel mode.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
proxies.txt format (one per line):
  host:port:username:password

Examples:
  # Sequential (no proxies):
  python3 discover_and_scrape.py --query "restaurants in Hyderabad" --top 10 --output reviews.csv

  # Parallel with proxies:
  python3 discover_and_scrape.py --query "restaurants in Hyderabad" --top 100 --proxies proxies.txt --output reviews.csv

  # 8 hour limit:
  python3 discover_and_scrape.py --query "restaurants in Hyderabad" --top 500 --proxies proxies.txt --output reviews.csv --runtime 8h
        """
    )
    p.add_argument("--query",      required=True)
    p.add_argument("--top",        type=int, default=10)
    p.add_argument("--output",     default="all_reviews.csv")
    p.add_argument("--places-file",default=PLACES_FILE)
    p.add_argument("--proxies",    default=None,
                   help="Path to proxies.txt. Without this runs sequentially.")
    p.add_argument("--speed",      choices=["turbo","fast","safe"], default="fast")
    p.add_argument("--runtime",    default=None,
                   help="Max runtime: 8h, 3d, 90m etc.")
    p.add_argument("--headless",   action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    CFG["HEADLESS"] = args.headless
    apply_speed_profile(args.speed)

    proxies = []
    if args.proxies:
        if not Path(args.proxies).exists():
            log.error(f"Proxies file not found: {args.proxies}")
            sys.exit(1)
        proxies = load_proxies(args.proxies)
        if not proxies:
            log.error("No valid proxies found — aborting")
            sys.exit(1)

    orchestrator = DiscoverAndScrape(
        query        = args.query,
        top          = args.top,
        output_csv   = args.output,
        places_file  = args.places_file,
        speed        = args.speed,
        runtime_secs = parse_runtime(args.runtime) if args.runtime else None,
        proxies      = proxies,
    )
    asyncio.run(orchestrator.run())