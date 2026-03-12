import scrapy
from scrapy_playwright.page import PageMethod
from playwright_stealth import Stealth
import logging
import random
import asyncio

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
# Human-like helpers                                                  #
# ------------------------------------------------------------------ #

async def human_delay(min_ms: int = 800, max_ms: int = 2500):
    """Random pause to mimic human reading/thinking time."""
    await asyncio.sleep(random.uniform(min_ms / 1000, max_ms / 1000))


async def human_scroll(page, container_selector: str = None, iterations: int = 15):
    """
    Scroll in irregular increments with random pauses,
    optionally targeting a specific scrollable container.
    """
    for i in range(iterations):
        distance = random.randint(600, 1800)
        pause    = random.uniform(1.0, 3.2)

        if container_selector:
            try:
                await page.eval_on_selector(
                    container_selector,
                    f"el => el.scrollBy(0, {distance})"
                )
            except Exception:
                await page.mouse.wheel(0, distance)
        else:
            await page.mouse.wheel(0, distance)

        # Occasionally pause longer — like a human stopping to read
        if random.random() < 0.2:
            await asyncio.sleep(random.uniform(2.5, 5.0))
        else:
            await asyncio.sleep(pause)

        logger.debug(f"Scroll {i + 1}/{iterations} ({distance}px)")


async def human_click(page, selector: str):
    """Move mouse naturally to element before clicking."""
    locator = page.locator(selector).first
    box = await locator.bounding_box()
    if box:
        x = box["x"] + box["width"]  * random.uniform(0.3, 0.7)
        y = box["y"] + box["height"] * random.uniform(0.3, 0.7)
        await page.mouse.move(x, y, steps=random.randint(8, 20))
        await human_delay(100, 400)
    await locator.click()


# ------------------------------------------------------------------ #
# Spider                                                              #
# ------------------------------------------------------------------ #

class GoogleReviewsSpider(scrapy.Spider):
    name = "google_reviews"

    # Put your target restaurant Google Maps URLs here
    start_urls = [
        "https://www.google.com/maps/place/Novotel+Hyderabad+Airport/@17.2313,78.4298,17z",
        # add more as needed
    ]

    SCROLL_ITERATIONS = 15  # ~10 reviews loaded per scroll

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "domcontentloaded"),
                    ],
                    "errback": self.errback,
                },
                callback=self.parse,
            )

    async def parse(self, response):
        page = response.meta["playwright_page"]

        try:
            # ── 1. Apply stealth patches ──────────────────────────────
            stealth = Stealth()
            await stealth.apply_stealth_async(page)

            # ── 2. Spoof extra JS properties Google checks ────────────
            await page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5],
                });
                window.chrome = { runtime: {} };
            """)

            # ── 3. Human pause after page load ───────────────────────
            await human_delay(1500, 3500)

            # ── 4. Dismiss consent / cookie banner ───────────────────
            for btn_text in ("Accept all", "Reject all", "I agree"):
                try:
                    await page.wait_for_selector(
                        f'button:has-text("{btn_text}")', timeout=4000
                    )
                    await human_click(page, f'button:has-text("{btn_text}")')
                    logger.info(f"Clicked consent: {btn_text}")
                    await human_delay(1000, 2000)
                    break
                except Exception:
                    pass

            # ── 5. Wait for place panel ───────────────────────────────
            await page.wait_for_selector("h1.DUwDvf", timeout=30000)
            restaurant_name = await page.inner_text("h1.DUwDvf")
            logger.info(f"Loaded: {restaurant_name}")
            await human_delay(800, 1800)

            # ── 6. Click Reviews tab ──────────────────────────────────
            reviews_selector = 'button[aria-label*="reviews"], button:has-text("reviews")'
            try:
                await page.wait_for_selector(reviews_selector, timeout=15000)
                await human_click(page, reviews_selector)
            except Exception:
                logger.warning("Falling back to second tab for reviews")
                await page.locator('div[role="tablist"] button').nth(1).click()

            await human_delay(2000, 4000)

            # ── 7. Sort by Newest ─────────────────────────────────────
            try:
                await human_click(page, 'button[aria-label="Sort reviews"]')
                await human_delay(600, 1200)
                await page.locator('li[data-index="1"]').click()
                await human_delay(1500, 3000)
            except Exception:
                logger.info("Could not change sort order — using default")

            # ── 8. Scroll reviews panel with human-like behavior ──────
            await human_scroll(
                page,
                container_selector='div[role="main"] div.m6QErb.DxyBCb',
                iterations=self.SCROLL_ITERATIONS,
            )

            # ── 9. Expand truncated reviews ───────────────────────────
            more_buttons = await page.locator('button.w8nwRe').all()
            for btn in more_buttons:
                try:
                    await btn.click()
                    await asyncio.sleep(random.uniform(0.1, 0.4))
                except Exception:
                    pass

            # ── 10. Parse and yield items ─────────────────────────────
            html     = await page.content()
            selector = scrapy.Selector(text=html)
            reviews  = selector.css("div.jftiEf")
            logger.info(f"Found {len(reviews)} reviews for: {restaurant_name}")

            for review in reviews:
                text_parts  = review.css("span.wiI7pd *::text, .wiI7pd::text").getall()
                review_text = " ".join(t.strip() for t in text_parts if t.strip())

                yield {
                    "restaurant": restaurant_name,
                    "reviewer":   review.css("div.d4r55::text").get(),
                    "rating":     review.css("span.kvMYJc::attr(aria-label)").get(),
                    "date":       review.css("span.rsqaWe::text").get(),
                    "review":     review_text or review.css(".wiI7pd::text").get(),
                    "source_url": response.url,
                }

        except Exception as exc:
            logger.error(f"Error parsing {response.url}: {exc}", exc_info=True)
        finally:
            await page.close()

    async def errback(self, failure):
        page = failure.request.meta.get("playwright_page")
        if page:
            await page.close()
        logger.error(f"Request failed: {failure.request.url} — {failure.value}")