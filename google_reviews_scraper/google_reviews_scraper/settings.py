BOT_NAME = "google_reviews_scraper"

SPIDER_MODULES = ["google_reviews_scraper.spiders"]
NEWSPIDER_MODULE = "google_reviews_scraper.spiders"

ADDONS = {}

# Must be False — Google Maps blocks scrapers via robots.txt
ROBOTSTXT_OBEY = False

# Realistic Chrome user-agent
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

# One request at a time with a delay — don't hammer Google
CONCURRENT_REQUESTS = 1
CONCURRENT_REQUESTS_PER_DOMAIN = 1
DOWNLOAD_DELAY = 3          # base delay between requests (seconds)

COOKIES_ENABLED = True      # Required for Google Maps session

FEED_EXPORT_ENCODING = "utf-8"

# Uncomment to auto-save output:
# FEEDS = {
#     "reviews.json": {"format": "json", "overwrite": True},
#     "reviews.csv":  {"format": "csv",  "overwrite": True},
# }

# ------------------------------------------------------------------ #
# Playwright                                                          #
# ------------------------------------------------------------------ #
DOWNLOAD_HANDLERS = {
    "http":  "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": False,      # keep False — headless is easier for Google to detect
    "args": [
        "--disable-blink-features=AutomationControlled",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--start-maximized",
    ],
}

# Give slow-loading review panels plenty of time
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 60000  # ms