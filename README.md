# Google Maps Review Scraper

A Google Maps review scraper built with Python 3 and Playwright. Automatically discovers all restaurants in any city, sorts them by review count, and scrapes every available review into a single CSV file, including full review text, structured attributes, sub-ratings.

---

## What It Does

**Phase 1 — Discovery**
Opens Google Maps, searches for your query (e.g. "restaurants in Hyderabad"), scrolls through all search results, and saves every discovered restaurant with its name, URL, rating, and review count to `discovered_places.csv`.

**Phase 2 — Scraping**
For each restaurant (sorted highest review count first), opens the listing page, clicks the Reviews tab, sorts by Newest, and scrolls through every review, saving each one to `all_reviews.csv` in real time.

---

## Project Files

```
google-review-scraper/
├── gmaps_reviews_scraper.py    ← core scraper engine (uses DOM)
├── gmaps_hybrid_scraper.py     ← scraper engine uses Network interception
├── discover_and_scrape.py      ← orchestrator (discover city + scrape many)
├── check_proxies.py            ← proxy health checker utility
└── README.md
```

Auto-generated at runtime:
```
├── discovered_places.csv       ← all restaurants found, with scraped status
├── all_reviews.csv             ← all collected reviews
├── scraper.log                 ← full log of every run
└── gmaps_session/              ← browser cookies and session data
```

---
 
## Output — CSV Columns
 
Every review is saved with these columns:
 
| Column | What it contains | Example |
|--------|-----------------|---------|
| `review_id` | Unique review identifier | `Ci9DQUlRQUNv...` |
| `place_name` | Restaurant name | `Paradise Biryani \| Secunderabad` |
| `reviewer_name` | Name of the reviewer | `Rahul Sharma` |
| `reviewer_id` | Google contributor ID | `109328685807050265243` |
| `local_guide` | Whether reviewer is a Local Guide | `True` |
| `rating` | Overall star rating 1–5 | `4` |
| `review_text` | Customer's full written review (never truncated) | `Best biryani in the city` |
| `likes` | Helpful votes on the review | `7` |
| `date` | When the review was posted | `2 weeks ago` |
| `attributes` | All structured attributes as JSON | see below |
 
The `attributes` JSON column contains everything the reviewer filled in:
 
```json
{
  "dining_mode": "Dine in",
  "meal_type": "Lunch",
  "price_per_person": "₹200–400",
  "food": 5,
  "service_rating": 4,
  "atmosphere": 5,
  "group_size": "2 people",
  "vegetarian_recommendation": "Would not recommend",
  "vegetarian_offerings": "Not sure",
  "recommended_dishes": "Mutton Haleem, Hyderabad Dum Biryani",
  "parking_space": "Difficult to find parking",
  "parking_options": "Valet",
  "kid_friendliness": "Not kid friendly",
  "wheelchair_accessibility": "Wheel chair accessibility is not available",
  "dietary_restrictions": "Options may not be available.",
  "noise_level": "Moderate noise",
  "wait_time": "No wait",
  "seating_type": "Indoor dining area"
}
```
 
Keys only appear if the reviewer filled them in.
 
---

## Installation

### Requirements

- Ubuntu Linux
- Python 3.10 or higher

### Steps

**1. Clone the repository**
```bash
git clone https://github.com/Vivashwan/google-review-scraper.git
cd google-review-scraper
```

**2. Create a virtual environment**
```bash
python3 -m venv venv
source venv/bin/activate
```

**3. Install Python dependencies**
```bash
pip install playwright playwright-stealth aiohttp
```

**4. Install the Chromium browser**
```bash
playwright install chromium
```

This downloads the Chrome browser that Playwright controls. Takes about 2 minutes.

---

## Running the Scraper

### Before every run — prevent screen lock

The scraper runs for 30–90 minutes per restaurant. Prevent your screen from sleeping mid-run:

```bash
export DISPLAY=:0
export GDK_BACKEND=x11
gsettings set org.gnome.desktop.session idle-delay 0
gsettings set org.gnome.settings-daemon.plugins.power sleep-inactive-ac-timeout 0
gsettings set org.gnome.settings-daemon.plugins.power sleep-inactive-battery-timeout 0
```

Restore when done:
```bash
gsettings set org.gnome.desktop.session idle-delay 300
gsettings set org.gnome.settings-daemon.plugins.power sleep-inactive-ac-timeout 1200
gsettings set org.gnome.settings-daemon.plugins.power sleep-inactive-battery-timeout 1200
```

---

### Mode 1 — Discover and Scrape a City

Finds all restaurants matching your query, then scrapes reviews from the top N sorted by review count.

```bash
python3 discover_and_scrape.py \
  --query "restaurants in Hyderabad" \
  --top 10 \
  --output all_reviews.csv \
  --speed fast
```

**Available flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--query` | required | Search query e.g. `"restaurants in Hyderabad"` |
| `--top` | `10` | Number of restaurants to scrape (highest review count first) |
| `--output` | `all_reviews.csv` | CSV file to write reviews into |
| `--speed` | `fast` | Speed profile — `turbo`, `fast`, or `safe` |
| `--runtime` | no limit | Auto-stop after this long — e.g. `8h`, `3d`, `90m` |
| `--headless` | off | Add this flag to hide the browser window |

**More examples:**

```bash
# Scrape top 50 restaurants, stop after 8 hours
python3 discover_and_scrape.py \
  --query "restaurants in Hyderabad" \
  --top 50 \
  --output all_reviews.csv \
  --runtime 8h

# Different city, safe speed for overnight
python3 discover_and_scrape.py \
  --query "restaurants in Mumbai" \
  --top 20 \
  --output mumbai_reviews.csv \
  --speed safe
```

---

### Mode 2 — Single Restaurant

Scrape all reviews from one specific restaurant.

**Step 1** — Get the correct URL:
1. Open Google Maps in Chrome
2. Search for the restaurant and click on it
3. Wait for the URL to change to something like:
   ```
   https://www.google.com/maps/place/RestaurantName/@17.44,78.48,17z/data=...
   ```
4. Copy that full URL

> Do not use search URLs like `/maps/search/...` — use only direct place URLs starting with `/maps/place/`.

**Step 2** — Run:

```bash
python3 gmaps_reviews_scraper.py \
  --url "https://www.google.com/maps/place/Paradise+Biryani/@17.44,78.48,17z/data=..." \
  --output paradise_reviews.csv \
  --speed fast
```


### To run gmaps_hybrid_scraper
To run gmaps_hybrid_scraper using discover_and_scrape.py, then make change to discover_and_scrape.py:

```bash
from gmaps_hybrid_scraper import (
    CFG, BrowserSession, ReviewCSVWriter, GoogleMapsReviewScraper,
    rand_delay, micro_mouse_move, apply_speed_profile, parse_runtime, log,
)
```
To run gmaps_hybrid_scraper to fetch single restaurant run:
 
```bash
python3 gmaps_hybrid_scraper.py \
  --url "https://www.google.com/maps/place/Paradise+Biryani/@17.4417141,78.4872154,17z/data=..." \
  --output reviews.csv
```

**Available flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | required | Full Google Maps listing URL |
| `--output` | `reviews.csv` | CSV file to write reviews into |
| `--max` | no limit | Stop after collecting this many reviews |
| `--speed` | `fast` | Speed profile — `turbo`, `fast`, or `safe` |
| `--runtime` | no limit | Auto-stop after this long — e.g. `8h`, `3d` |
| `--headless` | off | Add this flag to hide the browser window |

---

## Resume After Interruption

The scraper writes every review to disk immediately. If it stops for any reason — power cut, Ctrl+C, crash — nothing is lost.

To resume, run the **exact same command** with the **same `--output` file**:

```bash
# Interrupted after 3000 reviews? Just re-run:
python3 discover_and_scrape.py \
  --query "restaurants in Hyderabad" \
  --top 10 \
  --output all_reviews.csv \
  --speed fast
```

The scraper will:
- Skip all restaurants already marked as done in `discovered_places.csv`
- Skip all reviews already in `all_reviews.csv`
- Continue from the next unfinished restaurant

---

## How It Avoids Detection

The scraper mimics human browsing behaviour:

- **Randomised delays** between every scroll, click, and action
- **Mouse micro-movements** between scroll cycles
- **Idle pauses** every 100–180 scrolls to simulate reading
- **Reverse scrolls** occasionally to simulate re-reading
- **Browser fingerprint patches** that remove automation signals
- **Consent wall handler** that auto-dismisses cookie popups
- **URL drift detection** that recovers if accidentally navigated away

---

## Stopping Conditions

The scraper stops automatically when any of these occur:

| Condition | Log message |
|-----------|------------|
| 25 consecutive scroll cycles with no new reviews | `Plateau detected. Stopping.` |
| `--max` count reached | `Target of 5000 reviews reached` |
| `--runtime` limit reached | `Max runtime reached — stopping` |
| Manual Ctrl+C | `Shutdown signal received` |

---

## Overnight Run — Copy-Paste Ready

```bash
cd ~/google-review-scraper
source venv/bin/activate

export DISPLAY=:0
export GDK_BACKEND=x11
gsettings set org.gnome.desktop.session idle-delay 0
gsettings set org.gnome.settings-daemon.plugins.power sleep-inactive-ac-timeout 0
gsettings set org.gnome.settings-daemon.plugins.power sleep-inactive-battery-timeout 0

python3 discover_and_scrape.py \
  --query "restaurants in Hyderabad" \
  --top 20 \
  --output all_reviews.csv \
  --speed fast
```

---

## Common Issues

**`python` command not found**
Use `python3` — Ubuntu ships Python 3 only without a `python` alias unless you install `python-is-python3`.

**Wayland crash after long sessions**
The `export DISPLAY=:0` and `export GDK_BACKEND=x11` commands at the top of every run fix this by forcing Chrome to use X11 instead of native Wayland, which is stable for multi-hour sessions.

**Reviews stopped collecting mid-scrape**
The scraper reached the end of what Google serves for that restaurant and stopped automatically. Re-run the same command to continue with the next restaurant in the queue.


**Reviews ending with `… More` in the text**
The "More" button expander runs automatically. If some reviews still appear truncated, Google uses CSS-level truncation on those specific reviews — no button exists and the full text is not in the DOM until you click into the individual review. **This affects a some percentage of reviews if gmaps_review_scraper used, although gains in reviews scraped.**

---

## Stack

| Tool | Purpose |
|------|---------|
| Python 3 | Core language |
| Playwright (async) | Browser automation |
| playwright-stealth | Fingerprint hardening |
| aiohttp | Async HTTP for proxy checker |
| csv (stdlib) | Memory-safe append-only storage |
| asyncio (stdlib) | Async execution |

---
