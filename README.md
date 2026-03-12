# Google Maps Review Scraper

A web scraper built with **Scrapy**, **Playwright**, and **Playwright Stealth** to collect restaurant reviews from Google Maps.

This project loads Google Maps pages using a real browser engine, mimics human behavior (scrolling, delays, mouse movement), and extracts structured review data such as reviewer name, rating, date, and review text.

---

## Features

* Scrapes restaurant reviews directly from Google Maps
* Uses Playwright browser automation to render dynamic JavaScript content
* Integrates Playwright Stealth to reduce bot detection
* Human-like interaction (scrolling, clicking, random delays)
* Automatically expands truncated reviews
* Extracts structured review data
* Export data to CSV/JSON using Scrapy

---

## Tech Stack

* Python
* Scrapy
* Playwright
* Playwright Stealth
* Asyncio

---

## Project Structure

```
google-review-scraper
│
├── google_reviews_scraper
│   ├── google_reviews_scraper
│   │   ├── spiders
│   │   │   └── google_reviews.py
│   │   ├── items.py
│   │   ├── pipelines.py
│   │   ├── middlewares.py
│   │   ├── settings.py
│   │   └── __init__.py
│   │
│   └── scrapy.cfg
│
├── venv
├── .gitignore
└── README.md
```

---

## Installation

### 1. Clone the repository

```
git clone https://github.com/Vivashwan/google-review-scraper.git
cd google-review-scraper
```

### 2. Create a virtual environment

```
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```
pip install -r requirements.txt
```

### 4. Install Playwright browsers

```
playwright install
```

---

## Running the Scraper

Navigate to the Scrapy project directory:

```
cd google_reviews_scraper
```

Run the spider:

```
scrapy crawl google_reviews
```

Export results to CSV:

```
scrapy crawl google_reviews -o reviews.csv
```

Export results to JSON:

```
scrapy crawl google_reviews -o reviews.json
```

---

## Data Extracted

The scraper collects the following fields:

* restaurant
* reviewer
* rating
* date
* review
* source_url

---

## Example Output

```
restaurant,reviewer,rating,date,review
Novotel Hyderabad Airport,John Doe,5 stars,2 weeks ago,Great stay and excellent service!
```

---

## Anti-Bot Techniques

To improve scraping reliability, the spider implements:

* Playwright Stealth patches
* Navigator property spoofing
* Randomized scrolling
* Randomized delays
* Mouse movement simulation

These techniques help mimic real user behavior when interacting with Google Maps.

---

## Future Improvements

* Automatic discovery of restaurant URLs
* Proxy rotation
* CAPTCHA handling
* Database storage (PostgreSQL)
* Scheduled scraping jobs
* Distributed scraping

---
