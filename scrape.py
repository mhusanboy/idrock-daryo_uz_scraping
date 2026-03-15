"""Scrape news articles from daryo.uz API and save them to a CSV file.

Usage:
    python scrape.py
"""

from __future__ import annotations

import os
import csv
import json
import time
import logging
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


# ==========================================
# GLOBAL CONFIGURATION
# ==========================================
REQUESTS_PER_SECOND = 3  # Maximum number of parallel workers (reduced to avoid rate limiting)
CSV_FILENAME = "data/daryo_uz_news.csv"
Path("data").mkdir(exist_ok=True)
Path("logs").mkdir(exist_ok=True)
ERROR_FILENAME = "logs/errors.txt"
REQUEST_TIMEOUT = 15

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://daryo.uz/",
    "Origin": "https://daryo.uz",
    "Accept": "application/json, text/plain, */*"
}

CSV_FIELDS = ['id', 'title', 'author', 'category', 'content', 'tags']

# Configure professional logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# ==========================================
# HELPER FUNCTIONS
# ==========================================

def init_storage() -> None:
    """Initializes the CSV with headers and clears the old error log."""
    file_exists = os.path.isfile(CSV_FILENAME)
    
    with open(CSV_FILENAME, 'a', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if not file_exists or os.stat(CSV_FILENAME).st_size == 0:
            writer.writeheader()
            
    # Clear previous error log on fresh start
    # open(ERROR_FILENAME, 'w', encoding='utf-8').close()

def log_error(url: str, error_msg: str) -> None:
    """Logs failed URLs to the errors.txt file."""
    with open(ERROR_FILENAME, "a", encoding='utf-8') as err_file:
        err_file.write(f"{url} | {error_msg}\n")

def fetch_article_data(item: dict) -> dict[str, str] | None:
    """Fetches and parses a single article using the backend API. Designed to run in a thread."""
    slug = item.get("slug")
    if not slug:
        return None
        
    api_url = f"https://data.daryo.uz/api/v1/site/news/{slug}?user_id="
    
    try:
        # Simple retry mechanism
        for attempt in range(3):
            try:
                res = requests.get(api_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                res.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                if attempt == 2:
                    raise e
                time.sleep(2) # wait 2 seconds before retry
                
        data = res.json()
        
        # Extract fields
        article_id = item.get("id", "Noma'lum")
        
        # Fallback to local item if not in response
        title = data.get("title") or item.get("title") or "Noma'lum"
        author = data.get("author") or "Noma'lum"
        category = data.get("category") or item.get("category") or "Noma'lum"
        tags = data.get("hashtag") or "Noma'lum"
        
        content_html = data.get("content", "")
        if content_html:
            soup = BeautifulSoup(content_html, 'html.parser')
            # Extract text separating elements with a newline
            content = soup.get_text(separator='\n', strip=True) or "Noma'lum"
        else:
            content = "Noma'lum"
        # logging.info("Successfully fetched article data for slug: %s", slug)
        return {
            'id': str(article_id),
            'title': str(title),
            'author': str(author),
            'category': str(category),
            'content': str(content),
            'tags': str(tags)
        }

    except Exception as e:
        logging.error(f"Failed: {slug} -> {e}")
        log_error(slug, str(e))
        return None

def process_file(json_file: Path) -> None:
    """Process a single JSON file of links in chunks."""
    links = []
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        links.extend(data)
        
    unique_links = {link.get('slug'): link for link in links if link.get('slug')}
    all_links = list(unique_links.values())
    
    if not all_links:
        return
        
    logging.info(f"--- Fetching {json_file.name} ({len(all_links)} total items) ---")
    
    chunk_size = 50
    for i in range(0, len(all_links), chunk_size):
        batch_links = all_links[i:i + chunk_size]
        logging.info(f"Processing chunk {i//chunk_size + 1} of {json_file.name} ({len(batch_links)} items)...")
        
        batch_results = []
        with ThreadPoolExecutor(max_workers=REQUESTS_PER_SECOND) as executor:
            for result in executor.map(fetch_article_data, batch_links):
                if result:
                    batch_results.append(result)

        if batch_results:
            with open(CSV_FILENAME, 'a', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                writer.writerows(batch_results)
                
            logging.info(f"Flushed {len(batch_results)} articles to {CSV_FILENAME}.")

# ==========================================
# MAIN EXECUTION
# ==========================================
def scrape() -> None:
    """Orchestrate the scraping process pulling from local JSON links."""
    init_storage()
    
    links_dir = Path("data/links")
    if not links_dir.exists():
        logging.error("No links directory found at data/links/.")
        return
        
    # Get all json files and sort them if necessary
    json_files = list(links_dir.glob("*.json"))
    
    if not json_files:
        logging.error("No links found in data/links/ directory.")
        return
        
    logging.info(f"Found {len(json_files)} link files to process.")
    
    for json_file in json_files:
        process_file(json_file)

    logging.info("Scraping completed successfully.")

if __name__ == "__main__":
    scrape()