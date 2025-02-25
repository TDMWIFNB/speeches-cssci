import os
import requests
import csv
import time
import random
import logging
from pathlib import Path

def get_file(url):
    print(f"Getting page from {url}")
    response = requests.get(url)
    response.raise_for_status()
    return response

# Define base directories
BASE_DIR = Path.cwd()
DATA_DIR = BASE_DIR / "data"
HANDELINGEN_DIR = DATA_DIR / "handelingen"
META_DIR = DATA_DIR / "meta"
LINKS_DIR = DATA_DIR / "links"
OUTPUT_DIR = BASE_DIR / "output"
ERROR_LOG = BASE_DIR / "error_log.txt"

# Ensure directories exist
for directory in [DATA_DIR, HANDELINGEN_DIR, META_DIR, LINKS_DIR, OUTPUT_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Setup logging
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

def log_error(url, error):
    with ERROR_LOG.open("a") as f:
        f.write(f"Error retrieving {url}: {error}\n")
    logging.error(f"Error retrieving {url}: {error}")

# List of parliamentary years to scrape
vergaderjaren = ["2023-2024", "2022-2023", "2021-2022"]

# File to store last processed item
RESUME_FILE = BASE_DIR / "last_processed.txt"

def save_last_processed(vergaderjaar, filename):
    with RESUME_FILE.open("w") as f:
        f.write(f"{vergaderjaar},{filename}")

def get_last_processed():
    if not RESUME_FILE.exists():
        return None, None
    try:
        with RESUME_FILE.open("r") as f:
            content = f.read().strip()
            if content:
                return content.split(",")
    except Exception as e:
        logging.error(f"Error reading resume file: {e}")
    return None, None

# Get the last processed item
last_year, last_file = get_last_processed()
resume_processing = False if last_year is None else True

for vergaderjaar in vergaderjaren:
    # Skip years until we reach the last processed year
    if resume_processing and vergaderjaar != last_year:
        continue

    input_file = LINKS_DIR / f"{vergaderjaar}.csv"
    jaar_folder = HANDELINGEN_DIR / vergaderjaar
    jaar_folder.mkdir(exist_ok=True)

    if not input_file.exists():
        logging.warning(f"Missing links file: {input_file}. Make sure to run 'officiele_bekendmakingen_site.py' first.")
        continue

    file_list = [f.name for f in jaar_folder.iterdir() if f.is_file()]

    with open(input_file, newline="") as csvfile:
        url_reader = csv.reader(csvfile)
        next(url_reader)  # Skip header

        for _, row in enumerate(url_reader):
            # Skip until we reach the last processed file
            if resume_processing:
                if row[1] != last_file:
                    continue
                resume_processing = False  # Stop skipping after finding the last processed file
                continue  # Skip the last processed file itself

            if row[1] in file_list or "-ek-" in row[1]:
                continue

            url = f"https://zoek.officielebekendmakingen.nl/{row[1]}"
            time_to_sleep = random.uniform(1, 2)
            time.sleep(time_to_sleep)

            try:
                response = get_file(url)
            except requests.exceptions.RequestException as e:
                log_error(url, e)
                try:
                    url_html = url.replace("xml", "html")
                    response = get_file(url_html)
                    time.sleep(random.uniform(1, 3))
                    response = get_file(url)
                except requests.exceptions.RequestException as e:
                    log_error(url, e)
                    continue

            xml_filename = jaar_folder / row[1]
            with open(xml_filename, "wb") as f:
                f.write(response.content)
                logging.info(f"Saved {xml_filename}")

            # Save the current progress
            save_last_processed(vergaderjaar, row[1])
