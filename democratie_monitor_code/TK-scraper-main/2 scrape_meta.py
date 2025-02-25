import logging
import csv
from datetime import datetime
import requests
import time
import random
from pathlib import Path
from lxml import html

# Constants
KAMER = {
    "Tweede Kamer der Staten-Generaal": "tk",
    "Eerste Kamer der Staten-Generaal": "ek",
    "Verenigde Vergadering der Staten-Generaal": "vv",
}

# Define base directories
BASE_DIR = Path.cwd()
DATA_DIR = BASE_DIR / "data"
HANDELINGEN_DIR = DATA_DIR / "handelingen"
META_DIR = DATA_DIR / "meta"
ERROR_LOG = BASE_DIR / "meta_error_log.txt"
RESUME_FILE = BASE_DIR / "last_processed_meta.txt"

# Ensure directories exist
for directory in [DATA_DIR, HANDELINGEN_DIR, META_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s %(levelname)-5s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('meta_scraper.log')
    ]
)

def get_and_retry(url, max_retries=3, backoff=1):
    """Get URL with retry logic and random delays"""
    for i in range(max_retries):
        try:
            time.sleep(random.uniform(1, 2))
            r = requests.get(url)
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            if i == max_retries - 1:
                raise
            sleep_time = backoff * (i + 1) * random.uniform(1, 1.5)
            logging.warning(f"Error getting {url}, attempt {i+1}/{max_retries}, sleeping {sleep_time:.1f}s: {e}")
            time.sleep(sleep_time)

def log_error(name, error):
    """Log errors to file and logging system"""
    with ERROR_LOG.open("a") as f:
        f.write(f"{datetime.now().isoformat()}: Error processing {name}: {error}\n")
    logging.error(f"Error processing {name}: {error}")

def save_last_processed(vergaderjaar, filename):
    """Save last successfully processed file"""
    with RESUME_FILE.open("w") as f:
        f.write(f"{vergaderjaar},{filename}")

def get_last_processed():
    """Get last successfully processed file"""
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

def process_metadata(vergaderjaar):
    """Process metadata for a specific parliamentary year"""
    metafile = META_DIR / f"meta_{vergaderjaar}.csv"

    # Read existing metadata
    if metafile.exists():
        with metafile.open() as f:
            names = {row["file"] for row in csv.DictReader(f)}
        logging.info(f"Found {len(names)} existing entries in meta file")
    else:
        names = set()

    # Get files to process
    files = list((HANDELINGEN_DIR / vergaderjaar).glob("*.xml"))
    todo = {file.with_suffix("").name for file in files} - names
    logging.info(f"Found {len(todo)} files to process out of {len(files)} total files")

    # Get resume point if exists
    last_year, last_file = get_last_processed()
    resume_processing = False if last_year is None else (last_year == vergaderjaar)

    # Process files
    with metafile.open("a" if names else "w") as f:
        writer = csv.writer(f)
        if not names:
            writer.writerow(["file", "kamer", "jaar", "nr", "date", "document_nr", "title"])

        for i, name in enumerate(sorted(todo)):
            # Skip until last processed file if resuming
            if resume_processing and last_file and name <= last_file:
                continue

            logging.info(f"Processing {i+1}/{len(todo)}: {name}")

            try:
                url = f"https://zoek.officielebekendmakingen.nl/{name}.html"
                r = get_and_retry(url)
                doc = html.fromstring(r.content)

                # Get metadata table
                meta_table = doc.cssselect(".table--dataintro")
                if not meta_table:
                    raise ValueError("Metadata table not found")
                meta = {td.get("data-before"): td.text_content().strip()
                       for td in meta_table[0].cssselect("td")}

                # Extract and format date
                date = datetime.strptime(meta["Datum vergadering"], "%d-%m-%Y").isoformat()[:10]

                # Get kamer
                kamer = KAMER[meta["Organisatie"]]

                # Get document number and title if available
                document_nr = meta.get("Documentnummer", "")
                title = doc.cssselect("h1.title")[0].text_content().strip() if doc.cssselect("h1.title") else ""

                # Write row
                writer.writerow([
                    name, kamer, meta["Vergaderjaar"],
                    meta["Vergadernummer"], date, document_nr, title
                ])

                # Save progress
                save_last_processed(vergaderjaar, name)

            except Exception as e:
                log_error(name, str(e))
                continue

def main():
    """Main function to process all years"""
    vergaderjaren = ["2023-2024", "2022-2023", "2021-2022"]

    for vergaderjaar in vergaderjaren:
        logging.info(f"\nProcessing year: {vergaderjaar}")
        try:
            process_metadata(vergaderjaar)
        except Exception as e:
            logging.error(f"Failed to process year {vergaderjaar}: {e}")
            continue

if __name__ == "__main__":
    main()
