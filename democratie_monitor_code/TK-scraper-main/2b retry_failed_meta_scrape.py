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

# Failed files to retry
FAILED_FILES = [
    "h-tk-20222023-95-9"
]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s %(levelname)-5s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('meta_retry.log')
    ]
)

def get_with_timeout_handling(url, max_retries=5, initial_timeout=30):
    """Get URL with extended timeout handling and multiple retries"""
    for attempt in range(max_retries):
        current_timeout = initial_timeout * (attempt + 1)  # Increase timeout with each retry
        try:
            # Add exponential backoff between attempts
            if attempt > 0:
                sleep_time = (2 ** attempt) + random.uniform(1, 5)
                logging.info(f"Waiting {sleep_time:.1f}s before retry {attempt + 1}")
                time.sleep(sleep_time)

            logging.info(f"Attempting request with {current_timeout}s timeout")
            response = requests.get(url, timeout=current_timeout)
            response.raise_for_status()
            return response

        except requests.exceptions.Timeout:
            logging.warning(f"Timeout ({current_timeout}s) on attempt {attempt + 1}/{max_retries}")
            if attempt == max_retries - 1:
                raise
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            logging.warning(f"Request failed on attempt {attempt + 1}/{max_retries}: {e}")

def process_single_file(name, vergaderjaar):
    """Process a single file with enhanced error handling"""
    metafile = META_DIR / f"meta_{vergaderjaar}.csv"

    logging.info(f"Processing {name}")

    try:
        url = f"https://zoek.officielebekendmakingen.nl/{name}.html"
        r = get_with_timeout_handling(url)
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

        # Append to metadata file
        with metafile.open("a") as f:
            writer = csv.writer(f)
            writer.writerow([
                name, kamer, meta["Vergaderjaar"],
                meta["Vergadernummer"], date, document_nr, title
            ])

        logging.info(f"Successfully processed {name}")
        return True

    except Exception as e:
        logging.error(f"Error processing {name}: {e}")
        return False

def main():
    """Main function to retry failed files"""
    vergaderjaar = "2022-2023"  # All failed files are from this year

    # Ensure the meta file exists and has headers
    metafile = META_DIR / f"meta_{vergaderjaar}.csv"
    if not metafile.exists():
        with metafile.open("w") as f:
            writer = csv.writer(f)
            writer.writerow(["file", "kamer", "jaar", "nr", "date", "document_nr", "title"])

    successful = 0
    failed = 0

    for name in FAILED_FILES:
        if process_single_file(name, vergaderjaar):
            successful += 1
        else:
            failed += 1

        # Add a longer delay between files
        time.sleep(random.uniform(5, 10))

    logging.info(f"\nRetry Summary:")
    logging.info(f"Successfully processed: {successful}")
    logging.info(f"Failed: {failed}")

if __name__ == "__main__":
    main()
