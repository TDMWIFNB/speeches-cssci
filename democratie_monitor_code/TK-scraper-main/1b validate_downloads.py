import csv
from pathlib import Path
import logging

# Define base directories (same as in scraper.py)
BASE_DIR = Path.cwd()
DATA_DIR = BASE_DIR / "data"
HANDELINGEN_DIR = DATA_DIR / "handelingen"
LINKS_DIR = DATA_DIR / "links"
ERROR_LOG = BASE_DIR / "error_log.txt"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('validation_report.txt')
    ]
)

def read_error_log():
    """Read the error log and return a set of failed URLs"""
    if not ERROR_LOG.exists():
        return set()

    failed_urls = set()
    with open(ERROR_LOG, 'r') as f:
        for line in f:
            # Extract the URL from the error log line
            if 'Error retrieving' in line:
                url = line.split('Error retrieving ')[1].split(':')[0]
                # Convert full URL to just the ID
                if 'zoek.officielebekendmakingen.nl/' in url:
                    failed_urls.add(url.split('zoek.officielebekendmakingen.nl/')[1])
    return failed_urls

def validate_downloads():
    """Validate that all files from CSVs were downloaded unless they're in error log"""
    vergaderjaren = ["2023-2024", "2022-2023", "2021-2022"]
    failed_urls = read_error_log()

    total_expected = 0
    total_downloaded = 0
    total_missing = 0
    total_errors = 0

    for vergaderjaar in vergaderjaren:
        csv_file = LINKS_DIR / f"{vergaderjaar}.csv"
        if not csv_file.exists():
            logging.warning(f"CSV file not found for {vergaderjaar}")
            continue

        jaar_folder = HANDELINGEN_DIR / vergaderjaar
        downloaded_files = {f.name for f in jaar_folder.iterdir()} if jaar_folder.exists() else set()

        with open(csv_file, newline='') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip header

            year_expected = 0
            year_downloaded = 0
            year_missing = 0
            year_errors = 0

            for row in reader:
                if len(row) < 2 or "-ek-" in row[1]:  # Skip EK files as in original scraper
                    continue

                file_id = row[1]
                year_expected += 1

                if file_id in downloaded_files:
                    year_downloaded += 1
                elif file_id in failed_urls:
                    year_errors += 1
                else:
                    year_missing += 1
                    logging.warning(f"Missing file with no error log: {file_id}")

        # Log summary for this year
        logging.info(f"\nSummary for {vergaderjaar}:")
        logging.info(f"Expected files: {year_expected}")
        logging.info(f"Successfully downloaded: {year_downloaded}")
        logging.info(f"Failed with logged errors: {year_errors}")
        logging.info(f"Missing without error log: {year_missing}")

        # Add to totals
        total_expected += year_expected
        total_downloaded += year_downloaded
        total_missing += year_missing
        total_errors += year_errors

    # Log overall summary
    logging.info(f"\nOVERALL SUMMARY:")
    logging.info(f"Total expected files: {total_expected}")
    logging.info(f"Total successfully downloaded: {total_downloaded}")
    logging.info(f"Total failed with logged errors: {total_errors}")
    logging.info(f"Total missing without error log: {total_missing}")
    logging.info(f"Success rate: {(total_downloaded/total_expected*100):.1f}%")

if __name__ == "__main__":
    validate_downloads()
