import logging
import requests
import csv
import time
import random
from pathlib import Path
from lxml import html
import itertools

def get_and_retry(url, max_retries=5, retry_codes={500}, backoff=1, timeout=10):
    for i in range(max_retries):
        try:
            random_wait = random.uniform(1, 2)
            time.sleep(random_wait)
            r = requests.get(url, timeout=timeout)
            if r.status_code in retry_codes:
                sleep = backoff * (i + 1)
                logging.warning(
                    f"{r.status_code} for {url}, attempt {i+1}/{max_retries}, sleeping {sleep} seconds and retrying"
                )
                time.sleep(sleep)
                continue
            r.raise_for_status()
            return r
        except requests.exceptions.ReadTimeout:
            logging.warning(f"Read timeout for {url}, retrying {i+1}/{max_retries}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for {url}: {e}")
    raise requests.exceptions.RequestException(f"Max retries exceeded for {url}")

def get_links(url):
    logging.info(f"Scraping {url}")
    r = get_and_retry(url)
    doc = html.fromstring(r.text)
    links = doc.findall(".//div[@class='result--list result--list--publications']/ul/li/h2")
    for link in links:
        href = link.find(".//a").attrib["href"].replace("html", "xml")
        yield href

def get_npages(url):
    r = get_and_retry(url)
    doc = html.fromstring(r.text)
    pages = doc.findall(".//div[@class='pagination__index']/ul/li/a")
    numbers = [int(page.text) if page.text else 0 for page in pages]
    return max(numbers) if numbers else 1

# Define directories
BASE_DIR = Path.cwd()
DATA_DIR = BASE_DIR / "data"
LINKS_DIR = DATA_DIR / "links"
LINKS_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(format="[%(levelname)-7s:%(name)-15s] %(message)s", level=logging.INFO)

vergaderjaren = ["2023-2024", "2022-2023", "2021-2022"]

for vergaderjaar in vergaderjaren:
    file_path = LINKS_DIR / f"{vergaderjaar}.csv"
    with open(file_path, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["vergaderjaar", "id"])
        url = f"https://zoek.officielebekendmakingen.nl/resultaten?q=(c.product-area==%22officielepublicaties%22)and((w.publicatienaam==%22Handelingen%22))%20AND%20w.vergaderjaar==%22{vergaderjaar}%22&zv=&pg=1000&col=Handelingen&svel=Publicatiedatum&svol=Aflopend&sf=vj%7c{vergaderjaar}"
        try:
            max_page = get_npages(url)
            for page in range(1, max_page + 1):
                page_url = f"{url}&pagina={page}"
                for href in get_links(page_url):
                    writer.writerow([vergaderjaar, href])
            logging.info(f"Saved links for {vergaderjaar} to {file_path}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Failed to scrape {vergaderjaar}: {e}")
