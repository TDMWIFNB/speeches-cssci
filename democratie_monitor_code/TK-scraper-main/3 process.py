import logging
import csv
from datetime import datetime
from pathlib import Path
from lxml import etree
from lxml.etree import _Element
import pandas as pd

# Define base directories
BASE_DIR = Path.cwd()
DATA_DIR = BASE_DIR / "data"
HANDELINGEN_DIR = DATA_DIR / "handelingen"
META_DIR = DATA_DIR / "meta"
OUTPUT_DIR = DATA_DIR / "parsed"

# Create output directory
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Parliamentary years to process
VERGADERJAREN = ["2023-2024", "2022-2023", "2021-2022"]

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s %(levelname)-5s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('parser.log')
        ]
    )

def extract_party(speech_element):
    """Extract party information from speech element"""
    # Try different possible party tag locations
    party = None
    # Try politiek tag first (most common)
    politiek = speech_element.xpath(".//spreker/politiek")
    if politiek and politiek[0].text:
        party = politiek[0].text.strip()
        return party

    # Try partij tag next
    partij = speech_element.xpath(".//wie/partij")
    if partij and partij[0].text:
        party = partij[0].text.strip()
        return party

    return party

def get_doc(file_path):
    """Parse XML file and return document"""
    try:
        tree = etree.parse(file_path)
        return tree.getroot()
    except Exception as e:
        logging.error(f"Error parsing XML file {file_path}: {e}")
        raise

def get_meta(doc, file_path):
    """Get metadata for document from our scraped metadata files"""
    vergaderjaar = file_path.parent.name
    meta_file = META_DIR / f"meta_{vergaderjaar}.csv"

    if not meta_file.exists():
        raise FileNotFoundError(f"Metadata file not found: {meta_file}")

    df = pd.read_csv(meta_file)
    file_id = file_path.stem
    meta_row = df[df['file'] == file_id]

    if len(meta_row) == 0:
        raise ValueError(f"No metadata found for {file_id}")

    meta = meta_row.iloc[0]

    return {
        "jaar": vergaderjaar,
        "date": meta['date'],
        "kamer": meta['kamer'],
        "category": "handelingen",
        "title": meta['title'],
        "document_number": meta['document_nr'],
        "url": f"https://zoek.officielebekendmakingen.nl/{file_id}.html",
        "meta_url": f"https://zoek.officielebekendmakingen.nl/{file_id}.xml",
        "vergadernummer": meta['nr']
    }

def extract_speech_text(speech_element):
    """Extract text content from speech element"""
    text_parts = []
    for al in speech_element.xpath(".//tekst//al"):
        if al.text:
            text_parts.append(al.text.strip())
    return "\n".join(text_parts)

def process_speech(speech, file_id):
    """Process a single speech element"""
    try:
        # Get speaker element
        spreker = speech.find("spreker")
        if spreker is None:
            return None

        # Extract name components
        voorvoegsels = spreker.find("voorvoegsels")
        voorvoegsels = voorvoegsels.text if voorvoegsels is not None else ""

        achternaam = spreker.xpath(".//achternaam")
        achternaam = achternaam[0].text if achternaam else ""

        # Get party
        party = extract_party(speech)

        # Get speech text
        speech_text = extract_speech_text(speech)

        # Construct full name
        full_name = f"{voorvoegsels} {achternaam}".strip()

        return {
            "file_id": file_id,
            "speaker_name": full_name,
            "speaker_party": party,
            "speech_text": speech_text
        }
    except Exception as e:
        logging.error(f"Error processing speech: {e}")
        return None

def process_document(file_path):
    """Process single document and return list of speeches"""
    try:
        doc = get_doc(file_path)
        meta = get_meta(doc, file_path)

        speeches = []
        for speech in doc.xpath("//spreekbeurt"):
            speech_data = process_speech(speech, file_path.stem)
            if speech_data:
                speeches.append({**speech_data, **meta})

        return speeches
    except Exception as e:
        logging.error(f"Error processing document {file_path}: {e}")
        return None

def save_to_csv(speeches, output_file):
    """Save speeches to CSV file"""
    if not speeches:
        return

    fieldnames = list(speeches[0].keys())

    mode = 'a' if output_file.exists() else 'w'
    write_header = mode == 'w'

    with output_file.open(mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerows(speeches)

def main():
    setup_logging()

    for vergaderjaar in VERGADERJAREN:
        logging.info(f"Processing year: {vergaderjaar}")

        year_folder = HANDELINGEN_DIR / vergaderjaar
        if not year_folder.exists():
            logging.warning(f"Folder not found: {year_folder}")
            continue

        output_file = OUTPUT_DIR / f"speeches_{vergaderjaar}.csv"

        # Process all XML files in the folder
        files = list(year_folder.glob("*.xml"))
        logging.info(f"Found {len(files)} files to process")

        total_speeches = 0
        for i, file in enumerate(files, 1):
            speeches = process_document(file)
            if speeches:
                save_to_csv(speeches, output_file)
                total_speeches += len(speeches)

            if i % 10 == 0:  # Log progress every 10 files
                logging.info(f"Processed {i}/{len(files)} files, {total_speeches} speeches so far")

        logging.info(f"Completed {vergaderjaar}: processed {len(files)} files, found {total_speeches} speeches")
        logging.info(f"Results saved to {output_file}")

if __name__ == "__main__":
    main()
