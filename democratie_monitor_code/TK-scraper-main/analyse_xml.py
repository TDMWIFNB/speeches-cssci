from pathlib import Path
from lxml import etree
import logging

logging.basicConfig(level=logging.INFO)

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

def extract_speeches(xml_file):
    """Extract all speeches from a single XML file"""
    logging.info(f"\nProcessing file: {xml_file.name}")

    # Parse XML
    tree = etree.parse(xml_file)
    root = tree.getroot()

    # Find all speech elements
    speeches = root.xpath("//spreekbeurt")
    logging.info(f"Found {len(speeches)} speeches")

    results = []
    for speech in speeches:
        try:
            # Get speaker info
            spreker = speech.find("spreker")
            if spreker is None:
                continue

            # Extract name components
            voorvoegsels = spreker.find("voorvoegsels")
            voorvoegsels = voorvoegsels.text if voorvoegsels is not None else ""

            achternaam = spreker.xpath(".//achternaam")
            achternaam = achternaam[0].text if achternaam else ""

            # Extract party
            party = extract_party(speech)

            # Get speech text from tekst/al elements
            text_parts = []
            for al in speech.xpath(".//tekst//al"):
                if al.text:
                    text_parts.append(al.text.strip())

            speech_text = "\n".join(text_parts)

            # Construct full name
            full_name = f"{voorvoegsels} {achternaam}".strip()

            # Store result
            result = {
                "speaker": full_name,
                "party": party,
                "text": speech_text
            }
            results.append(result)

            # Print preview for verification
            preview = speech_text[:100] + "..." if len(speech_text) > 100 else speech_text
            logging.info(f"\nSpeaker: {full_name}")
            logging.info(f"Party: {party}")
            logging.info(f"Text preview: {preview}")

        except Exception as e:
            logging.error(f"Error processing speech: {e}")
            continue

    return results

def main():
    # Process first 5 files from 2022-2023
    data_dir = Path.cwd() / "data" / "handelingen" / "2022-2023"
    xml_files = list(data_dir.glob("*.xml"))[:5]

    total_speeches = 0
    for xml_file in xml_files:
        speeches = extract_speeches(xml_file)
        total_speeches += len(speeches)

    logging.info(f"\nTotal processed files: {len(xml_files)}")
    logging.info(f"Total speeches found: {total_speeches}")

if __name__ == "__main__":
    main()
