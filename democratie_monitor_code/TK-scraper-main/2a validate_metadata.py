import logging
from pathlib import Path
import csv
from datetime import datetime
import pandas as pd

# Define base directories
BASE_DIR = Path.cwd()
DATA_DIR = BASE_DIR / "data"
HANDELINGEN_DIR = DATA_DIR / "handelingen"
META_DIR = DATA_DIR / "meta"
ERROR_LOG = BASE_DIR / "meta_error_log.txt"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s %(levelname)-5s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('meta_validation.log')
    ]
)

def read_error_log():
    """Read the error log and return a set of failed files"""
    if not ERROR_LOG.exists():
        return set()

    failed_files = set()
    with ERROR_LOG.open('r') as f:
        for line in f:
            if 'Error processing' in line:
                # Extract filename from error message
                filename = line.split('Error processing ')[1].split(':')[0].strip()
                failed_files.add(filename)
    return failed_files

def validate_metadata_file(meta_file, year_folder):
    """Validate a single metadata CSV file"""
    if not meta_file.exists():
        logging.error(f"Metadata file missing: {meta_file}")
        return False, {}

    try:
        df = pd.read_csv(meta_file)
        validation_results = {
            'total_rows': len(df),
            'missing_values': df.isnull().sum().to_dict(),
            'invalid_dates': 0,
            'invalid_kamer': 0,
            'duplicate_files': len(df) - len(df['file'].unique()),
            'missing_files': 0,
            'unexpected_files': 0
        }

        # Check date format
        for date in df['date']:
            try:
                datetime.strptime(str(date), '%Y-%m-%d')
            except ValueError:
                validation_results['invalid_dates'] += 1

        # Check kamer values
        valid_kamer = {'tk', 'ek', 'vv'}
        validation_results['invalid_kamer'] = len(df[~df['kamer'].isin(valid_kamer)])

        # Check file existence
        xml_files = {f.stem for f in year_folder.glob('*.xml')}
        meta_files = set(df['file'])

        validation_results['missing_files'] = len(xml_files - meta_files)
        validation_results['unexpected_files'] = len(meta_files - xml_files)

        return True, validation_results

    except Exception as e:
        logging.error(f"Error validating {meta_file}: {e}")
        return False, {}

def validate_all_metadata():
    """Validate all metadata files and their consistency"""
    vergaderjaren = ["2023-2024", "2022-2023", "2021-2022", "2020-2021", "2019-2020"]
    failed_files = read_error_log()

    overall_stats = {
        'total_xml_files': 0,
        'total_meta_records': 0,
        'total_failed_files': len(failed_files),
        'total_missing_files': 0,
        'files_without_errors_but_missing': 0
    }

    logging.info("Starting metadata validation...")

    for vergaderjaar in vergaderjaren:
        logging.info(f"\nValidating year: {vergaderjaar}")

        year_folder = HANDELINGEN_DIR / vergaderjaar
        meta_file = META_DIR / f"meta_{vergaderjaar}.csv"

        if not year_folder.exists():
            logging.warning(f"Handelingen folder missing for {vergaderjaar}")
            continue

        # Count XML files
        xml_files = list(year_folder.glob('*.xml'))
        xml_file_count = len(xml_files)
        overall_stats['total_xml_files'] += xml_file_count

        # Validate metadata file
        success, results = validate_metadata_file(meta_file, year_folder)

        if success:
            logging.info(f"Results for {vergaderjaar}:")
            logging.info(f"  Total XML files: {xml_file_count}")
            logging.info(f"  Total metadata records: {results['total_rows']}")
            logging.info(f"  Missing values per column: {results['missing_values']}")
            logging.info(f"  Invalid dates: {results['invalid_dates']}")
            logging.info(f"  Invalid kamer values: {results['invalid_kamer']}")
            logging.info(f"  Duplicate file entries: {results['duplicate_files']}")
            logging.info(f"  Files in XML but not in metadata: {results['missing_files']}")
            logging.info(f"  Files in metadata but not in XML: {results['unexpected_files']}")

            overall_stats['total_meta_records'] += results['total_rows']
            overall_stats['total_missing_files'] += results['missing_files']

            # Check for files missing from metadata but not in error log
            if results['missing_files'] > 0:
                xml_files = {f.stem for f in year_folder.glob('*.xml')}
                meta_files = set(pd.read_csv(meta_file)['file'])
                missing_files = xml_files - meta_files
                missing_without_errors = missing_files - failed_files

                if missing_without_errors:
                    overall_stats['files_without_errors_but_missing'] += len(missing_without_errors)
                    logging.warning(f"Files missing from metadata but not in error log for {vergaderjaar}:")
                    for file in sorted(missing_without_errors)[:10]:  # Show first 10 examples
                        logging.warning(f"  - {file}")
                    if len(missing_without_errors) > 10:
                        logging.warning(f"  ... and {len(missing_without_errors) - 10} more")

    # Print overall summary
    logging.info("\nOVERALL SUMMARY:")
    logging.info(f"Total XML files: {overall_stats['total_xml_files']}")
    logging.info(f"Total metadata records: {overall_stats['total_meta_records']}")
    logging.info(f"Total files in error log: {overall_stats['total_failed_files']}")
    logging.info(f"Total files missing from metadata: {overall_stats['total_missing_files']}")
    logging.info(f"Files missing without logged errors: {overall_stats['files_without_errors_but_missing']}")

    coverage = (overall_stats['total_meta_records'] / overall_stats['total_xml_files']) * 100 if overall_stats['total_xml_files'] > 0 else 0
    logging.info(f"Overall metadata coverage: {coverage:.1f}%")

if __name__ == "__main__":
    validate_all_metadata()
