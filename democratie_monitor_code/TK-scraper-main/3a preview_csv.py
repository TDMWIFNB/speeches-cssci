import pandas as pd
from pathlib import Path

def preview_speeches(year="2022-2023", max_rows=10, text_length=100):
    """
    Preview the speeches CSV file

    Args:
        year: Parliamentary year to preview
        max_rows: Number of rows to show
        text_length: Maximum length for speech text preview
    """
    # Define paths
    data_dir = Path.cwd() / "data" / "parsed"
    csv_file = data_dir / f"speeches_{year}.csv"

    if not csv_file.exists():
        print(f"No CSV file found for {year}")
        return

    # Read CSV
    df = pd.read_csv(csv_file)

    # Truncate speech text
    df['speech_text'] = df['speech_text'].str[:text_length] + '...'

    # Select main columns for display
    display_columns = [
        'speaker_name',
        'speaker_party',
        'speech_text',
        'date',
        'kamer'
    ]

    # Display info
    print(f"\nTotal speeches in {year}: {len(df)}")
    print(f"\nFirst {max_rows} speeches:")
    print(df[display_columns].head(max_rows).to_string())

if __name__ == "__main__":
    preview_speeches()
