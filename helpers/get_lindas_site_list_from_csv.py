import pandas as pd
from typing import List
import argparse
import sys
from pathlib import Path

def get_river_station_codes(csv_file: str) -> List[str]:
    """
    Read CSV file and extract station codes for rivers (lhg_code == lhg_fluss).
    Removes '.htm' from lhg_url values.

    Args:
        csv_file: Path to the CSV file

    Returns:
        List of station codes without '.htm'
    """
    try:
        # Read CSV file
        df = pd.read_csv(csv_file, encoding='latin1')  # Using latin1 encoding for special characters

        # Filter for river stations and extract lhg_url
        river_stations = df[df['lhg_code'] == 'lhg_fluss']['lhg_url']

        # Remove '.htm' and convert to list
        station_codes = [url.replace('.htm', '') for url in river_stations]

        # Cast station_codes to integers
        station_codes = [int(code) for code in station_codes]

        return station_codes

    except Exception as e:
        print(f"Error processing CSV file: {e}")
        return []

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description='Extract river station codes from CSV file'
    )
    parser.add_argument(
        'csv_file',
        type=str,
        help='Path to the CSV file containing station information'
    )

    # Parse arguments
    args = parser.parse_args()

    # Get station codes
    station_codes = get_river_station_codes(args.csv_file)

    if station_codes:
        print("River station codes:")
        print(station_codes)
        print(f"\nTotal number of river stations: {len(station_codes)}")
    else:
        sys.exit(1)  # Exit with error status if no stations found

if __name__ == "__main__":

    main()