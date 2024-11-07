#
import pandas as pd
from datetime import datetime
import csv
from pathlib import Path
import logging
from typing import Optional, Set
from SPARQLWrapper import SPARQLWrapper, JSON
import os
import sys
import time

class LindasSparqlQueryBuilder:
    """Builder class for SPARQL queries for hydrological data."""

    BASE_URL = os.getenv('SPARQL_BASE_URL', 'https://environment.ld.admin.ch/foen/hydro')
    DIMENSION_URL = f"{BASE_URL}/dimension"

    # Dictionary mapping shorter parameter names to full URIs
    PARAMETER_MAPPING = {
        "station": f"{DIMENSION_URL}/station",
        "discharge": f"{DIMENSION_URL}/discharge",
        "measurementTime": f"{DIMENSION_URL}/measurementTime",
        "waterLevel": f"{DIMENSION_URL}/waterLevel",
        "dangerLevel": f"{DIMENSION_URL}/dangerLevel",
        "waterTemperature": f"{DIMENSION_URL}/waterTemperature",
        "isLiter": "http://example.com/isLiter"
    }

    def __init__(self):
        self.site_code = None
        self.parameters = []

    def add_site(self, site_code: str):
        """Add a single site code to the query."""
        try:
            code_int = int(site_code)
            if 1 <= code_int <= 9999:
                self.site_code = f"{code_int:d}"
            else:
                raise ValueError(f"Site code {site_code} is not <= 4-digit integer")
        except ValueError as e:
            raise ValueError(f"Invalid site code {site_code}: {str(e)}")
        return self

    def add_parameters(self, parameters: list):
        """Add parameters to the query."""
        # Validate parameters against known mappings
        invalid_params = [p for p in parameters if p not in self.PARAMETER_MAPPING]
        if invalid_params:
            raise ValueError(f"Invalid parameters: {invalid_params}")

        self.parameters.extend(parameters)
        return self

    def build_query(self) -> str:
        """Build the complete SPARQL query for a single site."""
        if not self.site_code:
            raise ValueError("No site code specified")
        if not self.parameters:
            raise ValueError("No parameters specified")

        # Build the FILTER clause for parameters
        params_filter = ",\n    ".join(
            f'<{self.PARAMETER_MAPPING[param]}>'
            for param in self.parameters
        )

        # Construct the query for a single site
        query = f"""
PREFIX schema: <http://schema.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?predicate ?object
FROM <https://lindas.admin.ch/foen/hydro>
WHERE {{
  BIND(<{self.BASE_URL}/river/observation/{self.site_code}> AS ?subject)
  ?subject ?predicate ?object .
  FILTER (?predicate IN (
    {params_filter}
  ))
}}
"""
        return query




class LindasSparqlHydroScraper:
    def __init__(self):
        # Set up logging
        self._setup_logging()

        # Get configuration from environment variables with defaults
        self.endpoint_url = os.getenv('SPARQL_ENDPOINT', 'https://example.com/sparql')
        self.data_dir = self._setup_data_dir()
        self.output_file = self.data_dir / 'lindas_hydro_data.csv'

        # Initialize query builder
        self.query_builder = LindasSparqlQueryBuilder()

        # Get site codes and parameters from environment or configuration
        self.site_codes = self._get_site_codes()
        self.parameters = self._get_parameters()

        # Initialize SPARQL client
        self.sparql = SPARQLWrapper(self.endpoint_url)
        self.sparql.setReturnFormat(JSON)

        # Ensure data directory exists
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Initialize CSV if needed
        if not self.output_file.exists():
            self._initialize_csv()

        # Set for tracking processed records
        self.processed_records: Set[str] = set()
        self._load_processed_records()

    def _setup_data_dir(self) -> Path:
        """
        Set up the data directory path based on environment.
        Uses environment variable if set, otherwise determines if running in Docker.
        Falls back to local './data' directory if neither.
        """
        # First priority: Check for environment variable
        env_data_dir = os.getenv('HYDRO_DATA_DIR')
        if env_data_dir:
            data_dir = Path(env_data_dir)
        # Second priority: Check if running in Docker
        elif os.path.exists('/.dockerenv'):
            data_dir = Path('/app/data')
        # Default: Use local data directory
        else:
            data_dir = Path.cwd() / 'data'

        # Ensure directory exists
        data_dir.mkdir(parents=True, exist_ok=True)

        return data_dir

    def _get_site_codes(self) -> list:
        """Get site codes from environment variable or default list."""
        env_codes = os.getenv('SITE_CODES')
        if env_codes:
            return env_codes.split(',')
        return ["2044", "2112", "2491", "2355"]  # Default codes

    def _get_parameters(self) -> list:
        """Get parameters from environment variable or default list."""
        env_params = os.getenv('PARAMETERS')
        if env_params:
            return env_params.split(',')
        return [
            "station",
            "discharge",
            "measurementTime",
            "waterLevel",
            "dangerLevel",
            "waterTemperature",
            "isLiter"
        ]

    def _setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _initialize_csv(self):
        """Create the CSV file with headers."""
        headers = ['timestamp', 'station_id', 'discharge', 'water_level',
                  'danger_level', 'water_temperature', 'is_liter']
        with open(self.output_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
        self.logger.info(f"Initialized CSV file at {self.output_file}")

    def _load_processed_records(self):
        """Load previously processed records to avoid duplicates."""
        if self.output_file.exists():
            try:
                df = pd.read_csv(self.output_file)
                self.processed_records = set(
                    df['timestamp'].astype(str) + '_' + df['station_id'].astype(str)
                )
            except Exception as e:
                self.logger.error(f"Error loading processed records: {e}")

    def _convert_value(self, value: Optional[str], type_hint: str) -> Optional[float]:
        """Convert string values to appropriate types."""
        if value is None:
            return None

        try:
            if type_hint in ['discharge', 'water_level', 'water_temperature', 'danger_level']:
                return float(value)
            else:
                return value
        except (ValueError, TypeError):
            self.logger.warning(f"Could not convert {value} for {type_hint}")
            return None

    def process_data(self, results: dict, site_code: str) -> list:
        """Process SPARQL results for a single site."""
        current_time = datetime.now().isoformat()

        try:
            # Initialize record with known site_id
            record = {
                'station_id': site_code,
                'timestamp': None,
                'discharge': None,
                'water_level': None,
                'water_temperature': None,
                'danger_level': None,
                'isLiter': None
            }

            # Process each predicate-object pair
            for result in results["results"]["bindings"]:
                predicate = result["predicate"]["value"]
                obj = result["object"]["value"]

                # Clean up predicate name
                pred_name = (predicate
                    .replace("https://environment.ld.admin.ch/foen/hydro/dimension/", "")
                    .replace("http://example.com/", ""))

                # Map predicates to record fields
                if pred_name == "measurementTime":
                    record['timestamp'] = obj
                elif pred_name == "discharge":
                    record['discharge'] = self._convert_value(obj, 'discharge')
                elif pred_name == "waterLevel":
                    record['water_level'] = self._convert_value(obj, 'water_level')
                elif pred_name == "waterTemperature":
                    record['water_temperature'] = self._convert_value(obj, 'water_temperature')
                elif pred_name == "dangerLevel":
                    record['danger_level'] = self._convert_value(obj, 'danger_level')

            # Check if we have any actual measurements
            if record['timestamp'] and any(record[key] is not None for key in [
                'discharge', 'water_level', 'water_temperature'
            ]):
                return [record]

            self.logger.warning(f"No valid measurements found for site {site_code}")
            return []

        except Exception as e:
            self.logger.error(f"Error processing data for site {site_code}: {str(e)}")
            return []

    def run(self):
        """Main method to run the scraper."""
        self.logger.info("Starting data collection...")
        all_records = []

        try:
            # Process each site individually
            for site_code in self.site_codes:
                self.logger.info(f"Processing site {site_code}")

                try:
                    # Create new query builder for this site
                    query_builder = LindasSparqlQueryBuilder()
                    query = (query_builder
                            .add_site(site_code)
                            .add_parameters(self.parameters)
                            .build_query())

                    # Configure SPARQL query
                    self.sparql.setQuery(query)

                    # Fetch data for this site
                    results = self.fetch_data()
                    if not results:
                        self.logger.warning(f"No data fetched for site {site_code}")
                        continue

                    # Process data for this site
                    records = self.process_data(results, site_code)

                    # Check for uniqueness before adding
                    new_records = []
                    for record in records:
                        record_key = f"{record['timestamp']}_{record['station_id']}"
                        if record_key not in self.processed_records:
                            self.processed_records.add(record_key)
                            new_records.append(record)

                    all_records.extend(new_records)

                except Exception as e:
                    self.logger.error(f"Error processing site {site_code}: {str(e)}")
                    continue  # Continue with next site even if one fails

            # Save all records at the end
            if all_records:
                self.save_data(all_records)
                self.logger.info(f"Successfully processed {len(all_records)} records")
            else:
                self.logger.warning("No records were created from any site")

        except Exception as e:
            self.logger.error(f"Error in data collection pipeline: {str(e)}")

    def fetch_data(self):
        """Fetch data with improved error handling and retry logic."""
        try:
            max_retries = 3
            retry_delay = 2  # seconds

            for attempt in range(max_retries):
                try:
                    results = self.sparql.query().convert()

                    # Validate results structure
                    if not results.get("results", {}).get("bindings"):
                        self.logger.warning("Query returned no bindings")
                        return None

                    self.logger.info(f"Successfully fetched {len(results['results']['bindings'])} results")
                    return results

                except Exception as e:
                    if attempt < max_retries - 1:
                        self.logger.warning(f"Attempt {attempt + 1} failed, retrying in {retry_delay} seconds...")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    else:
                        raise e

        except Exception as e:
            self.logger.error(f"Error fetching data: {str(e)}")
            return None


    def save_data(self, records: list):
        """Save records to CSV file with validation."""
        if not records:
            self.logger.warning("No records to save")
            return

        fieldnames = ['timestamp', 'station_id', 'discharge', 'water_level',
                     'danger_level', 'water_temperature', 'isLiter']

        try:
            # Check if file exists to determine if we need to write headers
            file_exists = self.output_file.exists()

            with open(self.output_file, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)

                # Write headers if new file
                if not file_exists:
                    writer.writeheader()

                # Filter out empty records before writing
                valid_records = []
                for record in records:
                    if record['timestamp']:  # Only save records with a timestamp
                        valid_records.append(record)

                writer.writerows(valid_records)

                self.logger.info(f"Added {len(valid_records)} records with data")

        except Exception as e:
            self.logger.error(f"Error writing to CSV: {str(e)}")

    def clean_csv_duplicates(self):
        """Read CSV file, remove exact duplicate rows, and save back the cleaned data."""
        try:
            if not self.output_file.exists():
                self.logger.warning("No CSV file exists yet to clean")
                return

            # Read the CSV file
            self.logger.info(f"Reading CSV file from {self.output_file}")
            df = pd.read_csv(self.output_file)
            initial_rows = len(df)

            # Drop exact duplicate rows
            df_cleaned = df.drop_duplicates(keep='first')
            dropped_rows = initial_rows - len(df_cleaned)

            if dropped_rows > 0:
                # Save the cleaned dataframe back to CSV
                df_cleaned.to_csv(self.output_file, index=False)
                self.logger.info(f"Removed {dropped_rows} exact duplicate rows from CSV")

                # Update the processed_records set
                self.processed_records = set(
                    df_cleaned['timestamp'].astype(str) + '_' + df_cleaned['station_id'].astype(str)
                )
            else:
                self.logger.info("No exact duplicates found in CSV file")

        except Exception as e:
            self.logger.error(f"Error cleaning CSV duplicates: {str(e)}")
            self.logger.exception("Full error details:")