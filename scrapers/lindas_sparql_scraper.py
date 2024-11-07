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
        self.site_codes = []
        self.parameters = []

    def add_sites(self, site_codes: list):
        """Add site codes to the query."""
        # Ensure all site codes are 4-digit integers
        validated_codes = []
        for code in site_codes:
            try:
                code_int = int(code)
                if 1 <= code_int <= 9999:
                    validated_codes.append(str(code_int))
                else:
                    raise ValueError(f"Site code {code} is not a 4-digit integer")
            except ValueError as e:
                raise ValueError(f"Invalid site code {code}: {str(e)}")

        self.site_codes.extend(validated_codes)
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
        """Build the complete SPARQL query."""
        if not self.site_codes:
            raise ValueError("No site codes specified")
        if not self.parameters:
            raise ValueError("No parameters specified")

        # Remove duplicates while preserving order
        self.site_codes = list(dict.fromkeys(self.site_codes))
        self.parameters = list(dict.fromkeys(self.parameters))

        # Build the VALUES clause for sites
        sites_values = "\n    ".join(
            f'<{self.BASE_URL}/river/observation/{code}>'
            for code in self.site_codes
        )

        # Build the FILTER clause for parameters
        params_filter = ",\n    ".join(
            f'<{self.PARAMETER_MAPPING[param]}>'
            for param in self.parameters
        )

        # Construct the complete query
        query = f"""
PREFIX schema: <http://schema.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?predicate ?object
FROM <https://lindas.admin.ch/foen/hydro>
WHERE {{
  VALUES ?subject {{
    {sites_values}
  }}
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

        # Build the query
        self.query = self._build_query()
        self.logger.debug(f"Built query: {self.query}")

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

    def _build_query(self) -> str:
        """Build the SPARQL query using the query builder."""
        try:
            return (self.query_builder
                    .add_sites(self.site_codes)
                    .add_parameters(self.parameters)
                    .build_query())
        except ValueError as e:
            self.logger.error(f"Error building query: {e}")
            raise

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
                  'danger_level', 'water_temperature', 'collection_time']
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

    def fetch_data(self) -> Optional[dict]:
        """Fetch data from SPARQL endpoint."""
        try:
            self.sparql.setQuery(self.query)
            results = self.sparql.query().convert()
            return results
        except Exception as e:
            self.logger.error(f"Error fetching data: {str(e)}")
            return None

    def process_data(self, results: dict) -> list:
        """Process SPARQL results into records."""
        current_time = datetime.now().isoformat()
        processed_records = []
        current_station = None
        current_record = {}

        try:
            for result in results["results"]["bindings"]:
                predicate = result["predicate"]["value"]
                obj = result["object"]["value"]

                # Clean up predicate names
                predicate = (predicate
                    .replace("https://environment.ld.admin.ch/foen/hydro/dimension/", "")
                    .replace("http://example.com/", ""))

                # Clean up object values
                obj = obj.replace("https://environment.ld.admin.ch/foen/hydro/station/", "")

                if predicate == "station":
                    if current_station and current_record:
                        current_record['collection_time'] = current_time
                        record_key = f"{current_record.get('timestamp', '')}_{current_record.get('station_id', '')}"
                        if record_key not in self.processed_records:
                            processed_records.append(current_record)
                            self.processed_records.add(record_key)

                    current_station = obj
                    current_record = {'station_id': obj}
                else:
                    field_mapping = {
                        'measurementTime': 'timestamp',
                        'discharge': 'discharge',
                        'waterLevel': 'water_level',
                        'dangerLevel': 'danger_level',
                        'waterTemperature': 'water_temperature'
                    }

                    if predicate in field_mapping:
                        current_record[field_mapping[predicate]] = obj

            # Don't forget the last record
            if current_record:
                current_record['collection_time'] = current_time
                record_key = f"{current_record.get('timestamp', '')}_{current_record.get('station_id', '')}"
                if record_key not in self.processed_records:
                    processed_records.append(current_record)
                    self.processed_records.add(record_key)

            return processed_records

        except Exception as e:
            self.logger.error(f"Error processing data: {str(e)}")
            return []

    def save_data(self, records: list):
        """Save new records to the CSV file."""
        if not records:
            return

        fieldnames = ['timestamp', 'station_id', 'discharge', 'water_level',
                     'danger_level', 'water_temperature', 'collection_time']

        try:
            with open(self.output_file, 'a', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writerows(records)
            self.logger.info(f"Added {len(records)} new records")
        except Exception as e:
            self.logger.error(f"Error writing to CSV: {str(e)}")

    def run(self):
        """Main method to run the scraper."""
        self.logger.info("Starting data collection...")

        try:
            # Fetch data
            results = self.fetch_data()
            if not results:
                self.logger.warning("No data fetched")
                return

            # Process data
            records = self.process_data(results)
            if not records:
                self.logger.warning("No new records to save")
                return

            # Save data
            self.save_data(records)

            self.logger.info("Data collection completed successfully")

        except Exception as e:
            self.logger.error(f"Error in data collection pipeline: {str(e)}")