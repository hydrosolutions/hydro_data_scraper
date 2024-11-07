
from dotenv import load_dotenv
from time import sleep

from scrapers.lindas_sparql_scraper import LindasSparqlHydroScraper as lindas_scraper


def main():

    # Load environment variables
    load_dotenv()

    # Initialize and run scraper
    scraper = lindas_scraper()
    scraper.run()
    # Sleep for 1 second
    sleep(1)
    # Clean duplicates
    scraper.clean_csv_duplicates()


if __name__ == "__main__":

    main()