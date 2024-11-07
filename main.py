
from dotenv import load_dotenv

from scrapers.lindas_sparql_scraper import LindasSparqlHydroScraper as lindas_scraper


def main():

    # Load environment variables
    load_dotenv()

    # Initialize and run scraper
    scraper = lindas_scraper()
    scraper.run()


if __name__ == "__main__":

    main()