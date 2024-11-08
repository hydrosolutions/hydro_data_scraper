# The hydro data scraper
<!-- GitHub Actions Status -->
![Build and Deploy](https://github.com/hydrosolutions/hydro_data_scraper/actions/workflows/docker-build-push.yml/badge.svg)

<!-- DockerHub Status -->
[![Docker Pulls](https://img.shields.io/docker/pulls/mabesa/hydro-scraper)](https://hub.docker.com/r/mabesa/hydro-scraper)
[![Docker Image Size](https://img.shields.io/docker/image-size/mabesa/hydro-scraper/latest)](https://hub.docker.com/r/mabesa/hydro-scraper)

<!-- Python Version -->
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-312/)

<!-- License -->
[![License](https://img.shields.io/github/license/mabesa/hydro-scraper)](https://github.com/mabesa/hydro-scraper/blob/main/LICENSE)

<!-- Code Quality & Maintenance -->
[![Last Commit](https://img.shields.io/github/last-commit/hydrosolutions/hydro_data_scraper)](https://github.com/hydrosolutions/hydro_data_scraper/commits/main)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

The hydro data scraper is a tool to collect operational hydrological data from the [Linked Data Services platform (LINDAS)](https://lindas.admin.ch/?lang=de). This repository contains the code that can be deployed on a server to periodically read the latest observations from LINDAS. 

# Example deployment
1. On your server, install docker and git.
2. Clone the git repository: git clone https://github.com/hydrosolutions/hydro_data_scraper.git (in the this example we clone it to path /data)
3. Edit the gauge station IDs in the .env file of the repository (a geospatial layer with station IDs is available on [map.geo.admin.ch](https://map.geo.admin.ch/#/map?lang=en&center=2660025,1189925&z=1&bgLayer=ch.swisstopo.pixelkarte-grau&topic=gewiss&layers=ch.bafu.hydrologie-hydromessstationen).
4. Pull the docker image that is created from the main branch of this repository: docker pull mabesa/hydro-scarper
5. Edit your crontabs to periodically run the hydro data scraper every 9 minutes:
   
    Open the crontab file for editing: crontab -e
   
    Add the following line: */9 * * * * docker run --rm -v /data/hydro_data_scraper:/app mabesa/hydro-scraper:latest >> /data/hydro_data_scraper/logfile.log 2>&1
   
    Save your edits and exit the editor  
  
Note that this will write a file called lindas_hydro_data.csv to path /data/hydro_data_scraper/data which will grow quickly over time.  
