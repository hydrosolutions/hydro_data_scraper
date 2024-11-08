# The hydro data scraper

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
