# Scraping FincaRaiz in airflow.

Vertical and horizontal scraping on the fincaraiz page with playwright and lxml, extracting data from flats for rent. This data is cleaned and stored in a SQLite database. This process runs in airflow and lasts for 2 minutes per page (30 items).

**Note**: An example is provided from the city of Medellín/Antioquia.
