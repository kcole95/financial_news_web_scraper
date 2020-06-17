# financial_news_web_scraper
A web scraper which extracts text from financial articles on Reuters' and Forbes' websites, and feeds them into a postgreSQL table using the psycopg2 module.

## Getting Started
It is recommended that a virtual environment is created. Download the modules listed in the requirements.txt file.

## Usage
Fill out the database_details.py file with the appropriate information. This will require an existing database to connect to, as well as a user profile.

Run scraper.py. This will prompt the user to confirm whether an existing postgreSQL table *with the correct columns* already exists (essentially checking if this script has been run before). If no such table exists, a new one will be created (script allows user to define the table name). 

scraper.py will also write the scraped data to csv files in the working directory to allow quick viewing of the information with a module such as pandas.
