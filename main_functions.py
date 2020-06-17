# Import webscraping Libraries
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
from time import sleep

# Import data analysis
import pandas as pd
import csv

# Import database libraries
import psycopg2
from pprint import pprint

import os

class DatabaseConnection():
    
    '''Class to create a connection with a postgreSQL database.'''
    
    def __init__(self, dbname, user, password):
        
        '''Initialise the Database object.'''
        self.dbname = dbname
        self.user = user
        self.password = password
        self.table_name = ''
    

    def create_connection(self):

        '''Function to connect to the postgreSQL database.'''

        try:
            # Initialise connection object. Connects to a RDMS (e.g. postgreSQL) and createS a cursor object. Automatically commits changes to the database.
            self.connection = psycopg2.connect(dbname=self.dbname, user=self.user, password=self.password)
            self.connection.autocommit = True
            self.cursor = self.connection.cursor()
        
        except:
            print('Cannot connect to the database')
            
    
    def create_table(self):
        
        '''Method to create a new table in postgreSQL.'''
        
        # Enable user to select name for new table.
        # global table_name
        self.table_name = input('Enter table name: ')
        
        # Define the SQL query for creating the new table. Column names relate to elements of an article from a financial services website.
        sql = "CREATE TABLE " + self.table_name + "(id serial PRIMARY KEY, date date, category varchar(50), title varchar(150), summary varchar(65535), full_text varchar(65535), link varchar(255))"
        
        # Execute the SQL query.
        self.cursor.execute(sql)
    
    
    def insert_new_record(self, data_row):
            
        '''Method to insert data into the new SQL table.'''
            
        # Create a tuple of the row of data
        new_record = data_row
        
        # Define the SQL query for inserting data.
        sql = "INSERT INTO " + self.table_name + "(date, category, title, summary, full_text, link) VALUES('" +new_record[0]+ "','" +new_record[1]+ "','" +new_record[2]+ "','" +new_record[3]+ "','" +new_record[4]+ "','" +new_record[5]+ "')"
        
        # Show in the console what data has been inserted into the table.
        pprint(sql)
        print("\n")
        
        # Execute the SQL query.
        self.cursor.execute(sql)


class UrlConnection():

    '''Class to connect to a financial services webpage and extract relevant article information.'''
    
    def __init__(self, url, database):

        '''Initialisation method. UrlConnection object parameter - the web address/url to connect to.'''
        
        self.url = url
        self.database = database
        
        # url prefix for reuters webpage to connect to.
        self.url_prefix = 'http://uk.reuters.com'

        # List of forbes.com urls to connect to.
        self.forbes_urls = ['https://www.forbes.com/fintech/','https://www.forbes.com/crypto-blockchain/','https://www.forbes.com/investing/', 'https://www.forbes.com/markets/']
        
        # Create a Set to hold the article info (will be stored as a tuple). Using a set ensures duplicate articles are not extracted.
        self.articles = set()

        # Set up the base directory
        BASE_DIR = os.getcwd()

        if 'raw_webpages' in os.listdir():
            RAW_WEB_DIR = os.path.join(BASE_DIR, 'raw_webpages')
        else:
            os.mkdir('raw_webpages')
            RAW_WEB_DIR = os.path.join(BASE_DIR, 'raw_webpages')

        
        # Define the csv files to save the extracted information to.
        if self.url == 'https://uk.reuters.com/business/markets/uk':
            self.filename = os.path.join(RAW_WEB_DIR, 'reuters_uk_markets.csv')

        elif self.url == 'https://uk.reuters.com/business/markets':
            self.filename = os.path.join(RAW_WEB_DIR, 'reuters_global_markets.csv')

        elif self.url in self.forbes_urls:
            self.filename = os.path.join(RAW_WEB_DIR, 'forbes_news.csv')


    def connect(self):

        '''function to connect to the relevant webpage.'''

        # Forbes webpages
        if self.url in self.forbes_urls:
            self.source = requests.get(self.url, headers={"cookie": "notice_gdpr_prefs"}).text
            self.soup = BeautifulSoup(self.source, 'html.parser')
        
        # Reuters webpages
        else:
            # Open the URL, and create a soup
            self.source = requests.get(self.url).text
            self.soup = BeautifulSoup(self.source, 'html.parser')


    def create_csv(self):
            
        '''Creates a csv file to save the article information to.'''

        f = open(self.filename, "w")

        headers = 'date, category, title, summary, full_text, link\n'
        f.write(headers)
        f.close()
    

    def div_classes(self):

        '''Webpage HTML elements for the Reuters articles. 
        Contains information on the webpage's Featured article and the article itself.'''

        self.classes = ['story-content', 'moduleBody']


    def show_articles(self):

        '''Prints summarised articles from webpage to the console.'''

        print(self.articles)


    
    def get_full_text(link):

        '''Gets full text of specified article.'''

        # Create a new soup object based on the article's unique url.
        new_source = requests.get(link).text
        new_soup = BeautifulSoup(new_source, 'html.parser')

        # Get all paragraph texts on the article new webpage.
        paragraphs = new_soup.findAll('p')
        text = ''

        # Join all text together into a large paragraph.
        for paragraph in paragraphs:
            para = paragraph.text
            text = text + para

        return text.strip().replace('\n','')



    def check_duplicates(filepath, link):
        
        '''Function to compare new articles with existing saved ones.
        Purpose is to ensure duplicate articles are not written to the save file.
        Compares the url of the new article to the urls of all saved articles,
        and only writes the new article if no match is found.'''
        
        try: 
            # Open the save file, and compare all urls.
            with open(filepath,'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    if row[5] == link:
                        print('Article already exists in the file, no data written.')
                        mode = 1
                        break
                    else:
                        mode = 2
        
        except IndexError:
            print('There was an error reading the save file. Duplicates could not be determined.')
            sleep(3)
            mode = 3

        return mode



    def article_generator(self):

        '''Extracts all articles on the webpage.''' 
        
        # FOR GLOBAL MARKETS WEBPAGE - Reuters.
        if self.url == 'https://uk.reuters.com/business/markets':
            
            # GLOBAL MARKET NEWS - LEAD STORY
            self.category = self.soup.find('header', class_='module-header').h1.text.strip()
            self.body = self.soup.find('div', class_='olympics-topStory').find('div', class_='column1 col col-6')

            try:
                # Evaluate date, title, summary and link for lead story.
                self.date = datetime.today().strftime('%Y-%m-%d')
                self.title = self.body.h2.text.strip().replace('\n','')
                self.summary = self.body.p.text.strip().replace('\n','')
                self.link = self.url_prefix + self.body.find('a')['href']

            # Avoids AttributeErrors such as: 'NoneType' object has no attribute 'text'.
            except AttributeError as e:
                pass

            # Obtain the full text of the lead story.
            self.full_text = UrlConnection.get_full_text(self.link)
            
            # Check for duplicates in the existing save file.
            option = UrlConnection.check_duplicates(self.filename, self.link)

            # If there are no duplicates:
            if option == 2 or option == 3:
                
                # Collate all the article information into a tuple.
                info = (str(self.date), self.category, self.title.replace(',', '|'), self.summary.replace(',', '|'), self.full_text.replace(',', '|'), self.link)
                
                # If the article information has been correctly formatted (e.g. 6 elements long) or is not an advert/spam article:
                if len(info) == 6:

                    # Insert the data into the postgreSQL table.
                    self.database.insert_new_record((str(self.date), self.category.replace("'", '/'), self.title.replace(',', '|').replace("'", '/'), self.summary.replace(',', '|').replace("'", '/'), self.full_text.replace(',', '|').replace("'", '/'), self.link))
                    
                    # Append the article information as a tuple to the main set.
                    self.articles.add(info)

            # Preview the article information in the console.
            print(self.date)
            print(self.category)
            print(self.title)
            print(self.summary)
            print(self.link)   
            print()

            

            # GLOBAL MARKET NEWS - STORIES
            self.body = self.soup.find('div', class_='olympics-topStory').find('div', class_='more-headlines gridPanel grid5')

            for article in self.body.find_all('li'):
                
                # Ignores any videos on the webpage.
                if not article.text:
                    pass
                else:
                    
                    # Evaluate all relevant information for the article.
                    self.category = self.soup.find('header', class_='module-header').h1.text.strip().replace('\n','')

                    # Format the date to remove any text (e.g. '1 hour ago' or '28 February 2020' converted to datetime objects.)
                    self.date = article.find('span', class_='timestamp').text.strip()
                    try:
                        self.date = datetime.strptime(''.join(self.date).strip(), '%d %b %Y')
                    except ValueError:
                        self.date = datetime.today().strftime('%Y-%m-%d')

                    self.title = article.a.text.strip().replace('\n','')
                    self.summary = article.a.text.strip().replace('\n','')

                    self.link = self.url_prefix + article.a['href']

                    self.full_text = UrlConnection.get_full_text(self.link)
                    
                    # Check for duplicates in the existing save file.
                    option = UrlConnection.check_duplicates(self.filename, self.link)

                    # If there are no duplicates:
                    if option == 2 or option == 3:
                        
                        # Collate all the article information into a tuple.
                        info = (str(self.date), self.category, self.title.replace(',', '|'), self.summary.replace(',', '|'), self.full_text.replace(',', '|'), self.link)
                        
                        # If the article information has been correctly formatted (e.g. 6 elements long) or is not an advert/spam article:
                        if len(info) == 6:

                            # Add data to postgreSQL database
                            self.database.insert_new_record((str(self.date), self.category.replace("'", '/'), self.title.replace(',', '|').replace("'", '/'), self.summary.replace(',', '|').replace("'", '/'), self.full_text.replace(',', '|').replace("'", '/'), self.link))
                            
                            # Append the articles as tuples to the set. Format so that they can be exported to a csv file without creating additional columns.
                            self.articles.add(info)

                    # Preview the article information in the console.
                    print(self.date)
                    print(self.category)
                    print(self.title)
                    print(self.summary)
                    print(self.link)   
                    print()



            # FEATURED HEADLINES
            self.ids = ['hp-evergreen-private-equity', 'hp-evergreen-technology', 'hp-evergreen-commodities', 'hp-evergreen-fxexpert', 'hp-evergreen-deals', 'hp-evergreen-companies']

            for x in self.ids:

                # Featured articles on the main webpage:
                self.body = self.soup.find('section', id=x)
                self.category = self.body.h4.text.strip().replace('\n','')

                # For the main featured article in each section, extract the relevant information.
                # No date information available, so take as today's date.
                self.date = datetime.today().strftime('%Y-%m-%d')

                try:
                    self.title = self.body.find('div', class_='story-headline').h2.text.strip()
                    self.summary = self.body.find('div', class_='story-headline').h2.text.strip().replace('\n','')
                except AttributeError:
                    self.title = self.body.find('h2', class_='story-title').text.strip()
                    self.summary = self.body.find('h2', class_='story-title').text.strip().replace('\n','')

                try:
                    self.link = self.url_prefix + self.body.find('div', class_='story-headline').a['href']
                except AttributeError:
                    self.link = self.url_prefix + self.body.find('h2', class_='story-title').a['href']

                self.full_text = UrlConnection.get_full_text(self.link)
                
                # Check for duplicates in the existing save file.
                option = UrlConnection.check_duplicates(self.filename, self.link)
                
                # If there are no duplicates:    
                if option == 2 or option == 3:

                    # Collate all the article information into a tuple.
                    info = (str(self.date), self.category, self.title.replace(',', '|'), self.summary.replace(',', '|'), self.full_text.replace(',', '|'), self.link)
                    
                    # If the article information has been correctly formatted (e.g. 6 elements long) or is not an advert/spam article:
                    if len(info) == 6:

                        # Add data to postgreSQL database
                        self.database.insert_new_record((str(self.date), self.category.replace("'", '/'), self.title.replace(',', '|').replace("'", '/'), self.summary.replace(',', '|').replace("'", '/'), self.full_text.replace(',', '|').replace("'", '/'), self.link))
                        
                        # Append the articles as tuples to the set. Format so that they can be exported to a csv file without creating additional columns.
                        self.articles.add(info)

                
                # Preview the article information in the console.
                print(self.date)
                print(self.category)
                print(self.title)
                print(self.summary)
                print(self.link)   
                print()


                # For the individual stories within the featured headlines (e.g. technology, commodities, FX etc.):
                for article in self.body.find_all('div', class_='story-content'):
                    
                    # Extract relevant information.
                    self.category = self.body.h4.text.strip().replace('\n','')
                    
                    # No date information available, so take as today's date.
                    self.date = datetime.today().strftime('%Y-%m-%d')
                    
                    self.title = article.h3.text.strip().replace('\n','')
                    self.summary = article.h3.text.strip().replace('\n','')
                    self.link = self.url_prefix + article.a['href']

                    self.full_text = UrlConnection.get_full_text(self.link)

                    # Check for duplicates in the existing save file.
                    option = UrlConnection.check_duplicates(self.filename, self.link)
                    
                    # If there are no duplicates:
                    if option == 2 or option == 3:

                        # Collate all the article information into a tuple.
                        info = (str(self.date), self.category, self.title.replace(',', '|'), self.summary.replace(',', '|'), self.full_text.replace(',', '|'), self.link)
                        
                        # If the article information has been correctly formatted (e.g. 6 elements long) or is not an advert/spam article:
                        if len(info) == 6:

                            # Add data to postgreSQL database
                            self.database.insert_new_record((str(self.date), self.category.replace("'", '/'), self.title.replace(',', '|').replace("'", '/'), self.summary.replace(',', '|').replace("'", '/'), self.full_text.replace(',', '|').replace("'", '/'), self.link))
                            
                            # Append the articles as tuples to the set. Format so that they can be exported to a csv file without creating additional columns.
                            self.articles.add(info)

                    # Preview the article information in the console.
                    print(self.date)
                    print(self.category)
                    print(self.title)
                    print(self.summary)
                    print(self.link)   
                    print()




            # REGIONAL MARKET NEWS
            tabs = ['tab-markets-emea','tab-markets-us','tab-markets-asia']
            for tab in tabs:
                
                # Extract relevant information.
                self.category = self.soup.find('section', class_= 'module tab-markets-regional').find('header', class_= 'module-header').h4.text.strip()

                if tab == tabs[0]:
                    self.category = self.category + ' - Europe & Middle East'
                elif tab == tabs[1]:
                    self.category = self.category + ' - United States'
                else:
                    self.category = self.category + ' - Asia Pacific'

                self.body = self.soup.find('section', id=tab)

                for article in self.body.find_all('article', class_='story'):
                    self.title = article.find('div', class_='story-content').h3.text.strip().replace('\n','')
                    self.summary = article.find('div', class_='story-content').p.text.strip().replace('\n','')
                    self.link = self.url_prefix + article.find('div', class_='story-content').a['href']
                    self.date = article.find('div', class_='story-content').find('time', class_='article-time').text.strip()
                    
                    # Format the date to remove any text (e.g. '8:30 GMT' converted to today's date).
                    if 'GMT' in self.date.split() or 'BST' in self.date.split():
                        self.date = datetime.today().strftime('%Y-%m-%d')
                    else:
                        try:
                            self.date = datetime.strptime(''.join(self.date).strip(), '%d %b %Y')
                        except ValueError:
                            pass

                    self.full_text = UrlConnection.get_full_text(self.link)
                    
                    # Check for duplicates in the existing save file.
                    option = UrlConnection.check_duplicates(self.filename, self.link)
                    
                    # If there are no duplicates:
                    if option == 2 or option == 3:

                        # Collate all the article information into a tuple.
                        info = (str(self.date), self.category, self.title.replace(',', '|'), self.summary.replace(',', '|'), self.full_text.replace(',', '|'), self.link)
                        
                        # If the article information has been correctly formatted (e.g. 6 elements long) or is not an advert/spam article:
                        if len(info) == 6:

                            # Add data to postgreSQL database
                            self.database.insert_new_record((str(self.date), self.category.replace("'", '/'), self.title.replace(',', '|').replace("'", '/'), self.summary.replace(',', '|').replace("'", '/'), self.full_text.replace(',', '|').replace("'", '/'), self.link))
                            
                            # Append the articles as tuples to the set. Format so that they can be exported to a csv file without creating additional columns.
                            self.articles.add(info)

                    # Preview the article information in the console.
                    print(self.date)
                    print(self.category)
                    print(self.title)
                    print(self.summary)
                    print(self.link)
                    print()


        # Extracting data from the Forbes webpage:
        else:

            # Determine which section of the Forbes webpage is being scraped.
            if self.url == self.forbes_urls[0]:
                self.category = 'Fintech'
            elif self.url == self.forbes_urls[1]:
                self.category = 'Cryptocurrency'
            elif self.url == self.forbes_urls[2]:
                self.category = 'Investing'
            else:
                self.category = 'Markets'


            self.body = self.soup.find_all('article', class_='stream-item et-promoblock-removeable-item et-promoblock-star-item')

            for article in self.body:
                
                # Extract relevant information.
                self.title = article.h2.text.strip().replace('\n','')
                self.summary = self.title

                # Format the date to remove any text (e.g. '1 hour ago' or '28 February 2020' converted to datetime objects.)
                self.date = article.find('div', class_='stream-item__date').text.strip()

                if 'minutes' in self.date.split() or 'minute' in self.date.split():
                    self.date = (datetime.today() - timedelta(hours=0, minutes=int(self.date.split()[0]))).strftime('%Y-%m-%d %H:%M')

                elif 'hours' in self.date.split() or 'hour' in self.date.split():
                    self.date = (datetime.today() - timedelta(hours=int(self.date.split()[0]), minutes=0)).strftime('%Y-%m-%d %H:%M')

                else:
                    try:
                        self.date = datetime.strptime(' '.join(''.join(self.date.split(',')).split()).strip(), '%b %d %Y')
                    except ValueError:
                        self.date = datetime.today().strftime('%Y-%m-%d')
                        
                self.link = article.find('a')['href']

                self.full_text = UrlConnection.get_full_text(self.link)

                # Check for duplicates in the existing save file.
                option = UrlConnection.check_duplicates(self.filename, self.link)
                
                # If there are no duplicates:
                if option == 2 or option == 3:

                    # Collate all the article information into a tuple.
                    info = (str(self.date), self.category, self.title.replace(',', '|'), self.summary.replace(',', '|'), self.full_text.replace(',', '|'), self.link)
                    
                    # If the article information has been correctly formatted (e.g. 6 elements long) or is not an advert/spam article:
                    if len(info) == 6:

                        # Add data to postgreSQL database
                        self.database.insert_new_record((str(self.date), self.category.replace("'", '/'), ' '.join(self.title.replace(',', '|').replace("'", '/').split()[:140]), self.summary.replace(',', '|').replace("'", '/'), self.full_text.replace(',', '|').replace("'", '/'), self.link))
                        
                        # Append the articles as tuples to the set. Format so that they can be exported to a csv file without creating additional columns.
                        self.articles.add(info)

                # Preview the article information in the console.
                print(self.date)
                print(self.category)
                print(self.title)
                print(self.summary)
                print(self.link)
                print()
                    


    def write_articles(self):

        '''Writes the formatted articles to a csv file.'''

        # Open file, append new articles and close the file. For each item in each tuple in the set, concatonate using a comma and specifiy a new line for the next tuple.
        with open(self.filename, 'a', encoding='utf-8') as f:
            for line in self.articles:
                f.write(','.join(s for s in line) + '\n')
        f.close()




        





