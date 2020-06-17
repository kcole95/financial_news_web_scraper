
from main_functions import DatabaseConnection, UrlConnection
from database_details import DBNAME, USER, PASSWORD
from time import sleep


if __name__ == '__main__':

    # Create a connection to the postgres database.
    postgres = DatabaseConnection(DBNAME, USER, PASSWORD)
    postgres.create_connection()

    # List of webpages to extract information from:
    webpages = ['https://uk.reuters.com/business/markets', 'https://www.forbes.com/fintech/','https://www.forbes.com/crypto-blockchain/','https://www.forbes.com/investing/', 'https://www.forbes.com/markets/']
    
    try:
        # Review the csv file and delete any erroneous rows
        with open('raw_webpages/forbes_news.csv', 'r+', encoding='utf-8') as inp, open('raw_webpages/forbes_news_clean.csv', 'w', newline='', encoding='utf-8') as out:
            
            # open the new clean file
            writer = csv.writer(out)
            
            # Read every row in the orginal unclean file
            for row in csv.reader(inp):
                
                # If the row is clean (e.g. 6 elements in total), compare it to the clean file to check for duplicates. If it's a clean row, write it to the clean file.
                if len(row) == 6:
                    writer.writerow(row)
                else:
                    pass
        
        inp.close()
        out.close()
    except:
        pass
    
    
    # Once a SQL table has been created once, it does not need to be created again. Statement is set to True if program is run for the first time.
    check = input('Do you have a table in the database already? (y/n): ')
    if check == 'y':
        
        checking = True
        while checking:
            t_name = input('Enter table name: ')
            postgres.cursor.execute("select * from information_schema.tables where table_name=%s", (t_name,))
            exists = postgres.cursor.fetchone()
            if exists:
                postgres.table_name = t_name
                checking = False
            else:
                print('Sorry, could not locate that table.')

    elif check == 'n':
        postgres.create_table()

    
    for webpage in webpages:

        # Create the UrlConnection object and connect to the webpage.
        url_connection = UrlConnection(webpage, postgres)
        url_connection.connect()
        
        # User to determine if new csv file is needed for each webpage. Creating a new file overwrites any existing.
        
        try:
            # Extract the articles and write to the csv file.
            url_connection.div_classes()

            url_connection.article_generator()
            url_connection.write_articles()
            sleep(1)
            print('\nData successfully written from ' + webpage)
            sleep(1)
            
        except AttributeError as e:
            print('\nThere was no file to save data to.')
            
            # User to determine if new csv file is needed for each webpage. Creating a new file overwrites any existing.
            choice = input('Create a new csv file for ' + webpage + '? (y/n): ')
            if choice == 'y':
                print('\nCreating new file for ' + webpage)
                print()
                url_connection.create_csv()
            else:
                pass


    postgres.connection.close()
    
    if postgres.connection:
        print('PostgreSQL connection still live.')
    else:
        print('PostgreSQL connection has been closed.')