# Import dependencies
# !pip install pandas
import pandas as pd
import numpy as np
from datetime import datetime
import requests
import sqlite3
from bs4 import BeautifulSoup


# ----Task 1 -------------------------------------------------------------------------
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    
    # Creating the current time and parsing into desired string format
    time_now = datetime.now().strftime('%Y-%h-%d-%H:%M:%S')
    
    # Append timestamp and message to process log file
    with open('code_log.txt', 'a') as file:
        file.write(f'{time_now} : {message}' + '\n')
        
        
# Creating the first log entry
log_progress("Preliminaries complete. Initiating ETL process")







# ----Task 2 -------------------------------------------------------------------------
def extract(url='https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks', 
            table_attribs=['Name', 'MC_USD_Billion']):
    
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    
    # Requesting wiki web page
    url = url
    page = requests.get(url)

    # Creating a soup object of the wiki page
    soup = BeautifulSoup(page.text, features="html.parser")
    
    # Finding the required table
    tables = soup.find_all('table')
    market_cap_tb = tables[0]    
    
    # Empty list to be populated with several other lists. Each of these lists will represent
    # a row in the wiki table
    extracted_data = []

    # Iterate through each row in the market cap table and create an empty list
    for row in market_cap_tb.find_all('tr'):
        extract_row = []

        # Iterate through each cell in the row and append the cell contents to a list
        for cell in row.find_all('td'):
            extract_row.append(cell.text[:-1]) # Each cell ends in "\n" so [:-1] will ignore the last two characters 

        # Append the list of cell contents to the extracted_data list
        extracted_data.append(extract_row)
    
    
    # As the first row of the wiki table contains table headers, "find_all('td')" will return empty
    # This will replace the first empty list in 'extracted_data' with the table headers
    table_headers = [header.text[:-1] for header in market_cap_tb.find_all('th')]
    extracted_data[0] = table_headers
    
    # Creating a pandas dataframe using extracted data
    df = pd.DataFrame(extracted_data)

    # Set first row as column names and drop that row
    df.columns = df.loc[0, :]
    df = df.drop(index=0)

    # Dropping the Rank column
    df = df.drop(columns='Rank')

    # Renaming columns
    df.columns = table_attribs

    # Type cast the market cap column to float
    df['MC_USD_Billion'] = df['MC_USD_Billion'].astype(float)
    
    # Log etl progress
    log_progress("Data extraction complete. Initiating Transformation process")
    
    
    return df


# Calling extract function
df = extract()






# ----Task 3 -------------------------------------------------------------------------
def transform(df, csv_path='data/exchange_rate.csv'):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    
    # Read exchange rate csv file
    er_df = pd.read_csv(csv_path)
    
    # Convert dataframe into a dictionairy where the first column represents the keys and the second column represents the values
    er_dict = {}
    for currency, rate in zip(er_df['Currency'], er_df['Rate']):
        er_dict[currency] = rate
    
    # Adding exchange rate adjusted columns to the dataframe
    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * er_dict['GBP'], 2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * er_dict['EUR'], 2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * er_dict['INR'], 2)

    # Log etl progress
    log_progress("Data transformation complete. Initiating Loading process")
    
    
    return df


# Calling transform function
df = transform(df)









# ----Task 4 -------------------------------------------------------------------------
def load_to_csv(df, output_path='./Largest_banks_data.csv'):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    
    # Save dataframe as csv file
    df.to_csv(output_path)
    
    # Log etl progress
    log_progress("Data saved to CSV file")



# Calling load to csv function
load_to_csv(df)








# ----Task 5 -------------------------------------------------------------------------
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    
    # Loading the dataframe into the database
    df.to_sql(name=table_name, con=sql_connection, if_exists='replace')
    
    # Log etl progress
    log_progress("Data loaded to Database as a table, Executing queries")



# Initiating the database connection and logging progress
conn = sqlite3.connect('data/Banks.db')
log_progress("SQL Connection initiated")

# Calling the function that will load to dataframe into a sql database
load_to_db(df, sql_connection=conn, table_name='Largest_banks')









# ----Task 6 -------------------------------------------------------------------------
def run_queries(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    
    # Querying table and creating a pandas dataframe from the result
    query = pd.read_sql(query_statement, sql_connection)
    
    # Log etl progress
    log_progress("Process Complete")
    
    
    # Return the query statement and the resulting table
    return query


# Query 1
query_1 = '''
SELECT * 
FROM Largest_banks
'''

# Query 2
query_2 = '''
SELECT AVG(MC_GBP_Billion) 
FROM Largest_banks
'''

# Query 3
query_3 = '''
SELECT Name 
FROM Largest_banks 
LIMIT 5
'''

# Executing all queries
run_queries(query_1, conn)
run_queries(query_2, conn)
run_queries(query_3, conn)


# Close server connection and create final log
conn.close()
log_progress("Server Connection closed")








# ----Task 7 -------------------------------------------------------------------------
# Tasks 7 is simply to clear folder of any outputs previously created by running chunks of the above code and then run the script in its entirety





















