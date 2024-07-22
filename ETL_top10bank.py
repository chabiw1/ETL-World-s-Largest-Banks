# Code for ETL operations on Country-GDP data
# Importing the required libraries
import pandas as pd 
from datetime import datetime
import sqlite3
from bs4 import BeautifulSoup 
import requests
import numpy as np

#set input variable

code_log = "code_log.txt"
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = pd.DataFrame(columns = ['Rank','Bank name','Market cap(US$ billion)'])
csv_path = '/Users/cha/Documents/My Fav Project/ETL Project/exchange_rate.csv'
db_name = 'Banks.db'
table_name =  'Largest_banks'
output_path = 'Largest_banks.csv'


sql_connection = sqlite3.connect('Banks.db')


def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. Thes
    function returns the data frame for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page,'html.parser')
    
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    count = 0

    for row in rows:
        if count < 10:
            col = row.find_all('td')
            if len(col) != 0 :
                scraping_dict = {'Rank' : col[0].get_text(strip=True),
                                 'Bank name' : col[1].get_text(strip=True),
                                 'Market cap(US$ billion)' : col[2].get_text(strip=True)
                                 }
                df1 = pd.DataFrame(scraping_dict, index=[0])
                table_attribs = pd.concat([table_attribs,df1], ignore_index=True)
                count+=1
        else:
            break
    df = table_attribs
    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    csv_df = pd.read_csv(csv_path).set_index('Currency')
    
    for currency in csv_df.index.to_list():
        df[f'MC_{currency}_Billion'] = np.round(df['Market cap(US$ billion)'].astype(float) * csv_df['Rate'][currency],2)
    
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(output_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    
    df.to_sql(table_name,con=sql_connection,if_exists='replace',index=False)
    

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    ''' Here, you define the required entities and call the relevant
    functions in the correct order to complete the project. Note that this
    portion is not inside any function.'''
    query_output = pd.read_sql(query_statement,sql_connection)
    return query_output
    


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(code_log,"a") as f:
        f.write(timestamp + ',' + message + '\n')

log_progress("ETL Start")

#extract top10 largest bank by webscraping
#---------------------------------------------------------------------------------------
log_progress("Preliminaries complete. Initiating ETL process") 
print("Scrapping!")
df_extracted = extract(url, table_attribs)
print(df_extracted)
log_progress("Data extraction complete. Initiating Transformation process")

#add currency columns
#---------------------------------------------------------------------------------------
log_progress("Transform phase Started")
df_transformed = transform(df_extracted, csv_path)
print("\n\nCurrency Added") 
print(df_transformed) 
log_progress("Data transformation complete. Initiating Loading process") 

#load to csv and database
#---------------------------------------------------------------------------------------
log_progress("Load phase Started") 
load_to_csv(df_transformed,output_path)

print(f'Load transformed data into {output_path}')
log_progress("Data saved to CSV file")

log_progress('SQL Connection initiated')
load_to_db(df_transformed, sql_connection, table_name)
print(f'Load{table_name} into Database')
log_progress("Data loaded to Database as a table, Executing queries")

#print query
#--------------------------------------------------------------------------------------- 
statement_list = [
    """SELECT * FROM Largest_banks""",
    """SELECT AVG(MC_GBP_Billion) FROM Largest_banks""",
    """SELECT `Bank name` FROM Largest_banks LIMIT 5"""
    ]

for i, query_statement in enumerate(statement_list,1) :
    print(f"Executing Query {i}: {query_statement}")
    
    dataframe = pd.read_sql(query_statement, sql_connection)
    print(dataframe)


log_progress("Process Complete") 
log_progress("Server Connection closed")










