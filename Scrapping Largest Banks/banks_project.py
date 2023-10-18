# Code for ETL operations on Country-GDP data
# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
from datetime import datetime

import sqlite3

conn = sqlite3.connect("Banks.db")

url = "https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks"
def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    with open("code_log.txt", "a") as file:
        file.write(str(datetime.now())+" : "+message+"\n")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    df = pd.DataFrame(table_attribs)

    response = requests.get(url).text

    doc = BeautifulSoup(response, "html.parser")

    table = doc.find("table")

    body = table.find("tbody")

    banks_dict = {"bank": [], "market_cap_usd": []}

    rows = body.find_all("tr")

    for row in rows:
        data_to_use = row.find_all("td")
        n = len(data_to_use)
        if n==0:
            pass
        else:
            banks_dict["bank"].append(data_to_use[1].get_text().replace("\n", ""))
            banks_dict["market_cap_usd"].append(float(data_to_use[2].get_text().replace("\n", "")))

    df = pd.DataFrame.from_dict(banks_dict)
    
    return df


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    # Removing \n from the end of banks name
    df["bank"] = [x.replace("\n", "") for x in df["bank"]]

    exchange_rate_df = pd.read_csv(csv_path)

    exchange_rate = {}

    for i in range(len(list(exchange_rate_df["Currency"]))):
        exchange_rate[exchange_rate_df["Currency"][i]] = exchange_rate_df["Rate"][i]

    df['market_cap_gbp'] = [np.round(x*exchange_rate['GBP'],2) for x in df['market_cap_usd']]
    df['market_cap_eur'] = [np.round(x*exchange_rate['EUR'],2) for x in df['market_cap_usd']]
    df['market_cap_inr'] = [np.round(x*exchange_rate['INR'],2) for x in df['market_cap_usd']]

    return df


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    c = sql_connection.cursor()

    c.execute(f"""
    create table if not exists {table_name} (
              bank text,
              market_cap_usd decimal,
              market_cap_gbp decimal,
              market_cap_eur decimal,
              market_cap_inr decimal
    )
    """)

    for i in range(len(df["bank"])):
        c.execute(f"INSERT INTO {table_name} (bank, market_cap_usd, market_cap_gbp, market_cap_eur, market_cap_inr) VALUES (?, ?, ?, ?, ?)", [df["bank"][i], df["market_cap_usd"][i], df["market_cap_gbp"][i], df["market_cap_eur"][i], df["market_cap_inr"][i]])


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    c = sql_connection.cursor()

    result = c.execute(query_statement)

    rows = result.fetchall()

    return rows


''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress("Preliminaries complete. Initiating ETL process")


df = extract(url , ["bank", "market_cap_usd"])
log_progress("Data extraction complete. Initiating Transformation process")

df = transform(df, "exchange_rate.csv")
log_progress("Data transformation complete. Initiating Loading process")

load_to_csv(df, "largest_banks.csv")
log_progress("Data saved to CSV file")

load_to_db(df, conn, "largest_banks")
log_progress("Data loaded to Database as a table, Executing queries")

rows = run_query("SELECT * FROM largest_banks", conn)

print("All Banks Data")
print("Bank\t\t\tMarket Cap USD\tMarket Cap GBP\tMarket Cap EUR\tMarket Cap INR")
for row in rows:
    print(row[0]+"\t\t"+str(row[1])+"\t\t"+str(row[2])+"\t\t"+str(row[3])+"\t\t"+str(row[4]))

print("-------------------")
rows = run_query("SELECT AVG(market_cap_gbp) FROM largest_banks", conn)
print("Average of Market Capital in GBP")
print(rows[0][0])

print("-------------------")
rows = run_query("SELECT bank from largest_banks LIMIT 5", conn)
print("Top 5 Banks")
print("Bank Name")
for row in rows:
    print(row[0])

log_progress("Process Complete")

conn.close()

log_progress("Server Connection closed")