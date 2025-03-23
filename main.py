import json
import os

import pandas as pd
import yfinance as yf

from google.cloud import bigquery

credentials_path = 'portfolio-monitor-privateKey.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

client = bigquery.Client()
dataset_id = 'portfolio-monitor-454515.portfolio.'


# The 25 largest S&P tickers by market cap, as of Mar 25
tickers = ["AAPL", "NVDA", "MSFT", "AMZN", "META", "GOOGL", "AVGO", "TSLA", "BRK-B", "GOOG", 
 "JPM", "LLY", "V", "XOM", "COST", "MA", "UNH", "NFLX", "WMT", "PG", 
 "JNJ", "HD", "ABBV", "BAC", "CRM"]


def writeData(table_id, rows_list):
    errors = client.insert_rows_json(table_id, rows_list)

    if errors == []:
        print(f'{len(rows_list)} Rows added.')
    else:
        print(f'Error: {errors}')

def readData(table_id, columns):
    unique_keys = []
    columns = ', '.join(columns)

    # Retrieve the latest 200 records, ordered by date in descending order  
    # to prioritize the most recent entries when checking for duplicates.
    QUERY = (
        f'SELECT {columns} FROM `{table_id}` '
        'ORDER BY date DESC '
        'LIMIT 200'
    )
    query_job = client.query(QUERY) 
    rows = query_job.result() 

    for row in rows:
        unique_keys.append(dict(row))
    return unique_keys

def updateCompany(tickers):
    """Update company info, unnecessary to run daily unless its for a refresh of market cap"""
    rows_to_insert = []

    for ticker in tickers:
        dat = yf.Ticker(ticker)
        info = dat.info

        rows_to_insert.append({
            'stockTicker': ticker,
            'city': info['city'], 
            'industry': info['industry'], 
            'marketCap': info['marketCap'], 
            'name': info['shortName'],
            'sector': info['sector'], 
            'state': info['state']})

    writeData(dataset_id + 'Company', rows_to_insert)

def update_historical_prices(tickers):

    for ticker in tickers:
        rows_to_insert = []
        dat = yf.Ticker(ticker)

        # Limit data to 5 days for performance
        prices = dat.history(period = '5d').reset_index()
        prices[['Date', 'Trading Period']] = prices['Date'].astype(str).str.split(' ', expand = True)
        
        existing_keys = readData(dataset_id + 'Historical Prices', ['date', 'stockTicker'])

        for _, row in prices.iterrows():
            primary_key = {'date': pd.to_datetime(row['Date']).date(), 'stockTicker': ticker}
            if primary_key not in existing_keys:

                rows_to_insert.append({
                    'date': row['Date'],
                    'open': row['Open'],
                    'high': row['High'],
                    'low': row['Low'],
                    'close': row['Close'],
                    'volume': row['Volume'],
                    'dividends': row['Dividends'],
                    'stockSplits': row['Stock Splits'],
                    'tradingPeriod': row['Trading Period'],
                    'stockTicker': ticker
                })

        if len(rows_to_insert) == 0:
            print(f'No rows to insert, data is up-to-date for {ticker}.')
        else:    
            writeData(dataset_id + 'Historical Prices', rows_to_insert)
            print(f'{len(rows_to_insert)} rows updated.')


if __name__ == '__main__':
    # update_historical_prices(tickers)
    print(readData(dataset_id + 'Historical Prices', ['date', 'stockTicker']))

