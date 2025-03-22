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


def readData(table_id, rows_list):
    errors = client.insert_rows_json(table_id, rows_list)
    if errors == []:
        print(f'{len(rows_list)} Rows added.')
    else:
        print(f'Error: {errors}')

def update(tickers):
    company_rows = []

    for ticker in tickers:
        prices_rows = []

        dat = yf.Ticker(ticker)
        info = dat.info
        prices = dat.history(period = '10y').reset_index()
        prices[['Date', 'Trading Period']] = prices['Date'].astype(str).str.split(' ', expand = True)


        company_rows.append({'stockTicker': ticker, 'city': info['city'], 
              'industry': info['industry'], 'marketCap': info['marketCap'], 
              'name': info['shortName'], 'sector': info['sector'], 'state': info['state']})
        
        for _, row in prices.iterrows():
            prices_rows.append({
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
        
        
    # readData(dataset_id + 'Company', company_rows)
        readData(dataset_id + 'Historical Prices', prices_rows)


if __name__ == '__main__':
    update(tickers)

