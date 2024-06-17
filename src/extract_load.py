# imports
import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# imports environments variables

load_dotenv()

DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

# pegar a cotacao dos ativos
commodities = ['CL=F', 'GC=F', 'SI=F']

def search_commodities_data(symbol, period='5d', interval='1d'):
    ticker = yf.Ticker(symbol)
    data = ticker.history(period=period, interval=interval)[['Close']]
    data['symbol'] = symbol
    return data

def search_commodities_data_all(commodities):
    all_data = []
    for symbol in commodities:
        data = search_commodities_data(symbol)
        all_data.append(data)
    return pd.concat(all_data)

def save_in_postgres(df, schema='public'):
    df.to_sql('commodidites', engine, if_exists='replace', index=True, index_label='Date', schema=schema)

if __name__ == "__main__":
    concatenated_data = search_commodities_data_all(commodities)
    save_in_postgres(concatenated_data, schema='public')