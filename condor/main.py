# import packages
import time
start_time = time.time()

import urllib.request
import json
import os
import pandas as pd
from google.cloud import bigquery


# get API keys from environmental variables & authenticate to Google BigQuery
quandl_api_key = os.environ['QUANDL_API_KEY']
alpha_vantage_api_key = os.environ['ALPHA_VANTAGE_API_KEY']
bigquery_credentials = os.environ['GOOGLE_CREDENTIALS']

def parse_json(url, api_key):
    """parse json object for a given url and api key"""
    with urllib.request.urlopen(url+api_key) as url:
        data = json.loads(url.read().decode())
    return(data)

def flatten_alpha_vantage_dict(dict):
    for i in dict:
        yield i, dict[i]['4. close']



# get Effective Federal Funds Rate from Quandl
effr_url = 'https://www.quandl.com/api/v3/datasets/FRED/EFFR.json?api_key='
effr_parsed = parse_json(effr_url, quandl_api_key)['dataset']['data']

effr_df = pd.DataFrame(effr_parsed, columns = ["date", "effr_rate"])
#effr_df = sc.parallelize(effr_parsed).toDF(["date", "effr_rate"])
#effr_df.show()

# set index as Datetime, forward-fill dates, sort values descending
effr_df.set_index('date', drop=True, inplace=True)
effr_df.index = pd.to_datetime(effr_df.index)
effr_df = effr_df.resample("D").ffill()
effr_df.reset_index(level=0, inplace=True)
effr_df.sort_values('date', ascending=False, inplace=True)

# dump locally as csv
#effr_df.to_csv('/Users/Tomek/condor/condor/data/effr.csv')
# update BigQuery table
effr_df.to_gbq(destination_table = 'macroeconomic.effr',
               project_id = 'peppy-bond-105017',
               if_exists='replace',
               private_key = bigquery_credentials
               #,table_schema = [{'name': 'date', 'type': 'STRING'}, {'name': 'effr_rate', 'type': 'FLOAT'}]
              )


# get S&P 500 daily close values from Alpha Vantage
inx_url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=INX&outputsize=full&apikey='
inx_parsed = parse_json(inx_url, alpha_vantage_api_key)['Time Series (Daily)']

inx_flattened = list(flatten_alpha_vantage_dict(inx_parsed))

inx_df = pd.DataFrame(inx_flattened, columns = ["date", "inx_close_value"])

# set index as Datetime, forward-fill dates, sort values descending
inx_df.set_index('date', drop=True, inplace=True)
inx_df.index = pd.to_datetime(inx_df.index)
inx_df = inx_df.resample("D").ffill()
inx_df.reset_index(level=0, inplace=True)
inx_df.sort_values('date', ascending=False, inplace=True)

# dump locally as csv
#inx_df.to_csv('/Users/Tomek/condor/condor/data/inx.csv')
# update BigQuery table
inx_df.to_gbq(destination_table = 'macroeconomic.inx',
              project_id = 'peppy-bond-105017',
              if_exists='replace',
              private_key = bigquery_credentials
              )

# get Gold Price: London Fixing from Quandl
gold_url = 'https://www.quandl.com/api/v3/datasets/LBMA/GOLD.json?api_key='
gold_parsed = parse_json(gold_url, quandl_api_key)['dataset']['data']
gold_parsed_columns = parse_json(gold_url, quandl_api_key)['dataset']['column_names']

gold_df = pd.DataFrame(gold_parsed, columns = gold_parsed_columns)[['Date', 'USD (PM)']].rename(columns={"Date": "date", "USD (PM)": "gold_price_usd"})
gold_df = gold_df[gold_df['date'] > '2000-01-01']

# set index as Datetime, forward-fill dates, sort values descending
gold_df.set_index('date', drop=True, inplace=True)
gold_df.index = pd.to_datetime(gold_df.index)
gold_df = gold_df.resample("D").ffill()
gold_df.reset_index(level=0, inplace=True)
gold_df.sort_values('date', ascending=False, inplace=True)

# dump locally as csv
#gold_df.to_csv('/Users/Tomek/condor/condor/data/gold.csv')
# update BigQuery table
gold_df.to_gbq(destination_table = 'macroeconomic.gold',
               project_id = 'peppy-bond-105017',
               if_exists='replace',
               private_key = bigquery_credentials
              )

print("--- %s seconds ---" % (time.time() - start_time))
