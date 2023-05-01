import pandas as pd
from polygon import RESTClient
import numpy as np
import http.client, urllib
import os
import sqlalchemy
from sqlalchemy import create_engine, text

# apis key from config
from polygonAPIkey import polygonAPIkey

# OR just assign your API as a string variable
# polygonAPIkey = 'apiKeyGoesHere'

# how many tickers to include in message
trimLength = 5

# empty dataframe to store log returns
stdDevsData = pd.DataFrame()

# create client and authenticate w/ API key // rate limit 5 requests per min
client = RESTClient(polygonAPIkey) # api_key is used

stockTickers = ['AAPL', 'A','AA', 'BAC', 'BA', 'C'] 

for i in stockTickers:
    
    # request daily bars
    dataRequest = client.get_aggs(ticker = i, 
                                  multiplier = 1,
                                  timespan = 'day',
                                  from_ = '2022-09-01',
                                  to = '2023-04-25')
    
    # list of polygon agg objects to DataFrame
    priceData = pd.DataFrame(dataRequest)
    
    #create Date column
    priceData['Date'] = priceData['timestamp'].apply(
                              lambda x: pd.to_datetime(x*1000000))
    

    priceData = priceData.set_index('Date')
        
    priceData['logReturns'] = np.log(priceData.close) - np.log(
        priceData.close.shift(1))
    
    #rolling stdDev window
    rollingStdDevWindow = 20
    
    #rolling stdDev log returns 
    priceData['stdDevs'] = priceData['logReturns'].rolling(
        center=False, window = rollingStdDevWindow).std()
    
    stdDevsData[i] = priceData.stdDevs

# trim data
stdDevsData = stdDevsData[rollingStdDevWindow:]

# rename index before transpose
stdDevsData.index = stdDevsData.index.rename('idx')

# transpose data
sortedData = stdDevsData[-1:].T

# reset index
sortedData.reset_index(inplace=True)



# rename column
sortedData = sortedData.rename(
    columns={sortedData.columns[0]: "tickers",
             sortedData.columns[1]: "stdDevs"})

# sort data
sortedData = sortedData.sort_values(by=['stdDevs'], 
                                    ascending=False,
                                    ignore_index=True)



# trim data
highestVol = sortedData[:trimLength]

print(highestVol)

'''
print(stdDevsData)
print(sortedData)
print(sortedData.columns)
'''

# create db
print(sqlalchemy.__version__)
print(os.getcwd())

# create sql alchemy engine object
engine = create_engine("sqlite+pysqlite:///volApp.db", echo=True, future=True)

# create table
highestVol.to_sql('volTable', engine)

# fetch rows
with engine.connect() as conn:
    result = conn.execute(text("SELECT * FROM volTable"))
    for row in result:
        print(f"""
              index: {row.index}
              ticker: {row.tickers}
              volData: {row.stdDevs}
              """)