#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
from functools import partial
import time
import concurrent.futures
from pathlib import Path
from dateutil import parser
from datetime import datetime, timedelta
import pandas as pd
from binance.client import Client
# important note: binance library code altered:
#     removed time.sleep(1) call after every 3rd API call within Client.get_historical_klines function
#     file: Python36\Lib\site-packages\binance\client.py
#     lines 837 and 838 commented out

def estimate_request_weight(symbol, interval, dst_dir, client):
    # get existing dataframe from file if it exists w/in dst_dir
    filepath = Path(dst_dir).joinpath(f'{symbol}-{interval}.csv')
    data_df = pd.read_csv(filepath) if filepath.exists() else pd.DataFrame()
    # get start and end points for api request
    start = pd.to_datetime(client._get_earliest_valid_timestamp(symbol, interval), unit='ms') if data_df.empty else parser.parse(data_df["timestamp"].iloc[-1])
    end = pd.to_datetime(client.get_klines(symbol=symbol, interval=interval, limit=1)[-1][0], unit='ms')
    if (end - start).days <= 0:
        # if less than one days data to download return 0 which will result in symbol being skipped
        return 0
    # use start, end, and interval to estimate request weight (1 request can fetch 1000 klines)
    klines = int((end - start)/pd.Timedelta(interval))
    erw = 1 + klines//1000
    return erw

def get_all_klines(symbol, client, interval, dst_dir):
    # get existing dataframe from file if it exists w/in dst_dir
    filepath = Path(dst_dir).joinpath(f'{symbol}-{interval}.csv')
    data_df = pd.read_csv(filepath) if filepath.exists() else pd.DataFrame()
    # get start and end points for api request
    start = pd.to_datetime(client._get_earliest_valid_timestamp(symbol, interval), unit='ms') if data_df.empty else parser.parse(data_df["timestamp"].iloc[-1])
    end = pd.to_datetime(client.get_klines(symbol=symbol, interval=interval, limit=1)[-1][0], unit='ms')
    # download kline data and  put in dataframe
    print(f'{symbol}: downloading {int((end - start)/pd.Timedelta(interval))} {interval} klines (from {start} to {end})')
    klines = client.get_historical_klines(symbol, interval, str(start), str(end), limit=1000)
    new_data_df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
    new_data_df['timestamp'] = pd.to_datetime(new_data_df['timestamp'], unit='ms')
    # append to old df if exists
    data_df = new_data_df if data_df.empty else data_df.append(new_data_df)
    data_df.set_index('timestamp', inplace=True)
    return filepath, data_df

def main():
    api_key = os.environ['BINANCE_API']
    api_secret = os.environ['BINANCE_SECRET']
    client = Client(api_key, api_secret)
    all_tickers = client.get_all_tickers()
    dst_dir = Path('Binance')
    interval = '5m'
    # fill in all get_all_kline args except for symbol so we can use in multithreading
    thread_func = partial(get_all_klines, client=client, interval=interval, dst_dir=dst_dir)

    total_erw = 0 # count of total estimated request weight
    symbol_batch = [] # list to be filled with symbols for single multithread run through
    for i, ticker in enumerate(all_tickers):

        symbol = ticker['symbol']
        erw = estimate_request_weight(symbol, interval, dst_dir, client)
        total_erw += 2 # add 2 for estimate_request_weight function calls just to be safe
        if total_erw + erw > 1100: # api request limit is 12000 per minute
            print(i, symbol_batch)

            start_time = datetime.now()
            with concurrent.futures.ThreadPoolExecutor() as executor:
                futures = list(executor.map(thread_func, symbol_batch))
            for filepath, data_df in futures:
                # save data outside of multithreading. suspected errors trying to save w/in multithreading. also easier to line up in console output.
                data_df.to_csv(filepath)
                print(f'saved to {filepath}')
            end_time = datetime.now()
            duration = start_time - end_time
            if duration < timedelta(minutes=1): # pause if outpacing api request limit
                sleep_time = timedelta(minutes=1) - duration
                time.sleep(sleep_time.seconds)
            # restart symbol batch and total_erw w/ data from current ticker
            symbol_batch  = [symbol]
            total_erw = erw
        elif erw != 0: 
            # skip symbols with 0 erw
            # 0 erw means less than days worth of data to download (see estimate_request_weight function)

            # add symbol to symbol_batch, update total_erw counter
            symbol_batch.append(symbol)
            total_erw += erw
        if i%100 == 0:
            print(i)

    # one last symbol_batch left
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = list(executor.map(thread_func, symbol_batch))
    for filepath, data_df in futures:
        data_df.to_csv(filepath)
        print(f'saved to {filepath}')

if __name__ == '__main__':
    main()

