import os, requests
from typing import List
from tqdm import tqdm
import hashlib
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
'''
This module will be used to download historical data from Binance Futures.
'''

base_url = 'https://data.binance.vision/data/futures/um/daily/'

def get_historical_url(symbol: str, date: str, channel: str) -> str:
    return f'{base_url}{channel}/{symbol}/{symbol}-{channel}-{date}.zip'

def download_and_check(path, url):
    checksum_url = url + '.CHECKSUM'
    try:
        resp = requests.get(url)
        checksum_resp = requests.get(checksum_url)
    except Exception as e:
        print(f'Error: {e}')
        return
    if resp.status_code != 200 or checksum_resp.status_code != 200:
        print(f'Error: {resp.status_code}')
        return
    # check if the checksum is correct
    checksum = hashlib.sha256(resp.content).hexdigest()
    if checksum != checksum_resp.text.split()[0]:
        print(f'Error: {url} checksum does not match')
        return
    with open(path, 'wb') as f:
        f.write(resp.content)

def download_historical_data(symbols: List[str], start_date: str, end_date: str, channels: List[str], output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    args_list = [(f'{output_dir}/{symbol}-{channel}-{date}.zip', get_historical_url(symbol, date.strftime('%Y-%m-%d'), channel)) for date in pd.date_range(start_date, end_date) for symbol in symbols for channel in channels]
    
    with ThreadPoolExecutor(max_workers=128) as executor:
        results = list(tqdm(executor.map(lambda args: download_and_check(*args), args_list), total=len(args_list)))