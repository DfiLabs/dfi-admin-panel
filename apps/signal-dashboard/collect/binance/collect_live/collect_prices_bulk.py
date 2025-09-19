import pandas as pd
import numpy as np
import asyncio
import aiohttp
import os
from datetime import datetime, timedelta
from urllib.parse import urlencode
from tqdm import tqdm 
from concurrent.futures import ThreadPoolExecutor 


import dfilabs.config.config as cf
from dfilabs.utils.log import _print, _print_error 
from dfilabs.utils.utils_date import format_datetime,sort_date_index,adjust_datetime
from dfilabs.utils.utils_os import get_root_directory
from dfilabs.utils.utils import is_file_in_folder

 

# Import your Google Drive functions


# Create a global ThreadPoolExecutor with a limited number of threads
executor = ThreadPoolExecutor(max_workers=8)  # Adjust max_workers as needed 

async def get_available_prices(pair: str, trading_type="future", current_date='yesterday', drive=False, verbose=0):
    """
    Retrieves only available historical OHLCV data for a given trading pair without collecting new data.
    This is an async version that works like get_prices with only_available=True.
    """
    _print("Function: get_available_prices (async)", 2, verbose)
    _print(f"Retrieving available data for {pair} ({trading_type}) without collecting new data", 1, verbose)
    
    try:
        import dfilabs.cloud.cloud as cl
        
        # Initialize service once if needed
        service = cl.authenticate_drive_api() if drive else None
        _print(f"Google Drive integration: {'enabled' if drive else 'disabled'}", 2, verbose)

        # Determine the target datetime based on current_date
        if current_date == 'now':
            target_datetime = datetime.now()
        elif current_date == 'yesterday':
            target_datetime = datetime.now() - timedelta(days=1)
            target_datetime = target_datetime.replace(hour=23, minute=59, second=0, microsecond=0)
        else:
            raise ValueError(f'current_date must be either "now" or "yesterday". Provided: {current_date}')

        flag_delisted = False
        _print(f"Target datetime is set to {target_datetime}", 1, verbose)

        # Check for delisted coins early
        if (date_delisted := cf.coin_to_date_delisted.get(pair)):
            date_delisted_datetime = datetime.strptime(date_delisted["date"], '%Y-%m-%d')
            if date_delisted_datetime <= target_datetime:
                _print(f'The pair {pair} is delisted (date: {date_delisted["date"]})', 1, verbose)
                _print(f'Adjusting target datetime to delisting date', 2, verbose)
                flag_delisted = True
                target_datetime = format_datetime(date_delisted_datetime)

        # Initialize key variables upfront
        root_directory = get_root_directory()
        root_directory = f'{root_directory}/storage/BINANCE'
        
        folder_name = f'BINANCE_{trading_type}_prices'
        file_name = f'BINANCE_{pair}_{trading_type}.csv'
        local_folder = f'{root_directory}/{folder_name}'
        file_path = f'{local_folder}/{file_name}'
        _print(f"Looking for data at: {file_path}", 2, verbose)

        # Get existing data (local or cloud) - run in executor to avoid blocking
        loop = asyncio.get_running_loop()
        
        def get_existing_data_sync():
            keep_columns = ['date', 'open', 'high', 'low', 'close', 'buy_volume', 'sell_volume', 'volume']
            
            # Check local data first
            _print(f"Checking for local data at {file_path}", 2, verbose)
            ohlc_old_local = None
            if os.path.exists(file_path):
                try:
                    ohlc_old_local = pd.read_csv(file_path, engine='pyarrow')
                    # Keep only required columns
                    ohlc_old_local = ohlc_old_local[keep_columns] if all(col in ohlc_old_local.columns for col in keep_columns) else ohlc_old_local
                    ohlc_old_local['date'] = ohlc_old_local['date'].apply(adjust_datetime)
                    ohlc_old_local.index = ohlc_old_local['date']
                    _print(f"Local data loaded: {len(ohlc_old_local)} rows", 2, verbose)
                except Exception as e:
                    _print(f"Error loading local data: {str(e)}", 1, verbose)
                    ohlc_old_local = None
            
            # Check cloud data if enabled
            ohlc_old_cloud = None
            if drive and service:
                _print("Checking cloud storage for data", 2, verbose)
                try:
                    folder_id = cl.get_folder_id_by_name(service, folder_name)
                    if cl.check_file_in_folder(service, folder_id, file_name):
                        file_id = cl.get_file_id_by_name_in_folder(service, folder_id, file_name)
                        ohlc_old_cloud = cl.read_csv_file_from_drive(service, file_id)
                        # Keep only required columns
                        ohlc_old_cloud = ohlc_old_cloud[keep_columns] if all(col in ohlc_old_cloud.columns for col in keep_columns) else ohlc_old_cloud
                        ohlc_old_cloud['date'] = ohlc_old_cloud['date'].apply(adjust_datetime)
                        ohlc_old_cloud.index = ohlc_old_cloud['date']
                        _print(f"Cloud data loaded: {len(ohlc_old_cloud)} rows", 2, verbose)
                except Exception as e:
                    _print(f"Error loading cloud data: {str(e)}", 1, verbose)
                    ohlc_old_cloud = None
            
            # Choose best data source
            if ohlc_old_local is not None and ohlc_old_cloud is not None:
                if ohlc_old_cloud.index[-1] > ohlc_old_local.index[-1]:
                    _print("Using cloud data as it is more up-to-date.", 1, verbose)
                    return ohlc_old_cloud
                else:
                    _print("Using local data as it is up-to-date.", 1, verbose)
                    return ohlc_old_local
            return ohlc_old_local if ohlc_old_local is not None else ohlc_old_cloud if ohlc_old_cloud is not None else pd.DataFrame()

        ohlc_old = await loop.run_in_executor(executor, get_existing_data_sync)
        _print(f"Retrieved {'non-empty' if not ohlc_old.empty else 'empty'} existing dataset", 2, verbose)

        # Check if any data is available
        if ohlc_old.empty:
            raise ValueError(f"No data available for pair {pair} ({trading_type})")
        
        # Apply delisted coin filtering if necessary
        if flag_delisted:
            _print(f"Filtering delisted coin data up to {target_datetime}", 2, verbose)
            ohlc_old = ohlc_old[ohlc_old.index <= target_datetime]
            
            # Check if we still have data after filtering
            if ohlc_old.empty:
                raise ValueError(f"No data available for delisted pair {pair} after filtering to delisting date")
        
        _print("Returning available data without date column", 2, verbose)
        # Safely remove date column if it exists
        if 'date' in ohlc_old.columns:
            ohlc_old = ohlc_old.drop('date', axis=1)
        return ohlc_old

    except Exception as e:
        _print_error(e)
        _print(f"Failed to retrieve available prices for {pair}", 1, verbose)
        raise

async def get_prices_live(session: aiohttp.ClientSession, pair: str, target_datetime, trading_type="future", verbose=0):
    import dfilabs.cloud.cloud as cl
    import os
    import pandas as pd
    service = cl.authenticate_drive_api()
    """
    Retrieves live OHLCV (Open, High, Low, Close, Volume) data for a given trading pair and trading type until a specified datetime.

    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for HTTP requests.
        pair (str): The trading pair for which to retrieve OHLCV data.
        trading_type (str, optional): The type of trading. Defaults to "future".
        target_datetime (datetime): The datetime until which to collect data.
        verbose (int, optional): Verbosity level:
            - 0: No output.
            - 1: Basic information about data collection.
            - 2: Detailed information including data sources.
            Defaults to 0.

    Returns:
        pandas.DataFrame: The OHLCV data for the specified trading pair and trading type.

    Raises:
        None
    """

    folder_name = f'BINANCE_{trading_type}_prices'
    folder_name_live = f'BINANCE_{trading_type}_prices'
    file_name = f'BINANCE_{pair}_{trading_type}.csv'
    
    root_directory = get_root_directory()
    root_directory = f'{root_directory}/storage/BINANCE'

    target_datetime = datetime.strptime(target_datetime, '%Y-%m-%d %H:%M:%S')

    date_delisted = cf.coin_to_date_delisted.get(pair, None)
    flag_delisted = False
    if date_delisted:
        date_delisted_datetime = datetime.strptime(date_delisted["date"], '%Y-%m-%d')
        if date_delisted_datetime <= target_datetime:
            _print(f'The pair {pair} is delisted', 1, verbose)
            target_datetime = format_datetime(date_delisted_datetime)
            flag_delisted = True

    try:
        loop = asyncio.get_running_loop()

        def blocking_operations():
            # Check if the file exists locally in the live folder
            keep_columns = ['date', 'open', 'high', 'low', 'close', 'buy_volume', 'sell_volume', 'volume']
            local_folder_live = f'{root_directory}/{folder_name_live}'
            local_file_path = f'{local_folder_live}/{file_name}'
            if os.path.exists(local_file_path):
                _print(f"Data for {pair} {trading_type} found locally in live folder.", 1, verbose)
                ohlc_old_local = pd.read_csv(local_file_path, engine='pyarrow')
                ohlc_old_local.index = ohlc_old_local['date']
                ohlc_old_local = ohlc_old_local[keep_columns]
                ohlc_old = ohlc_old_local.copy(deep=True)
            else:
                raise ValueError(f"Data for {pair} {trading_type} not found in local live folder.")

            if not ohlc_old.empty:
                ohlc_old = sort_date_index(ohlc_old)
                end_date_datetime = ohlc_old[ohlc_old.index <= target_datetime].index[-1]
                _print(f"Existing data available until {end_date_datetime}", 2, verbose)
                if end_date_datetime == target_datetime:
                    # Safely remove date column if it exists
                    if 'date' in ohlc_old.columns:
                        ohlc_old = ohlc_old.drop('date', axis=1)
                    return ohlc_old, end_date_datetime
            else:
                raise ValueError(f"Data for {pair} {trading_type} not found in local live folder or Google Drive.")

            return ohlc_old, end_date_datetime

        # Use executor for blocking operations
        ohlc_old, end_date_datetime = await loop.run_in_executor(executor, blocking_operations)

        # If the pair is delisted, return the available data
        if flag_delisted and format_datetime(ohlc_old.index[-1]) >= target_datetime:
            _print("Data is up to date but delisted", 1, verbose)
            _print("Returning existing data without date column", 2, verbose)
            

      
            if flag_delisted : 
                target_datetime = (target_datetime-timedelta(days=1)).replace(hour=23,minute=59,second=0)
                ohlc_old = ohlc_old[ohlc_old.index <= target_datetime]
            
            # Safely remove date column if it exists
            if 'date' in ohlc_old.columns:
                ohlc_old = ohlc_old.drop('date', axis=1)
            return ohlc_old
        
        if end_date_datetime == target_datetime:
            # Safely remove date column if it exists
            if 'date' in ohlc_old.columns:
                ohlc_old = ohlc_old.drop('date', axis=1)
            return ohlc_old

        # Continuously collect data until the target datetime is reached
        while end_date_datetime < target_datetime:
            current_datetime = datetime.now()
            _print(f"Current datetime is {current_datetime}. Target datetime is {target_datetime}.", 2, verbose)

            # Calculate the number of candles to retrieve
            n_candles = int(round((current_datetime - end_date_datetime) / timedelta(minutes=1), 0))
            n_candles += 5 
            _print(f"Need to collect {n_candles} candles for {pair} .", 1, verbose)

            # Retrieve new data asynchronously
            ohlc = await most_recent_market_data(session=session, pair=pair, n_candles=n_candles, trading_type=trading_type, timeframe='1m')
            ohlc = ohlc.reset_index(drop=True).dropna()
            ohlc.index = ohlc['date']

            if ohlc.empty:
                raise ValueError('There is an error during data collection and there is no data already available')
            else:
                _print('Concat old data and the collected data', 2, verbose)
                df_ohlc = pd.concat([ohlc_old, ohlc], axis=0)
                df_ohlc = sort_date_index(df_ohlc)
                
                
                df_ohlc = df_ohlc.drop_duplicates(subset=['date'], keep='first')
                df_ohlc = df_ohlc.dropna()

                last_value = df_ohlc.iloc[-1]
                last_date = df_ohlc.index[-1]

            


                end_date_datetime = df_ohlc.index[-1]
                _print(f"Data collected until {end_date_datetime}.", 2, verbose)

                

        # Safely remove date column if it exists
        if 'date' in df_ohlc.columns:
            df_ohlc.drop('date', axis=1, inplace=True)
        return df_ohlc

    except Exception as e:
        _print_error(e)
        return None





 

async def most_recent_market_data(session: aiohttp.ClientSession, pair: str, timeframe: str, n_candles: int, trading_type="future", verbose=0) -> pd.DataFrame:
    """
    Load the n_candles most recent OHLCV candlesticks for the given pair and timeframe.
    """
    ohlcs = []  # List to store the retrieved OHLCV data
    max_limit = 1000  # End time of the last retrieved candle 

    n_candles_needed = n_candles

    # Initialize variables for the loop
    current_end_time = None

    while n_candles_needed > 0:
        limit = min(max_limit, n_candles_needed)

        # Prepare parameters for klines function
        if current_end_time is None:
            new_ohlc = await klines(session = session,pair=pair, timeframe=timeframe, limit=limit, trading_type=trading_type)
        else:
            # Subtract 1 millisecond to avoid duplicate data
            new_ohlc = await klines(session = session, pair=pair, timeframe=timeframe, limit=limit, end_time=current_end_time, trading_type=trading_type)

        if len(new_ohlc) == 0:
            break  # No more data

        # Convert the retrieved data to a DataFrame
        new_ohlc_df = pd.DataFrame(
            new_ohlc,
            columns=[
                'open_time', 'open_price', 'high_price', 
                'low_price', 'close_price', 'volume',
                'close_time', 'quote_volume', 'n_trades', 
                'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
            ]
        )

        ohlcs.append(new_ohlc_df)

        n_candles_needed -= len(new_ohlc_df)

        # Update current_end_time to earliest open_time in new_ohlc_df
        current_end_time = int(new_ohlc_df['open_time'].min())

        # If we've retrieved fewer candles than requested, all data has been fetched
        if len(new_ohlc_df) < limit:
            break

    if not ohlcs:
        return pd.DataFrame()  # Return empty DataFrame if no data retrieved

    # Concatenate all dataframes into one
    ohlc = pd.concat(ohlcs, axis=0)
    

    # Drop duplicates
    ohlc.drop_duplicates(subset='open_time', inplace=True)

    # Convert columns to appropriate data types
    cols_to_numeric = ['open_price', 'high_price', 'low_price', 'close_price', 'volume', 'quote_volume', 'n_trades', 'taker_buy_base_volume', 'taker_buy_quote_volume']
    ohlc[cols_to_numeric] = ohlc[cols_to_numeric].apply(pd.to_numeric)

    # Convert times to datetime
    ohlc['open_time'] = pd.to_datetime(ohlc['open_time'], unit='ms')
    ohlc['close_time'] = pd.to_datetime(ohlc['close_time'], unit='ms')


    # Calculate taker sell quote volume
    ohlc['taker_sell_quote_volume'] = ohlc['quote_volume'] - ohlc['taker_buy_quote_volume']

    # Keep necessary columns
    ohlc = ohlc[['open_time', 'open_price', 'high_price', 'low_price', 'close_price',
                 'taker_buy_quote_volume', 'taker_sell_quote_volume', 'quote_volume']]

    # Sort by open_time
    ohlc.sort_values(by='open_time', inplace=True)

    # Keep the most recent n_candles
    ohlc = ohlc.iloc[-n_candles:].copy()

    # Reset index
    ohlc.reset_index(drop=True, inplace=True)

    # Rename columns
    ohlc.columns = ['date', 'open', 'high', 'low', 'close', 'buy_volume', 'sell_volume', 'volume']

    # Adjust date if necessary
    ohlc['date'] = ohlc['date'].apply(lambda x: adjust_datetime(x))

    return ohlc
    

async def klines(session: aiohttp.ClientSession, pair: str, timeframe: str, trading_type="future", **kwargs) -> list:
    """
    Kline/candlestick bars for a symbol.
    """
    # Set the URL path based on the trading type
    if trading_type == "future":
        url_path = "/fapi/v1/klines"
    elif trading_type == 'spot':
        url_path = "/api/v3/klines"
    else:
        raise ValueError(f"Trading type {trading_type}")

    # Set the initial parameters
    params = {'symbol': pair, 'interval': timeframe}

    # Add additional parameters to the request
    for key, value in kwargs.items():
        if key == 'start_time':
            params['startTime'] = int(value)
        elif key == 'end_time':
            params['endTime'] = int(value)
        elif key == 'limit':
            params['limit'] = min(value, 1000)

    # Send the request and return the response
    response = await send_public_request(
        session=session,
        url_path=url_path,
        payload=params,
        trading_type=trading_type
    )
    return response 

async def send_public_request(session: aiohttp.ClientSession, url_path: str, payload={}, trading_type: str = "future") -> dict:
    """
    Prepare and send an unsigned request to obtain public market data.
    """
    # Encode the payload as a query string
    query_string = urlencode(payload, True)

    # Construct the URL based on the trading type
    if trading_type == "future":
        url = 'https://fapi.binance.com' + url_path
    else:
        url = "https://api.binance.com" + url_path

    # Append the query string to the URL if it exists
    if query_string:
        url = url + '?' + query_string

    # Set headers
    headers = {
        'Content-Type': 'application/json;charset=utf-8',
        'X-MBX-APIKEY': ""  # Placeholder for the API key
    }

    # Send the GET request and return the response as JSON
    async with session.get(url, headers=headers) as response:
        response.raise_for_status()  # Raise exception for HTTP errors
        return await response.json()

async def get_bulk_prices(coins: list, trading_type="future",current_date = 'yesterday',drive = True, verbose=0):
    """
    Retrieve available historical OHLCV data for multiple coins in parallel, with a progress bar.
    Only returns existing data without collecting new data.
    """
    # Limit concurrency using a semaphore
    semaphore = asyncio.Semaphore(8)  # Adjust the value as needed

    async with aiohttp.ClientSession() as session:
        progress_bar = tqdm(total=len(coins), desc="Retrieving available data")
        lock = asyncio.Lock()

        async def sem_get_prices(coin):
            async with semaphore:
                _print(f"Starting available data retrieval for {coin}", 1, verbose)
                result = await get_available_prices(
                    pair=coin,
                    trading_type=trading_type,
                    current_date = current_date,
                    drive = drive,
                    verbose=verbose
                )
                async with lock:
                    progress_bar.update(1)
                return result

        tasks = [sem_get_prices(coin) for coin in coins]
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        progress_bar.close()
        results = dict(zip(coins, results_list))
    return results 

async def get_bulk_prices_live(coins: list, target_datetime, trading_type="future",keep_local=True, verbose=0):
    """
    Retrieve live OHLCV data for multiple coins in parallel, with a progress bar, and store them locally.
    """
    # Limit concurrency using a semaphore
    semaphore = asyncio.Semaphore(8)  # Adjust the value as needed

    async with aiohttp.ClientSession() as session:
        progress_bar = tqdm(total=len(coins), desc="Collecting live data")
        lock = asyncio.Lock()

        async def sem_get_prices_live(coin):
            async with semaphore:
                _print(f"Starting live data collection for {coin}", 1, verbose)
                result = await get_prices_live(
                    session,
                    pair=coin,
                    target_datetime=target_datetime,
                    trading_type=trading_type,
                    verbose=verbose
                )
                async with lock:
                    progress_bar.update(1)
                return coin, result

        tasks = [sem_get_prices_live(coin) for coin in coins]
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        progress_bar.close()

                # Store results after all data has been collected
        def save_result(coin, result):
            if result is not None:
                # Save the DataFrame to a CSV file
                local_folder = f'{get_root_directory()}/BINANCE_{trading_type}_prices'
                file_name = f'BINANCE_{coin}_{trading_type}.csv'
                result['date'] = result.index 
                result.to_csv(f'{local_folder}/{file_name}', index=False)
                _print(f"Data for {coin} saved locally to {local_folder}/{file_name}", 2, verbose)

        # Use ThreadPoolExecutor to parallelize saving with progress tracking
        if keep_local:
            with ThreadPoolExecutor(max_workers=8) as executor:
                list(tqdm(executor.map(lambda item: save_result(*item), results_list), total=len(results_list), desc="Saving data"))

        results = dict(results_list)

        
    return results 



if __name__ == '__main__':
    coins = cf.already_collected_futures_USDT 
    asyncio.run(get_bulk_prices(coins, trading_type='spot',current_date = 'yesterday',drive = 'True', verbose=3))









