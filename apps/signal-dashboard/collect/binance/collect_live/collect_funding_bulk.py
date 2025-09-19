import pandas as pd
import numpy as np
import asyncio
import aiohttp
from datetime import datetime, timedelta
from tqdm import tqdm 
from concurrent.futures import ThreadPoolExecutor 


import dfilabs.config.config as cf
import dfilabs.collect.api.binance_api as binance_api
import dfilabs.cloud.cloud as cl
import dfilabs.collect.utils.utils_collect as utils_collect
from dfilabs.utils.log import _print, _print_error 
from dfilabs.utils.utils_date import format_datetime, sort_date_index
from dfilabs.utils.utils_os import get_root_directory
from dfilabs.feature_computer.utils.utils_convert import convert_to_float32

# Create a global ThreadPoolExecutor with a limited number of threads
executor = ThreadPoolExecutor(max_workers=8)  # Adjust max_workers as needed 

async def get_available_funding_rates(pair: str, current_date='yesterday', drive=False, verbose=0):
    """
    Retrieves only available funding rate data for a given trading pair without collecting new data.
    This is an async version that works like get_funding_rates with only_available=True.
    
    Args:
        pair (str): The trading pair for which to retrieve funding data.
        current_date (str, optional): Either 'now' or 'yesterday'. Defaults to 'yesterday'.
        drive (bool, optional): Whether to use Google Drive for storage. Defaults to False.
        verbose (int, optional): Verbosity level. Defaults to 0.
        
    Returns:
        pandas.DataFrame: The available funding rate data for the specified trading pair.
    """
    _print("Function: get_available_funding_rates (async)", 2, verbose)
    _print(f"Retrieving available funding data for {pair} without collecting new data", 1, verbose)
    
    try:
        loop = asyncio.get_running_loop()
        
        def get_existing_funding_data_sync():
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
                _print(f'The pair {pair} is delisted (date: {date_delisted["date"]})', 1, verbose)
                date_delisted_datetime = datetime.strptime(date_delisted["date"], '%Y-%m-%d')
                if date_delisted_datetime <= target_datetime:
                    _print(f'Adjusting target datetime to delisting date', 2, verbose)
                    flag_delisted = True
                    target_datetime = format_datetime(date_delisted_datetime - timedelta(days=1))
                    target_datetime = target_datetime.replace(hour=23, minute=59, second=0)

            # Initialize key variables upfront
            folder_name = 'BINANCE_funding'
            file_name = f'BINANCE_{pair}_funding.csv'
            root_directory = get_root_directory()
            file_path = f'{root_directory}/storage/BINANCE/{folder_name}/{file_name}'
            _print(f"Looking for data at: {file_path}", 2, verbose)

            # Get existing data (local or cloud)
            _print("Attempting to retrieve existing data...", 2, verbose)
            funding_old = get_existing_data(
                pair=pair,
                file_path=file_path,
                service=service,
                folder_name=folder_name,
                file_name=file_name,
                drive=drive,
                verbose=verbose,
                keep_columns=['date', 'funding']
            )
            _print(f"Retrieved {'non-empty' if not funding_old.empty else 'empty'} existing dataset", 2, verbose)

            # Check if any data is available
            if funding_old.empty:
                raise ValueError(f"No funding data available for pair {pair}")
            
            # Apply delisted coin filtering if necessary
            if flag_delisted:
                _print(f"Filtering delisted coin funding data up to {target_datetime}", 2, verbose)
                funding_old = funding_old[funding_old.index <= target_datetime]
                
                # Check if we still have data after filtering
                if funding_old.empty:
                    raise ValueError(f"No funding data available for delisted pair {pair} after filtering to delisting date")

            # Format and return the data
            funding_result = funding_old[['funding']]
            funding_result.columns = ['close']
            # Clean microseconds before returning existing data
            funding_result.index = [x.replace(microsecond=0) for x in funding_result.index]
            _print("Returning available funding data", 2, verbose)
            return funding_result
        
        # Execute blocking operations in a thread to prevent blocking the event loop
        result = await loop.run_in_executor(executor, get_existing_funding_data_sync)
        return result

    except Exception as e:
        _print_error(e)
        _print(f"Failed to retrieve available funding rates for {pair}", 1, verbose)
        raise

def get_existing_data(pair: str, file_path: str, service, folder_name: str, file_name: str, drive: bool, verbose: int, keep_columns: list) -> pd.DataFrame:
    """
    Retrieves existing funding rate data from local and cloud storage, returning the most up-to-date version.
    """
    import os
    _print("Function: get_existing_data", 2, verbose)
    ohlc_old_local = None
    ohlc_old_cloud = None

    # Check local data first
    _print(f"Checking for local data at {file_path}", 2, verbose)
    if os.path.exists(file_path):
        _print(f"Local file found for {pair}", 1, verbose)
        ohlc_old_local = utils_collect.load_local_data(file_path, keep_columns, verbose)
        if ohlc_old_local is not None:
            _print(f"Local data available until {ohlc_old_local.index[-1]}", 2, verbose)

    # Check cloud data if enabled
    if drive and service:
        _print("Checking cloud storage for data", 2, verbose)
        ohlc_old_cloud = utils_collect.load_cloud_data(
            pair=pair,
            folder_name=folder_name,
            file_name=file_name,
            service=service,
            keep_columns=keep_columns,
            verbose=verbose
        )
        if ohlc_old_cloud is not None:
            _print(f"Cloud data available until {ohlc_old_cloud.index[-1]}", 2, verbose)

    # Use utility function to choose best data source
    result = utils_collect.choose_best_data_source(
        local_data=ohlc_old_local, 
        cloud_data=ohlc_old_cloud, 
        folder_name=folder_name, 
        file_name=file_name, 
        service=service, 
        drive=drive, 
        verbose=verbose
    )
    
    if len(result) > 0:
        result['date_datetime'] = [x for x in result.index]
        result['date_without_time'] = result['date_datetime'].dt.strftime('%Y-%m-%d')
        result['floor_hour'] = result.date_datetime.dt.floor('h').dt.hour
        result = result.groupby(['floor_hour','date_without_time'], as_index=False).first()
        result.index = result['date_datetime']
        result = sort_date_index(result)
        result.index = [x.replace(minute=0, second=0, microsecond=0) for x in result.index]
        result = result.drop(['date_datetime','floor_hour','date_without_time'], axis=1)
    return result

async def get_bulk_funding_rates(coins: list, current_date='yesterday', drive=True, verbose=0):
    """
    Retrieve available funding rate data for multiple coins in parallel, with a progress bar.
    Only returns existing data without collecting new data.
    
    Args:
        coins (list): List of trading pairs for which to retrieve funding data.
        current_date (str, optional): Either 'now' or 'yesterday'. Defaults to 'yesterday'.
        drive (bool, optional): Whether to use Google Drive for storage. Defaults to True.
        verbose (int, optional): Verbosity level. Defaults to 0.
        
    Returns:
        dict: Dictionary mapping coin pairs to their funding rate data.
    """
    # Limit concurrency using a semaphore
    semaphore = asyncio.Semaphore(8)  # Adjust the value as needed

    progress_bar = tqdm(total=len(coins), desc="Retrieving available funding data")
    lock = asyncio.Lock()

    async def sem_get_funding_rates(coin):
        async with semaphore:
            _print(f"Starting available funding data retrieval for {coin}", 1, verbose)
            result = await get_available_funding_rates(
                pair=coin,
                current_date=current_date,
                drive=drive,
                verbose=verbose
            )
            async with lock:
                progress_bar.update(1)
            return result

    tasks = [sem_get_funding_rates(coin) for coin in coins]
    results_list = await asyncio.gather(*tasks, return_exceptions=True)
    progress_bar.close()
    results = dict(zip(coins, results_list))
    return results

async def get_funding_rates_live(session: aiohttp.ClientSession, pair: str, target_datetime, verbose=0):
    """
    Retrieve live funding rate data for a single trading pair until a specified datetime.
    
    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for HTTP requests.
        pair (str): The trading pair for which to retrieve funding data.
        target_datetime (str): The target datetime in 'YYYY-MM-DD HH:MM:SS' format.
        verbose (int, optional): Verbosity level. Defaults to 0.
        
    Returns:
        pandas.DataFrame: The funding rate data for the specified trading pair.
    """
    try:
        loop = asyncio.get_running_loop()
        
        # Parse target datetime once outside blocking_operations
        target_datetime_dt = datetime.strptime(target_datetime, '%Y-%m-%d %H:%M:%S')
        
        def blocking_operations():
            import os
            # Remove the timeout-prone Google Drive authentication for now
            # service = cl.authenticate_drive_api()
            
            folder_name = 'BINANCE_funding'
            file_name = f'BINANCE_{pair}_funding.csv'
            root_directory = get_root_directory()
            root_directory = f'{root_directory}/storage/BINANCE'

            # Check for delisted coins
            date_delisted = cf.coin_to_date_delisted.get(pair, None)
            adjusted_target_datetime = target_datetime_dt
            if date_delisted:
                date_delisted_datetime = datetime.strptime(date_delisted["date"], '%Y-%m-%d')
                if date_delisted_datetime <= target_datetime_dt:
                    _print(f'The pair {pair} is delisted (date: {date_delisted["date"]})', 1, verbose)
                    # Adjust target datetime to delisting date
                    adjusted_target_datetime = date_delisted_datetime.replace(hour=23, minute=59, second=0, microsecond=0) - timedelta(days=1)
                    _print(f'Adjusting target datetime to {adjusted_target_datetime}', 2, verbose)

            # Check if the file exists locally
            local_file_path = f'{root_directory}/{folder_name}/{file_name}'
            if os.path.exists(local_file_path):
                try:
                    _print(f"Data for {pair} funding found locally.", 1, verbose)
                    # Use default pandas engine instead of pyarrow to avoid timeout issues
                    funding_old_local = pd.read_csv(local_file_path)
                    funding_old_local = funding_old_local[['date', 'funding']]
                    funding_old_local.index = pd.to_datetime(funding_old_local['date'])
                    # Clean microseconds from loaded local data
                    funding_old_local.index = [x.replace(microsecond=0) for x in funding_old_local.index]
                    funding_old_local.columns = ['date', 'close']
                    funding_old = funding_old_local.copy(deep=True)
                    _print(f"Successfully loaded local data with {len(funding_old)} rows", 2, verbose)
                except Exception as e:
                    _print_error(f"Error reading local file: {e}")
                    # Instead of raising an error, let's try to collect fresh data
                    _print("Will attempt to collect fresh data from API", 1, verbose)
                    return pd.DataFrame(), datetime(2020, 1, 1), adjusted_target_datetime
            else:
                _print(f"Local file not found: {local_file_path}", 1, verbose)
                _print("Will attempt to collect fresh data from API", 1, verbose)
                return pd.DataFrame(), datetime(2020, 1, 1), adjusted_target_datetime

            if not funding_old.empty:
                funding_old = sort_date_index(funding_old)
                end_date_datetime = funding_old[funding_old.index <= adjusted_target_datetime].index[-1]
                _print(f"Existing data available until {end_date_datetime}", 2, verbose)
                return funding_old, end_date_datetime, adjusted_target_datetime
            else:
                _print("Loaded data is empty, will collect fresh data", 2, verbose)
                return pd.DataFrame(), datetime(2020, 1, 1), adjusted_target_datetime

        # Use executor for blocking operations
        funding_old, end_date_datetime, adjusted_target_datetime = await loop.run_in_executor(executor, blocking_operations)

        # Use the adjusted target datetime (which might be changed for delisted coins)
        target_datetime_dt = adjusted_target_datetime

        # If we already have enough data, return it
        if end_date_datetime >= target_datetime_dt:
            funding_old = funding_old[funding_old.index <= target_datetime_dt]
            # Clean microseconds from existing data before returning
            funding_old.index = [x.replace(microsecond=0) for x in funding_old.index]
            # Safely remove date column if it exists
            if 'date' in funding_old.columns:
                funding_old = funding_old.drop('date', axis=1)
            return funding_old

        # Continuously collect data until the target datetime is reached
        while end_date_datetime < target_datetime_dt:
            current_datetime = datetime.now()
            _print(f"Current datetime is {current_datetime}. Target datetime is {target_datetime_dt} for pair {pair}.", 2, verbose)

            # Calculate collection period
            start_date_datetime = end_date_datetime - timedelta(days=3)
            
            _print(f"Collecting funding data from {start_date_datetime} to {target_datetime_dt} for {pair}.", 1, verbose)

            # Retrieve new data using async function
            funding = await most_recent_binance_funding_rate_live(
                session=session,
                ticker=pair,
                start_date=start_date_datetime.strftime('%Y-%m-%d'),
                end_date=(target_datetime_dt + timedelta(days=2)).strftime('%Y-%m-%d'),
                verbose=verbose
            )

            if funding.empty:
                # If no new data is available and we have existing data, return what we have
                if not funding_old.empty:
                    _print(f'No new data available, returning existing data up to {funding_old.index[-1]}', 1, verbose)
                    funding_old = funding_old[funding_old.index <= target_datetime_dt]
                    # Safely remove date column if it exists
                    if 'date' in funding_old.columns:
                        funding_old.drop('date', axis=1, inplace=True)
                    # Clean microseconds before returning existing data
                    funding_old.index = [x.replace(microsecond=0) for x in funding_old.index]
                    return funding_old
                else:
                    raise ValueError('There is an error during funding data collection and no data is available')
            else:
                _print('Concatenating old data with collected data', 2, verbose)
                df_funding = pd.concat([funding_old, funding], axis=0)
                df_funding = df_funding.drop_duplicates(subset=['date'], keep='first')
                df_funding = sort_date_index(df_funding)
                df_funding = df_funding.dropna()
                # Clean microseconds after concatenation and sorting
                df_funding.index = [x.replace(microsecond=0) for x in df_funding.index]

                end_date_datetime = df_funding.index[-1]
                _print(f"Data collected until {end_date_datetime}.", 2, verbose)
                funding_old = df_funding.copy()

        df_funding = df_funding[df_funding.index <= target_datetime_dt]
        # Safely remove date column if it exists
        if 'date' in df_funding.columns:
            df_funding.drop('date', axis=1, inplace=True)
        # Final cleanup of microseconds before returning
        df_funding.index = [x.replace(microsecond=0) for x in df_funding.index]
        return df_funding
        
    except Exception as e:
        _print_error(e)
        return None

async def get_bulk_funding_rates_live(coins: list, target_datetime, keep_local=True, verbose=0):
    """
    Retrieve live funding rate data for multiple coins in parallel, with a progress bar, and store them locally.
    
    Args:
        coins (list): List of trading pairs for which to retrieve funding data.
        target_datetime (str): The target datetime in 'YYYY-MM-DD HH:MM:SS' format.
        keep_local (bool, optional): Whether to save data locally. Defaults to True.
        verbose (int, optional): Verbosity level. Defaults to 0.
        
    Returns:
        dict: Dictionary mapping coin pairs to their funding rate data.
    """
    # Limit concurrency using a semaphore
    semaphore = asyncio.Semaphore(8)  # Adjust the value as needed

    async with aiohttp.ClientSession() as session:
        progress_bar = tqdm(total=len(coins), desc="Collecting live funding data")
        lock = asyncio.Lock()

        async def sem_get_funding_rates_live(coin):
            async with semaphore:
                _print(f"Starting live funding rate collection for {coin}", 1, verbose)
                result = await get_funding_rates_live(
                    session=session,
                    pair=coin,
                    target_datetime=target_datetime,
                    verbose=verbose
                )
                async with lock:
                    progress_bar.update(1)
                return coin, result

        tasks = [sem_get_funding_rates_live(coin) for coin in coins]
        results_list = await asyncio.gather(*tasks, return_exceptions=True)
        progress_bar.close()

        # Store results after all data has been collected
        def save_result(coin, result):
            if result is not None:
                # Save the DataFrame to a CSV file
                local_folder = f'{get_root_directory()}/BINANCE_funding'
                file_name = f'BINANCE_{coin}_funding.csv'
                result['date'] = result.index 
                result.to_csv(f'{local_folder}/{file_name}', index=False)
                _print(f"Funding data for {coin} saved locally to {local_folder}/{file_name}", 2, verbose)

        # Use ThreadPoolExecutor to parallelize saving with progress tracking
        if keep_local:
            with ThreadPoolExecutor(max_workers=8) as executor_save:
                list(tqdm(executor_save.map(lambda item: save_result(*item), results_list), total=len(results_list), desc="Saving funding data"))

        results = dict(results_list)
        return results

async def most_recent_binance_funding_rate_live(session: aiohttp.ClientSession, ticker: str, start_date: str, end_date: str, verbose=0):
    """
    Async version of most_recent_binance_funding_rate using aiohttp.
    
    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for HTTP requests.
        ticker (str): The trading pair symbol.
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.
        verbose (int, optional): Verbosity level. Defaults to 0.
        
    Returns:
        pandas.DataFrame: The funding rate data.
    """
    from urllib.parse import urlencode
    
    # Convert dates to timestamps in milliseconds
    start_timestamp = int(pd.to_datetime(start_date).timestamp() * 1000)
    end_timestamp = int(pd.to_datetime(end_date).timestamp() * 1000)
    
    funding_rates = []
    url_path = '/fapi/v1/fundingRate'
    
    # Binance API allows a maximum limit of 1000 items per request
    limit = 1000
    current_timestamp = start_timestamp
    
    while current_timestamp < end_timestamp:
        try:
            params = {
                'symbol': ticker,
                'startTime': current_timestamp,
                'endTime': end_timestamp,
                'limit': limit
            }
            
            response = await send_public_request_funding(
                session=session,
                url_path=url_path,
                payload=params
            )
            
            if not response:
                break
            
            funding_rates.extend(response)
            current_timestamp = response[-1]['fundingTime'] + 1  # Move to the next timestamp
            
            # If the data length is less than the limit, it means we've fetched all available data
            if len(response) < limit:
                break
                
        except Exception as e:
            _print_error(e)
            break
    
    # Convert to DataFrame
    df = pd.DataFrame(funding_rates)
    
    if df.empty:
        return df
    
    # Convert fundingTime to datetime
    df['date'] = pd.to_datetime(df['fundingTime'], unit='ms')
    df['funding'] = pd.to_numeric(df['fundingRate'])

    df.drop(['fundingTime', 'fundingRate'], axis=1, inplace=True)
    
    # Ensure the DataFrame is sorted by fundingTime
    df.sort_values('date', inplace=True)
    
    # Set the fundingTime as the index
    df = df.reset_index(drop=True)
    df = df.drop(['symbol', 'markPrice'], axis=1)
    
    df = df.dropna()
    df = df[['date', 'funding']]
    df.columns = ['date', 'close']
    df['minute'] = df.date.dt.strftime('%M')
    df = df[df.minute == '00']
    df.drop('minute', axis=1, inplace=True)
    df.index = df['date']
    # Clean microseconds from API data index
    df.index = [x.replace(microsecond=0) for x in df.index]

    df = convert_to_float32(df, verbose)
    
    return df

async def send_public_request_funding(session: aiohttp.ClientSession, url_path: str, payload={}) -> dict:
    """
    Prepare and send an unsigned request to obtain public funding data.
    """
    from urllib.parse import urlencode
    
    # Encode the payload as a query string
    query_string = urlencode(payload, True)

    # Construct the URL for futures API
    url = 'https://fapi.binance.com' + url_path

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

if __name__ == '__main__':
    # Example usage - uncomment and modify as needed
    coins = cf.already_collected_futures_USDT 
    asyncio.run(get_bulk_funding_rates(coins, current_date='yesterday', drive=True, verbose=3)) 