import pandas as pd
import numpy as np
import asyncio
import aiohttp
import os
import json
from datetime import datetime, timedelta
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

from dfilabs.collect.api.unravel import get_portfolio_factors_historical
import dfilabs.cloud.cloud as cl
import dfilabs.collect.utils.utils_collect as utils_collect
from dfilabs.feature_computer.utils.utils_convert import convert_to_float32
from dfilabs.utils.utils_date import format_datetime, adjust_datetime, sort_date_index
from dfilabs.utils.utils_os import get_root_directory
from dfilabs.utils.log import _print, _print_error


# Create a global ThreadPoolExecutor with a limited number of threads
executor = ThreadPoolExecutor(max_workers=8)


def load_unravel_credentials():
    """Load Unravel API credentials from credentials file."""
    root_directory = get_root_directory()
    credentials_path = f'{root_directory}/credentials/unravel.json'
    
    try:
        with open(credentials_path, 'r') as f:
            credentials = json.load(f)
        return credentials.get('API_KEY')
    except Exception as e:
        _print_error(f"Error loading Unravel credentials: {str(e)}")
        return None


async def get_factors_live(portfolio: str, ticker: str, target_date: str, drive=False, verbose=0):
    """
    Retrieves live factor data for a given portfolio and ticker until a specified date.
    Similar to get_prices_live in Binance - continuously collects until target_date is reached.
    
    Args:
        portfolio (str): Portfolio name (e.g., 'retail_flow')
        ticker (str): Ticker symbol with USDT suffix (e.g., 'BTCUSDT')
        target_date (str): Target date in 'YYYY-MM-DD' format
        drive (bool): Whether to use Google Drive for cloud storage
        verbose (int): Verbosity level for logging
    
    Returns:
        pd.DataFrame: Factor data with DatetimeIndex and 'close' column
    """
    _print("Function: get_factors_live", 2, verbose)
    
    try:
        loop = asyncio.get_running_loop()
        
        # Parse target date once outside blocking_operations
        target_date_dt = datetime.strptime(target_date, '%Y-%m-%d').date()
        target_datetime = datetime.combine(target_date_dt, datetime.min.time()).replace(hour=23, minute=55, second=0, microsecond=0)
        
        def blocking_operations():
            _print(f"Starting live factor collection for {ticker} in portfolio {portfolio}", 1, verbose)
            
            # Load API credentials
            api_key = load_unravel_credentials()
            if not api_key:
                raise ValueError("Failed to load Unravel API credentials")
            
            # Initialize service once if needed
            service = cl.authenticate_drive_api() if drive else None
            _print(f"Google Drive integration: {'enabled' if drive else 'disabled'}", 2, verbose)
            
            # Initialize key variables upfront
            root_directory = get_root_directory()
            root_directory = f'{root_directory}/storage/UNRAVEL'
            
            folder_name = f'{portfolio}'
            file_name = f'{ticker}_{portfolio}.csv'
            local_folder = f'{root_directory}/{folder_name}'
            file_path = f'{local_folder}/{file_name}'
            _print(f"File will be saved as: {file_path}", 2, verbose)

            # Ensure directory exists
            os.makedirs(local_folder, exist_ok=True)
            
            _print(f"Target date is set to {target_date} (will add 23:55 timestamp)", 1, verbose)

            # Get existing data using the original unravel.py method
            _print("Attempting to retrieve existing data...", 2, verbose)
            from dfilabs.collect.unravel.unravel import get_existing_data
            try:
                factors_old = get_existing_data(
                    ticker=ticker,
                    file_path=file_path,
                    service=service,
                    folder_name=folder_name,
                    file_name=file_name,
                    drive=drive,
                    verbose=verbose
                )
                _print(f"DEBUG: get_existing_data completed successfully", 3, verbose)
                _print(f"Retrieved {'non-empty' if not factors_old.empty else 'empty'} existing dataset", 2, verbose)
            except Exception as e:
                _print(f"DEBUG: Error in get_existing_data: {str(e)}", 1, verbose)
                import traceback
                traceback.print_exc()
                raise

            if not factors_old.empty:
                factors_old = sort_date_index(factors_old)
                end_date_datetime = factors_old.index[-1]
                _print(f"Existing data available until {end_date_datetime}", 2, verbose)
                
                # Check if data is already up to date
                if end_date_datetime.date() >= target_date_dt:
                    _print("Data is already up to date.", 1, verbose)
                    # Create boolean mask for filtering
                    date_mask = pd.Series([dt.date() <= target_date_dt for dt in factors_old.index], index=factors_old.index)
                    factors_filtered = factors_old[date_mask]
                    # Debug: check columns before dropping
                    _print(f"Columns in factors_filtered: {factors_filtered.columns.tolist()}", 2, verbose)
                    # Safely remove date column if it exists
                    if 'date' in factors_filtered.columns:
                        factors_filtered = factors_filtered.drop('date', axis=1)
                    # Return a special marker to indicate early return
                    return ('EARLY_RETURN', format_factor_data_as_close_live(factors_filtered, verbose), None, None)
                
                _print(f"Data for {ticker} {portfolio} is available until {end_date_datetime}. Collecting further data.", 1, verbose)
                _print(f"DEBUG: About to return from blocking_operations", 3, verbose)
            else:
                _print(f"No existing data found for {ticker} {portfolio}.", 1, verbose)
                end_date_datetime = datetime.strptime('2020-01-01', '%Y-%m-%d')
                factors_old = pd.DataFrame()
                _print(f"Starting data collection from {end_date_datetime}.", 2, verbose)
                _print(f"DEBUG: About to return from blocking_operations (no existing data)", 3, verbose)

            _print(f"DEBUG: Return values - factors_old type: {type(factors_old)}, end_date_datetime type: {type(end_date_datetime)}", 3, verbose)
            try:
                return factors_old, end_date_datetime, api_key, service
            except Exception as e:
                _print(f"DEBUG: Error in return statement: {str(e)}", 1, verbose)
                import traceback
                traceback.print_exc()
                raise
        
        # Execute blocking operations in a thread to prevent blocking the event loop
        try:
            _print(f"DEBUG: About to call run_in_executor", 3, verbose)
            result = await loop.run_in_executor(executor, blocking_operations)
            _print(f"DEBUG: run_in_executor completed", 3, verbose)
        except Exception as e:
            _print(f"DEBUG: Error in run_in_executor: {str(e)}", 1, verbose)
            import traceback
            traceback.print_exc()
            raise
        
        # Check if this is an early return (data already up to date)
        _print(f"DEBUG: About to check result type", 3, verbose)
        _print(f"DEBUG: result type: {type(result)}, len: {len(result) if hasattr(result, '__len__') else 'N/A'}", 3, verbose)
        _print(f"DEBUG: result[0] type: {type(result[0])}, value: {result[0] if not hasattr(result[0], 'shape') else f'DataFrame with shape {result[0].shape}'}", 3, verbose)
        if isinstance(result, tuple) and len(result) == 4 and isinstance(result[0], str) and result[0] == 'EARLY_RETURN':
            return result[1]  # Return the formatted DataFrame directly
        
        # Normal case: unpack the 4 values
        _print(f"DEBUG: About to unpack result", 3, verbose)
        factors_old, end_date_datetime, api_key, service = result
        _print(f"DEBUG: Unpacked result successfully", 3, verbose)
        _print(f"DEBUG: factors_old type: {type(factors_old)}, shape: {factors_old.shape if hasattr(factors_old, 'shape') else 'N/A'}", 3, verbose)
        _print(f"DEBUG: end_date_datetime: {end_date_datetime}, type: {type(end_date_datetime)}", 3, verbose)

        # Calculate what data we need to collect
        _print(f"DEBUG: About to compare dates", 3, verbose)
        if end_date_datetime.date() < target_date_dt:
            days_missing = (target_date_dt - end_date_datetime.date()).days
            _print(f"Need to collect {days_missing} days of data for {ticker}.", 1, verbose)

        # Continuously collect data until the target date is reached (like Binance live collection)
        while end_date_datetime.date() < target_date_dt:
            current_datetime = datetime.now()
            _print(f"Current datetime is {current_datetime}. Target date is {target_date_dt} for {ticker}.", 2, verbose)

            # Collect new data from API
            _print(f"Collecting factor data for {ticker} in portfolio {portfolio}.", 1, verbose)
            
            try:
                # Get factor data from Unravel API
                api_ticker = ticker.replace('USDT', '') if ticker.endswith('USDT') else ticker
                _print(f"DEBUG: About to call get_portfolio_factors_historical", 3, verbose)
                factor_data = get_portfolio_factors_historical(
                    portfolioId=portfolio,
                    tickers=[api_ticker],
                    API_KEY=api_key
                )
                _print(f"DEBUG: get_portfolio_factors_historical completed", 3, verbose)
            except Exception as e:
                _print(f"DEBUG: Error in get_portfolio_factors_historical: {str(e)}", 1, verbose)
                raise

            if factor_data is None or factor_data.empty:
                _print('Error when collecting new data.', 1, verbose)
                # If no new data is available and we have existing data, return what we have
                if not factors_old.empty:
                    _print('Returning the currently available data.', 1, verbose)
                    # Create boolean mask for filtering
                    date_mask = pd.Series([dt.date() <= target_date_dt for dt in factors_old.index], index=factors_old.index)
                    factors_filtered = factors_old[date_mask]
                    # Safely remove date column if it exists
                    if 'date' in factors_filtered.columns:
                        factors_filtered = factors_filtered.drop('date', axis=1)
                    return format_factor_data_as_close_live(factors_filtered, verbose)
                else:
                    raise ValueError('Error during data collection and no existing data available.')
            else:
                _print(f"API returned {len(factor_data)} data points", 1, verbose)
                
                # Adjust timestamps to 23:55:00 for daily data collection time (API constraint)
                factor_data.index = factor_data.index.normalize() + pd.Timedelta(hours=23, minutes=55)
                factor_data['date'] = factor_data.index
                
                # Show the corrected date range with proper timestamps
                _print(f"API data date range (with 23:55 timestamps): {factor_data.index.min()} to {factor_data.index.max()}", 1, verbose)
                
                _print('Concatenating old data with new data.', 2, verbose)
                df_factors = pd.concat([factors_old, factor_data], axis=0)
                df_factors = df_factors.drop_duplicates(subset=['date'], keep='last')
                df_factors = sort_date_index(df_factors)
                df_factors = df_factors.dropna()

                end_date_datetime = df_factors.index[-1]
                _print(f"Data collected until {end_date_datetime}.", 2, verbose)
                
                # Check if we have the target date with correct timestamp
                target_timestamp = datetime.combine(target_date_dt, datetime.min.time()).replace(hour=23, minute=55, second=0, microsecond=0)
                
                if target_timestamp in df_factors.index:
                    _print(f"✅ Target date {target_date_dt} found with correct timestamp 23:55:00", 1, verbose)
                    break  # We have the target date, exit the loop
                elif end_date_datetime.date() >= target_date_dt:
                    # We have data for the target date but not with 23:55 timestamp - this should not happen
                    _print(f"❌ ERROR: Data exists for {target_date_dt} but not with 23:55:00 timestamp", 1, verbose)
                    _print(f"Available timestamps for {target_date_dt}: {[ts for ts in df_factors.index if ts.date() == target_date_dt]}", 1, verbose)
                    raise ValueError(f"Data for {target_date_dt} exists but not with expected 23:55:00 timestamp. API constraint violated.")
                else:
                    remaining_days = (target_date_dt - end_date_datetime.date()).days
                    _print(f'Data collected but end_date is before target date. Still missing {remaining_days} days.', 1, verbose)
                
                factors_old = df_factors.copy()

        # Filter to target date and validate final data
        date_mask = pd.Series([dt.date() <= target_date_dt for dt in df_factors.index], index=df_factors.index)
        df_factors = df_factors[date_mask]
        
        # Final validation: ensure target date has correct timestamp
        target_timestamp = datetime.combine(target_date_dt, datetime.min.time()).replace(hour=23, minute=55, second=0, microsecond=0)
        if target_timestamp not in df_factors.index:
            _print(f"❌ FINAL VALIDATION FAILED: Target date {target_date_dt} not found with 23:55:00 timestamp", 1, verbose)
            available_dates = sorted(set(ts.date() for ts in df_factors.index))
            _print(f"Available dates: {available_dates}", 1, verbose)
            if target_date_dt in [ts.date() for ts in df_factors.index]:
                wrong_timestamps = [ts for ts in df_factors.index if ts.date() == target_date_dt]
                _print(f"Wrong timestamps for {target_date_dt}: {wrong_timestamps}", 1, verbose)
                raise ValueError(f"Target date {target_date_dt} found but with wrong timestamp. Expected 23:55:00, got: {wrong_timestamps}")
            else:
                raise ValueError(f"Target date {target_date_dt} not available in collected data. API has not provided this date yet.")
        else:
            _print(f"✅ FINAL VALIDATION PASSED: Target date {target_date_dt} confirmed with 23:55:00 timestamp", 1, verbose)
        
        # Note: Unlike the original approach, we don't save data locally here
        # This is consistent with Binance live collection which only reads existing data
        # Local saving should be handled by bulk operations if needed
        
        _print(f"Successfully completed live factor collection for {ticker}", 1, verbose)
        # Safely remove date column if it exists
        if 'date' in df_factors.columns:
            df_factors = df_factors.drop('date', axis=1)
        return format_factor_data_as_close_live(df_factors, verbose)

    except Exception as e:
        _print_error(e)
        _print(f"Failed to collect live factors for {ticker}", 1, verbose)
        return None




# Note: store_factor_data_locally_live function removed to maintain consistency
# with Binance approach where individual live functions don't save data locally




def format_factor_data_as_close_live(df_factors, verbose=0):
    """
    Format factor data to have a single 'close' column with DatetimeIndex for live collection.
    
    Args:
        df_factors (pd.DataFrame): Factor data with datetime index
        verbose (int): Verbosity level for logging
    
    Returns:
        pd.DataFrame: Formatted DataFrame with DatetimeIndex and 'close' column
    """
    _print("Function: format_factor_data_as_close_live", 2, verbose)
    
    if df_factors.empty:
        _print("Empty factor data provided, returning empty DataFrame with 'close' column", 1, verbose)
        return pd.DataFrame(columns=['close'])
    
    # Take the first numeric column as the 'close' value
    numeric_columns = df_factors.select_dtypes(include=[np.number]).columns
    
    if len(numeric_columns) == 0:
        _print("No numeric columns found in factor data", 1, verbose)
        return pd.DataFrame(index=df_factors.index, columns=['close'])
    
    # Use the first numeric column as 'close'
    close_column = numeric_columns[0]
    _print(f"Using column '{close_column}' as 'close' values", 2, verbose)
    
    result_df = pd.DataFrame(
        data={'close': df_factors[close_column]},
        index=df_factors.index
    )
    
    _print(f"Formatted factor data: {result_df.shape[0]} rows with 'close' column", 2, verbose)
    return result_df


async def get_available_factors(portfolio: str, ticker: str, target_date: str, drive=False, verbose=0):
    """
    Retrieves only available factor data for a given portfolio and ticker without collecting new data.
    This is an async version that works like get_factors with only_available=True.
    
    Args:
        portfolio (str): Portfolio name (e.g., 'retail_flow')
        ticker (str): Ticker symbol with USDT suffix (e.g., 'BTCUSDT')
        target_date (str): Target date in 'YYYY-MM-DD' format
        drive (bool): Whether to use Google Drive for cloud storage
        verbose (int): Verbosity level for logging
    
    Returns:
        pd.DataFrame: Available factor data with DatetimeIndex and 'close' column
    """
    _print("Function: get_available_factors (async)", 2, verbose)
    _print(f"Retrieving available factor data for {ticker} in portfolio {portfolio} without collecting new data", 1, verbose)
    
    try:
        loop = asyncio.get_running_loop()
        
        # Parse target date once
        target_date_dt = datetime.strptime(target_date, '%Y-%m-%d').date()
        
        def get_existing_factors_data_sync():
            # Load API credentials (for validation, though we won't use them)
            api_key = load_unravel_credentials()
            if not api_key:
                raise ValueError("Failed to load Unravel API credentials")
            
            # Initialize service once if needed
            service = cl.authenticate_drive_api() if drive else None
            _print(f"Google Drive integration: {'enabled' if drive else 'disabled'}", 2, verbose)
            
            # Initialize key variables upfront
            root_directory = get_root_directory()
            root_directory = f'{root_directory}/storage/UNRAVEL'
            
            folder_name = f'{portfolio}'
            file_name = f'{ticker}_{portfolio}.csv'
            local_folder = f'{root_directory}/{folder_name}'
            file_path = f'{local_folder}/{file_name}'
            _print(f"Looking for data at: {file_path}", 2, verbose)

            # Ensure directory exists
            os.makedirs(local_folder, exist_ok=True)
            
            _print(f"Target date is set to {target_date}", 1, verbose)

            # Get existing data using the original unravel.py method
            _print("Attempting to retrieve existing data...", 2, verbose)
            from dfilabs.collect.unravel.unravel import get_existing_data
            factors_old = get_existing_data(
                ticker=ticker,
                file_path=file_path,
                service=service,
                folder_name=folder_name,
                file_name=file_name,
                drive=drive,
                verbose=verbose
            )
            _print(f"Retrieved {'non-empty' if not factors_old.empty else 'empty'} existing dataset", 2, verbose)

            # Check if any data is available
            if factors_old.empty:
                raise ValueError(f"No factor data available for ticker {ticker} in portfolio {portfolio}")
            
            # Filter to target date (factors don't have delisted coin logic like prices/funding)
            date_mask = pd.Series([dt.date() <= target_date_dt for dt in factors_old.index], index=factors_old.index)
            factors_filtered = factors_old[date_mask]
            
            # Check if we still have data after filtering
            if factors_filtered.empty:
                raise ValueError(f"No factor data available for ticker {ticker} in portfolio {portfolio} up to date {target_date}")
            
            _print("Returning available factor data formatted as 'close' column", 2, verbose)
            # Safely remove date column if it exists
            if 'date' in factors_filtered.columns:
                factors_filtered = factors_filtered.drop('date', axis=1)
            return format_factor_data_as_close_live(factors_filtered, verbose)
        
        # Execute blocking operations in a thread to prevent blocking the event loop
        result = await loop.run_in_executor(executor, get_existing_factors_data_sync)
        return result

    except Exception as e:
        _print_error(e)
        _print(f"Failed to retrieve available factors for {ticker} in portfolio {portfolio}", 1, verbose)
        raise


async def get_bulk_factors_live(portfolios_tickers: list, target_date: str, drive=False, verbose=0):
    """
    Retrieve live factor data for multiple portfolio-ticker combinations in parallel, with a progress bar.
    
    Args:
        portfolios_tickers (list): List of tuples (portfolio, ticker) for which to retrieve factor data.
        target_date (str): Target date in 'YYYY-MM-DD' format.
        drive (bool, optional): Whether to use Google Drive for storage. Defaults to False.
        verbose (int, optional): Verbosity level. Defaults to 0.
        
    Returns:
        dict: Dictionary mapping (portfolio, ticker) tuples to their factor data.
    """
    # Limit concurrency using a semaphore
    semaphore = asyncio.Semaphore(8)  # Adjust the value as needed

    progress_bar = tqdm(total=len(portfolios_tickers), desc="Collecting live factor data")
    lock = asyncio.Lock()

    async def sem_get_factors_live(portfolio_ticker):
        async with semaphore:
            portfolio, ticker = portfolio_ticker
            _print(f"Starting live factor collection for {ticker} in portfolio {portfolio}", 1, verbose)
            result = await get_factors_live(
                portfolio=portfolio,
                ticker=ticker,
                target_date=target_date,
                drive=drive,
                verbose=verbose
            )
            async with lock:
                progress_bar.update(1)
            return portfolio_ticker, result

    tasks = [sem_get_factors_live(pt) for pt in portfolios_tickers]
    results_list = await asyncio.gather(*tasks, return_exceptions=True)
    progress_bar.close()
    results = dict(results_list)
    return results


async def get_bulk_available_factors(portfolios_tickers: list, target_date: str, drive=False, verbose=0):
    """
    Retrieve available factor data for multiple portfolio-ticker combinations in parallel, with a progress bar.
    Only returns existing data without collecting new data.
    
    Args:
        portfolios_tickers (list): List of tuples (portfolio, ticker) for which to retrieve factor data.
        target_date (str): Target date in 'YYYY-MM-DD' format.
        drive (bool, optional): Whether to use Google Drive for storage. Defaults to False.
        verbose (int, optional): Verbosity level. Defaults to 0.
        
    Returns:
        dict: Dictionary mapping (portfolio, ticker) tuples to their factor data.
    """
    # Limit concurrency using a semaphore
    semaphore = asyncio.Semaphore(8)  # Adjust the value as needed

    progress_bar = tqdm(total=len(portfolios_tickers), desc="Retrieving available factor data")
    lock = asyncio.Lock()

    async def sem_get_available_factors(portfolio_ticker):
        async with semaphore:
            portfolio, ticker = portfolio_ticker
            _print(f"Starting available factor data retrieval for {ticker} in portfolio {portfolio}", 1, verbose)
            result = await get_available_factors(
                portfolio=portfolio,
                ticker=ticker,
                target_date=target_date,
                drive=drive,
                verbose=verbose
            )
            async with lock:
                progress_bar.update(1)
            return portfolio_ticker, result

    tasks = [sem_get_available_factors(pt) for pt in portfolios_tickers]
    results_list = await asyncio.gather(*tasks, return_exceptions=True)
    progress_bar.close()
    results = dict(results_list)
    return results


if __name__ == '__main__':
    # Example usage - uncomment and modify as needed
    # portfolios_tickers = [('retail_flow', 'BTCUSDT'), ('retail_flow', 'ETHUSDT')]
    # target_date = '2024-01-15'
    # asyncio.run(get_bulk_factors_live(portfolios_tickers, target_date=target_date, drive=False, verbose=3))
    pass
