import pandas as _pd
import os as _os  
import numpy as _np 
from datetime import datetime, timedelta  


import dfilabs.collect.api.binance_api as binance_api 
import dfilabs.cloud.cloud as cl 
import dfilabs.config.config as cf 
import dfilabs.collect.utils.utils_collect as utils_collect
from dfilabs.feature_computer.utils.utils_convert import convert_to_float32
from dfilabs.utils.utils_date import format_datetime,adjust_datetime,sort_date_index
from dfilabs.utils.utils_os import get_root_directory
from dfilabs.utils.log import _print, _print_error   

# Import the original get_prices function from prices.py
from dfilabs.collect.binance.prices import get_prices as get_prices_original

binance_creation = datetime.strptime('2015-01-01','%Y-%m-%d')

def get_prices(pair: str, drive=False, n_candles_add=0, verbose=0):
    """
    Retrieves enhanced OHLCV data by merging future and spot data with future priority.
    When a date is available in spot but not in future, uses spot data.
    
    Args:
        pair (str): Trading pair symbol
        drive (bool): Whether to use Google Drive integration
        n_candles_add (int): Additional candles to collect
        verbose (int): Verbosity level (0-2)
        
    Returns:
        pandas.DataFrame: Enhanced OHLCV data without date column
    """
    _print("Function: get_enhanced_prices", 2, verbose)
    
    try:
        _print(f"Starting enhanced price collection for {pair}", 1, verbose)
        
        # Initialize key variables for enhanced storage
        root_directory = get_root_directory()
        root_directory = f'{root_directory}/storage/BINANCE'
        
        folder_name = f'BINANCE_enhance_price'
        file_name = f'BINANCE_{pair}_enhanced.csv'
        local_folder = f'{root_directory}/{folder_name}'
        file_path = f'{local_folder}/{file_name}'
        _print(f"Enhanced file will be saved as: {file_path}", 2, verbose)

        # Initialize service once if needed
        service = cl.authenticate_drive_api() if drive else None
        _print(f"Google Drive integration: {'enabled' if drive else 'disabled'}", 2, verbose)

        # Calculate target datetime once
        target_datetime = (datetime.now() - timedelta(days=1)).replace(hour=23, minute=59, second=0, microsecond=0)
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

        # Get existing enhanced data (local or cloud)
        _print("Attempting to retrieve existing enhanced data...", 2, verbose)
        ohlc_old = get_existing_data(
            pair = pair, 
            file_path = file_path, 
            service = service, 
            folder_name = folder_name, 
            file_name = file_name, 
            drive = drive, 
            verbose = verbose
        )
        _print(f"Retrieved {'non-empty' if not ohlc_old.empty else 'empty'} existing enhanced dataset", 2, verbose)

        # Check if enhanced data is up to date
        if not ohlc_old.empty and ohlc_old.index[-1] == target_datetime and n_candles_add == 0:
            _print("Enhanced data is already up to date.", 1, verbose)
            _print("Returning existing enhanced data without date column", 2, verbose)
            
            if flag_delisted : 
                target_datetime = (target_datetime-timedelta(days=1)).replace(hour=23,minute=59,second=0)
                ohlc_old = ohlc_old[ohlc_old.index <= target_datetime]
            
            return ohlc_old.drop('date', axis=1)
            
        if flag_delisted and not ohlc_old.empty and format_datetime(ohlc_old.index[-1]) >= target_datetime:
            _print("Enhanced data is up to date but delisted", 1, verbose)
            _print("Returning existing enhanced data without date column", 2, verbose)
            
            if flag_delisted : 
                target_datetime = (target_datetime-timedelta(days=1)).replace(hour=23,minute=59,second=0)
                ohlc_old = ohlc_old[ohlc_old.index <= target_datetime]
            
            return ohlc_old.drop('date', axis=1)

        # Check if this is first time collection
        is_first_time = ohlc_old.empty
        _print(f"Collection type: {'First time' if is_first_time else 'Update existing data'}", 2, verbose)
        
        # Collect future and spot data
        _print("Collecting future data...", 1, verbose)
        future_data = get_prices_original(
            pair=pair, 
            trading_type="future", 
            drive=drive, 
            n_candles_add=n_candles_add, 
            verbose=verbose
        )
        _print(f"Future data collection {'successful' if future_data is not None else 'failed'}", 2, verbose)
        
        # Validation: If not first time and not delisted, future data should be available
        if not is_first_time and not flag_delisted and (future_data is None or future_data.empty):
            error_msg = f"Future data is not available for {pair} but this is not the first collection and pair is not delisted. This indicates a data collection issue."
            _print_error(error_msg)
            raise ValueError(error_msg)
        
        _print("Collecting spot data...", 1, verbose)
        spot_data = get_prices_original(
            pair=pair, 
            trading_type="spot", 
            drive=drive, 
            n_candles_add=n_candles_add, 
            verbose=verbose
        )
        _print(f"Spot data collection {'successful' if spot_data is not None else 'failed'}", 2, verbose)

        # Merge future and spot data with future priority
        _print("Merging future and spot data with future priority...", 1, verbose)
        enhanced_data = merge_future_spot_data(future_data, spot_data, target_datetime, flag_delisted, verbose)
        
        if enhanced_data is None or enhanced_data.empty:
            _print("Failed to create enhanced data", 1, verbose)
            return None
            
        _print(f"Successfully merged data. Final shape: {enhanced_data.shape}", 2, verbose)

        # Store the enhanced data
        _print("Storing enhanced data...", 2, verbose)
        enhanced_data = convert_to_float32(enhanced_data, verbose)
        utils_collect.store_data(
            df_ohlc=enhanced_data,
            root_directory=root_directory, 
            folder_name=folder_name, 
            file_name=file_name, 
            service=service, 
            drive=drive, 
            verbose=verbose
        )
        _print("Enhanced data storage complete", 2, verbose)

        _print(f"Successfully completed enhanced price collection for {pair}", 1, verbose)
        return enhanced_data.drop('date', axis=1)

    except Exception as e:
        _print_error(e)
        _print(f"Failed to collect enhanced prices for {pair}", 1, verbose)
        return None


def merge_future_spot_data(future_data, spot_data, target_datetime, flag_delisted, verbose=0):
    """
    Merges future and spot data with future priority.
    
    Priority Logic:
    1. If spot data is not available -> use future data with priority
    2. If future data is not available -> use spot data only
    3. If both available -> merge with future priority (future overwrites spot for same dates)
    4. If neither available -> return empty DataFrame
    
    Args:
        future_data (pd.DataFrame): Future trading data  
        spot_data (pd.DataFrame): Spot trading data
        target_datetime (datetime): Target datetime for filtering
        flag_delisted (bool): Whether the pair is delisted
        verbose (int): Verbosity level
        
    Returns:
        pd.DataFrame: Merged data with date column
    """
    _print("Function: merge_future_spot_data", 2, verbose)
    
    # Handle cases where one or both datasets are None or empty
    if future_data is None and spot_data is None:
        _print("Both future and spot data are None", 1, verbose)
        return _pd.DataFrame()
        
    if future_data is None or future_data.empty:
        _print("Future data is None/empty, using spot data only", 1, verbose)
        if spot_data is None or spot_data.empty:
            return _pd.DataFrame()
        result = spot_data.copy()
    elif spot_data is None or spot_data.empty:
        _print("Spot data is not available, using future data with priority", 1, verbose)
        result = future_data.copy()
    else:
        # Both datasets have data - merge with future priority
        _print("Both future and spot data available, merging with future priority", 2, verbose)
        
        # Add date column back if it was removed
        if 'date' not in future_data.columns:
            future_data = future_data.copy()
            future_data['date'] = future_data.index
            
        if 'date' not in spot_data.columns:
            spot_data = spot_data.copy()
            spot_data['date'] = spot_data.index
        
        # Reset index to use date column for merging
        future_df = future_data.reset_index(drop=True)
        spot_df = spot_data.reset_index(drop=True)
        
        # Create a combined dataset
        # First, take all future data
        result = future_df.copy()
        
        # Then, add spot data for dates not present in future data
        future_dates = set(future_df['date'])
        spot_only = spot_df[~spot_df['date'].isin(future_dates)]
        
        if not spot_only.empty:
            _print(f"Adding {len(spot_only)} dates from spot data that are missing in future data", 2, verbose)
            result = _pd.concat([result, spot_only], ignore_index=True)
        
        # Sort by date
        result = result.sort_values('date').reset_index(drop=True)
        _print(f"Merged data contains {len(result)} total records", 2, verbose)
    
    # Set date as index
    if 'date' in result.columns:
        result.index = result['date']
    
    # Apply datetime adjustments and filtering
    result['date'] = result['date'].apply(adjust_datetime)
    result = sort_date_index(result)
    
    # Filter by target datetime
    if flag_delisted:
        target_datetime = (target_datetime-timedelta(days=1)).replace(hour=23,minute=59,second=0)
        
    result = result[result['date'] <= target_datetime]
    _print(f"After filtering to target datetime, data contains {len(result)} records", 2, verbose)
    
    return result


def get_existing_data(pair, file_path, service, folder_name, file_name, drive, verbose=0):
    """Helper function to get existing enhanced data from local or cloud storage"""
    _print("Function: get_existing_data (enhanced)", 2, verbose)
    keep_columns = ['date', 'open', 'high', 'low', 'close', 'buy_volume', 'sell_volume', 'volume']
    
    # Check local data first
    _print(f"Checking for local enhanced data at {file_path}", 2, verbose)
    ohlc_old_local = utils_collect.load_local_data(file_path, keep_columns, verbose) if _os.path.exists(file_path) else None
    
    # Check cloud data if enabled
    ohlc_old_cloud = None 
    
    if drive and service:
        _print("Checking cloud storage for enhanced data", 2, verbose)
        ohlc_old_cloud = utils_collect.load_cloud_data(pair, folder_name, file_name, service, keep_columns, verbose)
    
    return utils_collect.choose_best_data_source(local_data=ohlc_old_local, cloud_data=ohlc_old_cloud,folder_name=folder_name,file_name = file_name,service=service,drive=drive, verbose=verbose)


