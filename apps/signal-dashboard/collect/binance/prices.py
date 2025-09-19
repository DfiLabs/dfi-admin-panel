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

binance_creation = datetime.strptime('2015-01-01','%Y-%m-%d')

def get_prices(pair: str, trading_type="future", drive=False, n_candles_add=0, only_available = False,verbose=0):
    _print("Function: get_prices", 2, verbose)
    """
    Retrieves historical OHLCV data for a given trading pair and trading type.
    """
    

    try:
        _print(f"Starting price collection for {pair} ({trading_type})", 1, verbose)
        
        # Initialize key variables upfront
        root_directory = get_root_directory()
        root_directory = f'{root_directory}/storage/BINANCE'
        
        folder_name = f'BINANCE_{trading_type}_prices'
        file_name = f'BINANCE_{pair}_{trading_type}.csv'
        local_folder = f'{root_directory}/{folder_name}'
        file_path = f'{local_folder}/{file_name}'
        _print(f"File will be saved as: {file_path}", 2, verbose)

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

        # Get existing data (local or cloud)
        _print("Attempting to retrieve existing data...", 2, verbose)
        ohlc_old = get_existing_data(
            pair = pair, 
            file_path = file_path, 
            service = service, 
            folder_name = folder_name, 
            file_name = file_name, 
            drive = drive, 
            verbose = verbose
        )
        _print(f"Retrieved {'non-empty' if not ohlc_old.empty else 'empty'} existing dataset", 2, verbose)
        
        # Handle only_available parameter
        if only_available:
            _print("only_available is True - returning existing data without collecting new data", 1, verbose)
            
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
            return ohlc_old.drop('date', axis=1)

        # Check if ohlc_old is up to date
        if not ohlc_old.empty and ohlc_old.index[-1] == target_datetime and n_candles_add == 0:
            _print("Data is already up to date.", 1, verbose)
            _print("Returning existing data without date column", 2, verbose)
            _print("Data is up to date but delisted", 1, verbose)
            _print("Returning existing data without date column", 2, verbose)
            
           
            
            if flag_delisted : 
                target_datetime = (target_datetime-timedelta(days=1)).replace(hour=23,minute=59,second=0)
                ohlc_old = ohlc_old[ohlc_old.index <= target_datetime]
            
            return ohlc_old.drop('date', axis=1)
        if flag_delisted and format_datetime(ohlc_old.index[-1]) >= target_datetime:
            _print("Data is up to date but delisted", 1, verbose)
            _print("Returning existing data without date column", 2, verbose)
            

      
            if flag_delisted : 
                target_datetime = (target_datetime-timedelta(days=1)).replace(hour=23,minute=59,second=0)
                ohlc_old = ohlc_old[ohlc_old.index <= target_datetime]
            
            return ohlc_old.drop('date', axis=1)

        # Calculate data collection parameters
        start_date_datetime = (
            ohlc_old.index[-1] - timedelta(minutes=1)
            if not ohlc_old.empty
            else datetime.strptime('2018-01-01', '%Y-%m-%d')
        )
        _print(f"Collection start date set to: {start_date_datetime}", 2, verbose)

        # Calculate required candles
        n_candles = utils_collect.calculate_required_candles(start_date_datetime = start_date_datetime, 
                                                             target_datetime = target_datetime, 
                                                             timeframe_minute = 1,
                                                             n_candles_add = n_candles_add, 
                                                             verbose = verbose
        )
        _print(f"Need to collect {n_candles} candles (including {n_candles_add} additional candles)", 1, verbose)

        # Collect new data
        _print("Starting collection of new data...", 1, verbose)
        new_data = collect_new_data(
            pair = pair, 
            n_candles = n_candles, 
            trading_type = trading_type, 
            target_datetime = target_datetime,
            ohlc_old = ohlc_old,  
            verbose = verbose
        )
        _print(f"Collected {'non-empty' if not new_data.empty else 'empty'} new dataset", 2, verbose)

        # Store the new data
        _print("Storing collected data...", 2, verbose)
        # Convert numeric columns to float32 for memory efficiency.

        new_data = convert_to_float32(new_data, verbose)
        utils_collect.store_data(
            df_ohlc=new_data,
            root_directory=root_directory, 
            folder_name=folder_name, 
            file_name=file_name, 
            service=service, 
            drive=drive, 
            verbose=verbose
        )
        _print("Data storage complete", 2, verbose)

        _print(f"Successfully completed price collection for {pair}", 1, verbose)
        return new_data.drop('date', axis=1)

    except Exception as e:
        _print_error(e)
        _print(f"Failed to collect prices for {pair}", 1, verbose)
        return None 




def get_existing_data(pair, file_path, service, folder_name, file_name, drive, verbose=0):
    _print("Function: get_existing_data", 2, verbose)
    """Helper function to get existing data from local or cloud storage"""
    keep_columns = ['date', 'open', 'high', 'low', 'close', 'buy_volume', 'sell_volume', 'volume']
    
    # Check local data first
    _print(f"Checking for local data at {file_path}", 2, verbose)
    ohlc_old_local = utils_collect.load_local_data(file_path, keep_columns, verbose) if _os.path.exists(file_path) else None
    
    # Check cloud data if enabled
    ohlc_old_cloud = None 
    
    if drive and service:
        _print("Checking cloud storage for data", 2, verbose)
        ohlc_old_cloud = utils_collect.load_cloud_data(pair, folder_name, file_name, service, keep_columns, verbose)
    
    return utils_collect.choose_best_data_source(local_data=ohlc_old_local, cloud_data=ohlc_old_cloud,folder_name=folder_name,file_name = file_name,service=service,drive=drive, verbose=verbose)



def collect_new_data(pair, n_candles, trading_type, target_datetime, ohlc_old, verbose):
    _print("Function: collect_new_data", 2, verbose)
    """Helper function to collect and process new data"""
    
    _print(f"Collecting new data for {pair} with {n_candles} candles", 1, verbose)
    
    ohlc = binance_api.most_recent_market_data(
        pair=pair, n_candles=n_candles,
        trading_type=trading_type, timeframe='1m'
    )
    ohlc = convert_to_float32(ohlc, verbose)
    ohlc.index = ohlc['date']
    if ohlc.empty:
        if not ohlc_old.empty:
            _print('Return the current available data', 0, verbose)
            return process_existing_data(ohlc_old, target_datetime, verbose)
        raise ValueError('No data available')

    _print(f"Collected new data for {pair}, processing...", 1, verbose)
    ohlc['date'] = ohlc['date'].apply(adjust_datetime)



    # Process and save new data
    df_ohlc = process_new_data(
        ohlc = ohlc, 
        ohlc_old = ohlc_old, 
        target_datetime = target_datetime, 
        verbose = verbose
    )

    _print(f"New data processed and saved for {pair}", 1, verbose)

    return df_ohlc


def process_existing_data(ohlc_old, target_datetime,verbose):
    _print("Function: process_existing_data", 2, verbose)
    """Process and filter existing data."""
    df_ohlc = ohlc_old.copy(deep=True)
    df_ohlc = df_ohlc[df_ohlc.date <= target_datetime]
    df_ohlc = sort_date_index(df_ohlc)
    return df_ohlc

def process_new_data(ohlc, ohlc_old, target_datetime, verbose):
    _print("Function: process_new_data", 2, verbose)
    """Process and save new market data."""
    _print("Starting to process new market data", 2, verbose)
    ohlc['date_day'] = ohlc['date'].dt.date
    ohlc = ohlc.drop('date_day', axis=1)

    
    
    # Add diagnostic prints for incoming data types
    _print(f"New data (ohlc) index type: {type(ohlc.index)}, date column type: {type(ohlc['date'].iloc[0])}", 2, verbose)
    if not ohlc_old.empty:
        _print(f"Existing data (ohlc_old) index type: {type(ohlc_old.index)}, date column type: {type(ohlc_old['date'].iloc[0])}", 2, verbose)
    
    if not ohlc_old.empty:
        _print("Merging old and new data", 2, verbose)
        df_ohlc = _pd.concat([ohlc_old, ohlc], axis=0)
        df_ohlc = df_ohlc.drop_duplicates(subset=['date'], keep='first')
        _print(f"Combined data shape after deduplication: {df_ohlc.shape}", 2, verbose)
        _print(f"Combined data index type: {type(df_ohlc.index)}, date column type: {type(df_ohlc['date'].iloc[0])}", 2, verbose)
    else:
        _print("No existing data to merge, using new data only", 2, verbose)
        df_ohlc = ohlc.copy(deep=True)
    
    _print("Sorting data by date index", 2, verbose)
    df_ohlc = sort_date_index(df_ohlc)
    _print(f"Sorted data index type: {type(df_ohlc.index)}, date column type: {type(df_ohlc['date'].iloc[0])}", 2, verbose)
    
    _print(f"Filtering data up to target datetime: {target_datetime}", 2, verbose)
    df_ohlc = df_ohlc.loc[df_ohlc.date <= target_datetime]
    _print(f"Final data index type: {type(df_ohlc.index)}, date column type: {type(df_ohlc['date'].iloc[0])}", 2, verbose)

    # Calculate cumulative daily volume
    
    
    return df_ohlc

 



