import pandas as _pd
import os as _os
from datetime import datetime, timedelta


import dfilabs.utils.utils as ut 
import dfilabs.cloud.cloud as cl 
import dfilabs.config.config as cf  
from dfilabs.utils.log import _print, _print_error   
import dfilabs.collect.utils.utils_collect as utils_collect 
import dfilabs.collect.binance.prices as prices 
from dfilabs.utils.utils_date import format_datetime
from dfilabs.collect.binance.funding_rates import get_funding_rates
from dfilabs.utils.utils_date import sort_date_index 


def get_basis(pair: str, drive=False, verbose=0):
    """
    Retrieves historical OHLCV data for a given trading pair and trading type.
    """
    _print(f"DEBUG: Starting get_basis for pair: {pair}, verbose: {verbose}", 1, verbose)
    
    # Initialize service only if drive is enabled
    service = cl.authenticate_drive_api() if drive else None
    _print(f"Google Drive integration: {'enabled' if drive else 'disabled'}", 2, verbose)

    try:
        # Initialize key variables
        folder_name = f'BINANCE_basis_prices'
        file_name = f'BINANCE_{pair}_basis.csv'
        root_directory = ut.get_root_directory()
        root_directory = f'{root_directory}/storage/BINANCE'
        file_path = f'{root_directory}/{folder_name}/{file_name}'

        # Calculate target datetime
        yesterday_datetime = datetime.now() - timedelta(days=1) 
        target_datetime = format_datetime(yesterday_datetime)
        flag_delisted = False
        
        # Debug: Show target datetime
        _print(f"DEBUG: Initial target_datetime: {target_datetime}", 1, verbose)
        
        # Check for delisted coins
        if (date_delisted := cf.coin_to_date_delisted.get(pair)):
            date_delisted_datetime = datetime.strptime(date_delisted["date"], '%Y-%m-%d')
            _print(f"DEBUG: Delisted date: {date_delisted_datetime}, Target: {target_datetime}", 1, verbose)
            if date_delisted_datetime <= target_datetime:
                _print(f'The pair {pair} is delisted (date: {date_delisted["date"]})', 1, verbose)
                _print(f'Adjusting target datetime to delisting date', 2, verbose)
                flag_delisted = True
                target_datetime = format_datetime(date_delisted_datetime)
                _print(f"DEBUG: Adjusted target_datetime: {target_datetime}", 1, verbose)
        
        _print(f"DEBUG: Final flag_delisted: {flag_delisted}", 1, verbose)

        # Get existing data using utility functions
        keep_columns = ['date', 'close_spot', 'close_future', 'volume_spot', 'volume_future']
        
        # Debug: Check file existence
        _print(f"DEBUG: File exists: {_os.path.exists(file_path)}", 1, verbose)
        _print(f"DEBUG: File path: {file_path}", 1, verbose)
        
        ohlc_old_local = utils_collect.load_local_data(file_path, keep_columns, verbose) if _os.path.exists(file_path) else None
        
        # Debug: Check local data
        if ohlc_old_local is not None:
            _print(f"DEBUG: Local data loaded - Shape: {ohlc_old_local.shape}", 1, verbose)
            _print(f"DEBUG: Local data index type: {type(ohlc_old_local.index[0]) if len(ohlc_old_local) > 0 else 'Empty'}", 1, verbose)
            _print(f"DEBUG: Local data columns: {list(ohlc_old_local.columns)}", 1, verbose)
            _print(f"DEBUG: Local data last date: {ohlc_old_local.index[-1] if len(ohlc_old_local) > 0 else 'Empty'}", 1, verbose)
        else:
            _print(f"DEBUG: Local data is None", 1, verbose)
        
        ohlc_old_cloud = utils_collect.load_cloud_data(pair, folder_name, file_name, service, keep_columns, verbose) if drive and service else None
        
        # Debug: Check cloud data
        if ohlc_old_cloud is not None:
            _print(f"DEBUG: Cloud data loaded - Shape: {ohlc_old_cloud.shape}", 1, verbose)
        else:
            _print(f"DEBUG: Cloud data is None", 1, verbose)
        
        ohlc_old = utils_collect.choose_best_data_source(local_data = ohlc_old_local, 
                                                         cloud_data = ohlc_old_cloud, 
                                                         folder_name = folder_name, 
                                                         file_name = file_name,
                                                         service = service,
                                                         drive = drive,
                                                         verbose = verbose)
        
        # Debug: Check final chosen data
        if ohlc_old is not None and not ohlc_old.empty:
            _print(f"DEBUG: Final data - Shape: {ohlc_old.shape}", 1, verbose)
            _print(f"DEBUG: Final data index type: {type(ohlc_old.index[0]) if len(ohlc_old) > 0 else 'Empty'}", 1, verbose)
            _print(f"DEBUG: Final data last date: {ohlc_old.index[-1] if len(ohlc_old) > 0 else 'Empty'}", 1, verbose)
        else:
            _print(f"DEBUG: Final data is None or empty", 1, verbose)

        # Check if data is up to date (inspired by get_prices logic)
        if not ohlc_old.empty:
            end_date_datetime = format_datetime(ohlc_old.index[-1])
            _print(f"DEBUG: end_date_datetime: {end_date_datetime}", 1, verbose)
            _print(f"DEBUG: target_datetime: {target_datetime}", 1, verbose)
            _print(f"DEBUG: end_date == target: {end_date_datetime == target_datetime}", 1, verbose)
            _print(f"DEBUG: end_date >= target: {end_date_datetime >= target_datetime}", 1, verbose)
            
            # For non-delisted coins, check exact equality
            if not flag_delisted and end_date_datetime == target_datetime:
                _print("Data is already up to date.", 1, verbose)
                _print("Returning existing data without date column", 2, verbose)
                return ohlc_old.drop('date', axis=1)
            
            # For delisted coins, check if data ends on the day before delisting (which means complete)
            if flag_delisted:
                # Data is complete if it ends on the day before the delisting date
                day_before_delisting = target_datetime - timedelta(days=1)
                _print(f"DEBUG: Day before delisting: {day_before_delisting}", 1, verbose)
                _print(f"DEBUG: Data ends on day before delisting: {format_datetime(ohlc_old.index[-1]) == day_before_delisting}", 1, verbose)
                
                if format_datetime(ohlc_old.index[-1]) >= day_before_delisting:
                    _print("Data is up to date but delisted", 1, verbose)
                    _print("Returning existing data without date column", 2, verbose)
                    _print(f"DEBUG: Delisted condition - last index: {format_datetime(ohlc_old.index[-1])}, day before delisting: {day_before_delisting}", 1, verbose)
                    
                    # Filter data to end at day before delisting at 23:59
                    filter_target = day_before_delisting.replace(hour=23, minute=59, second=0)
                    _print(f"DEBUG: Filtering to: {filter_target}", 1, verbose)
                    _print(f"DEBUG: Data shape before filtering: {ohlc_old.shape}", 1, verbose)
                    ohlc_old = ohlc_old[ohlc_old.index <= filter_target]
                    _print(f"DEBUG: Data shape after filtering: {ohlc_old.shape}", 1, verbose)
                    
                    return ohlc_old.drop('date', axis=1)

            _print(f"Data for {pair} basis is available until {end_date_datetime}. Collecting further data.", 1, verbose)
            
            # Collect updated data from where we left off
            _print("DEBUG: Collecting updated data for existing file", 1, verbose)
            df_spot = prices.get_prices(pair,'spot',drive=drive, verbose=verbose)
            _print(f"DEBUG: Spot data shape: {df_spot.shape if df_spot is not None else 'None'}", 1, verbose)
            df_future = prices.get_prices(pair,'future',drive=drive, verbose=verbose)  
            _print(f"DEBUG: Future data shape: {df_future.shape if df_future is not None else 'None'}", 1, verbose)
            
            if df_spot is not None and df_future is not None:
                df = _pd.concat([df_spot['close'],df_spot['volume'],df_future['close'],df_future['volume']],axis=1)
                df.columns = ['close_spot','volume_spot','close_future','volume_future']
                df['date'] = [x for x in df.index]
                df = sort_date_index(df)
                _print(f"DEBUG: Final updated data shape: {df.shape}", 1, verbose)
                _print(f"DEBUG: Final updated data index range: {df.index[0]} to {df.index[-1] if len(df) > 0 else 'Empty'}", 1, verbose)
                utils_collect.store_data(df_ohlc = df,
                                         root_directory = root_directory,
                                         folder_name = folder_name,
                                         file_name = file_name,
                                         service = service,
                                         drive = drive,
                                         verbose = verbose)
                _print("DEBUG: Updated data stored successfully, returning result", 1, verbose)
                return df.drop('date', axis=1)
            else:
                _print("DEBUG: Failed to collect spot or future data, returning existing data", 1, verbose)
                return ohlc_old.drop('date', axis=1)
            
        else:
            _print("DEBUG: No existing data found, collecting fresh data", 1, verbose)
            df_spot = prices.get_prices(pair,'spot',drive=drive, verbose=verbose)
            _print(f"DEBUG: Spot data shape: {df_spot.shape if df_spot is not None else 'None'}", 1, verbose)
            df_future = prices.get_prices(pair,'future',drive=drive, verbose=verbose)
            _print(f"DEBUG: Future data shape: {df_future.shape if df_future is not None else 'None'}", 1, verbose)
            df = _pd.concat([df_spot['close'],df_spot['volume'],df_future['close'],df_future['volume']],axis=1)
            df.columns = ['close_spot','volume_spot','close_future','volume_future']
            df['date'] = [x for x in df.index]
            df = sort_date_index(df)
            _print(f"DEBUG: Final combined data shape: {df.shape}", 1, verbose)
            _print(f"DEBUG: Final combined data index range: {df.index[0]} to {df.index[-1] if len(df) > 0 else 'Empty'}", 1, verbose)
            utils_collect.store_data(df_ohlc = df,
                                     root_directory = root_directory,
                                     folder_name = folder_name,
                                     file_name = file_name,
                                     service = service,
                                     drive = drive,
                                     verbose = verbose)
            _print("DEBUG: Data stored successfully, returning result", 1, verbose)
            return df.drop('date', axis=1)

            
            

        

    except Exception as e:
        _print_error(e)
        return None

 

 





 

