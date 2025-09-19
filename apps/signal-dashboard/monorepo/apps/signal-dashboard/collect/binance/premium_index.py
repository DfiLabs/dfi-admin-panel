import pandas as _pd
from datetime import datetime, timedelta 

import dfilabs.utils.utils as ut
import dfilabs.collect.api.binance_api as binance_api 
import dfilabs.cloud.cloud as cl 
import dfilabs.config.config as cf 
import dfilabs.collect.utils.utils_collect as utils_collect   
from dfilabs.utils.log import _print, _print_error   

def get_premium_index(pair: str, drive=False, n_candles_add=0, verbose=0):
    _print("Function: get_premium_index", 2, verbose)
    """
    Retrieves historical premium index data for a given trading pair.
    """
    try:
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
                target_datetime = ut.format_datetime(date_delisted_datetime)

        # Initialize key variables upfront
        folder_name = 'BINANCE_premium_index'
        file_name = f'BINANCE_{pair}_premium_index.csv'
        root_directory = ut.get_root_directory()
        root_directory = f'{root_directory}/storage/BINANCE'
        file_path = f'{root_directory}/{folder_name}/{file_name}'
        _print(f"File will be saved as: {file_path}", 2, verbose)

        # Get existing data (local or cloud)
        _print("Attempting to retrieve existing data...", 2, verbose)
        ohlc_old = get_existing_data(
            pair=pair,
            file_path=file_path,
            service=service,
            folder_name=folder_name,
            file_name=file_name,
            drive=drive,
            verbose=verbose
        )
        _print(f"Retrieved {'non-empty' if not ohlc_old.empty else 'empty'} existing dataset", 2, verbose)

        # Check if data is up to date
        if not ohlc_old.empty and ohlc_old.index[-1] >= target_datetime:
            _print("Data is already up to date.", 1, verbose)
            if flag_delisted : 
                target_datetime = (target_datetime-timedelta(days=1)).replace(hour=23,minute=59,second=0)
                ohlc_old = ohlc_old[ohlc_old.index <= target_datetime]
            return ohlc_old.drop('date', axis=1)
        if flag_delisted and ut.format_datetime(ohlc_old.index[-1]) == target_datetime:
            _print("Data is up to date but delisted", 1, verbose)
            _print("Returning existing data without date column", 2, verbose)
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
        n_candles = utils_collect.calculate_required_candles(
            start_date_datetime, target_datetime, n_candles_add, verbose
        )
        _print(f"Need to collect {n_candles} candles", 1, verbose)

        if n_candles <= 0:
            _print("No new data to collect.", 1, verbose)
            return ohlc_old.drop('date', axis=1) if not ohlc_old.empty else _pd.DataFrame()

        # Collect new data
        _print("Starting collection of new data...", 1, verbose)
        new_data = collect_new_data(
            pair=pair,
            n_candles=n_candles,
            target_datetime=target_datetime,
            ohlc_old=ohlc_old,
            verbose=verbose
        )
        _print(f"Collected {'non-empty' if not new_data.empty else 'empty'} new dataset", 2, verbose)

        # Store the new data
        _print("Storing collected data...", 2, verbose)
        new_data['8_hour_funding'] = new_data['close'].rolling(60*8).apply(lambda x : sum([i*premium_index for i,premium_index in enumerate(x)])/sum([i for i in range(len(x))]))
        new_data['4_hour_funding'] = new_data['close'].rolling(60*4).apply(lambda x : sum([i*premium_index for i,premium_index in enumerate(x)])/sum([i for i in range(len(x))]))
        new_data['2_hour_funding'] = new_data['close'].rolling(60*2).apply(lambda x : sum([i*premium_index for i,premium_index in enumerate(x)])/sum([i for i in range(len(x))]))
        new_data['1_hour_funding'] = new_data['close'].rolling(60*1).apply(lambda x : sum([i*premium_index for i,premium_index in enumerate(x)])/sum([i for i in range(len(x))]))
        utils_collect.store_data(new_data, root_directory, folder_name, file_name, service, drive, verbose)
        _print("Data storage complete", 2, verbose)

        _print(f"Successfully completed premium index collection for {pair}", 1, verbose)
        return new_data.drop('date', axis=1)

    except ValueError as ve:
        if str(ve) == 'No data available':
            _print_error(f"No data available for {pair}. Neither new nor existing data could be found.")
            return _pd.DataFrame()  # Return empty DataFrame
        else:
            # Re-raise other ValueErrors or let them be caught by the generic Exception handler
            _print_error(f"An unexpected ValueError occurred: {ve}")
            return None # Or re-raise ve
    except Exception as e:
        _print_error(e)
        return None



def get_existing_data(pair, file_path, service, folder_name, file_name, drive, verbose):
    """Helper function to get existing data from local or cloud storage"""
    keep_columns = ['date', 'open', 'high', 'low', 'close']
    
    # Check local data first
    ohlc_old_local = utils_collect.load_local_data(file_path, keep_columns, verbose)
    
    # Check cloud data if enabled
    """if drive and service:
        ohlc_old_cloud = utils_collect.load_cloud_data(
            pair=pair,
            folder_name=folder_name,
            file_name=file_name,
            service=service,
            keep_columns=keep_columns,
            verbose=verbose
        )
        
        return utils_collect.choose_best_data_source(ohlc_old_local, ohlc_old_cloud, verbose)
    """
    return ohlc_old_local if ohlc_old_local is not None else _pd.DataFrame()



def collect_new_data(pair, n_candles, target_datetime, ohlc_old, verbose):
    """Helper function to collect and process new data"""
    ohlc = binance_api.most_recent_premium_index(pair=pair, n_candles=n_candles, timeframe='1m')
    ohlc.index = ohlc['date']
    
    if ohlc.empty:
        if not ohlc_old.empty:
            _print('Return the current available data', 0, verbose)
            return process_existing_data(ohlc_old, target_datetime, verbose)
        raise ValueError('No data available')

    if not ohlc_old.empty:
        df_ohlc = _pd.concat([ohlc_old, ohlc], axis=0)
        df_ohlc = df_ohlc.drop_duplicates(subset=['date'], keep='first')
    else:
        df_ohlc = ohlc

    df_ohlc = ut.sort_date_index(df_ohlc)
    df_ohlc = df_ohlc[df_ohlc.date <= target_datetime]
    
    
    return df_ohlc

def process_existing_data(ohlc_old, target_datetime, verbose):
    """Process and filter existing data."""
    df_ohlc = ohlc_old.copy(deep=True)
    df_ohlc = df_ohlc[df_ohlc.date <= target_datetime]
    df_ohlc = ut.sort_date_index(df_ohlc)
    return df_ohlc.drop('date', axis=1)










