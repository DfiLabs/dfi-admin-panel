import pandas as _pd
import os as _os  
from datetime import datetime, timedelta  



import dfilabs.collect.api.binance_api as binance_api 
import dfilabs.cloud.cloud as cl
import dfilabs.collect.utils.utils_collect as utils_collect 
import dfilabs.config.config as cf  
from dfilabs.feature_computer.utils.utils_convert import convert_to_float32
from dfilabs.utils.utils_date import sort_date_index,format_datetime
from dfilabs.utils.utils_os import get_root_directory  
from dfilabs.utils.log import _print, _print_error   


def get_funding_rates(pair: str, drive=False, only_available=False, verbose=0):
    _print("Function: get_funding_rates", 2, verbose)
    """
    Retrieves historical funding rate data for a given trading pair.
    """
    try:
        # Initialize service once if needed
        service = cl.authenticate_drive_api() if drive else None
        _print(f"Google Drive integration: {'enabled' if drive else 'disabled'}", 2, verbose)
        target_datetime = datetime.now().replace(hour=23, minute=59, second=0, microsecond=0)
        flag_delisted = False

        # Check for delisted coins early
        if (date_delisted := cf.coin_to_date_delisted.get(pair)):
            _print(f'The pair {pair} is delisted (date: {date_delisted["date"]})', 1, verbose)
            date_delisted_datetime = datetime.strptime(date_delisted["date"], '%Y-%m-%d')
            if date_delisted_datetime <= target_datetime:
                _print(f'The pair {pair} is delisted (date: {date_delisted["date"]})', 1, verbose)
                _print(f'Adjusting target datetime to delisting date', 2, verbose)
                flag_delisted = True
                target_datetime = format_datetime(date_delisted_datetime - timedelta(days=1))
                target_datetime = target_datetime.replace(hour=23,minute=59,second=0)
         
            
        # Calculate target datetime once
        
        
        _print(f"Target datetime is set to {target_datetime}", 1, verbose)


        # Initialize key variables upfront
        folder_name = 'BINANCE_funding'
        file_name = f'BINANCE_{pair}_funding.csv'
        root_directory = get_root_directory()
        root_directory = f'{root_directory}/storage/BINANCE'
        
        file_path = f'{root_directory}/{folder_name}/{file_name}'
        _print(f"File will be saved as: {file_path}", 2, verbose)

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
        
        # Handle only_available parameter
        if only_available:
            _print("only_available is True - returning existing data without collecting new data", 1, verbose)
            
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

        # Check if data is up to date
        if not funding_old.empty and funding_old.index[-1] >= target_datetime:
            _print("Data is already up to date.", 1, verbose)
            funding_old = funding_old[funding_old.index <= target_datetime]
            funding_old = funding_old[['funding']]
            funding_old.columns = ['close']
            # Clean microseconds before returning existing data
            funding_old.index = [x.replace(microsecond=0) for x in funding_old.index]
            return funding_old
       

        # Calculate data collection parameters
        start_date_datetime = (
            funding_old.index[-1] - timedelta(days=1)
            if not funding_old.empty
            else datetime.strptime('2018-01-01', '%Y-%m-%d')
        )
        _print(f"Collection start date set to: {start_date_datetime}", 2, verbose)

        # Collect new data
        _print("Starting collection of new data...", 1, verbose)
        funding = binance_api.most_recent_binance_funding_rate(
            ticker=pair,
            start_date=start_date_datetime.strftime('%Y-%m-%d'),
            end_date=(target_datetime + timedelta(days=2)).strftime('%Y-%m-%d')
        )
        
        if funding.empty:
            if not funding_old.empty:
                _print('Return the current available data', 0, verbose)
                funding_old_clean = funding_old[['funding']]
                funding_old_clean.columns = ['close']
                # Clean microseconds before returning existing data
                funding_old_clean.index = [x.replace(microsecond=0) for x in funding_old_clean.index]
                return funding_old_clean
            raise ValueError('No data available')

        # Process and merge data
        if not funding_old.empty:
            df_funding = _pd.concat([funding_old, funding], axis=0)
            df_funding = df_funding.drop_duplicates(subset=['date'], keep='first')
        else:
            df_funding = funding.copy(deep=True)

        df_funding = sort_date_index(df_funding)
        df_funding = df_funding[df_funding.date <= target_datetime]

        df_funding = convert_to_float32(df_funding, verbose)
        

        # Store the new data
        _print("Storing collected data...", 2, verbose)
        utils_collect.store_data(df_funding, root_directory, folder_name, file_name, service, drive, verbose)
        _print("Data storage complete", 2, verbose)

        _print(f"Successfully completed funding rate collection for {pair}", 1, verbose)
        df_funding = df_funding[['funding']]
        df_funding.columns = ['close']
        # Clean microseconds before returning final data
        df_funding.index = [x.replace(microsecond=0) for x in df_funding.index]
        return df_funding

    except Exception as e:
        _print_error(e)
        return None


def get_existing_data(pair: str, file_path: str, service, folder_name: str, file_name: str, drive: bool, verbose: int, keep_columns: list) -> _pd.DataFrame:
    """
    Retrieves existing funding rate data from local and cloud storage, returning the most up-to-date version.
    """
    _print("Function: get_existing_data", 2, verbose)
    ohlc_old_local = None
    ohlc_old_cloud = None

    # Check local data first
    _print(f"Checking for local data at {file_path}", 2, verbose)
    if _os.path.exists(file_path):
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
    result = utils_collect.choose_best_data_source(local_data=ohlc_old_local, cloud_data=ohlc_old_cloud, folder_name=folder_name, file_name=file_name, service=service, drive=drive, verbose=verbose)
    if len(result) > 0 :
        result['date_datetime'] = [x for x in result.index]
        result['date_without_time'] = result['date_datetime'].dt.strftime('%Y-%m-%d')
        result['floor_hour'] = result.date_datetime.dt.floor('h').dt.hour
        result  = result.groupby(['floor_hour','date_without_time'], as_index=False).first()
        result.index = result['date_datetime']
        result = sort_date_index(result)
        result.index = [x.replace(minute=0, second=0, microsecond=0) for x in result.index]
        result = result.drop(['date_datetime','floor_hour','date_without_time'], axis=1)
    return result

