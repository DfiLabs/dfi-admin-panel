import pandas as _pd
import os as _os  
from datetime import datetime, timedelta 
import alive_progress  


import dfilabs.utils.utils as ut
import dfilabs.cloud.cloud as cl
import dfilabs.collect.utils.utils_collect as utils_collect 
import dfilabs.collect.api.glassnode_api as glassnode_api
from dfilabs.utils.log import _print, _print_error   


def get_glassnode(pair: str,section, freq = '1h', drive=False, target_indicators=[], verbose=0):
    _print("Function: get_glassnode", 2, verbose)
    """
    Retrieves historical Glassnode data for a given trading pair.
    """
    try:
        # Initialize service once if needed
        service = cl.authenticate_drive_api() if drive else None
        _print(f"Google Drive integration: {'enabled' if drive else 'disabled'}", 2, verbose)
        
        # Initialize key variables upfront
        folder_name = f'GLASSNODE_{section}'
        file_name = f'GLASSNODE_{pair}_{section}_{freq}.csv'
        root_directory = ut.get_root_directory()
        _os.makedirs(f'{root_directory}/{folder_name}', exist_ok=True)
        file_path = f'{root_directory}/{folder_name}/{file_name}'
        _print(f"File will be saved as: {file_path}", 2, verbose)

        # Get existing data (local or cloud)
        _print("Attempting to retrieve existing data...", 2, verbose)
        glassnode_old = get_existing_data(
            pair=pair,
            file_path=file_path,
            service=service,
            folder_name=folder_name,
            file_name=file_name,
            drive=drive,
            verbose=verbose,
            keep_columns=None 
        )
        print(f'Glassnode old: {glassnode_old.tail(5)}')
        map_indicators  = {indicator: [column for column in glassnode_old.columns if column.startswith(indicator)] for indicator in target_indicators}
        print(f'Map indicators: {map_indicators}')

        missing_indicator = [indicator for indicator, columns in map_indicators.items() if len(columns) == 0]
        print(f'Missing indicator: {missing_indicator}')
        if missing_indicator: 
            with alive_progress.alive_bar(len(missing_indicator), title='Collecting missing indicators') as bar:
                print('Start collecting missing indicators')
                indicators_to_collect = {}
                for indicator in missing_indicator:
                    print(f'Collect Indicator: {indicator}')
                    try: 
                        series = glassnode_api.get_glassnode_indicator(product = pair, indicator = indicator, section = section,freq = freq)
                        indicators_to_collect[(indicator,section)] = series
                    except Exception as e:
                        _print_error(e)
                    bar()
            glassnode_new = _pd.concat(list(indicators_to_collect.values()), axis=1)
            glassnode_new = ut.sort_date_index(glassnode_new)

            df_glassnode = _pd.concat([glassnode_old, glassnode_new], axis=1)
            df_glassnode = ut.sort_date_index(df_glassnode)
        else: 
            df_glassnode = glassnode_old
        df_glassnode['date'] = [x for x in df_glassnode.index]

        # Store the new data
        _print("Storing collected data...", 2, verbose)
        utils_collect.store_data(df_glassnode, root_directory, folder_name, file_name, service, drive, verbose)
        _print("Data storage complete", 2, verbose)

        return df_glassnode.drop(columns=['date'],axis=1)

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
    
    return result