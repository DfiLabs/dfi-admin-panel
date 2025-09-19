import pandas as _pd
import dfilabs.cloud.cloud as cl
from dfilabs.utils.log import _print
from dfilabs.feature_computer.utils.utils_convert import convert_to_float32
from dfilabs.utils.utils_os import get_root_directory

def load_local_data(file_path, keep_columns=None, verbose=0):
    _print("Function: load_local_data", 2, verbose)
    """Load data from a local CSV file."""
    try:
        # Read the CSV without forcing dtypes
        ohlc_old_local = _pd.read_csv(file_path, engine='pyarrow')
        
        keep_columns = list(ohlc_old_local.columns) if keep_columns is None else keep_columns
        ohlc_old_local = ohlc_old_local[keep_columns]
        
        # Convert each numeric column (integers and floats) to float32
        ohlc_old_local = convert_to_float32(ohlc_old_local, verbose)
        
        ohlc_old_local.index = ohlc_old_local['date']
        
        _print(f"Local data loaded from {file_path}", 2, verbose)
        return ohlc_old_local
    except Exception as e:
        _print(f"Error loading local data: {str(e)}", 1, verbose)
        return None

def load_cloud_data(pair, folder_name, file_name, service, keep_columns=None, verbose=0):
    _print("Function: load_cloud_data", 2, verbose)
    """Load data from Google Drive."""
    try:
        folder_id = cl.get_folder_id_by_name(service, folder_name)
        if not cl.check_file_in_folder(service, folder_id, file_name):
            return None
            
        file_id = cl.get_file_id_by_name_in_folder(service, folder_id, file_name)
        ohlc_old_cloud = cl.read_csv_file_from_drive(service, file_id)
        
        keep_columns = list(ohlc_old_cloud.columns) if keep_columns is None else keep_columns
        ohlc_old_cloud = ohlc_old_cloud[keep_columns]
        ohlc_old_cloud.index = ohlc_old_cloud['date']
        ohlc_old_cloud = convert_to_float32(ohlc_old_cloud, verbose)
        
        _print(f"Cloud data loaded for {pair}", 2, verbose)
        return ohlc_old_cloud
    except Exception as e:
        _print(f"Error loading cloud data: {str(e)}", 1, verbose)
        return None

def choose_best_data_source(local_data, cloud_data, folder_name, file_name, service, drive, verbose):
    """Choose the most up-to-date data source"""
    if local_data is not None and cloud_data is not None:
        if cloud_data.index[-1] > local_data.index[-1]:
            _print("Using cloud data as it is more up-to-date.", 1, verbose)
            store_data(df_ohlc=cloud_data,
                      root_directory=get_root_directory(),
                      folder_name=folder_name,
                      file_name=file_name,
                      service=service,
                      drive=False,
                      verbose=verbose)
            return cloud_data
        else:
            _print("Using local data as it is more up-to-date.", 1, verbose)
            return local_data
    return local_data if local_data is not None else cloud_data if cloud_data is not None else _pd.DataFrame()

def store_data(df_ohlc, root_directory, folder_name, file_name, service, drive, verbose):
    """Store data locally and in cloud if enabled"""
    # Save locally
    local_folder = f'{root_directory}/{folder_name}'
    df_ohlc_save = df_ohlc.reset_index(drop=True) 
    
    df_ohlc_save.to_csv(f'{local_folder}/{file_name}', index=False)
    
    _print(f"Data saved locally to {local_folder}/{file_name}", 2, verbose)
    # Save to cloud if enabled
    
    if drive and service:
        folder_id = cl.get_folder_id_by_name(service, folder_name)
        if cl.check_file_in_folder(service, folder_id, file_name):
            file_id = cl.get_file_id_by_name_in_folder(service, folder_id, file_name)
            cl.delete_file(service, file_id)
            cl.empty_trash(service)
        cl.save_dataframe_to_drive(service, df_ohlc_save, folder_id, file_name)
        _print(f"Data saved to Google Drive in folder '{folder_name}'", 2, verbose) 