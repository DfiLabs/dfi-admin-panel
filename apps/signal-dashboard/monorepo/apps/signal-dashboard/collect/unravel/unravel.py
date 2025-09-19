import pandas as pd
import numpy as np
import os
import json
from datetime import datetime, timedelta

from dfilabs.collect.api.unravel import get_portfolio_factors_historical
import dfilabs.cloud.cloud as cl
import dfilabs.collect.utils.utils_collect as utils_collect
from dfilabs.feature_computer.utils.utils_convert import convert_to_float32
from dfilabs.utils.utils_date import format_datetime, adjust_datetime, sort_date_index
from dfilabs.utils.utils_os import get_root_directory
from dfilabs.utils.log import _print, _print_error


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


def get_factors(portfolio: str, ticker: str, drive=False, only_available=False, verbose=0):
    """
    Retrieves historical factor data for a given portfolio and ticker.
    
    Args:
        portfolio (str): Portfolio name (e.g., 'retail_flow')
        ticker (str): Ticker symbol with USDT suffix (e.g., 'BTCUSDT')
        drive (bool): Whether to use Google Drive for cloud storage
        verbose (int): Verbosity level for logging
    
    Returns:
        pd.DataFrame: Historical factor data with DatetimeIndex and 'close' column
    """
    _print("Function: get_factors", 2, verbose)
    
    try:
        _print(f"Starting factor collection for {ticker} in portfolio {portfolio}", 1, verbose)
        
        # Load API credentials
        api_key = load_unravel_credentials()
        if not api_key:
            raise ValueError("Failed to load Unravel API credentials")
        
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

        # Initialize service once if needed
        service = cl.authenticate_drive_api() if drive else None
        _print(f"Google Drive integration: {'enabled' if drive else 'disabled'}", 2, verbose)

        # Calculate target datetime once (end of yesterday)
        target_datetime = (datetime.now() - timedelta(days=1)).replace(hour=23, minute=59, second=0, microsecond=0)
        _print(f"Target datetime is set to {target_datetime}", 1, verbose)

        # Get existing data (local or cloud)
        _print("Attempting to retrieve existing data...", 2, verbose)
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
        
        # Handle only_available parameter
        if only_available:
            _print("only_available is True - returning existing data without collecting new data", 1, verbose)
            
            # Check if any data is available
            if factors_old.empty:
                raise ValueError(f"No factor data available for ticker {ticker} in portfolio {portfolio}")
            
            _print("Returning available factor data formatted as 'close' column", 2, verbose)
            # Safely remove date column if it exists
            if 'date' in factors_old.columns:
                factors_old = factors_old.drop('date', axis=1)
            return format_factor_data_as_close(factors_old, verbose)

        # Check if factors_old is up to date (compare by date only, ignore time)
        if not factors_old.empty:
            last_date = pd.to_datetime(factors_old.index[-1]).date()
            target_date = target_datetime.date()
            if last_date >= target_date:
                _print("Data is already up to date.", 1, verbose)
                _print("Returning existing data formatted as 'close' column", 2, verbose)
                # Convert to standard format with 'close' column
                # Safely remove date column if it exists
                if 'date' in factors_old.columns:
                    factors_old = factors_old.drop('date', axis=1)
                return format_factor_data_as_close(factors_old, verbose)

        # Collect new data
        _print("Starting collection of new factor data...", 1, verbose)
        new_data = collect_new_data(
            portfolio=portfolio,
            ticker=ticker,
            api_key=api_key,
            target_datetime=target_datetime,
            factors_old=factors_old,
            verbose=verbose
        )
        _print(f"Collected {'non-empty' if not new_data.empty else 'empty'} new dataset", 2, verbose)

        # Store the new data
        _print("Storing collected data...", 2, verbose)
        new_data = convert_to_float32(new_data, verbose)
        store_factor_data_locally(
            df_factors=new_data,
            root_directory=root_directory,
            folder_name=folder_name,
            file_name=file_name,
            verbose=verbose
        )
        
        # Store to cloud if enabled
        if drive and service:
            try:
                folder_id = cl.get_folder_id_by_name(service, folder_name)
                if cl.check_file_in_folder(service, folder_id, file_name):
                    file_id = cl.get_file_id_by_name_in_folder(service, folder_id, file_name)
                    cl.delete_file(service, file_id)
                    cl.empty_trash(service)
                cl.save_dataframe_to_drive(service, new_data, folder_id, file_name)
                _print(f"Data saved to Google Drive in folder '{folder_name}'", 2, verbose)
            except Exception as e:
                _print(f"Warning: Failed to save to Google Drive: {str(e)}", 1, verbose)
        
        _print("Data storage complete", 2, verbose)

        _print(f"Successfully completed factor collection for {ticker}", 1, verbose)
        # Convert to standard format with 'close' column
        # Safely remove date column if it exists
        if 'date' in new_data.columns:
            new_data = new_data.drop('date', axis=1)
        return format_factor_data_as_close(new_data, verbose)

    except Exception as e:
        _print_error(e)
        _print(f"Failed to collect factors for {ticker}", 1, verbose)
        return None


def get_existing_data(ticker, file_path, service, folder_name, file_name, drive, verbose=0):
    """Helper function to get existing data from local or cloud storage"""
    _print("Function: get_existing_data", 2, verbose)
    
    # Check local data first
    _print(f"Checking for local data at {file_path}", 2, verbose)
    factors_old_local = load_local_factor_data(file_path, verbose) if os.path.exists(file_path) else None
    
    # Check cloud data if enabled
    factors_old_cloud = None
    if drive and service:
        _print("Checking cloud storage for data", 2, verbose)
        factors_old_cloud = load_cloud_factor_data(ticker, folder_name, file_name, service, verbose)
    
    return choose_best_factor_data_source(
        local_data=factors_old_local,
        cloud_data=factors_old_cloud,
        folder_name=folder_name,
        file_name=file_name,
        service=service,
        drive=drive,
        verbose=verbose
    )


def load_local_factor_data(file_path, verbose=0):
    """Load factor data from a local CSV file."""
    _print("Function: load_local_factor_data", 2, verbose)
    try:
        # Read CSV with date as index column
        factors_data = pd.read_csv(file_path, engine='pyarrow', index_col='date')
        
        # Ensure the index is a proper DatetimeIndex
        factors_data.index = pd.to_datetime(factors_data.index)
        
        # Convert numeric columns to float32 for memory efficiency
        factors_data = convert_to_float32(factors_data, verbose)
        
        # Ensure we have a date column for compatibility with existing code
        factors_data['date'] = factors_data.index
        
        _print(f"Local factor data loaded from {file_path}", 2, verbose)
        return factors_data
    except Exception as e:
        _print(f"Error loading local factor data: {str(e)}", 1, verbose)
        return None


def load_cloud_factor_data(ticker, folder_name, file_name, service, verbose=0):
    """Load factor data from Google Drive."""
    _print("Function: load_cloud_factor_data", 2, verbose)
    try:
        folder_id = cl.get_folder_id_by_name(service, folder_name)
        if not cl.check_file_in_folder(service, folder_id, file_name):
            return None
            
        file_id = cl.get_file_id_by_name_in_folder(service, folder_id, file_name)
        factors_data = cl.read_csv_file_from_drive(service, file_id)
        
        # Ensure index is datetime
        if not isinstance(factors_data.index, pd.DatetimeIndex):
            factors_data.index = pd.to_datetime(factors_data.index)
        
        factors_data = convert_to_float32(factors_data, verbose)
        
        # Ensure we have a date column for compatibility
        if 'date' not in factors_data.columns:
            factors_data['date'] = pd.to_datetime(factors_data.index)
        
        _print(f"Cloud factor data loaded for {ticker}", 2, verbose)
        return factors_data
    except Exception as e:
        _print(f"Error loading cloud factor data: {str(e)}", 1, verbose)
        return None


def choose_best_factor_data_source(local_data, cloud_data, folder_name, file_name, service, drive, verbose):
    """Choose the most up-to-date factor data source"""
    if local_data is not None and cloud_data is not None:
        # Check if both DataFrames are non-empty before comparing
        if not local_data.empty and not cloud_data.empty:
            if cloud_data.index[-1] > local_data.index[-1]:
                _print("Using cloud data as it is more up-to-date.", 1, verbose)
                # Store cloud data locally
                root_directory = get_root_directory()
                store_factor_data_locally(
                    df_factors=cloud_data,
                    root_directory=f'{root_directory}/storage/UNRAVEL',
                    folder_name=folder_name,
                    file_name=file_name,
                    verbose=verbose
                )
                return cloud_data
            else:
                _print("Using local data as it is up-to-date.", 1, verbose)
                return local_data
        elif not cloud_data.empty:
            _print("Using cloud data as local data is empty.", 1, verbose)
            # Store cloud data locally
            root_directory = get_root_directory()
            store_factor_data_locally(
                df_factors=cloud_data,
                root_directory=f'{root_directory}/storage/UNRAVEL',
                folder_name=folder_name,
                file_name=file_name,
                verbose=verbose
            )
            return cloud_data
        elif not local_data.empty:
            _print("Using local data as cloud data is empty.", 1, verbose)
            return local_data
        else:
            _print("Both local and cloud data are empty.", 1, verbose)
            return pd.DataFrame()
    return local_data if local_data is not None else cloud_data if cloud_data is not None else pd.DataFrame()


def store_factor_data_locally(df_factors, root_directory, folder_name, file_name, verbose):
    """Store factor data locally"""
    local_folder = f'{root_directory}/{folder_name}'
    os.makedirs(local_folder, exist_ok=True)
    
    # Prepare data for saving - ensure datetime index and proper format
    df_factors_save = df_factors.copy()
    
    # Make sure the index is datetime
    if not isinstance(df_factors_save.index, pd.DatetimeIndex):
        df_factors_save.index = pd.to_datetime(df_factors_save.index)
    
    # Remove the 'date' column if it exists to avoid duplication
    if 'date' in df_factors_save.columns:
        df_factors_save = df_factors_save.drop('date', axis=1)
    
    # Save with proper datetime index
    df_factors_save.to_csv(f'{local_folder}/{file_name}', index_label='date')
    
    _print(f"Factor data saved locally to {local_folder}/{file_name}", 2, verbose)


def collect_new_data(portfolio, ticker, api_key, target_datetime, factors_old, verbose):
    """Helper function to collect and process new factor data"""
    _print("Function: collect_new_data", 2, verbose)
    
    _print(f"Collecting new factor data for {ticker} in portfolio {portfolio}", 1, verbose)
    
    # Prepare ticker list for API call - remove USDT suffix
    api_ticker = ticker.replace('USDT', '') if ticker.endswith('USDT') else ticker
    tickers = [api_ticker]
    
    try:
        # Get factor data from Unravel API
        factor_data = get_portfolio_factors_historical(
            portfolioId=portfolio,
            tickers=tickers,
            API_KEY=api_key
        )
        
        if factor_data.empty:
            if not factors_old.empty:
                _print('No new data available, returning existing data', 1, verbose)
                return process_existing_factor_data(factors_old, target_datetime, verbose)
            raise ValueError('No factor data available')

        _print(f"Collected new factor data for {ticker}, processing...", 1, verbose)
        
        # Validate that we have daily data (only one point per day)
        dates_only = factor_data.index.date
        unique_dates = len(set(dates_only))
        total_points = len(factor_data)
        
        if total_points != unique_dates:
            raise ValueError(f"API returned {total_points} data points but only {unique_dates} unique dates. Expected one point per day.")
        
        _print(f"Validated daily data: {total_points} points for {unique_dates} unique dates", 2, verbose)
        
        # Convert index to daily data collected at 22:55:00
        factor_data.index = factor_data.index.normalize() + pd.Timedelta(hours=23, minutes=55)
        _print("Adjusted timestamps to 22:55:00 for daily data collection time", 2, verbose)
        
        # Add date column for compatibility
        factor_data['date'] = factor_data.index
        
        # Process and save new data
        df_factors = process_new_factor_data(
            factor_data=factor_data,
            factors_old=factors_old,
            target_datetime=target_datetime,
            verbose=verbose
        )

        _print(f"New factor data processed for {ticker}", 1, verbose)
        return df_factors

    except Exception as e:
        _print_error(f"Error collecting factor data: {str(e)}")
        if not factors_old.empty:
            _print('Returning existing data due to collection error', 1, verbose)
            return process_existing_factor_data(factors_old, target_datetime, verbose)
        raise


def process_existing_factor_data(factors_old, target_datetime, verbose):
    """Process and filter existing factor data."""
    _print("Function: process_existing_factor_data", 2, verbose)
    df_factors = factors_old.copy(deep=True)
    # Filter by date only (ignore time for daily data)
    target_date = target_datetime.date()
    df_factors = df_factors[df_factors.index.date <= target_date]
    df_factors = sort_date_index(df_factors)
    return df_factors


def process_new_factor_data(factor_data, factors_old, target_datetime, verbose):
    """Process and save new factor data."""
    _print("Function: process_new_factor_data", 2, verbose)
    _print("Starting to process new factor data", 2, verbose)
    
    # Add diagnostic prints for incoming data types
    _print(f"New factor data index type: {type(factor_data.index)}, shape: {factor_data.shape}", 2, verbose)
    if not factors_old.empty:
        _print(f"Existing factor data index type: {type(factors_old.index)}, shape: {factors_old.shape}", 2, verbose)
    
    if not factors_old.empty:
        _print("Merging old and new factor data", 2, verbose)
        df_factors = pd.concat([factors_old, factor_data], axis=0)
        df_factors = df_factors.drop_duplicates(subset=['date'], keep='last')
        _print(f"Combined factor data shape after deduplication: {df_factors.shape}", 2, verbose)
    else:
        _print("No existing factor data to merge, using new data only", 2, verbose)
        df_factors = factor_data.copy(deep=True)
    
    _print("Sorting factor data by date index", 2, verbose)
    df_factors = sort_date_index(df_factors)
    
    _print(f"Filtering factor data up to target date: {target_datetime.date()}", 2, verbose)
    # Filter by date only (ignore time for daily data)
    target_date = target_datetime.date()
    df_factors = df_factors.loc[df_factors.index.date <= target_date]
    
    return df_factors


def format_factor_data_as_close(df_factors, verbose=0):
    """
    Format factor data to have a single 'close' column with DatetimeIndex.
    
    Args:
        df_factors (pd.DataFrame): Factor data with datetime index
        verbose (int): Verbosity level for logging
    
    Returns:
        pd.DataFrame: Formatted DataFrame with DatetimeIndex and 'close' column
    """
    _print("Function: format_factor_data_as_close", 2, verbose)
    
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


# Legacy function for backward compatibility
def get_factor(portfolio, ticker, API_KEY):
    """Legacy function - use get_factors instead"""
    _print("Warning: get_factor is deprecated, use get_factors instead", 1, 1)
    return get_factors(portfolio, ticker) 