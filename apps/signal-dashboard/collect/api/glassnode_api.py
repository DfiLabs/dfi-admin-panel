import pandas as _pd 
import numpy as _np 
from datetime import datetime, timedelta  
from urllib.parse import urlencode 
import requests 


import dfilabs.utils.utils as ut  
from dfilabs.utils.log import _print, _print_error   




def get_glassnode_indicator(product,indicator,section,freq):
    """
    Fetches indicator data from the Glassnode API.

    Parameters:
    - indicator (str): The Glassnode indicator name (e.g., 'sopr').
    - asset (str): The asset symbol (default: "BTC").

    Returns:
    - DataFrame: A pandas DataFrame containing the indicator data.
    """
    import json
    import os

    # Build a path to the credentials file based on this file's location.
    root_directory = ut.get_root_directory()
    credentials_path = os.path.join(root_directory, "credentials", "glassnode.json")
    product = product.replace('USDT','')
    API_KEY = retrieve_api_key(credentials_path)
    

    # Construct the Glassnode API endpoint using the indicator from the argument.
    url = f"https://api.glassnode.com/v1/metrics/{section}/{indicator}"
    params = {'a': product, 'api_key': API_KEY,'i':freq}

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception if the request returned an error status
        response_json = response.json()
        response_index = [response_["t"] for response_ in response_json]
        response_return = [response_["o"] for response_ in response_json]


        df = _pd.DataFrame(response_return,index=response_index)
        df.index = _pd.to_datetime(df.index,unit='s')

   

        df.columns = [f'{indicator}_{column}' for column in df.columns]
        
        return df
    except Exception as err:
        _print_error(f"Error fetching data from Glassnode API: {err}")
        return None
    
def retrieve_api_key(credentials_path):
    """
    Retrieve the API key from a JSON credentials file.

    Parameters:
    - credentials_path (str): Path to the JSON credentials file.

    Returns:
    - str or None: The API key if found, otherwise None.
    """
    import json

    try:
        with open(credentials_path, "r") as f:
            data = json.load(f)
        api_key = data.get("api_key")
        if not api_key:
            _print_error("API key not found in the credentials file: " + credentials_path)
            return None
        return api_key
    except Exception as err:
        _print_error(f"Error reading API key from credentials file {credentials_path}: {err}")
        return None