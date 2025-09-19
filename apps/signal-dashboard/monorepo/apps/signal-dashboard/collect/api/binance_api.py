import pandas as _pd 
import numpy as _np 
from datetime import datetime, timedelta  
from urllib.parse import urlencode 
import requests 


  
from dfilabs.utils.log import _print, _print_error
from dfilabs.utils.utils_date import adjust_datetime
from dfilabs.feature_computer.utils.utils_convert import convert_to_float32


def get_futures_symbols(against):
    """
    Fetches the names of cryptocurrencies that are quoted in USDT futures from Binance API.
    
    Returns:
    - list: A list of symbols for futures trading with USDT as the quote currency.
    """
    # Binance API endpoint to get futures symbols
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    
    # Make the API request
    response = requests.get(url)
    
    # Check if the request was successful
    if response.status_code != 200:
        raise Exception(f"Error fetching data from Binance API: {response.status_code}")
    
    # Parse the response JSON
    data = response.json()
    
    # Initialize an empty list to store the USDT futures symbols
    usdt_futures_symbols = []
    
    # Loop through the symbols in the exchangeInfo response
    for symbol_info in data['symbols']:
        # Check if the quote asset is USDT
        if symbol_info['quoteAsset'] == against:
            # Add the base asset (crypto name) to the list
            usdt_futures_symbols.append(symbol_info['symbol'])
    
    return usdt_futures_symbols 





 

 

def most_recent_binance_funding_rate(ticker,start_date,end_date):
    # Convert dates to timestamps in milliseconds
    start_timestamp = int(_pd.to_datetime(start_date).timestamp() * 1000)
    end_timestamp = int(_pd.to_datetime(end_date).timestamp() * 1000)
    
    funding_rates = []
    url = 'https://fapi.binance.com/fapi/v1/fundingRate'
    
    # Binance API allows a maximum limit of 1000 items per request
    limit = 1000
    current_timestamp = start_timestamp
    
    while current_timestamp < end_timestamp:
        try : 
            params = {
                'symbol': ticker,
                'startTime': current_timestamp,
                'endTime': end_timestamp,
                'limit': limit
            }
            
            response = requests.get(url, params=params)
            data = response.json()
            
            if not data:
                break  
            
            funding_rates.extend(data)
            current_timestamp = data[-1]['fundingTime'] + 1  # Move to the next timestamp
            
            # If the data length is less than the limit, it means we've fetched all available data
            if len(data) < limit:
                break
        except Exception as e:
            _print_error(e)
            break
    
    # Convert to DataFrame
    df = _pd.DataFrame(funding_rates)   
    
    # Convert fundingTime to datetime
    df['date'] = _pd.to_datetime(df['fundingTime'], unit='ms')
    df['funding'] = _pd.to_numeric(df['fundingRate'])

    df.drop(['fundingTime','fundingRate'],axis=1,inplace=True)
    
    # Ensure the DataFrame is sorted by fundingTime
    df.sort_values('date', inplace=True)
    
    
    # Set the fundingTime as the index
    df = df.reset_index(drop=True) 
    df = df.drop(['symbol','markPrice'],axis=1) 
    
    df = df.dropna()
    df = df[['date','funding']]
    df.columns = ['date','close']
    df['minute'] = df.date.dt.strftime('%M')
    df = df[df.minute == '00']
    df.drop('minute',axis=1,inplace=True)
    df.index = df['date']
    # Clean microseconds from API data index
    df.index = [x.replace(microsecond=0) for x in df.index]

    df = convert_to_float32(df)
    
    
    # Resample to minute frequency, interpolate or fill missing data as needed 
    # df = df.resample('T').asfreq().interpolate(method='time')
    
    return df 
 
def most_recent_open_interest(pair: str, timeframe: str, n_candles: int, verbose=0) -> _pd.DataFrame:
    """
    Load the n_candles most recent OHLC candlesticks for the given pair and timeframe.

    Args:
        pair (str): The trading pair for which to retrieve OHLCV data.
        timeframe (str): The timeframe for the OHLCV data (e.g., '1m', '1h').
        n_candles (int): The number of most recent candles to retrieve.
        trading_type (str, optional): The type of trading ('future' or 'spot'). Defaults to "future".

    Returns:
        pandas.DataFrame: The most recent OHLCV data for the specified trading pair and timeframe.
    """
    # Initialize variables
    ohlcs = []  # List to store the retrieved OHLCV data

    max_limit = 500

    n_candles_needed = n_candles

    # Initialize variables for the loop
    current_end_time = None

    while n_candles_needed > 0:
        limit = min(max_limit, n_candles_needed)
        _print(f'Limit {limit}',2,verbose)
        # Prepare parameters for klines function
        if current_end_time is None:
            new_ohlc = klines_open_interest(pair=pair, timeframe=timeframe, limit=limit,verbose=verbose)
        else:
            # Subtract 1 millisecond to avoid duplicate data
            new_ohlc = klines_open_interest(pair=pair, timeframe=timeframe, limit=limit, end_time=current_end_time,verbose=verbose)

        if len(new_ohlc) == 0:
            break  # No more data

        # Convert the retrieved data to a DataFrame
        new_ohlc_df = _pd.DataFrame(
            new_ohlc,
            columns=['symbol','sumOpenInterest','sumOpenInterestValue','timestamp'])

        ohlcs.append(new_ohlc_df)

        n_candles_needed -= len(new_ohlc_df)
        _print(f'N candles needed {n_candles_needed}',2,verbose)
        # Update current_end_time to earliest open_time in new_ohlc_df
        current_end_time = int(new_ohlc_df['timestamp'].min())
        _print(f'Size ohlc {len(new_ohlc_df)}',2,verbose)

        # If we've retrieved fewer candles than requested, all data has been fetched
        if len(new_ohlc_df) < limit:
            break

    if not ohlcs:
        return _pd.DataFrame()  # Return empty DataFrame if no data retrieved

    # Concatenate all dataframes into one
    ohlc = _pd.concat(ohlcs, axis=0)

    # Drop duplicates
    ohlc.drop_duplicates(subset='timestamp', inplace=True)
    ohlc.drop('symbol',axis=1,inplace=True)

    # Convert columns to appropriate data types
    cols_to_numeric = ['timestamp','sumOpenInterest','sumOpenInterestValue']
    ohlc[cols_to_numeric] = ohlc[cols_to_numeric].apply(_pd.to_numeric)

    # Convert times to datetime
    ohlc['timestamp'] = _pd.to_datetime(ohlc['timestamp'], unit='ms')


    # Keep necessary columns
    ohlc = ohlc[['timestamp','sumOpenInterest','sumOpenInterestValue']]

    # Sort by open_time
    ohlc.sort_values(by='timestamp', inplace=True)

    # Keep the most recent n_candles
    ohlc = ohlc.iloc[-n_candles:].copy()

    # Reset index
    ohlc.reset_index(drop=True, inplace=True)

    # Rename columns
    ohlc.columns = ['date', 'open_interest', 'open_interest_quote']

    # Adjust date if necessary
    ohlc['date'] = ohlc['date'].apply(lambda x: adjust_datetime(x))

    return ohlc 

def most_recent_market_data(pair: str, timeframe: str, n_candles: int, trading_type="future", verbose=0) -> _pd.DataFrame:
    """
    Load the n_candles most recent OHLC candlesticks for the given pair and timeframe.

    Args:
        pair (str): The trading pair for which to retrieve OHLCV data.
        timeframe (str): The timeframe for the OHLCV data (e.g., '1m', '1h').
        n_candles (int): The number of most recent candles to retrieve.
        trading_type (str, optional): The type of trading ('future' or 'spot'). Defaults to "future".

    Returns:
        pandas.DataFrame: The most recent OHLCV data for the specified trading pair and timeframe.
    """
    # Initialize variables
    ohlcs = []  # List to store the retrieved OHLCV data

    max_limit = 1000

    n_candles_needed = n_candles

    # Initialize variables for the loop
    current_end_time = None

    while n_candles_needed > 0:
        limit = min(max_limit, n_candles_needed)

        # Prepare parameters for klines function
        if current_end_time is None:
            new_ohlc = klines(pair=pair, timeframe=timeframe, limit=limit, trading_type=trading_type)
        else:
            # Subtract 1 millisecond to avoid duplicate data
            new_ohlc = klines(pair=pair, timeframe=timeframe, limit=limit, end_time=current_end_time, trading_type=trading_type)

        if len(new_ohlc) == 0:
            break  # No more data

        # Convert the retrieved data to a DataFrame
        new_ohlc_df = _pd.DataFrame(
            new_ohlc,
            columns=[
                'open_time', 'open_price', 'high_price', 
                'low_price', 'close_price', 'volume',
                'close_time', 'quote_volume', 'n_trades', 
                'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
            ]
        )

        ohlcs.append(new_ohlc_df)

        n_candles_needed -= len(new_ohlc_df)

        # Update current_end_time to earliest open_time in new_ohlc_df
        current_end_time = int(new_ohlc_df['open_time'].min())

        # If we've retrieved fewer candles than requested, all data has been fetched
        if len(new_ohlc_df) < limit:
            break

    if not ohlcs:
        return _pd.DataFrame()  # Return empty DataFrame if no data retrieved

    # Concatenate all dataframes into one
    ohlc = _pd.concat(ohlcs, axis=0)

    # Drop duplicates
    ohlc.drop_duplicates(subset='open_time', inplace=True)

    # Convert columns to appropriate data types
    cols_to_numeric = ['open_price', 'high_price', 'low_price', 'close_price', 'volume', 'quote_volume', 'n_trades', 'taker_buy_base_volume', 'taker_buy_quote_volume']
    ohlc[cols_to_numeric] = ohlc[cols_to_numeric].apply(_pd.to_numeric)

    # Convert times to datetime
    ohlc['open_time'] = _pd.to_datetime(ohlc['open_time'], unit='ms')
    ohlc['close_time'] = _pd.to_datetime(ohlc['close_time'], unit='ms')

    # Calculate taker sell quote volume
    ohlc['taker_sell_quote_volume'] = ohlc['quote_volume'] - ohlc['taker_buy_quote_volume']

    # Keep necessary columns
    ohlc = ohlc[['open_time', 'open_price', 'high_price', 'low_price', 'close_price',
                 'taker_buy_quote_volume', 'taker_sell_quote_volume', 'quote_volume']]

    # Sort by open_time
    ohlc.sort_values(by='open_time', inplace=True)

    # Keep the most recent n_candles
    ohlc = ohlc.iloc[-n_candles:].copy()

    # Reset index
    ohlc.reset_index(drop=True, inplace=True)

    # Rename columns
    ohlc.columns = ['date', 'open', 'high', 'low', 'close', 'buy_volume', 'sell_volume', 'volume']

    # Adjust date if necessary
    ohlc['date'] = ohlc['date'].apply(lambda x: adjust_datetime(x))


    return ohlc

def most_recent_premium_index(pair: str, timeframe: str, n_candles: int) -> _pd.DataFrame:
    """
    Load the n_candles most recent ohlc candlesticks for the given pair and timeframe.

    Args:
        pair (str): The trading pair for which to retrieve OHLCV data.
        timeframe (str): The timeframe for the OHLCV data.
        n_candles (int): The number of most recent candles to retrieve.

    Returns:
        pandas.DataFrame: The most recent OHLCV data for the specified trading pair and timeframe.
    """
    # Initialize variables
    ohlc = []  # List to store the retrieved OHLCV data
    end_time = 0  # End time of the last retrieved candle
    n_c = n_candles  # Number of candles left to retrieve
    end_of_history = False  # Flag to indicate if the end of the history is reached

    # Retrieve the most recent candles
    while (n_c + 10 > 0) and (not end_of_history):
        limit = _np.min([1000, n_c + 10])  # Calculate the limit for the API request

        # Retrieve the candles
        if end_time == 0:
            new_ohlc = klines_premium_index(pair=pair, timeframe=timeframe, limit=limit)
        else:
            new_ohlc = klines_premium_index(pair=pair, timeframe=timeframe, limit=limit, end_time=end_time)

        # Check if any candles are retrieved
        if len(new_ohlc) != 0:
            # Convert the retrieved candles to a DataFrame and append to the list
            new_ohlc = _pd.DataFrame(
                new_ohlc,
                columns=[
                    'open_time', 'open_price', 'high_price',
                    'low_price', 'close_price', 'volume',
                    'close_time', 'quote_volume', 'n_trades',
                    'taker_buy_base_volume', 'taker_buy_quote_volume', 'ignore'
                ]
            )
            ohlc.append(new_ohlc)

            # Update the end time and number of candles left to retrieve
            end_time = _pd.Timestamp(_pd.to_datetime(new_ohlc['close_time'], unit='ms').min())
            end_time = int(end_time.timestamp() * 1000 + 1)
            n_c -= limit
        else:
            # Set the end of history flag if no more candles are retrieved
            end_of_history = True

    # Concatenate the retrieved candles into a single DataFrame
    ohlc = _pd.concat(ohlc, axis=0)

    # Sort the DataFrame by the open time
    ohlc.sort_values(by='open_time', inplace=True)

    # Calculate the duration of a single candle in seconds
    timedelta = _pd.Timedelta(timeframe).seconds

    # Calculate the close time of each candle
    ohlc['close_time'] = ohlc['open_time'] + 1000 * timedelta

    # Drop any duplicate candles
    ohlc.drop_duplicates(inplace=True)

    # Convert the columns to numeric data types
    for col in ohlc.columns:
        ohlc[col] = _pd.to_numeric(ohlc[col])

    # Convert the open time and close time columns to datetime data types
    ohlc['open_time'] = _pd.to_datetime(ohlc['open_time'], unit='ms')
    ohlc['close_time'] = _pd.to_datetime(ohlc['close_time'], unit='ms')

    # Select the required columns
    ohlc = ohlc[['close_time', 'open_price', 'high_price', 'low_price', 'close_price']]

    # Select the most recent candles
    ohlc = ohlc.iloc[-n_candles:].copy()

    # Reset the index of the DataFrame
    ohlc = ohlc.reset_index(drop=True)

    # Rename the columns
    ohlc.columns = ['date', 'open', 'high', 'low', 'close'] 

    ohlc['date'] = ohlc['date'].apply(lambda x: datetime.strptime(x.strftime('%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S'))


    return ohlc
def klines(pair: str, timeframe: str, trading_type="future", **kwargs) -> list:
    """
    Kline/candlestick bars for a symbol.

    Args:
        pair (str): The trading pair.
        timeframe (str): The timeframe of the candlesticks.
        trading_type (str, optional): The type of trading. Defaults to "future".
        **kwargs: Additional parameters for the API request.

    Returns:
        list: The list of OHLCV data.

    Klines are uniquely identified by their open time.
    """
    # Set the URL path based on the trading type
    if trading_type == "future":
        url_path = "/fapi/v1/klines"
    elif trading_type == 'spot':
        url_path = "/api/v3/klines"
    else : 
        raise ValueError(f"Trading type {trading_type}")

    # Set the initial parameters
    params = {'symbol': pair, 'interval': timeframe}

    # Add additional parameters to the request
    for key, value in kwargs.items():
        if key == 'start_time':
            params['startTime'] = int(value)
        elif key == 'end_time':
            params['endTime'] = int(value)
        elif key == 'limit':
            params['limit'] = _np.min([value, 1000])

    # Send the request and return the response
    response = send_public_request(
        url_path=url_path,
        payload=params,
        trading_type=trading_type
    )
    return response 
def klines_open_interest(pair: str, timeframe: str,verbose=0, **kwargs) -> list:
    """
    Kline/candlestick bars for a symbol.

    Args:
        pair (str): The trading pair.
        timeframe (str): The timeframe of the candlesticks.
        trading_type (str, optional): The type of trading. Defaults to "future".
        **kwargs: Additional parameters for the API request.

    Returns:
        list: The list of OHLCV data.

    Klines are uniquely identified by their open time.
    """
    # Set the URL path based on the trading type
    url_path = "/futures/data/openInterestHist"

    # Set the initial parameters
    params = {'symbol': pair, 'period': timeframe}

    # Add additional parameters to the request
    for key, value in kwargs.items():
        if key == 'start_time':
            params['startTime'] = int(value)
        elif key == 'end_time':
            params['endTime'] = int(value)
        elif key == 'limit':
            params['limit'] = _np.min([value, 1000])

    # Send the request and return the response
    response = send_public_request(
        url_path=url_path,
        payload=params,
        verbose=verbose
    )
    return response 
def get_binance_time():
    # Binance API endpoint for server time
    url = 'https://api.binance.com/api/v3/time'
    
    try:
        # Send a GET request to the endpoint
        response = requests.get(url)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Extract the server time from the response (in milliseconds)
            server_time = response.json()['serverTime']
            readable_time = datetime.fromtimestamp(server_time / 1000.0)
            
            # Convert the server time to a readable format
            
            return f"Binance Server Time (UTC): {readable_time}"
        else:
            return f"Error: Unable to fetch time from Binance (Status code: {response.status_code})"
    
    except Exception as e:
        return f"Exception occurred: {str(e)}" 
def klines_premium_index(pair: str, timeframe: str, **kwargs) -> list:
    """
    Kline/candlestick bars for a symbol.

    Args:
        pair (str): The trading pair.
        timeframe (str): The timeframe of the candlesticks.
        trading_type (str, optional): The type of trading. Defaults to "future".
        **kwargs: Additional parameters for the API request.

    Returns:
        list: The list of OHLCV data.

    Klines are uniquely identified by their open time.
    """
    # Set the URL path based on the trading type
    url_path = "/fapi/v1/premiumIndexKlines"

    # Set the initial parameters
    params = {'symbol': pair, 'interval': timeframe}

    # Add additional parameters to the request
    for key, value in kwargs.items():
        if key == 'start_time':
            params['startTime'] = int(value)
        elif key == 'end_time':
            params['endTime'] = int(value)
        elif key == 'limit':
            params['limit'] = _np.min([value, 1000])

    # Send the request and return the response
    response = send_public_request(
        url_path=url_path,
        payload=params,
        trading_type='future'
    )
    return response
def send_public_request(url_path: str, payload={}, trading_type: str = "future", verbose=0) -> dict:
    """
    Prepare and send an unsigned request to obtain public market data.

    Args:
        url_path (str): The URL path for the API request.
        payload (dict, optional): Additional parameters for the API request. Defaults to {}.
        trading_type (str, optional): The type of trading. Defaults to "future".
        verbose (int, optional): Verbosity level. Defaults to 0.

    Returns:
        dict: The response from the API request in JSON format.
    """
    try:
        # Encode the payload as a query string
        query_string = urlencode(payload, True)

        # Construct the URL based on the trading type
        if trading_type == "future":
            url = 'https://fapi.binance.com' + url_path 
        elif url_path == "/futures/data/openInterestHist":
            url = 'https://fapi.binance.com' + url_path 
        else:
            url = "https://api.binance.com" + url_path

        # Append the query string to the URL if it exists
        if query_string:
            url = url + '?' + query_string
            
        _print(f"Sending request to: {url}", verbosity_level=1, current_verbose_level=verbose)

        # Send the GET request and return the response as JSON
        response = dispatch_request('GET')(url=url)
        
        if response.status_code != 200:
            _print_error(f"Request failed with status code {response.status_code}: {response.text}")
            return {}
            
        _print(f"Request successful", verbosity_level=2, current_verbose_level=verbose)
        return response.json()
        
    except Exception as e:
        _print_error(f"Error in send_public_request: {str(e)}")
        return {}



def dispatch_request(http_method: str):
    """
    Prepare a request with the given HTTP method.

    Args:
        http_method (str): The HTTP method to use for the request.

    Returns:
        requests.Session: A session object with the appropriate headers and HTTP method.

    This function creates a session object and sets the headers and HTTP method based on the provided HTTP method.
    The session object is then returned.
    """
    # Create a session object
    session = requests.Session()

    # Set the headers for the session
    session.headers.update({
        'Content-Type': 'application/json;charset=utf-8',
        'X-MBX-APIKEY': ""  # Placeholder for the API key
    })

    # Get the appropriate HTTP method from the dictionary
    response = {
        'GET': session.get,
        'DELETE': session.delete,
        'PUT': session.put,
        'POST': session.post,
    }.get(http_method, 'GET')

    # Return the session object with the appropriate HTTP method
    return response



