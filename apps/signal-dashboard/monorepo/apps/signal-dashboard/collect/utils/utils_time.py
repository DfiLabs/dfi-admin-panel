from datetime import datetime, timedelta
import dfilabs.config.config as cf
from dfilabs.utils.log import _print

def calculate_required_candles(start_date_datetime, target_datetime, timeframe_minute=1, n_candles_add=0, verbose=0):
    _print("Function: calculate_required_candles", 2, verbose)
    """Calculate number of candles needed between dates."""
    n_candles = int(round((target_datetime - start_date_datetime) / timedelta(minutes=timeframe_minute), 0))
    n_candles = n_candles + 1440 + n_candles_add  # Add one day buffer plus additional candles
    return n_candles

def calculate_target_datetime(pair, verbose):
    """Calculate and validate target datetime"""
    target_datetime = datetime.now() - timedelta(days=1)
    target_datetime = target_datetime.replace(hour=23, minute=59, second=0, microsecond=0)

    # Check for delisted coins
    if (date_delisted := cf.coin_to_date_delisted.get(pair)):
        date_delisted_datetime = datetime.strptime(date_delisted["date"], '%Y-%m-%d')
        if date_delisted_datetime <= target_datetime:
            _print(f'The pair {pair} is delisted (date: {date_delisted["date"]})', 1, verbose)
            target_datetime = date_delisted_datetime

    return target_datetime 