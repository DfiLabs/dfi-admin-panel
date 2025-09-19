import pandas as pd
import requests


WEBSITE = "https://unravel.finance"
BASEAPI = f"{WEBSITE}/api/v1"


def get_portfolio_factors_historical(
    portfolioId: str,
    tickers: list,
    API_KEY: str,
) -> pd.DataFrame:
    """
    Fetch historical factors for a portfolio from the Unravel API.

    Args:
        portfolioId (str): The portfolio ID
        tickers (list[str]): List of tickers in the portfolio
        API_KEY (str): The API key to use for the request
    Returns:
        pd.DataFrame: Historical factor data for the input tickers
    """
    url = f"{BASEAPI}/portfolio/factors"
    if portfolioId == 'altair':
        params = {"id": portfolioId, "tickers": ",".join(tickers),"smoothing":0}
    else : 
        params = {"id": portfolioId, "tickers": ",".join(tickers)}
    headers = {"X-API-KEY": API_KEY}
    response = requests.get(url, headers=headers, params=params)
    assert (
        response.status_code == 200
    ), f"Error fetching factors for {portfolioId}, response: {response.json()}"

    response = response.json()
    return pd.DataFrame(
        response["data"],
        index=pd.to_datetime(response["index"]),
        columns=response["columns"],
    )