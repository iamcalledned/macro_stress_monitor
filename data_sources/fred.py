"""
Data source for fetching data from the Federal Reserve Economic Data (FRED).
"""
import os
from typing import Optional, Dict

import pandas as pd
from fredapi import Fred
from dotenv import load_dotenv

load_dotenv()

FRED_API_KEY = os.getenv("FRED_API_KEY")

def get_fred_series(
    series_map: Dict[str, str], 
    start_date: pd.Timestamp
) -> Dict[str, Optional[pd.Series]]:
    """
    Fetches multiple FRED series and returns them as a dictionary of pandas Series.

    Args:
        series_map: A dictionary mapping a custom name to a FRED series ID.
        start_date: The start date for fetching the data.

    Returns:
        A dictionary where keys are the custom names and values are the pandas Series.
        If a series fails to download, the value will be None.
    """
    try:
        fred = Fred(api_key=FRED_API_KEY)
    except ValueError as e:
        print(f"Error initializing FRED API: {e}. Is FRED_API_KEY set?")
        return {name: None for name in series_map}

    data: Dict[str, Optional[pd.Series]] = {}
    for name, series_id in series_map.items():
        try:
            series_data = fred.get_series(series_id, start_time=start_date)
            series_data = series_data.ffill() # Forward fill missing values
            series_data.index = pd.to_datetime(series_data.index)
            data[name] = series_data
            print(f"Successfully fetched FRED series: {name} ({series_id})")
        except Exception as e:
            print(f"ERROR: Could not fetch FRED series {name} ({series_id}): {e}")
            data[name] = None
    return data
