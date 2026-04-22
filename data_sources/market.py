"""
Data source for fetching market data using Polygon.
"""
import os
import json
from urllib.parse import quote
from urllib.request import urlopen
from typing import List, Dict, Optional
import pandas as pd

POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
POLYGON_BASE_URL = "https://api.polygon.io/v2/aggs/ticker"


def _to_polygon_ticker(ticker: str) -> str:
    """Maps internal symbols to Polygon ticker format."""
    if ticker == "JPY=X":
        return "C:USDJPY"
    return ticker


def _fetch_polygon_agg(
    polygon_ticker: str,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> Optional[pd.DataFrame]:
    if not POLYGON_API_KEY:
        return None

    start_str = pd.Timestamp(start_date).strftime("%Y-%m-%d")
    end_str = pd.Timestamp(end_date).strftime("%Y-%m-%d")
    ticker_encoded = quote(polygon_ticker, safe="")
    url = (
        f"{POLYGON_BASE_URL}/{ticker_encoded}/range/1/day/{start_str}/{end_str}"
        f"?adjusted=true&sort=asc&limit=50000&apiKey={POLYGON_API_KEY}"
    )
    with urlopen(url, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    results = payload.get("results", [])
    if not results:
        return None

    df = pd.DataFrame(results)
    if df.empty:
        return None

    rename_map = {
        "o": "Open",
        "h": "High",
        "l": "Low",
        "c": "Close",
        "v": "Volume",
        "t": "timestamp_ms",
    }
    df = df.rename(columns=rename_map)
    for required_col in ("Open", "High", "Low", "Close", "Volume", "timestamp_ms"):
        if required_col not in df.columns:
            if required_col == "Volume":
                df[required_col] = 0.0
            else:
                return None

    df["timestamp_ms"] = pd.to_numeric(df["timestamp_ms"], errors="coerce")
    df = df.dropna(subset=["timestamp_ms"])
    dt_index = pd.to_datetime(df["timestamp_ms"], unit="ms", utc=True).dt.tz_convert(None)
    df.index = pd.DatetimeIndex(dt_index)
    df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
    df = df.sort_index()
    return df if not df.empty else None


def get_market_data(
    tickers: List[str], 
    start_date: pd.Timestamp, 
    end_date: pd.Timestamp
) -> Dict[str, Optional[pd.DataFrame]]:
    """
    Fetches historical market data for a list of tickers.

    Args:
        tickers: A list of ticker symbols to fetch.
        start_date: The start date for the data.
        end_date: The end date for the data.

    Returns:
        A dictionary where keys are ticker symbols and values are pandas DataFrames
        with 'Open', 'High', 'Low', 'Close', 'Volume'.
        If a ticker fails to download, the value will be None.
    """
    data: Dict[str, Optional[pd.DataFrame]] = {}
    if not POLYGON_API_KEY:
        print("WARNING: POLYGON_API_KEY not set. Market data will be unavailable.")
        return {ticker: None for ticker in tickers}

    unique_tickers = list(dict.fromkeys(tickers))
    for ticker in unique_tickers:
        polygon_ticker = _to_polygon_ticker(ticker)
        try:
            ticker_data = _fetch_polygon_agg(polygon_ticker, start_date, end_date)
            if ticker_data is None or ticker_data.empty:
                print(f"WARNING: No Polygon data found for ticker: {ticker} ({polygon_ticker})")
                data[ticker] = None
            else:
                data[ticker] = ticker_data
                print(f"Successfully fetched Polygon data for: {ticker} ({polygon_ticker})")
        except Exception as e:
            print(f"ERROR: Could not fetch Polygon data for {ticker} ({polygon_ticker}): {e}")
            data[ticker] = None

    return data
