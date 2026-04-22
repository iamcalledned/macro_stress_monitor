"""
Interest Rate based indicators, primarily the 30-year Treasury yield.
"""
from typing import Dict, Any, Optional
import pandas as pd

def calculate_30y_yield_indicator(
    dgs30_data: Optional[pd.Series]
) -> Optional[Dict[str, Any]]:
    """
    Calculates the 30-year Treasury yield indicator.

    Args:
        dgs30_data: FRED series for DGS30.

    Returns:
        A dictionary with computed metrics or None if data is insufficient.
    """
    if dgs30_data is None or len(dgs30_data) < 252:
        print("WARNING: Insufficient data for 30Y Yield indicator, need at least 1 year.")
        return None
        
    series = dgs30_data.dropna()
    latest = series.iloc[-1]

    # 20-day change in bps
    change_20d_bps = (latest - series.iloc[-21]) * 100

    # Z-score of the yield level over 1 year
    one_year_series = series.iloc[-252:]
    mean_1y = one_year_series.mean()
    std_1y = one_year_series.std()
    
    if std_1y == 0:
        z_score = 0
    else:
        z_score = (latest - mean_1y) / std_1y

    # Is yield below its 200DMA?
    ma200 = series.rolling(window=200).mean().iloc[-1]
    is_below_200dma = latest < ma200

    if change_20d_bps <= -25:
        dgs30_signal = "risk_off_duration_bid"
    elif change_20d_bps >= 25:
        dgs30_signal = "policy_shock"
    else:
        dgs30_signal = "neutral"

    return {
        "name": "30Y Treasury Yield (DGS30)",
        "latest_yield": latest,
        "dgs30_20d_bps": change_20d_bps,
        "change_20d_bps": change_20d_bps,
        "z_score_1y": z_score,
        "is_below_200dma": bool(is_below_200dma),
        "dgs30_signal": dgs30_signal,
    }
