"""
JPY Spike Risk Indicator
"""
import os
from typing import Dict, Any, Optional
import pandas as pd

def calculate_jpy_risk_indicator(
    usdjpy_data: Optional[pd.DataFrame]
) -> Optional[Dict[str, Any]]:
    """
    Calculates the JPY spike risk indicator using USD/JPY data.
    A sharp drop in USD/JPY means JPY is strengthening, often a risk-off signal.

    Args:
        usdjpy_data: DataFrame with at least 'Close' prices for USD/JPY.

    Returns:
        A dictionary with computed metrics or None if data is insufficient.
    """
    if usdjpy_data is None or len(usdjpy_data) < 252:
        print("WARNING: Insufficient data for JPY Risk indicator, need at least 1 year.")
        return None

    series = usdjpy_data['Close'].dropna()
    latest = series.iloc[-1]
    
    # 5-day % move
    move_5d_pct = (latest / series.iloc[-6] - 1) * 100

    # Realized volatility percentile (1-year)
    returns = series.pct_change().dropna()
    realized_vol_10d = returns.rolling(window=10).std()
    
    # Percentile rank of current 10d vol over the last year
    vol_percentile_1y = realized_vol_10d.iloc[-252:].dropna().rank(pct=True).iloc[-1] * 100

    # Risk flag base: JPY strengthens (USDJPY drops) sharply or vol spikes
    sharp_drop = move_5d_pct <= -3.0
    vol_spike = vol_percentile_1y > 90.0
    base_risk_flag = sharp_drop or vol_spike

    ma20 = series.rolling(window=20).mean().iloc[-1]
    ma50 = series.rolling(window=50).mean().iloc[-1]
    below_ma_confirmation = latest < ma20 or latest < ma50

    level_threshold = float(os.getenv("USDJPY_CONFIRM_LEVEL", "145"))
    if len(series) >= 2:
        below_level_two_closes = (series.iloc[-1] < level_threshold) and (series.iloc[-2] < level_threshold)
    else:
        below_level_two_closes = False

    jpy_confirmed = base_risk_flag and (below_ma_confirmation or below_level_two_closes)
    risk_flag = base_risk_flag

    return {
        "name": "JPY Spike Risk (USD/JPY)",
        "latest": latest,
        "move_5d_pct": move_5d_pct,
        "vol_percentile_1y": vol_percentile_1y,
        "risk_flag": bool(risk_flag),
        "base_risk_flag": bool(base_risk_flag),
        "sharp_drop": bool(sharp_drop),
        "vol_spike": bool(vol_spike),
        "usdjpy_20dma": ma20,
        "usdjpy_50dma": ma50,
        "below_level_two_closes": bool(below_level_two_closes),
        "jpy_confirmed": bool(jpy_confirmed),
        "level_threshold": level_threshold,
    }
