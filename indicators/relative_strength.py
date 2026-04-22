"""
Relative strength indicators, e.g., XLF vs SPY.
"""
from typing import Dict, Any, Optional
import pandas as pd


def _calculate_ratio_indicator(
    lhs_data: Optional[pd.DataFrame],
    rhs_data: Optional[pd.DataFrame],
    lhs_name: str,
    rhs_name: str,
    display_name: str,
) -> Optional[Dict[str, Any]]:
    if lhs_data is None or rhs_data is None:
        return None

    data = pd.DataFrame({
        lhs_name: lhs_data["Close"],
        rhs_name: rhs_data["Close"],
    }).dropna()
    if len(data) < 252:
        print(f"WARNING: Insufficient data for {display_name} indicator, need at least 1 year.")
        return None

    ratio = (data[lhs_name] / data[rhs_name]).dropna()
    latest_ratio = ratio.iloc[-1]

    ma50 = ratio.rolling(window=50).mean().iloc[-1]
    ma200 = ratio.rolling(window=200).mean().iloc[-1]
    breakdown_flag = latest_ratio < ma200

    ratio_vs_ma50 = (latest_ratio / ma50) - 1
    ratio_vs_ma200 = (latest_ratio / ma200) - 1

    one_year_series = ratio.iloc[-252:]
    mean_1y = one_year_series.mean()
    std_1y = one_year_series.std()
    z_score = 0 if std_1y == 0 else (latest_ratio - mean_1y) / std_1y

    return {
        "name": display_name,
        "latest_ratio": latest_ratio,
        "ratio_vs_ma50": ratio_vs_ma50,
        "ratio_vs_ma200": ratio_vs_ma200,
        "z_score_1y": z_score,
        "breakdown_flag": bool(breakdown_flag),
    }


def calculate_xlf_spy_indicator(
    xlf_data: Optional[pd.DataFrame], 
    spy_data: Optional[pd.DataFrame]
) -> Optional[Dict[str, Any]]:
    """
    Calculates the XLF vs SPY relative strength indicator.

    Args:
        xlf_data: DataFrame for XLF.
        spy_data: DataFrame for SPY.

    Returns:
        A dictionary with computed metrics or None if data is insufficient.
    """
    return _calculate_ratio_indicator(
        lhs_data=xlf_data,
        rhs_data=spy_data,
        lhs_name="XLF",
        rhs_name="SPY",
        display_name="XLF/SPY Relative Strength",
    )


def calculate_kre_spy_indicator(
    kre_data: Optional[pd.DataFrame],
    spy_data: Optional[pd.DataFrame],
) -> Optional[Dict[str, Any]]:
    """Calculates KRE vs SPY regional bank relative strength."""
    return _calculate_ratio_indicator(
        lhs_data=kre_data,
        rhs_data=spy_data,
        lhs_name="KRE",
        rhs_name="SPY",
        display_name="Regional Banks (KRE/SPY)",
    )
