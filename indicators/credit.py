"""
Credit-based indicators: IG spreads and leveraged loan stress.
"""
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np


def _calculate_series_stats(
    series: pd.Series,
    higher_is_worse: bool,
) -> Optional[Dict[str, Any]]:
    """Computes common indicator stats for a time series."""
    series = series.dropna()
    if len(series) < 252:
        return None

    latest = series.iloc[-1]
    change_5d = latest - series.iloc[-6]
    change_20d = latest - series.iloc[-21]
    change_60d = latest - series.iloc[-61]

    one_year_series = series.iloc[-252:]
    mean_1y = one_year_series.mean()
    std_1y = one_year_series.std()
    z_score = 0.0 if std_1y == 0 else (latest - mean_1y) / std_1y

    if not higher_is_worse:
        z_score = -z_score

    return {
        "latest": latest,
        "change_5d": change_5d,
        "change_20d": change_20d,
        "change_60d": change_60d,
        "z_score_1y": z_score,
        "higher_is_worse": higher_is_worse,
    }


def calculate_ig_spread_indicator(
    fred_data: Optional[pd.Series], 
    proxy_data: Optional[Dict[str, Optional[pd.DataFrame]]]
) -> Optional[Dict[str, Any]]:
    """
    Calculates the Investment Grade (IG) credit spread indicator.
    
    Uses FRED series 'BAMLC0A0CM' if available, otherwise falls back to a
    price-based proxy using IEF/LQD ratio.

    Returns:
        A dictionary with computed metrics or None if data is insufficient.
    """
    if fred_data is not None and not fred_data.empty:
        stats = _calculate_series_stats(fred_data, higher_is_worse=True)
        if stats is None:
            print("WARNING: Insufficient data for IG Spread indicator, need at least 1 year.")
            return None
        return {
            "name": "IG OAS (FRED)",
            "data_source": "fred",
            "ig_data_quality": "fred_primary",
            **stats,
        }

    if proxy_data is None:
        return None

    lqd = proxy_data.get("LQD")
    ief = proxy_data.get("IEF")
    govt = proxy_data.get("GOVT")
    shy = proxy_data.get("SHY")
    if lqd is None or ief is None:
        return None

    proxy_1 = (lqd["Close"] / ief["Close"]).dropna()
    proxy_2_label = None
    proxy_2 = None
    if govt is not None:
        proxy_2 = (lqd["Close"] / govt["Close"]).dropna()
        proxy_2_label = "LQD/GOVT"
    elif shy is not None:
        proxy_2 = (lqd["Close"] / shy["Close"]).dropna()
        proxy_2_label = "LQD/SHY"

    proxy_1_stats = _calculate_series_stats(proxy_1, higher_is_worse=False)
    proxy_2_stats = _calculate_series_stats(proxy_2, higher_is_worse=False) if proxy_2 is not None else None
    if proxy_1_stats is None or proxy_2_stats is None:
        print("WARNING: Insufficient data for IG proxy indicators, need at least 1 year.")
        return None

    proxy_threshold = 1.0
    proxy_agree_stress = (
        proxy_1_stats["z_score_1y"] >= proxy_threshold
        and proxy_2_stats["z_score_1y"] >= proxy_threshold
    )
    ig_data_quality = "proxy_agreement" if proxy_agree_stress else "proxy_disagreement"

    return {
        "name": "IG Spread Proxy (LQD vs Rates ETFs)",
        "data_source": "proxy",
        "ig_data_quality": ig_data_quality,
        "proxy_agree_stress": bool(proxy_agree_stress),
        "proxy_threshold": proxy_threshold,
        "proxy_z_scores": {
            "lqd_ief": proxy_1_stats["z_score_1y"],
            "lqd_alt": proxy_2_stats["z_score_1y"],
        },
        "proxy_legs": {
            "primary": "LQD/IEF",
            "alternate": proxy_2_label,
        },
        "latest": proxy_1_stats["latest"],
        "change_5d": proxy_1_stats["change_5d"],
        "change_20d": proxy_1_stats["change_20d"],
        "change_60d": proxy_1_stats["change_60d"],
        "z_score_1y": proxy_1_stats["z_score_1y"],
        "higher_is_worse": proxy_1_stats["higher_is_worse"],
    }


def calculate_hy_credit_indicator(
    fred_data: Optional[pd.Series],
    proxy_data: Optional[Dict[str, Optional[pd.DataFrame]]],
) -> Optional[Dict[str, Any]]:
    """Calculates High Yield credit stress using FRED OAS or ETF proxy."""
    if fred_data is not None and not fred_data.empty:
        stats = _calculate_series_stats(fred_data, higher_is_worse=True)
        if stats is None:
            print("WARNING: Insufficient data for HY OAS indicator, need at least 1 year.")
            return None
        return {
            "name": "HY OAS (FRED)",
            "data_source": "fred",
            **stats,
        }

    if proxy_data is None:
        return None

    hyg = proxy_data.get("HYG")
    ief = proxy_data.get("IEF")
    shy = proxy_data.get("SHY")
    proxy_series = None
    proxy_label = None
    if hyg is not None and ief is not None:
        proxy_series = (hyg["Close"] / ief["Close"]).dropna()
        proxy_label = "HYG/IEF"
    elif hyg is not None and shy is not None:
        proxy_series = (hyg["Close"] / shy["Close"]).dropna()
        proxy_label = "HYG/SHY"

    if proxy_series is None:
        return None

    stats = _calculate_series_stats(proxy_series, higher_is_worse=False)
    if stats is None:
        print("WARNING: Insufficient data for HY proxy indicator, need at least 1 year.")
        return None

    return {
        "name": f"HY Proxy ({proxy_label})",
        "data_source": "proxy",
        "proxy_label": proxy_label,
        **stats,
    }


def calculate_hy_ig_gap(
    hy_indicator: Optional[Dict[str, Any]],
    ig_indicator: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Calculates HY-IG credit gap.
    If both are FRED OAS levels, gap is direct spread difference.
    Otherwise, gap is proxied using stress z-score differential.
    """
    if hy_indicator is None or ig_indicator is None:
        return {"hy_ig_oas_gap": None, "hy_ig_oas_gap_method": "unavailable"}

    hy_source = hy_indicator.get("data_source")
    ig_source = ig_indicator.get("data_source")
    if hy_source == "fred" and ig_source == "fred":
        gap = hy_indicator["latest"] - ig_indicator["latest"]
        return {
            "hy_ig_oas_gap": gap,
            "hy_ig_oas_gap_method": "oas_bps_difference",
        }

    gap = hy_indicator.get("z_score_1y", 0.0) - ig_indicator.get("z_score_1y", 0.0)
    return {
        "hy_ig_oas_gap": gap,
        "hy_ig_oas_gap_method": "proxy_z_score_difference",
    }

def calculate_leveraged_loan_indicator(
    bkln_data: Optional[pd.DataFrame]
) -> Optional[Dict[str, Any]]:
    """
    Calculates the Leveraged Loan (BKLN) stress indicator.

    Returns:
        A dictionary with computed metrics or None if data is insufficient.
    """
    if bkln_data is None or len(bkln_data) < 252:
        print("WARNING: Insufficient data for Leveraged Loan indicator, need at least 1 year.")
        return None

    series = bkln_data['Close']
    latest_price = series.iloc[-1]

    # Price vs 200DMA
    ma200 = series.rolling(window=200).mean().iloc[-1]
    price_vs_ma200 = (latest_price / ma200) - 1

    # 30-day drawdown
    rolling_max_30d = series.rolling(window=30, min_periods=30).max()
    drawdown_30d = (latest_price / rolling_max_30d.iloc[-1]) - 1

    # Z-score of 30-day returns volatility
    returns = series.pct_change().dropna()
    volatility_30d = returns.rolling(window=30).std() * np.sqrt(252) # Annualized
    
    one_year_vol = volatility_30d.iloc[-252:].dropna()
    if len(one_year_vol) < 30:
         return None

    latest_vol = one_year_vol.iloc[-1]
    mean_vol_1y = one_year_vol.mean()
    std_vol_1y = one_year_vol.std()

    if std_vol_1y == 0:
        vol_z_score = 0
    else:
        vol_z_score = (latest_vol - mean_vol_1y) / std_vol_1y

    return {
        "name": "Leveraged Loans (BKLN)",
        "latest_price": latest_price,
        "price_vs_200dma": price_vs_ma200,
        "drawdown_30d": drawdown_30d,
        "volatility_z_score": vol_z_score,
    }
