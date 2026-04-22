"""
Deterministic market context builders for structural Macro Stress Monitor runs.

The functions in this module produce compact, JSON-safe summaries from already
fetched FRED series and market price frames. They intentionally avoid storing
raw provider payloads.
"""
import math
from typing import Any, Dict, Iterable, Optional, Tuple

import pandas as pd


EQUITY_INDEX_TICKERS = ["SPY", "QQQ", "IWM", "DIA"]
SECTOR_TICKERS = ["XLF", "KRE", "XLK", "XLI", "XLE", "XLU", "XLP", "XLY", "XLV", "SMH"]
TREASURY_PROXY_TICKERS = ["TLT", "IEF", "SHY"]
DEFENSIVE_TICKERS = ["XLU", "XLP", "XLV"]
CYCLICAL_TICKERS = ["XLY", "XLI", "XLF", "XLE", "SMH"]
POSITIONING_TICKERS = ["SPY", "QQQ", "IWM", "DIA", "TLT", "GLD", "UUP", "VIXY", "HYG", "LQD", "BKLN"]

RATE_SERIES = {
    "2y": "DGS2",
    "5y": "DGS5",
    "10y": "DGS10",
    "30y": "DGS30",
}

CROSS_ASSET_PAIRS: Dict[str, Tuple[str, str]] = {
    "xlf_spy": ("XLF", "SPY"),
    "kre_spy": ("KRE", "SPY"),
    "hyg_lqd": ("HYG", "LQD"),
    "hyg_ief": ("HYG", "IEF"),
    "iwm_spy": ("IWM", "SPY"),
    "qqq_spy": ("QQQ", "SPY"),
    "smh_spy": ("SMH", "SPY"),
    "xlu_xly": ("XLU", "XLY"),
    "xlp_xly": ("XLP", "XLY"),
    "gld_tlt": ("GLD", "TLT"),
    "rsp_spy": ("RSP", "SPY"),
}


def _num(value: Any, digits: int = 6) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return round(numeric, digits)


def _series_from_frame(frame: Optional[pd.DataFrame]) -> Optional[pd.Series]:
    if frame is None or frame.empty or "Close" not in frame.columns:
        return None
    series = pd.to_numeric(frame["Close"], errors="coerce").dropna()
    return series if not series.empty else None


def _clean_series(series: Optional[pd.Series]) -> Optional[pd.Series]:
    if series is None:
        return None
    cleaned = pd.to_numeric(series, errors="coerce").dropna()
    return cleaned if not cleaned.empty else None


def _pct_return(series: pd.Series, window: int) -> Optional[float]:
    if len(series) <= window:
        return None
    previous = series.iloc[-window - 1]
    if previous == 0:
        return None
    return _num((series.iloc[-1] / previous) - 1)


def _change(series: pd.Series, window: int, multiplier: float = 1.0) -> Optional[float]:
    if len(series) <= window:
        return None
    return _num((series.iloc[-1] - series.iloc[-window - 1]) * multiplier)


def _rolling_z_score(series: pd.Series, window: int = 252) -> Optional[float]:
    if len(series) < max(30, window):
        return None
    sample = series.iloc[-window:]
    std = sample.std()
    if std == 0 or pd.isna(std):
        return 0.0
    return _num((sample.iloc[-1] - sample.mean()) / std)


def _percentile(series: pd.Series, window: int = 252) -> Optional[float]:
    if len(series) < max(30, window):
        return None
    sample = series.iloc[-window:]
    rank = sample.rank(pct=True).iloc[-1] * 100
    return _num(rank, digits=2)


def _realized_vol(series: pd.Series, window: int = 20) -> Optional[float]:
    if len(series) <= window:
        return None
    returns = series.pct_change().dropna()
    if len(returns) < window:
        return None
    return _num(returns.iloc[-window:].std() * math.sqrt(252))


def _rolling_drawdown(series: pd.Series, window: int = 252) -> Optional[float]:
    if len(series) < 2:
        return None
    sample = series.iloc[-min(window, len(series)):]
    high = sample.max()
    if high == 0:
        return None
    return _num((series.iloc[-1] / high) - 1)


def _ma_gap(series: pd.Series, window: int) -> Optional[float]:
    if len(series) < window:
        return None
    ma = series.rolling(window=window).mean().iloc[-1]
    if ma == 0 or pd.isna(ma):
        return None
    return _num((series.iloc[-1] / ma) - 1)


def _rsi(series: pd.Series, window: int = 14) -> Optional[float]:
    if len(series) <= window + 1:
        return None
    delta = series.diff().dropna()
    gains = delta.clip(lower=0)
    losses = -delta.clip(upper=0)
    avg_gain = gains.rolling(window=window).mean().iloc[-1]
    avg_loss = losses.rolling(window=window).mean().iloc[-1]
    if pd.isna(avg_gain) or pd.isna(avg_loss):
        return None
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return _num(100 - (100 / (1 + rs)), digits=2)


def _trend_state(distance_50dma: Optional[float], distance_200dma: Optional[float]) -> str:
    if distance_50dma is None or distance_200dma is None:
        return "insufficient_data"
    if distance_50dma > 0 and distance_200dma > 0:
        return "uptrend"
    if distance_50dma < 0 and distance_200dma < 0:
        return "downtrend"
    return "mixed"


def _vol_state(vol_percentile: Optional[float]) -> str:
    if vol_percentile is None:
        return "insufficient_data"
    if vol_percentile >= 85:
        return "high_vol"
    if vol_percentile >= 65:
        return "elevated_vol"
    if vol_percentile <= 20:
        return "compressed_vol"
    return "normal_vol"


def _stretch_state(rsi: Optional[float], distance_50dma: Optional[float], z_score: Optional[float]) -> str:
    if rsi is None and distance_50dma is None and z_score is None:
        return "insufficient_data"
    overbought = (
        (rsi is not None and rsi >= 70)
        or (distance_50dma is not None and distance_50dma >= 0.08)
        or (z_score is not None and z_score >= 2.0)
    )
    oversold = (
        (rsi is not None and rsi <= 30)
        or (distance_50dma is not None and distance_50dma <= -0.08)
        or (z_score is not None and z_score <= -2.0)
    )
    if overbought:
        return "overbought"
    if oversold:
        return "oversold"
    return "neutral"


def _asset_state(series: Optional[pd.Series]) -> Dict[str, Any]:
    series = _clean_series(series)
    if series is None or len(series) < 2:
        return {"available": False, "state": "insufficient_data"}

    distance_50dma = _ma_gap(series, 50)
    distance_200dma = _ma_gap(series, 200)
    returns = series.pct_change().dropna()
    vol_20d = _realized_vol(series, 20)
    vol_pct_series = returns.rolling(window=20).std().dropna() * math.sqrt(252)
    vol_percentile = _percentile(vol_pct_series, 252) if not vol_pct_series.empty else None
    rsi_14d = _rsi(series, 14)
    z_score_1y = _rolling_z_score(series, 252)

    return {
        "available": True,
        "latest": _num(series.iloc[-1]),
        "return_1d": _pct_return(series, 1),
        "return_5d": _pct_return(series, 5),
        "return_20d": _pct_return(series, 20),
        "return_60d": _pct_return(series, 60),
        "drawdown_252d": _rolling_drawdown(series, 252),
        "distance_50dma": distance_50dma,
        "distance_200dma": distance_200dma,
        "realized_vol_20d": vol_20d,
        "realized_vol_percentile_1y": vol_percentile,
        "z_score_1y": z_score_1y,
        "rsi_14d": rsi_14d,
        "trend_state": _trend_state(distance_50dma, distance_200dma),
        "vol_state": _vol_state(vol_percentile),
        "stretch_state": _stretch_state(rsi_14d, distance_50dma, z_score_1y),
    }


def _ratio_series(lhs: Optional[pd.Series], rhs: Optional[pd.Series]) -> Optional[pd.Series]:
    lhs = _clean_series(lhs)
    rhs = _clean_series(rhs)
    if lhs is None or rhs is None:
        return None
    aligned = pd.DataFrame({"lhs": lhs, "rhs": rhs}).dropna()
    if aligned.empty or (aligned["rhs"] == 0).any():
        aligned = aligned[aligned["rhs"] != 0]
    if aligned.empty:
        return None
    return aligned["lhs"] / aligned["rhs"]


def _relationship_state(ratio: Optional[pd.Series]) -> Dict[str, Any]:
    ratio = _clean_series(ratio)
    if ratio is None or len(ratio) < 30:
        return {"available": False, "state": "insufficient_data"}
    distance_50dma = _ma_gap(ratio, 50)
    distance_200dma = _ma_gap(ratio, 200)
    z_score_1y = _rolling_z_score(ratio, 252)
    if distance_50dma is None or distance_200dma is None:
        state = "insufficient_data"
    elif distance_50dma > 0 and distance_200dma > 0:
        state = "outperforming"
    elif distance_50dma < 0 and distance_200dma < 0:
        state = "underperforming"
    else:
        state = "mixed"
    return {
        "available": True,
        "latest_ratio": _num(ratio.iloc[-1]),
        "return_5d": _pct_return(ratio, 5),
        "return_20d": _pct_return(ratio, 20),
        "return_60d": _pct_return(ratio, 60),
        "distance_50dma": distance_50dma,
        "distance_200dma": distance_200dma,
        "z_score_1y": z_score_1y,
        "state": state,
        "stretch_state": _stretch_state(_rsi(ratio, 14), distance_50dma, z_score_1y),
    }


def _rate_state(series: Optional[pd.Series]) -> Dict[str, Any]:
    series = _clean_series(series)
    if series is None or len(series) < 2:
        return {"available": False, "state": "insufficient_data"}
    z_score = _rolling_z_score(series, 252)
    change_20d_bps = _change(series, 20, multiplier=100.0)
    if change_20d_bps is None:
        state = "insufficient_data"
    elif change_20d_bps <= -25:
        state = "falling_fast"
    elif change_20d_bps >= 25:
        state = "rising_fast"
    else:
        state = "range_bound"
    return {
        "available": True,
        "latest": _num(series.iloc[-1]),
        "change_5d_bps": _change(series, 5, multiplier=100.0),
        "change_20d_bps": change_20d_bps,
        "change_60d_bps": _change(series, 60, multiplier=100.0),
        "z_score_1y": z_score,
        "percentile_1y": _percentile(series, 252),
        "state": state,
    }


def _macro_level_state(series: Optional[pd.Series]) -> Dict[str, Any]:
    series = _clean_series(series)
    if series is None or len(series) < 2:
        return {"available": False, "state": "insufficient_data"}
    change_60d = _change(series, 60)
    if change_60d is None:
        state = "insufficient_data"
    elif change_60d > 0:
        state = "rising"
    elif change_60d < 0:
        state = "falling"
    else:
        state = "flat"
    return {
        "available": True,
        "latest": _num(series.iloc[-1]),
        "change_5d": _change(series, 5),
        "change_20d": _change(series, 20),
        "change_60d": change_60d,
        "z_score_1y": _rolling_z_score(series, 252),
        "percentile_1y": _percentile(series, 252),
        "state": state,
    }


def _curve_state(spread: Optional[pd.Series]) -> Dict[str, Any]:
    spread = _clean_series(spread)
    if spread is None or len(spread) < 2:
        return {"available": False, "state": "insufficient_data"}
    latest_bps = _num(spread.iloc[-1] * 100)
    change_20d_bps = _change(spread, 20, multiplier=100.0)
    if latest_bps is None:
        state = "insufficient_data"
    elif latest_bps < 0:
        state = "inverted"
    elif latest_bps < 50:
        state = "flat"
    else:
        state = "steep"
    return {
        "available": True,
        "latest_bps": latest_bps,
        "change_5d_bps": _change(spread, 5, multiplier=100.0),
        "change_20d_bps": change_20d_bps,
        "change_60d_bps": _change(spread, 60, multiplier=100.0),
        "z_score_1y": _rolling_z_score(spread, 252),
        "state": state,
    }


def _asset_states(market_data: Dict[str, Optional[pd.DataFrame]], tickers: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    return {
        ticker: _asset_state(_series_from_frame(market_data.get(ticker)))
        for ticker in tickers
    }


def _relationship_states(
    market_data: Dict[str, Optional[pd.DataFrame]],
    pairs: Dict[str, Tuple[str, str]],
) -> Dict[str, Dict[str, Any]]:
    states: Dict[str, Dict[str, Any]] = {}
    for name, (lhs, rhs) in pairs.items():
        lhs_series = _series_from_frame(market_data.get(lhs))
        rhs_series = _series_from_frame(market_data.get(rhs))
        state = _relationship_state(_ratio_series(lhs_series, rhs_series))
        state["lhs"] = lhs
        state["rhs"] = rhs
        states[name] = state
    return states


def _sector_state(market_data: Dict[str, Optional[pd.DataFrame]]) -> Dict[str, Dict[str, Any]]:
    spy = _series_from_frame(market_data.get("SPY"))
    sectors: Dict[str, Dict[str, Any]] = {}
    for ticker in SECTOR_TICKERS:
        series = _series_from_frame(market_data.get(ticker))
        asset = _asset_state(series)
        relative = _relationship_state(_ratio_series(series, spy))
        leadership_flag = bool(
            asset.get("available")
            and relative.get("available")
            and asset.get("return_20d") is not None
            and asset.get("return_20d") > 0
            and relative.get("return_20d") is not None
            and relative.get("return_20d") > 0
        )
        laggard_flag = bool(
            asset.get("available")
            and relative.get("available")
            and asset.get("return_20d") is not None
            and asset.get("return_20d") < 0
            and relative.get("return_20d") is not None
            and relative.get("return_20d") < 0
        )
        sectors[ticker] = {
            "absolute": asset,
            "relative_to_spy": relative,
            "leadership_flag": leadership_flag,
            "laggard_flag": laggard_flag,
        }
    return sectors


def _breadth_context(market_data: Dict[str, Optional[pd.DataFrame]]) -> Dict[str, Any]:
    tracked = list(dict.fromkeys(EQUITY_INDEX_TICKERS + SECTOR_TICKERS + TREASURY_PROXY_TICKERS + ["HYG", "LQD", "BKLN", "GLD", "UUP"]))
    states = _asset_states(market_data, tracked)
    available = {ticker: state for ticker, state in states.items() if state.get("available")}

    def count(predicate) -> int:
        return sum(1 for state in available.values() if predicate(state))

    total = len(available)
    above_50 = count(lambda state: (state.get("distance_50dma") or 0) > 0)
    above_200 = count(lambda state: (state.get("distance_200dma") or 0) > 0)
    positive_20d = count(lambda state: (state.get("return_20d") or 0) > 0)
    advancers_1d = count(lambda state: (state.get("return_1d") or 0) > 0)
    advancers_5d = count(lambda state: (state.get("return_5d") or 0) > 0)

    sector_states = {ticker: states.get(ticker, {}) for ticker in SECTOR_TICKERS}
    sectors_above_50 = sum(1 for state in sector_states.values() if state.get("available") and (state.get("distance_50dma") or 0) > 0)
    sectors_above_200 = sum(1 for state in sector_states.values() if state.get("available") and (state.get("distance_200dma") or 0) > 0)

    return {
        "method": "tracked_etf_proxy",
        "tracked_count": total,
        "above_50dma_count": above_50,
        "above_50dma_pct": _num(above_50 / total) if total else None,
        "above_200dma_count": above_200,
        "above_200dma_pct": _num(above_200 / total) if total else None,
        "positive_20d_trend_count": positive_20d,
        "positive_20d_trend_pct": _num(positive_20d / total) if total else None,
        "advancers_1d_count": advancers_1d,
        "advancers_5d_count": advancers_5d,
        "sector_count": sum(1 for state in sector_states.values() if state.get("available")),
        "sectors_above_50dma_count": sectors_above_50,
        "sectors_above_200dma_count": sectors_above_200,
        "equal_weight_vs_cap_weight": _relationship_state(
            _ratio_series(_series_from_frame(market_data.get("RSP")), _series_from_frame(market_data.get("SPY")))
        ),
    }


def _credit_context(
    fred_data: Dict[str, Optional[pd.Series]],
    market_data: Dict[str, Optional[pd.DataFrame]],
    hy_ig_gap: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    relationships = _relationship_states(
        market_data,
        {
            "hyg_lqd": ("HYG", "LQD"),
            "hyg_ief": ("HYG", "IEF"),
            "lqd_ief": ("LQD", "IEF"),
            "bkln_spy": ("BKLN", "SPY"),
        },
    )
    return {
        "ig_oas": _rate_state(fred_data.get("BAMLC0A0CM")),
        "hy_oas": _rate_state(fred_data.get("BAMLH0A0HYM2")),
        "hy_minus_ig_gap": hy_ig_gap or {"hy_ig_oas_gap": None, "hy_ig_oas_gap_method": "unavailable"},
        "loan_proxy": _asset_state(_series_from_frame(market_data.get("BKLN"))),
        "credit_etf_relationships": relationships,
        "liquidity_sensitive_proxies": {
            "hyg": _asset_state(_series_from_frame(market_data.get("HYG"))),
            "lqd": _asset_state(_series_from_frame(market_data.get("LQD"))),
            "bkln": _asset_state(_series_from_frame(market_data.get("BKLN"))),
        },
    }


def _macro_rates_context(fred_data: Dict[str, Optional[pd.Series]], market_data: Dict[str, Optional[pd.DataFrame]]) -> Dict[str, Any]:
    rates = {
        label: _rate_state(fred_data.get(series_id))
        for label, series_id in RATE_SERIES.items()
    }

    dgs2 = _clean_series(fred_data.get("DGS2"))
    dgs5 = _clean_series(fred_data.get("DGS5"))
    dgs10 = _clean_series(fred_data.get("DGS10"))
    dgs30 = _clean_series(fred_data.get("DGS30"))
    curves = {
        "2s10s": _curve_state(dgs10 - dgs2 if dgs10 is not None and dgs2 is not None else None),
        "5s30s": _curve_state(dgs30 - dgs5 if dgs30 is not None and dgs5 is not None else None),
        "10s30s": _curve_state(dgs30 - dgs10 if dgs30 is not None and dgs10 is not None else None),
    }

    return {
        "rates": rates,
        "curve_spreads": curves,
        "inflation_growth": {
            "10y_breakeven_inflation": _rate_state(fred_data.get("T10YIE")),
            "unemployment_rate": _macro_level_state(fred_data.get("UNRATE")),
            "nonfarm_payrolls": _macro_level_state(fred_data.get("PAYEMS")),
        },
        "dollar_proxy": {
            "method": "UUP ETF proxy",
            "state": _asset_state(_series_from_frame(market_data.get("UUP"))),
        },
        "real_rate_proxy": {
            "method": "10Y nominal yield minus 10Y breakeven inflation",
            "state": _rate_state(
                dgs10 - _clean_series(fred_data.get("T10YIE"))
                if dgs10 is not None and _clean_series(fred_data.get("T10YIE")) is not None
                else None
            ),
        },
    }


def _volatility_context(market_data: Dict[str, Optional[pd.DataFrame]]) -> Dict[str, Any]:
    spy = _asset_state(_series_from_frame(market_data.get("SPY")))
    qqq = _asset_state(_series_from_frame(market_data.get("QQQ")))
    tlt = _asset_state(_series_from_frame(market_data.get("TLT")))
    jpy = _asset_state(_series_from_frame(market_data.get("JPY=X")))
    vixy = _asset_state(_series_from_frame(market_data.get("VIXY")))
    return {
        "vix_proxy": {
            "method": "VIXY ETF proxy for VIX futures exposure",
            "state": vixy,
        },
        "move_proxy": {
            "method": "TLT realized volatility proxy for bond volatility",
            "state": tlt,
        },
        "realized_volatility": {
            "spy": spy,
            "qqq": qqq,
            "tlt": tlt,
            "usdjpy": jpy,
        },
        "stress_flags": {
            "equity_vol_high": spy.get("vol_state") in {"high_vol", "elevated_vol"},
            "bond_vol_high": tlt.get("vol_state") in {"high_vol", "elevated_vol"},
            "fx_vol_high": jpy.get("vol_state") in {"high_vol", "elevated_vol"},
            "vix_proxy_uptrend": vixy.get("trend_state") == "uptrend",
        },
    }


def _flight_to_safety_context(market_data: Dict[str, Optional[pd.DataFrame]]) -> Dict[str, Any]:
    relationships = _relationship_states(
        market_data,
        {
            "xlu_xly": ("XLU", "XLY"),
            "xlp_xly": ("XLP", "XLY"),
            "tlt_spy": ("TLT", "SPY"),
            "gld_spy": ("GLD", "SPY"),
            "uup_spy": ("UUP", "SPY"),
        },
    )
    defensive_states = _asset_states(market_data, DEFENSIVE_TICKERS)
    cyclical_states = _asset_states(market_data, CYCLICAL_TICKERS)
    defensive_positive = sum(1 for state in defensive_states.values() if state.get("available") and (state.get("return_20d") or 0) > 0)
    cyclical_positive = sum(1 for state in cyclical_states.values() if state.get("available") and (state.get("return_20d") or 0) > 0)
    return {
        "treasury_proxies": _asset_states(market_data, TREASURY_PROXY_TICKERS),
        "gold_proxy": _asset_state(_series_from_frame(market_data.get("GLD"))),
        "dollar_proxy": _asset_state(_series_from_frame(market_data.get("UUP"))),
        "jpy_proxy": _asset_state(_series_from_frame(market_data.get("JPY=X"))),
        "defensive_vs_cyclical": relationships,
        "rotation_summary": {
            "method": "sector_etf_proxy",
            "defensive_positive_20d_count": defensive_positive,
            "cyclical_positive_20d_count": cyclical_positive,
            "defensive_leadership_flag": defensive_positive > cyclical_positive,
        },
    }


def _positioning_context(market_data: Dict[str, Optional[pd.DataFrame]]) -> Dict[str, Any]:
    assets = _asset_states(market_data, POSITIONING_TICKERS)
    stretched = {
        ticker: {
            "rsi_14d": state.get("rsi_14d"),
            "distance_50dma": state.get("distance_50dma"),
            "distance_200dma": state.get("distance_200dma"),
            "z_score_1y": state.get("z_score_1y"),
            "drawdown_252d": state.get("drawdown_252d"),
            "stretch_state": state.get("stretch_state"),
        }
        for ticker, state in assets.items()
        if state.get("available")
    }
    relationship_stretch = {
        name: {
            "lhs": state.get("lhs"),
            "rhs": state.get("rhs"),
            "latest_ratio": state.get("latest_ratio"),
            "distance_50dma": state.get("distance_50dma"),
            "z_score_1y": state.get("z_score_1y"),
            "stretch_state": state.get("stretch_state"),
        }
        for name, state in _relationship_states(market_data, CROSS_ASSET_PAIRS).items()
        if state.get("available")
    }
    return {
        "method": "price_momentum_volatility_proxy",
        "assets": stretched,
        "relationships": relationship_stretch,
    }


def build_market_context(
    fred_data: Dict[str, Optional[pd.Series]],
    market_data: Dict[str, Optional[pd.DataFrame]],
    hy_ig_gap: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Builds the structural market-state package for downstream consumers."""
    return {
        "meta": {
            "context_version": "1.0",
            "exactness": {
                "rates": "FRED Treasury and macro series where available",
                "credit": "FRED OAS where available plus ETF proxies",
                "volatility": "ETF/realized-vol proxies; direct VIX/MOVE indices are not required",
                "breadth": "tracked ETF basket proxy, not exchange-level breadth",
                "positioning": "price/volatility stretch proxies, not institutional positioning data",
            },
        },
        "macro_rates": _macro_rates_context(fred_data, market_data),
        "credit_liquidity": _credit_context(fred_data, market_data, hy_ig_gap=hy_ig_gap),
        "equity_index_state": _asset_states(market_data, EQUITY_INDEX_TICKERS),
        "sector_state": _sector_state(market_data),
        "volatility_stress": _volatility_context(market_data),
        "flight_to_safety": _flight_to_safety_context(market_data),
        "cross_asset_relationships": _relationship_states(market_data, CROSS_ASSET_PAIRS),
        "breadth_participation": _breadth_context(market_data),
        "positioning_stretch": _positioning_context(market_data),
    }
