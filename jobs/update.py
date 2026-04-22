"""
Job orchestration for full structural updates and intraday preview updates.
"""
import logging
import math
import os
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

try:
    from ..data_sources import fred, market
    from ..indicators import credit, jpy, rates, relative_strength
    from ..scoring import score
    from ..services.delta import compute_preview_delta, compute_structural_delta
    from ..services.health import (
        PREVIEW_RULE_VERSION,
        PREVIEW_STALE_AFTER_SECONDS,
        STRUCTURAL_STALE_AFTER_SECONDS,
        build_health_snapshot,
        freshness_for_snapshot,
        snapshot_meta,
    )
    from ..services.market_context import build_market_context
    from ..storage.redis_client import RedisClient
except ImportError:  # pragma: no cover - direct script compatibility
    from data_sources import fred, market
    from indicators import credit, jpy, rates, relative_strength
    from scoring import score
    from services.delta import compute_preview_delta, compute_structural_delta
    from services.health import (
        PREVIEW_RULE_VERSION,
        PREVIEW_STALE_AFTER_SECONDS,
        STRUCTURAL_STALE_AFTER_SECONDS,
        build_health_snapshot,
        freshness_for_snapshot,
        snapshot_meta,
    )
    from services.market_context import build_market_context
    from storage.redis_client import RedisClient

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# --- Configuration ---
FRED_SERIES = {
    "BAMLC0A0CM": "BAMLC0A0CM",
    "BAMLH0A0HYM2": "BAMLH0A0HYM2",
    "DGS2": "DGS2",
    "DGS5": "DGS5",
    "DGS10": "DGS10",
    "DGS30": "DGS30",
    "T10YIE": "T10YIE",
    "UNRATE": "UNRATE",
    "PAYEMS": "PAYEMS",
}
FULL_MARKET_TICKERS = list(dict.fromkeys([
    "SPY", "QQQ", "IWM", "DIA", "RSP",
    "LQD", "HYG", "IEF", "GOVT", "SHY", "TLT", "BKLN",
    "XLF", "KRE", "XLK", "XLI", "XLE", "XLU", "XLP", "XLY", "XLV", "SMH",
    "GLD", "UUP", "VIXY", "JPY=X",
]))
PREVIEW_MARKET_TICKERS = ["BKLN", "XLF", "SPY", "JPY=X", "KRE"]
HISTORY_YEARS = 2

SNAPSHOT_COMPONENT_ORDER = [
    "ig_spreads",
    "hy_credit",
    "leveraged_loans",
    "xlf_spy",
    "kre_spy",
    "30y_yield",
    "jpy_risk",
]
SNAPSHOT_COMPONENT_LABELS = {
    "ig_spreads": "Investment Grade Credit",
    "hy_credit": "High Yield Credit",
    "leveraged_loans": "Leveraged Loans",
    "xlf_spy": "Financials",
    "kre_spy": "Regional Banks",
    "30y_yield": "30Y Treasury Yield",
    "jpy_risk": "JPY Risk",
}

PREVIEW_COMPONENT_ORDER = ["leveraged_loans", "xlf_spy", "jpy_risk", "kre_spy"]
PREVIEW_COMPONENT_LABELS = {
    "leveraged_loans": "Loans",
    "xlf_spy": "Financials",
    "jpy_risk": "JPY",
    "kre_spy": "Regional Banks",
}

FOUNDATION_SCOPE = {
    "complete_for_v1": True,
    "run_frequency": "twice_daily",
    "intended_consumers": [
        "dashboard",
        "market_sniffer",
        "advisor_bot",
        "portfolio_overlay",
        "alerts",
    ],
}

INPUT_FIELDS = {
    "ig_spreads": [
        "latest",
        "change_5d",
        "change_20d",
        "change_60d",
        "z_score_1y",
        "proxy_threshold",
    ],
    "hy_credit": [
        "latest",
        "change_5d",
        "change_20d",
        "change_60d",
        "z_score_1y",
    ],
    "leveraged_loans": [
        "latest_price",
        "price_vs_200dma",
        "drawdown_30d",
        "volatility_z_score",
    ],
    "xlf_spy": [
        "latest_ratio",
        "ratio_vs_ma50",
        "ratio_vs_ma200",
        "z_score_1y",
    ],
    "kre_spy": [
        "latest_ratio",
        "ratio_vs_ma50",
        "ratio_vs_ma200",
        "z_score_1y",
    ],
    "30y_yield": [
        "latest_yield",
        "dgs30_20d_bps",
        "change_20d_bps",
        "z_score_1y",
    ],
    "jpy_risk": [
        "latest",
        "move_5d_pct",
        "vol_percentile_1y",
        "usdjpy_20dma",
        "usdjpy_50dma",
        "level_threshold",
    ],
}


# --- Alerting ---
def fire_webhook(alert_data: Dict[str, Any]):
    """Placeholder for a webhook call."""
    webhook_url = os.getenv("ALERT_WEBHOOK_URL")
    if webhook_url:
        logging.info("Firing webhook for alert: %s", alert_data["reason"])
        # Placeholder for requests.post(webhook_url, json=alert_data)
    else:
        logging.info("Webhook URL not set, skipping webhook.")


def _parse_bool_env(name: str, default: bool = False) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    return val.strip().lower() in {"1", "true", "yes", "y", "on"}


def _make_mock_series(start_date: pd.Timestamp, end_date: pd.Timestamp, base: float, trend: float = 0.0) -> pd.Series:
    idx = pd.bdate_range(start=start_date, end=end_date)
    values = [base + trend * i for i in range(len(idx))]
    return pd.Series(values, index=idx, dtype=float)


def _make_mock_market_df(start_date: pd.Timestamp, end_date: pd.Timestamp, base: float, trend: float = 0.0) -> pd.DataFrame:
    series = _make_mock_series(start_date, end_date, base, trend)
    return pd.DataFrame({"Close": series, "Open": series, "High": series, "Low": series, "Volume": 1000.0})


def _mock_data(start_date: pd.Timestamp, end_date: pd.Timestamp) -> Tuple[Dict[str, pd.Series], Dict[str, pd.DataFrame]]:
    fred_data = {
        "BAMLC0A0CM": _make_mock_series(start_date, end_date, 1.25, 0.0002),
        "BAMLH0A0HYM2": _make_mock_series(start_date, end_date, 4.25, 0.0003),
        "DGS2": _make_mock_series(start_date, end_date, 4.6, -0.0002),
        "DGS5": _make_mock_series(start_date, end_date, 4.3, -0.00015),
        "DGS10": _make_mock_series(start_date, end_date, 4.2, -0.00012),
        "DGS30": _make_mock_series(start_date, end_date, 4.1, -0.0001),
        "T10YIE": _make_mock_series(start_date, end_date, 2.3, -0.00005),
        "UNRATE": _make_mock_series(start_date, end_date, 4.0, 0.00002),
        "PAYEMS": _make_mock_series(start_date, end_date, 158000.0, 2.5),
    }
    market_data = {
        "SPY": _make_mock_market_df(start_date, end_date, 450.0, 0.02),
        "QQQ": _make_mock_market_df(start_date, end_date, 380.0, 0.025),
        "IWM": _make_mock_market_df(start_date, end_date, 190.0, 0.005),
        "DIA": _make_mock_market_df(start_date, end_date, 350.0, 0.015),
        "RSP": _make_mock_market_df(start_date, end_date, 150.0, 0.01),
        "LQD": _make_mock_market_df(start_date, end_date, 108.0, -0.004),
        "HYG": _make_mock_market_df(start_date, end_date, 76.0, -0.004),
        "IEF": _make_mock_market_df(start_date, end_date, 96.0, 0.002),
        "GOVT": _make_mock_market_df(start_date, end_date, 23.0, 0.0004),
        "SHY": _make_mock_market_df(start_date, end_date, 81.0, 0.0001),
        "TLT": _make_mock_market_df(start_date, end_date, 90.0, 0.003),
        "BKLN": _make_mock_market_df(start_date, end_date, 20.0, -0.001),
        "XLF": _make_mock_market_df(start_date, end_date, 39.0, -0.002),
        "KRE": _make_mock_market_df(start_date, end_date, 45.0, -0.006),
        "XLK": _make_mock_market_df(start_date, end_date, 170.0, 0.025),
        "XLI": _make_mock_market_df(start_date, end_date, 110.0, 0.01),
        "XLE": _make_mock_market_df(start_date, end_date, 85.0, 0.005),
        "XLU": _make_mock_market_df(start_date, end_date, 65.0, 0.004),
        "XLP": _make_mock_market_df(start_date, end_date, 74.0, 0.004),
        "XLY": _make_mock_market_df(start_date, end_date, 175.0, 0.015),
        "XLV": _make_mock_market_df(start_date, end_date, 135.0, 0.006),
        "SMH": _make_mock_market_df(start_date, end_date, 210.0, 0.03),
        "GLD": _make_mock_market_df(start_date, end_date, 190.0, 0.006),
        "UUP": _make_mock_market_df(start_date, end_date, 29.0, 0.0008),
        "VIXY": _make_mock_market_df(start_date, end_date, 14.0, -0.002),
        "JPY=X": _make_mock_market_df(start_date, end_date, 147.0, -0.01),
    }
    return fred_data, market_data


def _safe_number(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        return round(numeric, 6) if math.isfinite(numeric) else None
    return None


def _compact_value(value: Any) -> Any:
    numeric = _safe_number(value)
    if numeric is not None:
        return numeric
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value
    return None


def _last_index_iso(data: Any) -> Optional[str]:
    if data is None or getattr(data, "empty", False):
        return None
    try:
        idx = data.index
        if len(idx) == 0:
            return None
        ts = pd.Timestamp(idx[-1])
        if ts.tzinfo is None:
            return ts.isoformat()
        return ts.tz_convert("UTC").isoformat()
    except Exception:
        return None


def _source_status(expected: List[str], data: Dict[str, Any]) -> str:
    available = [name for name in expected if data.get(name) is not None]
    if len(available) == len(expected):
        return "success"
    if available:
        return "partial"
    return "failed"


def _build_source_metadata(
    source_name: str,
    expected: List[str],
    data: Dict[str, Any],
    fetched_at_utc: str,
    item_label: str,
) -> Dict[str, Any]:
    available = [name for name in expected if data.get(name) is not None]
    missing = [name for name in expected if data.get(name) is None]
    return {
        "status": _source_status(expected, data),
        "fetched_at_utc": fetched_at_utc,
        "details": {
            item_label: expected,
            "available": available,
            "missing": missing,
            "available_count": len(available),
            "expected_count": len(expected),
            "latest_observation": {
                name: _last_index_iso(data.get(name))
                for name in expected
                if data.get(name) is not None
            },
        },
    }


def _source_warnings(sources: Dict[str, Dict[str, Any]]) -> List[str]:
    warnings = []
    for name, meta in sources.items():
        if meta.get("status") != "success":
            missing = meta.get("details", {}).get("missing", [])
            warnings.append(f"{name} source status={meta.get('status')} missing={','.join(missing)}")
    return warnings


def _extract_inputs(indicators: Dict[str, Dict[str, Any]], output: Dict[str, Any]) -> Dict[str, Any]:
    inputs: Dict[str, Any] = {}
    for component, fields in INPUT_FIELDS.items():
        indicator = indicators.get(component) or {}
        values = {
            field: _compact_value(indicator.get(field))
            for field in fields
            if _compact_value(indicator.get(field)) is not None
        }
        if values:
            inputs[component] = values

    top_level = {
        "hy_ig_oas_gap": _compact_value(output.get("hy_ig_oas_gap")),
        "missing_components": output.get("missing_components", 0),
    }
    inputs["cross_component"] = {
        key: value
        for key, value in top_level.items()
        if value is not None
    }
    return inputs


def _run_config(mode: str) -> Dict[str, Any]:
    return {
        "mode": mode,
        "history_years": HISTORY_YEARS,
        "full_market_tickers": FULL_MARKET_TICKERS if mode == "structural" else None,
        "preview_market_tickers": PREVIEW_MARKET_TICKERS if mode == "preview" else None,
        "fred_series": FRED_SERIES if mode == "structural" else None,
        "freshness": {
            "structural_stale_after_seconds": STRUCTURAL_STALE_AFTER_SECONDS,
            "preview_stale_after_seconds": PREVIEW_STALE_AFTER_SECONDS,
        },
        "scoring": score.get_scoring_config(),
        "confirmation": {
            "usdjpy_confirm_level": float(os.getenv("USDJPY_CONFIRM_LEVEL", "145")),
        },
    }


def _build_components(
    indicators: Dict[str, Dict[str, Any]],
    component_subscores: Dict[str, Optional[int]],
    component_states: Dict[str, Dict[str, str]],
    reason_map: Dict[str, str],
) -> Dict[str, Dict[str, Any]]:
    return {
        component: {
            "id": component,
            "label": SNAPSHOT_COMPONENT_LABELS.get(component, component),
            "subscore": component_subscores.get(component),
            "state": component_states.get(component),
            "reason": reason_map.get(component),
            "indicator": indicators.get(component, {"name": component, "data_missing": True}),
        }
        for component in SNAPSHOT_COMPONENT_ORDER
    }


def _run_status(status: str, warnings: Optional[List[str]] = None, errors: Optional[List[str]] = None) -> Dict[str, Any]:
    return {
        "status": status,
        "warnings": warnings or [],
        "errors": errors or [],
    }


def _preview_session(now: datetime) -> Dict[str, Any]:
    if ZoneInfo is None:
        et = now.astimezone(timezone.utc)
    else:
        et = now.astimezone(ZoneInfo("America/New_York"))

    minutes = et.hour * 60 + et.minute
    is_weekday = et.weekday() < 5
    regular_open = 9 * 60 + 30
    regular_close = 16 * 60

    if not is_weekday:
        market_session = "closed"
    elif minutes < regular_open:
        market_session = "pre_market"
    elif minutes < regular_close:
        market_session = "regular"
    else:
        market_session = "after_hours"

    return {
        "market_session": market_session,
        "market_open": market_session == "regular",
        "computed_at_et": et.isoformat(),
    }


def _eastern_time(now: datetime) -> datetime:
    if ZoneInfo is None:
        return now.astimezone(timezone.utc)
    return now.astimezone(ZoneInfo("America/New_York"))


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    day = datetime(year, month, 1).date()
    while day.weekday() != weekday:
        day += timedelta(days=1)
    return day + timedelta(days=7 * (n - 1))


def _is_market_half_day(market_date: date) -> bool:
    thanksgiving = _nth_weekday(market_date.year, 11, 3, 4)
    black_friday = thanksgiving + timedelta(days=1)
    christmas_eve = datetime(market_date.year, 12, 24).date()
    july_3 = datetime(market_date.year, 7, 3).date()
    return market_date in {black_friday, christmas_eve, july_3} and market_date.weekday() < 5


def _previous_trading_day(market_date: date) -> date:
    previous = market_date - timedelta(days=1)
    while previous.weekday() >= 5:
        previous -= timedelta(days=1)
    return previous


def _market_datetime_utc(market_date: date, hour: int, minute: int) -> str:
    if ZoneInfo is None:
        dt = datetime(market_date.year, market_date.month, market_date.day, hour, minute, tzinfo=timezone.utc)
    else:
        dt = datetime(
            market_date.year,
            market_date.month,
            market_date.day,
            hour,
            minute,
            tzinfo=ZoneInfo("America/New_York"),
        )
    return dt.astimezone(timezone.utc).isoformat()


def _market_time_context(now: datetime) -> Dict[str, Any]:
    et = _eastern_time(now)
    market_date = et.date()
    is_trading_day = et.weekday() < 5
    is_half_day = _is_market_half_day(market_date) if is_trading_day else False
    close_hour = 13 if is_half_day else 16
    close_minute = 0
    open_utc = _market_datetime_utc(market_date, 9, 30) if is_trading_day else None
    current_close_utc = _market_datetime_utc(market_date, close_hour, close_minute) if is_trading_day else None

    open_dt = _parse_iso(open_utc) if open_utc else None
    close_dt = _parse_iso(current_close_utc) if current_close_utc else None
    if is_trading_day and close_dt is not None and now >= close_dt:
        previous_close_utc = current_close_utc
        data_cutoff_utc = current_close_utc
    elif is_trading_day and open_dt is not None and close_dt is not None and open_dt <= now < close_dt:
        previous_close_utc = _market_datetime_utc(_previous_trading_day(market_date), 16, 0)
        data_cutoff_utc = now.isoformat()
    else:
        previous_close_utc = _market_datetime_utc(_previous_trading_day(market_date), 16, 0)
        data_cutoff_utc = previous_close_utc if not is_trading_day else now.isoformat()

    return {
        "market_date": market_date.isoformat(),
        "is_trading_day": is_trading_day,
        "is_half_day": is_half_day,
        "previous_close_utc": previous_close_utc,
        "current_session_open_utc": open_utc,
        "data_cutoff_utc": data_cutoff_utc,
    }


def _source_missing_count(sources: Dict[str, Dict[str, Any]]) -> int:
    return sum(len(meta.get("details", {}).get("missing", []) or []) for meta in sources.values())


def _data_quality(
    sources: Dict[str, Dict[str, Any]],
    critical_by_source: Dict[str, List[str]],
) -> Dict[str, Any]:
    expected_total = 0
    available_total = 0
    missing_critical = False
    critical_missing_items: List[str] = []
    missing_noncritical_count = 0

    for source_name, meta in sources.items():
        details = meta.get("details", {})
        expected = details.get("series") or details.get("tickers") or []
        available = set(details.get("available") or [])
        missing = set(details.get("missing") or [])
        critical = set(critical_by_source.get(source_name, []))
        expected_total += len(expected)
        available_total += len(available)
        missing_critical_items = sorted(missing & critical)
        if missing_critical_items:
            missing_critical = True
            critical_missing_items.extend([f"{source_name}:{item}" for item in missing_critical_items])
        missing_noncritical_count += len(missing - critical)

    completeness = (available_total / expected_total) if expected_total else 0.0
    confidence_adjustment = 0.0
    if missing_critical:
        confidence_adjustment -= 0.15
    confidence_adjustment -= min(0.25, missing_noncritical_count * 0.025)

    return {
        "score": round(max(0.0, min(1.0, completeness + confidence_adjustment)), 4),
        "raw_completeness": round(completeness, 4),
        "missing_critical": missing_critical,
        "missing_critical_items": critical_missing_items,
        "missing_noncritical_count": missing_noncritical_count,
        "confidence_adjustment": round(confidence_adjustment, 4),
    }


def _market_context_value(market_context: Dict[str, Any], path: List[str]) -> Any:
    value: Any = market_context
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def _signal_summary(trigger_states: Dict[str, bool], market_context: Dict[str, Any]) -> Dict[str, List[str]]:
    dominant: List[str] = []
    secondary: List[str] = []
    ignored: List[str] = []

    if trigger_states.get("ig_widening") or trigger_states.get("hy_widening") or trigger_states.get("loans_below_200dma"):
        dominant.append("credit_weakness")
    else:
        ignored.append("credit_stable")

    if trigger_states.get("xlf_breakdown"):
        dominant.append("financials_breakdown")
    else:
        ignored.append("financials_stable")

    if trigger_states.get("jpy_confirmed"):
        dominant.append("jpy_deleveraging")
    if trigger_states.get("dgs30_sharp_move"):
        secondary.append("rates_shock")

    breadth = _market_context_value(market_context, ["breadth_participation", "above_50dma_pct"])
    if isinstance(breadth, (int, float)) and breadth < 0.4:
        secondary.append("breadth_weakness")
    elif isinstance(breadth, (int, float)) and breadth > 0.6:
        ignored.append("breadth_supportive")

    stress_flags = _market_context_value(market_context, ["volatility_stress", "stress_flags"]) or {}
    if any(stress_flags.get(key) for key in ("equity_vol_high", "bond_vol_high", "fx_vol_high", "vix_proxy_uptrend")):
        secondary.append("volatility_rising")
    else:
        ignored.append("volatility_normal")

    if not trigger_states.get("dgs30_sharp_move"):
        ignored.append("macro_rates_stable")

    return {
        "dominant_factors": dominant,
        "secondary_factors": secondary,
        "ignored_factors": ignored,
    }


def _regime_history_summary(current_regime: str, history_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not history_data:
        return {
            "current_regime_duration_days": None,
            "previous_regime": None,
            "regime_changes_last_30d": 0,
        }

    newest_first = history_data[:30]
    current_duration = 0
    previous_regime = None
    for item in newest_first:
        regime = str(item.get("regime", "Unknown"))
        if regime == current_regime and previous_regime is None:
            current_duration += 1
        elif previous_regime is None:
            previous_regime = regime

    oldest_first = list(reversed(newest_first))
    changes = 0
    last_regime = None
    for item in oldest_first:
        regime = str(item.get("regime", "Unknown"))
        if last_regime is not None and regime != last_regime:
            changes += 1
        last_regime = regime

    return {
        "current_regime_duration_days": current_duration or None,
        "previous_regime": previous_regime,
        "regime_changes_last_30d": changes,
    }


def _anomaly_flags(trigger_states: Dict[str, bool], market_context: Dict[str, Any]) -> Dict[str, bool]:
    equity_states = _market_context_value(market_context, ["equity_index_state"]) or {}
    relationships = _market_context_value(market_context, ["cross_asset_relationships"]) or {}
    stress_flags = _market_context_value(market_context, ["volatility_stress", "stress_flags"]) or {}
    extreme_equity_move = any(
        isinstance(state, dict)
        and (
            abs(state.get("return_1d") or 0) >= 0.03
            or abs(state.get("return_5d") or 0) >= 0.06
            or abs(state.get("z_score_1y") or 0) >= 2.5
        )
        for state in equity_states.values()
    )
    extreme_relationship = any(
        isinstance(state, dict) and abs(state.get("z_score_1y") or 0) >= 2.0
        for state in relationships.values()
    )
    qqq = relationships.get("qqq_spy", {}) if isinstance(relationships, dict) else {}
    iwm = relationships.get("iwm_spy", {}) if isinstance(relationships, dict) else {}
    credit_weak = trigger_states.get("ig_widening") or trigger_states.get("hy_widening") or trigger_states.get("loans_below_200dma")
    multi_asset_divergence = bool(
        (qqq.get("state") == "outperforming" and iwm.get("state") == "underperforming")
        or (credit_weak and not trigger_states.get("xlf_breakdown"))
        or extreme_relationship
    )
    volatility_spike = any(stress_flags.get(key) for key in ("equity_vol_high", "bond_vol_high", "fx_vol_high", "vix_proxy_uptrend"))
    return {
        "extreme_move_detected": bool(extreme_equity_move or trigger_states.get("dgs30_sharp_move")),
        "multi_asset_divergence": multi_asset_divergence,
        "volatility_spike": bool(volatility_spike),
    }


def _state_confidence(
    confidence_label: str,
    data_quality: Dict[str, Any],
    signal_summary: Dict[str, List[str]],
    anomaly_flags: Dict[str, bool],
) -> Dict[str, Any]:
    base = {"HIGH": 0.9, "MED": 0.75, "LOW": 0.55}.get(confidence_label.upper(), 0.65)
    score_val = base + float(data_quality.get("confidence_adjustment", 0.0))
    drivers: List[str] = []
    penalties: List[str] = []

    if "credit_weakness" in signal_summary.get("dominant_factors", []):
        drivers.append("credit_confirmed")
    if "financials_breakdown" in signal_summary.get("dominant_factors", []):
        drivers.append("equity_financials_confirmed")
    if not signal_summary.get("dominant_factors"):
        drivers.append("signals_orderly")

    if data_quality.get("missing_critical"):
        penalties.append("missing_critical_data")
    elif data_quality.get("missing_noncritical_count", 0):
        penalties.append("missing_noncritical_data")
    if anomaly_flags.get("volatility_spike"):
        penalties.append("high_volatility_environment")
        score_val -= 0.05
    if not signal_summary.get("dominant_factors") and signal_summary.get("secondary_factors"):
        penalties.append("mixed_or_secondary_only_signals")
        score_val -= 0.03

    return {
        "score": round(max(0.0, min(1.0, score_val)), 4),
        "label": confidence_label.upper(),
        "drivers": drivers,
        "penalties": penalties,
    }


def _headline_summary(signal_summary: Dict[str, List[str]], anomaly_flags: Dict[str, bool]) -> Dict[str, str]:
    dominant = signal_summary.get("dominant_factors", [])
    secondary = signal_summary.get("secondary_factors", [])
    parts = []
    if "credit_weakness" in dominant:
        parts.append("credit weakening")
    if "financials_breakdown" in dominant:
        parts.append("financials breaking down")
    if "volatility_rising" in secondary or anomaly_flags.get("volatility_spike"):
        parts.append("volatility rising")
    if not parts:
        parts.append("major signals orderly")

    if "credit_weakness" in dominant and anomaly_flags.get("volatility_spike"):
        risk_bias = "downside_risk_building"
    elif dominant:
        risk_bias = "risk_watch"
    elif secondary:
        risk_bias = "mixed"
    else:
        risk_bias = "neutral"
    return {
        "one_line": ", ".join(parts),
        "risk_bias": risk_bias,
    }


def _build_indicator_reasons(indicators: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    reasons: Dict[str, str] = {}

    ig = indicators.get("ig_spreads") or {}
    if not ig.get("data_missing"):
        quality = ig.get("ig_data_quality", "unknown")
        reasons["ig_spreads"] = f"IG z={ig.get('z_score_1y', 0.0):+.2f}; quality={quality}"

    ll = indicators.get("leveraged_loans") or {}
    if not ll.get("data_missing"):
        reasons["leveraged_loans"] = (
            f"BKLN vs 200DMA {ll.get('price_vs_200dma', 0.0) * 100:+.1f}% "
            f"and 30d drawdown {ll.get('drawdown_30d', 0.0) * 100:+.1f}%"
        )

    xlf = indicators.get("xlf_spy") or {}
    if not xlf.get("data_missing"):
        breakdown_txt = "below" if xlf.get("breakdown_flag") else "above"
        reasons["xlf_spy"] = (
            f"XLF/SPY z={xlf.get('z_score_1y', 0.0):+.2f}; "
            f"{breakdown_txt} 200DMA"
        )

    y30 = indicators.get("30y_yield") or {}
    if not y30.get("data_missing"):
        reasons["30y_yield"] = (
            f"DGS30 {y30.get('dgs30_20d_bps', 0.0):+.1f} bps/20d "
            f"({y30.get('dgs30_signal', 'neutral')})"
        )

    jpy_ind = indicators.get("jpy_risk") or {}
    if not jpy_ind.get("data_missing"):
        conf = "confirmed" if jpy_ind.get("jpy_confirmed") else "unconfirmed"
        reasons["jpy_risk"] = (
            f"USDJPY {jpy_ind.get('move_5d_pct', 0.0):+.2f}%/5d, "
            f"vol {jpy_ind.get('vol_percentile_1y', 0.0):.0f}pctl ({conf})"
        )

    hy = indicators.get("hy_credit") or {}
    if not hy.get("data_missing"):
        reasons["hy_credit"] = f"HY z={hy.get('z_score_1y', 0.0):+.2f} from {hy.get('data_source', 'unknown')}"

    kre = indicators.get("kre_spy") or {}
    if not kre.get("data_missing"):
        reasons["kre_spy"] = (
            f"KRE/SPY z={kre.get('z_score_1y', 0.0):+.2f}; "
            f"{'below' if kre.get('breakdown_flag') else 'above'} 200DMA"
        )

    return reasons


def _score_confidence(missing_components: int, ig_data_quality: str) -> Dict[str, Any]:
    if missing_components >= 3 or ig_data_quality == "proxy_disagreement":
        return {"score_confidence": "LOW", "composite_reliable": False}
    if missing_components == 2:
        return {"score_confidence": "MED", "composite_reliable": True}
    return {"score_confidence": "HIGH", "composite_reliable": True}


def _to_int_score(value: Any) -> Optional[int]:
    return int(round(float(value))) if isinstance(value, (int, float)) else None


def _monitor_pseudo_score(component: str, indicator: Dict[str, Any]) -> Optional[int]:
    if not indicator or indicator.get("data_missing"):
        return None
    z_score = indicator.get("z_score_1y")
    breakdown = bool(indicator.get("breakdown_flag"))
    if component == "hy_credit":
        if not isinstance(z_score, (int, float)):
            return None
        if z_score >= 2.0:
            return 90
        if z_score >= 1.0:
            return 70
        if z_score >= 0.25:
            return 45
        return 20

    if component == "kre_spy":
        if breakdown or (isinstance(z_score, (int, float)) and z_score <= -2.0):
            return 90
        if isinstance(z_score, (int, float)) and z_score <= -1.0:
            return 70
        if isinstance(z_score, (int, float)) and z_score <= -0.25:
            return 45
        return 20
    return None


def _component_score_for_snapshot(
    component: str,
    subscores: Dict[str, Dict[str, Any]],
    indicators: Dict[str, Dict[str, Any]],
) -> Optional[int]:
    sub = _to_int_score(subscores.get(component, {}).get("score"))
    if sub is not None:
        return sub
    return _monitor_pseudo_score(component, indicators.get(component, {}))


def _state_from_score(score_val: Optional[int]) -> Dict[str, str]:
    if score_val is None:
        return {"level": "no_data", "text": "No Data"}
    if score_val <= 35:
        return {"level": "stable", "text": "Stable"}
    if score_val <= 70:
        return {"level": "watch", "text": "Watch"}
    return {"level": "breakdown", "text": "Breakdown"}


def _preview_state_from_score(score_val: Optional[int], available: bool) -> Dict[str, str]:
    if not available:
        return {"level": "unavailable", "text": "Unavailable"}
    return _state_from_score(score_val)


def _trigger_states(
    indicators: Dict[str, Dict[str, Any]],
    component_subscores: Dict[str, Optional[int]],
) -> Dict[str, bool]:
    ig_z = indicators.get("ig_spreads", {}).get("z_score_1y")
    hy_z = indicators.get("hy_credit", {}).get("z_score_1y")
    loans_vs_200dma = indicators.get("leveraged_loans", {}).get("price_vs_200dma")
    dgs30_20d_bps = indicators.get("30y_yield", {}).get("dgs30_20d_bps")

    ig_widening = bool((isinstance(ig_z, (int, float)) and ig_z >= 1.0) or (component_subscores.get("ig_spreads") or 0) >= 60)
    hy_widening = bool((isinstance(hy_z, (int, float)) and hy_z >= 1.0) or (component_subscores.get("hy_credit") or 0) >= 70)
    loans_below_200dma = bool(
        (isinstance(loans_vs_200dma, (int, float)) and loans_vs_200dma < 0)
        or (component_subscores.get("leveraged_loans") or 0) >= 60
    )

    return {
        "ig_widening": ig_widening,
        "hy_widening": hy_widening,
        "loans_below_200dma": loans_below_200dma,
        "jpy_confirmed": bool(indicators.get("jpy_risk", {}).get("jpy_confirmed")),
        "xlf_breakdown": bool(indicators.get("xlf_spy", {}).get("breakdown_flag"))
        or (component_subscores.get("xlf_spy") or 0) >= 80,
        "dgs30_sharp_move": bool(isinstance(dgs30_20d_bps, (int, float)) and abs(dgs30_20d_bps) >= 25.0),
    }


def _build_run_snapshot(
    run_id: str,
    computed_at_utc: str,
    output: Dict[str, Any],
    reason_map: Dict[str, str],
    sources: Dict[str, Dict[str, Any]],
    previous_snapshot: Optional[Dict[str, Any]],
    market_context: Optional[Dict[str, Any]],
    market_time_context: Dict[str, Any],
    data_quality: Dict[str, Any],
    regime_history_summary: Dict[str, Any],
    execution: Dict[str, Optional[float]],
) -> Dict[str, Any]:
    indicators = output.get("indicators", {})
    subscores = output.get("subscores", {})

    component_subscores: Dict[str, Optional[int]] = {}
    component_states: Dict[str, Dict[str, str]] = {}
    for component in SNAPSHOT_COMPONENT_ORDER:
        score_val = _component_score_for_snapshot(component, subscores, indicators)
        component_subscores[component] = score_val
        component_states[component] = _state_from_score(score_val)

    trigger_states = _trigger_states(indicators, component_subscores)
    components = _build_components(indicators, component_subscores, component_states, reason_map)
    context = market_context or {}
    signal_summary = _signal_summary(trigger_states, context)
    anomaly_flags = _anomaly_flags(trigger_states, context)
    state_confidence = _state_confidence(
        confidence_label=str(output.get("score_confidence", "N/A")).upper(),
        data_quality=data_quality,
        signal_summary=signal_summary,
        anomaly_flags=anomaly_flags,
    )

    scored_components = [
        (name, score_val)
        for name, score_val in component_subscores.items()
        if isinstance(score_val, int)
    ]
    scored_components.sort(key=lambda row: row[1], reverse=True)
    primary_drivers = [
        SNAPSHOT_COMPONENT_LABELS.get(name, name)
        for name, score_val in scored_components
        if score_val >= 45
    ][:3]

    warnings = _source_warnings(sources)
    missing_components = output.get("missing_components", 0)
    if missing_components:
        warnings.append(f"{missing_components} structural scoring component(s) missing")
    status = "partial" if warnings else "success"

    snapshot = {
        "run_id": run_id,
        "computed_at_utc": computed_at_utc,
        "mode": "structural",
        "foundation_scope": FOUNDATION_SCOPE,
        "market_time_context": market_time_context,
        "data_quality": data_quality,
        "signal_summary": signal_summary,
        "regime_history_summary": regime_history_summary,
        "anomaly_flags": anomaly_flags,
        "execution": execution,
        "run_status": _run_status(status=status, warnings=warnings),
        "sources": sources,
        "inputs": _extract_inputs(indicators, output),
        "config": _run_config("structural"),
        "composite_score": _to_int_score(output.get("composite_score")) or 0,
        "regime": str(output.get("regime", "Unknown")),
        "regime_label": str(output.get("regime", "Unknown")).upper().replace(" ", "-"),
        "confidence": str(output.get("score_confidence", "N/A")).upper(),
        "state_confidence": state_confidence,
        "headline_summary": _headline_summary(signal_summary, anomaly_flags),
        "reliable": bool(output.get("composite_reliable", False)),
        "primary_drivers": primary_drivers,
        "component_subscores": component_subscores,
        "component_states": component_states,
        "components": components,
        "trigger_states": trigger_states,
        "component_reasons": reason_map,
        "subscores": subscores,
        "indicators": indicators,
        "market_context": context,
        "missing_components": missing_components,
        "history_key": RedisClient.HISTORY_KEY,
        "delta": {},
        "meta": snapshot_meta(),
        "is_stale": False,
        "stale_reason": None,
    }
    snapshot["delta"] = compute_structural_delta(snapshot, previous_snapshot)
    freshness = freshness_for_snapshot(snapshot, "structural")
    snapshot["is_stale"] = freshness["is_stale"]
    snapshot["stale_reason"] = freshness["stale_reason"]
    return snapshot


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts)
    except ValueError:
        return None


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _fetch_market_with_retry(
    tickers: List[str],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    retries: int = 1,
    backoff_seconds: float = 0.75,
) -> Dict[str, Optional[pd.DataFrame]]:
    max_attempts = max(1, int(retries))
    merged: Dict[str, Optional[pd.DataFrame]] = {ticker: None for ticker in tickers}

    for attempt in range(1, max_attempts + 1):
        needed = [ticker for ticker in tickers if merged.get(ticker) is None]
        if not needed:
            break
        batch = market.get_market_data(needed, start_date, end_date)
        for ticker in needed:
            if batch.get(ticker) is not None:
                merged[ticker] = batch[ticker]

        if attempt < max_attempts and any(merged.get(ticker) is None for ticker in tickers):
            sleep_s = max(0.1, backoff_seconds * attempt)
            logging.warning("Market fetch retry %s/%s in %.1fs for missing tickers.", attempt, max_attempts, sleep_s)
            time.sleep(sleep_s)
    return merged


def _preview_assessment(component_states: Dict[str, Dict[str, str]], jpy_confirmed: bool) -> str:
    loans_level = component_states.get("leveraged_loans", {}).get("level")
    financials_level = component_states.get("xlf_spy", {}).get("level")
    jpy_level = component_states.get("jpy_risk", {}).get("level")

    if "unavailable" in {loans_level, financials_level, jpy_level}:
        return "Early spillover: watch credit"

    loans_stable = loans_level == "stable"
    loans_weak = loans_level in {"watch", "breakdown"}
    financials_stable = financials_level == "stable"
    financials_breakdown = financials_level == "breakdown"

    if jpy_confirmed and (loans_weak or financials_breakdown):
        return "Deleveraging risk rising"
    if loans_stable and financials_stable and not jpy_confirmed:
        return "Contained to equities"
    if (loans_weak or financials_breakdown) and not jpy_confirmed:
        return "Early spillover: watch credit"
    return "Contained to equities"


def _preview_component_payload(
    component: str,
    indicator: Optional[Dict[str, Any]],
    score_val: Optional[int],
    status: Optional[str],
) -> Dict[str, Any]:
    available = bool(indicator and not indicator.get("data_missing"))
    state = _preview_state_from_score(score_val, available=available)
    return {
        "id": component,
        "label": PREVIEW_COMPONENT_LABELS.get(component, component),
        "subscore": score_val if available else None,
        "status": status if available else "Unavailable",
        "state": state,
        "available": available,
        "indicator": indicator if indicator else {"name": component, "data_missing": True},
    }


def check_alerts(
    composite_score: int,
    subscores: Dict[str, Dict[str, Any]],
    indicator_values: Dict[str, Any],
    redis: RedisClient,
):
    """Evaluates and fires alerts based on current data."""
    reasons: List[str] = []
    now = _utc_now()
    last_alert = redis.get_last_alert() or {}
    last_score = last_alert.get("score_at_alert")
    cooldown_until_dt = _parse_iso(last_alert.get("cooldown_until"))
    is_active = bool(last_alert.get("active", False))

    if composite_score <= 60 and is_active:
        cleared_alert = dict(last_alert)
        cleared_alert["active"] = False
        cleared_alert["cleared_at"] = now.isoformat()
        redis.write_alert(cleared_alert)

    if composite_score >= 70:
        reasons.append(f"Composite Score reached {composite_score} (>= 70)")

    high_subscores = [name for name, data in subscores.items() if data["score"] >= 80]
    if len(high_subscores) >= 2:
        reasons.append(f"2+ components have subscore >= 80: {', '.join(high_subscores)}")

    ig_stress_high = subscores.get("ig_spreads", {}).get("score", 0) >= 80
    xlf_breakdown = indicator_values.get("xlf_spy", {}).get("breakdown_flag", False)
    if ig_stress_high and xlf_breakdown:
        reasons.append("IG Spread subscore >= 80 AND XLF/SPY is in breakdown")

    if not reasons:
        return

    score_jump_override = isinstance(last_score, (int, float)) and composite_score >= (last_score + 10)
    in_cooldown = cooldown_until_dt is not None and now < cooldown_until_dt
    if in_cooldown and not score_jump_override:
        logging.info("Alert conditions met, but cooldown is active.")
        return

    alert_data = {
        "timestamp": now.isoformat(),
        "reason": reasons[0],
        "reasons": reasons,
        "composite_score": composite_score,
        "score_at_alert": composite_score,
        "cooldown_until": (now + timedelta(hours=24)).isoformat(),
        "subscores": subscores,
        "active": True,
    }
    redis.write_alert(alert_data)
    fire_webhook(alert_data)


def run_full_update() -> Optional[str]:
    logging.info("Starting Macro Stress Monitor structural update job.")
    job_start = time.perf_counter()

    try:
        redis = RedisClient()
    except Exception:
        logging.error("Failed to connect to Redis. Aborting structural job.")
        return None

    end_date = pd.Timestamp.now()
    start_date = end_date - pd.DateOffset(years=HISTORY_YEARS)

    no_network_mock = _parse_bool_env("MSM_NO_NETWORK_MOCK", default=False)
    sources_called: List[str] = []

    fetch_start = time.perf_counter()
    if no_network_mock:
        logging.info("MSM_NO_NETWORK_MOCK enabled. Using synthetic local data.")
        fred_data, market_data = _mock_data(start_date, end_date)
        sources_called.extend(["mock:fred", "mock:polygon"])
    else:
        logging.info("Fetching full source set: FRED + Polygon")
        fred_data = fred.get_fred_series(FRED_SERIES, start_date)
        market_data = _fetch_market_with_retry(
            FULL_MARKET_TICKERS,
            start_date,
            end_date,
            retries=2,
            backoff_seconds=0.75,
        )
        sources_called.extend([f"fred:{series}" for series in FRED_SERIES])
        sources_called.extend([f"polygon:{ticker}" for ticker in FULL_MARKET_TICKERS])
    fetch_duration = time.perf_counter() - fetch_start
    fetched_at_utc = _utc_now().isoformat()
    sources = {
        "fred" if not no_network_mock else "mock_fred": _build_source_metadata(
            "fred" if not no_network_mock else "mock_fred",
            list(FRED_SERIES.keys()),
            fred_data,
            fetched_at_utc,
            "series",
        ),
        "polygon" if not no_network_mock else "mock_polygon": _build_source_metadata(
            "polygon" if not no_network_mock else "mock_polygon",
            FULL_MARKET_TICKERS,
            market_data,
            fetched_at_utc,
            "tickers",
        ),
    }
    data_quality = _data_quality(
        sources=sources,
        critical_by_source={
            "fred": ["BAMLC0A0CM", "BAMLH0A0HYM2", "DGS10", "DGS30"],
            "mock_fred": ["BAMLC0A0CM", "BAMLH0A0HYM2", "DGS10", "DGS30"],
            "polygon": ["SPY", "HYG", "LQD", "BKLN", "XLF", "JPY=X"],
            "mock_polygon": ["SPY", "HYG", "LQD", "BKLN", "XLF", "JPY=X"],
        },
    )

    compute_start = time.perf_counter()
    ig_proxy_data = {
        "LQD": market_data.get("LQD"),
        "IEF": market_data.get("IEF"),
        "GOVT": market_data.get("GOVT"),
        "SHY": market_data.get("SHY"),
    }
    hy_proxy_data = {
        "HYG": market_data.get("HYG"),
        "IEF": market_data.get("IEF"),
        "SHY": market_data.get("SHY"),
    }

    logging.info("Computing structural indicators...")
    ig_indicator = credit.calculate_ig_spread_indicator(fred_data.get("BAMLC0A0CM"), ig_proxy_data)
    hy_indicator = credit.calculate_hy_credit_indicator(fred_data.get("BAMLH0A0HYM2"), hy_proxy_data)
    hy_ig_gap = credit.calculate_hy_ig_gap(hy_indicator, ig_indicator)
    market_context = build_market_context(
        fred_data=fred_data,
        market_data=market_data,
        hy_ig_gap=hy_ig_gap,
    )

    indicators = {
        "ig_spreads": ig_indicator,
        "hy_credit": hy_indicator,
        "leveraged_loans": credit.calculate_leveraged_loan_indicator(market_data.get("BKLN")),
        "xlf_spy": relative_strength.calculate_xlf_spy_indicator(market_data.get("XLF"), market_data.get("SPY")),
        "kre_spy": relative_strength.calculate_kre_spy_indicator(market_data.get("KRE"), market_data.get("SPY")),
        "30y_yield": rates.calculate_30y_yield_indicator(fred_data.get("DGS30")),
        "jpy_risk": jpy.calculate_jpy_risk_indicator(market_data.get("JPY=X")),
    }

    logging.info("Computing structural subscores...")
    subscore_funcs = {
        "ig_spreads": score.calculate_ig_spread_subscore,
        "leveraged_loans": score.calculate_leveraged_loan_subscore,
        "xlf_spy": score.calculate_xlf_spy_subscore,
        "30y_yield": score.calculate_30y_yield_subscore,
        "jpy_risk": score.calculate_jpy_risk_subscore,
    }

    subscores: Dict[str, Dict[str, Any]] = {}
    missing_components = 0
    for name, func in subscore_funcs.items():
        indicator = indicators.get(name)
        score_val, status = func(indicator)
        subscores[name] = {"score": score_val, "status": status}
        if indicator is None:
            indicators[name] = {"name": name, "data_missing": True}
            missing_components += 1
            logging.warning("Component '%s' has missing data, subscore is 0.", name)

    logging.info("Computing structural composite score...")
    composite_score, regime = score.get_composite_score({k: v["score"] for k, v in subscores.items()})

    for extra_key in ("hy_credit", "kre_spy"):
        if indicators.get(extra_key) is None:
            indicators[extra_key] = {"name": extra_key, "data_missing": True}

    reason_map = _build_indicator_reasons(indicators)
    for key, reason in reason_map.items():
        if indicators.get(key) and not indicators[key].get("data_missing"):
            indicators[key]["reason"] = reason

    confidence = _score_confidence(
        missing_components=missing_components,
        ig_data_quality=indicators.get("ig_spreads", {}).get("ig_data_quality", "unknown"),
    )

    now = _utc_now()
    today_str = now.strftime("%Y-%m-%d")
    output = {
        "timestamp": now.isoformat(),
        "date": today_str,
        "composite_score": composite_score,
        "regime": regime,
        "subscores": subscores,
        "indicators": indicators,
        "market_context": market_context,
        "dgs30_20d_bps": indicators.get("30y_yield", {}).get("dgs30_20d_bps"),
        "dgs30_signal": indicators.get("30y_yield", {}).get("dgs30_signal"),
        "ig_data_quality": indicators.get("ig_spreads", {}).get("ig_data_quality"),
        "usdjpy_20dma": indicators.get("jpy_risk", {}).get("usdjpy_20dma"),
        "jpy_confirmed": indicators.get("jpy_risk", {}).get("jpy_confirmed"),
        "hy_ig_oas_gap": hy_ig_gap.get("hy_ig_oas_gap"),
        "hy_ig_oas_gap_method": hy_ig_gap.get("hy_ig_oas_gap_method"),
        "score_confidence": confidence["score_confidence"],
        "composite_reliable": confidence["composite_reliable"],
        "missing_components": missing_components,
    }

    logging.info("Writing structural latest/history to Redis. Composite Score: %s (%s)", composite_score, regime)
    write_start = time.perf_counter()
    redis.write_data(output, today_str)
    history_data = redis.get_history()

    logging.info("Checking alerts for structural mode...")
    check_alerts(composite_score, subscores, indicators, redis)

    run_id = now.strftime("structural_%Y%m%dT%H%M%S.%fZ")
    previous_snapshot = redis.get_latest_structural_snapshot()
    compute_duration = time.perf_counter() - compute_start
    execution = {
        "duration_seconds": None,
        "fetch_duration": round(fetch_duration, 4),
        "compute_duration": round(compute_duration, 4),
        "write_duration": None,
    }
    snapshot = _build_run_snapshot(
        run_id=run_id,
        computed_at_utc=now.isoformat(),
        output=output,
        reason_map=reason_map,
        sources=sources,
        previous_snapshot=previous_snapshot,
        market_context=market_context,
        market_time_context=_market_time_context(now),
        data_quality=data_quality,
        regime_history_summary=_regime_history_summary(regime, history_data),
        execution=execution,
    )
    redis.write_structural_run_snapshot(run_id=run_id, snapshot=snapshot)
    execution["write_duration"] = round(time.perf_counter() - write_start, 4)
    execution["duration_seconds"] = round(time.perf_counter() - job_start, 4)
    snapshot["execution"] = execution
    redis.write_structural_run_snapshot(run_id=run_id, snapshot=snapshot)
    redis.write_health_snapshot(build_health_snapshot(redis))

    logging.info("Run complete | mode=structural | run_id=%s | sources=%s", run_id, ",".join(sources_called))
    return run_id


def run_preview_update() -> Optional[str]:
    logging.info("Starting Macro Stress Monitor intraday preview update job.")
    job_start = time.perf_counter()

    try:
        redis = RedisClient()
    except Exception:
        logging.error("Failed to connect to Redis. Aborting preview job.")
        return None

    end_date = pd.Timestamp.now()
    start_date = end_date - pd.DateOffset(years=HISTORY_YEARS)

    no_network_mock = _parse_bool_env("MSM_NO_NETWORK_MOCK", default=False)
    sources_called: List[str] = []

    fetch_start = time.perf_counter()
    if no_network_mock:
        logging.info("MSM_NO_NETWORK_MOCK enabled. Using synthetic local preview data.")
        _, market_all = _mock_data(start_date, end_date)
        market_data = {ticker: market_all.get(ticker) for ticker in PREVIEW_MARKET_TICKERS}
        sources_called.extend(["mock:polygon"])
    else:
        logging.info("Fetching preview market proxies only via Polygon (with retry/backoff).")
        market_data = _fetch_market_with_retry(
            PREVIEW_MARKET_TICKERS,
            start_date,
            end_date,
            retries=3,
            backoff_seconds=1.0,
        )
        sources_called.extend([f"polygon:{ticker}" for ticker in PREVIEW_MARKET_TICKERS])
    fetch_duration = time.perf_counter() - fetch_start
    fetched_at_utc = _utc_now().isoformat()
    sources = {
        "polygon" if not no_network_mock else "mock_polygon": _build_source_metadata(
            "polygon" if not no_network_mock else "mock_polygon",
            PREVIEW_MARKET_TICKERS,
            market_data,
            fetched_at_utc,
            "tickers",
        )
    }
    data_quality = _data_quality(
        sources=sources,
        critical_by_source={
            "polygon": ["SPY", "BKLN", "XLF", "JPY=X"],
            "mock_polygon": ["SPY", "BKLN", "XLF", "JPY=X"],
        },
    )

    compute_start = time.perf_counter()
    component_payloads: Dict[str, Dict[str, Any]] = {}

    def _safe_indicator(name: str, fn):
        try:
            return fn()
        except Exception as exc:
            logging.warning("Preview component '%s' unavailable: %s", name, exc)
            return None

    # Loans
    loans_indicator = _safe_indicator("leveraged_loans", lambda: credit.calculate_leveraged_loan_indicator(market_data.get("BKLN")))
    if loans_indicator is not None:
        try:
            loans_score, loans_status = score.calculate_leveraged_loan_subscore(loans_indicator)
        except Exception as exc:
            logging.warning("Preview scoring unavailable for leveraged_loans: %s", exc)
            loans_score, loans_status = None, None
    else:
        loans_score, loans_status = None, None
    component_payloads["leveraged_loans"] = _preview_component_payload(
        "leveraged_loans", loans_indicator, _to_int_score(loans_score), loans_status
    )

    # Financials
    xlf_indicator = _safe_indicator(
        "xlf_spy", lambda: relative_strength.calculate_xlf_spy_indicator(market_data.get("XLF"), market_data.get("SPY"))
    )
    if xlf_indicator is not None:
        try:
            xlf_score, xlf_status = score.calculate_xlf_spy_subscore(xlf_indicator)
        except Exception as exc:
            logging.warning("Preview scoring unavailable for xlf_spy: %s", exc)
            xlf_score, xlf_status = None, None
    else:
        xlf_score, xlf_status = None, None
    component_payloads["xlf_spy"] = _preview_component_payload("xlf_spy", xlf_indicator, _to_int_score(xlf_score), xlf_status)

    # JPY risk
    jpy_indicator = _safe_indicator("jpy_risk", lambda: jpy.calculate_jpy_risk_indicator(market_data.get("JPY=X")))
    if jpy_indicator is not None:
        try:
            jpy_score, jpy_status = score.calculate_jpy_risk_subscore(jpy_indicator)
        except Exception as exc:
            logging.warning("Preview scoring unavailable for jpy_risk: %s", exc)
            jpy_score, jpy_status = None, None
    else:
        jpy_score, jpy_status = None, None
    component_payloads["jpy_risk"] = _preview_component_payload("jpy_risk", jpy_indicator, _to_int_score(jpy_score), jpy_status)

    # Regional banks (monitor style pseudo-score)
    kre_indicator = _safe_indicator(
        "kre_spy", lambda: relative_strength.calculate_kre_spy_indicator(market_data.get("KRE"), market_data.get("SPY"))
    )
    kre_score = _monitor_pseudo_score("kre_spy", kre_indicator or {}) if kre_indicator is not None else None
    kre_state = _state_from_score(_to_int_score(kre_score)) if kre_indicator is not None else {"text": "Unavailable"}
    component_payloads["kre_spy"] = _preview_component_payload(
        "kre_spy",
        kre_indicator,
        _to_int_score(kre_score),
        kre_state.get("text"),
    )

    component_subscores = {
        key: value.get("subscore")
        for key, value in component_payloads.items()
    }
    component_states = {
        key: value.get("state")
        for key, value in component_payloads.items()
    }

    jpy_confirmed = bool((jpy_indicator or {}).get("jpy_confirmed", False))
    preview_spillover = _preview_assessment(component_states=component_states, jpy_confirmed=jpy_confirmed)

    now = _utc_now()
    run_id = now.strftime("preview_%Y%m%dT%H%M%S.%fZ")
    unavailable_components = [
        key
        for key, payload in component_payloads.items()
        if not payload.get("available")
    ]
    warnings = _source_warnings(sources)
    if unavailable_components:
        warnings.append(f"preview component(s) unavailable={','.join(unavailable_components)}")
    if len(unavailable_components) == len(component_payloads):
        status = "failed"
    elif warnings:
        status = "partial"
    else:
        status = "success"

    preview_trigger_states = {
        "jpy_confirmed": jpy_confirmed,
        "loans_below_200dma": component_states.get("leveraged_loans", {}).get("level") in {"watch", "breakdown"},
        "xlf_breakdown": component_states.get("xlf_spy", {}).get("level") == "breakdown",
    }
    signal_summary = _signal_summary(preview_trigger_states, {})
    anomaly_flags = {
        "extreme_move_detected": False,
        "multi_asset_divergence": bool(preview_trigger_states.get("xlf_breakdown") and not preview_trigger_states.get("loans_below_200dma")),
        "volatility_spike": component_states.get("jpy_risk", {}).get("level") == "breakdown",
    }
    confidence_label = "LOW" if status == "failed" else "MED" if status == "partial" else "HIGH"
    state_confidence = _state_confidence(confidence_label, data_quality, signal_summary, anomaly_flags)
    compute_duration = time.perf_counter() - compute_start
    execution = {
        "duration_seconds": None,
        "fetch_duration": round(fetch_duration, 4),
        "compute_duration": round(compute_duration, 4),
        "write_duration": None,
    }

    snapshot = {
        "run_id": run_id,
        "computed_at_utc": now.isoformat(),
        "mode": "preview",
        "foundation_scope": FOUNDATION_SCOPE,
        "market_time_context": _market_time_context(now),
        "data_quality": data_quality,
        "signal_summary": signal_summary,
        "regime_history_summary": {
            "current_regime_duration_days": None,
            "previous_regime": None,
            "regime_changes_last_30d": None,
        },
        "anomaly_flags": anomaly_flags,
        "execution": execution,
        "state_confidence": state_confidence,
        "headline_summary": _headline_summary(signal_summary, anomaly_flags),
        "run_status": _run_status(status=status, warnings=warnings),
        "sources": sources,
        "inputs": _extract_inputs(
            {key: value.get("indicator", {}) for key, value in component_payloads.items()},
            {"missing_components": len(unavailable_components)},
        ),
        "config": _run_config("preview"),
        "component_subscores": component_subscores,
        "component_states": component_states,
        "component_statuses": {key: value.get("status") for key, value in component_payloads.items()},
        "components": component_payloads,
        "preview_spillover_assessment": preview_spillover,
        "session": _preview_session(now),
        "trigger_states": preview_trigger_states,
        "delta": {},
        "meta": snapshot_meta({"preview_rule_version": PREVIEW_RULE_VERSION}),
        "is_stale": False,
        "stale_reason": None,
    }
    previous_snapshot = redis.get_latest_preview_snapshot()
    snapshot["delta"] = compute_preview_delta(snapshot, previous_snapshot)
    freshness = freshness_for_snapshot(snapshot, "preview")
    snapshot["is_stale"] = freshness["is_stale"]
    snapshot["stale_reason"] = freshness["stale_reason"]
    write_start = time.perf_counter()
    redis.write_preview_run_snapshot(run_id=run_id, snapshot=snapshot)
    execution["write_duration"] = round(time.perf_counter() - write_start, 4)
    execution["duration_seconds"] = round(time.perf_counter() - job_start, 4)
    snapshot["execution"] = execution
    redis.write_preview_run_snapshot(run_id=run_id, snapshot=snapshot)
    redis.write_health_snapshot(build_health_snapshot(redis))

    logging.info("Run complete | mode=preview | run_id=%s | sources=%s", run_id, ",".join(sources_called))
    return run_id


def main():
    """Legacy entrypoint: defaults to full structural update."""
    run_full_update()


if __name__ == "__main__":
    main()
