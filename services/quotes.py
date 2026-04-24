"""
Redis-backed quote and intraday services powered by Polygon/Massive aggregates.
"""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import pandas as pd

try:
    from ..data_sources import market
    from ..storage.redis_client import RedisClient
    from .dashboard_config import get_quote_symbols, load_dashboard_config
except ImportError:  # pragma: no cover - direct script compatibility
    from data_sources import market
    from storage.redis_client import RedisClient
    from services.dashboard_config import get_quote_symbols, load_dashboard_config


QUOTE_TTL_SECONDS = 60
INTRADAY_TTL_SECONDS = 60
DAILY_LOOKBACK_DAYS = 400
INTRADAY_LOOKBACK_DAYS = 5


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    normalized = str(ts).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _is_fresh(payload: Optional[Dict[str, Any]], ttl_seconds: int) -> bool:
    if not payload:
        return False
    fetched_at = _parse_iso(payload.get("fetched_at_utc") or payload.get("timestamp"))
    if fetched_at is None:
        return False
    return (_utc_now() - fetched_at).total_seconds() <= ttl_seconds


def _daily_range() -> tuple[pd.Timestamp, pd.Timestamp]:
    end_date = pd.Timestamp.now(tz="UTC")
    start_date = end_date - pd.Timedelta(days=DAILY_LOOKBACK_DAYS)
    return start_date, end_date


def _intraday_range() -> tuple[pd.Timestamp, pd.Timestamp]:
    end_date = pd.Timestamp.now(tz="UTC")
    start_date = end_date - pd.Timedelta(days=INTRADAY_LOOKBACK_DAYS)
    return start_date, end_date


def _latest_session(intraday: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    if intraday is None or intraday.empty:
        return None
    frame = intraday.copy()
    session_date = frame.index.max().date()
    session = frame[frame.index.date == session_date]
    return session if not session.empty else None


def _performance(daily: pd.DataFrame, price: float) -> Dict[str, Optional[float]]:
    closes = pd.to_numeric(daily["Close"], errors="coerce").dropna()
    if closes.empty:
        return {
            "1d": None,
            "5d": None,
            "1m": None,
            "6m": None,
            "ytd": None,
            "high_52w": None,
            "low_52w": None,
        }

    def _pct_from_index(offset: int) -> Optional[float]:
        if len(closes) <= offset:
            return None
        previous = float(closes.iloc[-offset - 1])
        if previous == 0:
            return None
        return round(((price - previous) / previous) * 100, 2)

    ytd_series = closes[closes.index >= pd.Timestamp(year=_utc_now().year, month=1, day=1)]
    ytd = None
    if not ytd_series.empty:
        first = float(ytd_series.iloc[0])
        if first != 0:
            ytd = round(((price - first) / first) * 100, 2)

    return {
        "1d": _pct_from_index(1),
        "5d": _pct_from_index(5),
        "1m": _pct_from_index(21),
        "6m": round(((price - float(closes.iloc[0])) / float(closes.iloc[0])) * 100, 2) if len(closes) > 0 and float(closes.iloc[0]) != 0 else None,
        "ytd": ytd,
        "high_52w": round(float(closes.max()), 2),
        "low_52w": round(float(closes.min()), 2),
    }


def build_quote_payload(
    symbol: str,
    daily: Optional[pd.DataFrame],
    intraday: Optional[pd.DataFrame],
    fetched_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    fetched_at = fetched_at or _utc_now()
    daily = daily if daily is not None else pd.DataFrame()
    intraday = intraday if intraday is not None else pd.DataFrame()

    if daily.empty and intraday.empty:
        return {
            "symbol": symbol.upper(),
            "error": "No Polygon/Massive quote data available",
            "source": "polygon",
            "fetched_at_utc": fetched_at.isoformat(),
        }

    session = _latest_session(intraday)
    if session is not None and not session.empty:
        last_row = session.iloc[-1]
        price = float(last_row["Close"])
        open_price = float(session.iloc[0]["Open"])
        high = float(session["High"].max())
        low = float(session["Low"].min())
        volume = int(session["Volume"].fillna(0).sum())
        timestamp = session.index[-1].to_pydatetime().replace(tzinfo=timezone.utc).isoformat()
    else:
        last_row = daily.iloc[-1]
        price = float(last_row["Close"])
        open_price = float(last_row["Open"])
        high = float(last_row["High"])
        low = float(last_row["Low"])
        volume = int(float(last_row["Volume"])) if "Volume" in daily.columns else 0
        ts = daily.index[-1]
        timestamp = pd.Timestamp(ts).to_pydatetime().replace(tzinfo=timezone.utc).isoformat()

    previous_close = None
    if len(daily.index) > 1:
        previous_close = float(daily["Close"].iloc[-2])

    change = price - open_price if open_price else 0.0
    percent = ((change / open_price) * 100) if open_price else 0.0

    return {
        "symbol": symbol.upper(),
        "price": round(price, 4),
        "open": round(open_price, 4),
        "high": round(high, 4),
        "low": round(low, 4),
        "volume": volume,
        "change": round(change, 4),
        "percent": round(percent, 2),
        "previous_close": round(previous_close, 4) if previous_close is not None else None,
        "performance": _performance(daily, price) if not daily.empty else None,
        "timestamp": timestamp,
        "fetched_at_utc": fetched_at.isoformat(),
        "source": "polygon",
    }


def build_intraday_payload(symbol: str, intraday: Optional[pd.DataFrame], fetched_at: Optional[datetime] = None) -> Dict[str, Any]:
    fetched_at = fetched_at or _utc_now()
    if intraday is None or intraday.empty:
        return {
            "symbol": symbol.upper(),
            "points": [],
            "fetched_at_utc": fetched_at.isoformat(),
            "source": "polygon",
            "error": "No Polygon/Massive intraday data available",
        }

    session = _latest_session(intraday)
    if session is None or session.empty:
        session = intraday

    points = [
        {
            "timestamp": pd.Timestamp(idx).to_pydatetime().replace(tzinfo=timezone.utc).isoformat(),
            "open": round(float(row["Open"]), 4),
            "high": round(float(row["High"]), 4),
            "low": round(float(row["Low"]), 4),
            "close": round(float(row["Close"]), 4),
            "volume": int(float(row["Volume"])) if row.get("Volume") is not None else 0,
        }
        for idx, row in session.iterrows()
    ]

    return {
        "symbol": symbol.upper(),
        "points": points,
        "fetched_at_utc": fetched_at.isoformat(),
        "source": "polygon",
    }


def _fetch_quote_payload(symbol: str) -> Dict[str, Any]:
    daily_start, daily_end = _daily_range()
    intraday_start, intraday_end = _intraday_range()
    fetched_at = _utc_now()
    daily = market.get_daily_bars(symbol, daily_start, daily_end)
    intraday = market.get_intraday_bars(symbol, intraday_start, intraday_end, interval_minutes=1)
    return build_quote_payload(symbol=symbol, daily=daily, intraday=intraday, fetched_at=fetched_at)


def _fetch_intraday_payload(symbol: str) -> Dict[str, Any]:
    intraday_start, intraday_end = _intraday_range()
    fetched_at = _utc_now()
    intraday = market.get_intraday_bars(symbol, intraday_start, intraday_end, interval_minutes=1)
    return build_intraday_payload(symbol=symbol, intraday=intraday, fetched_at=fetched_at)


def get_quote_payload(
    symbol: str,
    redis_client: Optional[RedisClient] = None,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    redis = redis_client or RedisClient()
    normalized = str(symbol).strip().upper()
    if not normalized:
        return {"error": "Symbol is required"}

    cached = redis.get_quote(normalized)
    if not force_refresh and _is_fresh(cached, QUOTE_TTL_SECONDS):
        return cached

    payload = _fetch_quote_payload(normalized)
    if not payload.get("error"):
        redis.write_quote(normalized, payload, ttl_seconds=QUOTE_TTL_SECONDS)
    return payload


def get_intraday_payload(
    symbol: str,
    redis_client: Optional[RedisClient] = None,
    force_refresh: bool = False,
) -> Dict[str, Any]:
    redis = redis_client or RedisClient()
    normalized = str(symbol).strip().upper()
    if not normalized:
        return {"error": "Symbol is required"}

    cached = redis.get_intraday(normalized)
    if not force_refresh and _is_fresh(cached, INTRADAY_TTL_SECONDS):
        return cached

    payload = _fetch_intraday_payload(normalized)
    if not payload.get("error"):
        redis.write_intraday(normalized, payload, ttl_seconds=INTRADAY_TTL_SECONDS)
    return payload


def refresh_configured_quotes(redis_client: Optional[RedisClient] = None) -> List[str]:
    redis = redis_client or RedisClient()
    config = load_dashboard_config()
    symbols = get_quote_symbols(config)
    refreshed: List[str] = []

    for symbol in symbols:
        try:
            quote_payload = _fetch_quote_payload(symbol)
            intraday_payload = _fetch_intraday_payload(symbol)
        except Exception:
            continue
        if not quote_payload.get("error"):
            redis.write_quote(symbol, quote_payload, ttl_seconds=QUOTE_TTL_SECONDS)
        if not intraday_payload.get("error"):
            redis.write_intraday(symbol, intraday_payload, ttl_seconds=INTRADAY_TTL_SECONDS)
        refreshed.append(symbol)

    return refreshed
