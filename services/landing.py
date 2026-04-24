"""
Presentation helpers for the main landing dashboard.
"""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


SECTOR_LABELS = {
    "XLF": "Financials",
    "KRE": "Regional Banks",
    "XLK": "Technology",
    "XLI": "Industrials",
    "XLE": "Energy",
    "XLU": "Utilities",
    "XLP": "Staples",
    "XLY": "Discretionary",
    "XLV": "Health Care",
    "SMH": "Semiconductors",
}

TRIGGER_LABELS = {
    "ig_widening": "IG widening",
    "hy_widening": "HY widening",
    "loans_below_200dma": "Loans below 200DMA",
    "jpy_confirmed": "JPY confirmed",
    "xlf_breakdown": "Financials breakdown",
    "dgs30_sharp_move": "30Y sharp move",
}


def _num(value: Any) -> Optional[float]:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric == numeric and numeric not in (float("inf"), float("-inf")):
            return numeric
    return None


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


def _format_timestamp(ts: Optional[str]) -> str:
    dt = _parse_iso(ts)
    if dt is None:
        return "Unavailable"
    if ZoneInfo is not None:
        dt = dt.astimezone(ZoneInfo("America/New_York"))
        return dt.strftime("%b %d, %Y %I:%M %p ET")
    return dt.astimezone(timezone.utc).strftime("%b %d, %Y %H:%M UTC")


def _labelize(value: Optional[str]) -> str:
    if not value:
        return "Unavailable"
    return str(value).replace("_", " ").replace("-", " ").title()


def _format_percent(value: Any, digits: int = 1) -> str:
    numeric = _num(value)
    if numeric is None:
        return "--"
    return f"{numeric * 100:+.{digits}f}%"


def _format_percent_plain(value: Any, digits: int = 1) -> str:
    numeric = _num(value)
    if numeric is None:
        return "--"
    return f"{numeric * 100:.{digits}f}%"


def _format_number(value: Any, digits: int = 2, suffix: str = "") -> str:
    numeric = _num(value)
    if numeric is None:
        return "--"
    return f"{numeric:.{digits}f}{suffix}"


def _format_signed_number(value: Any, digits: int = 1, suffix: str = "") -> str:
    numeric = _num(value)
    if numeric is None:
        return "--"
    return f"{numeric:+.{digits}f}{suffix}"


def _format_count(value: Any, total: Any) -> str:
    count = _num(value)
    whole = _num(total)
    if count is None or whole is None:
        return "--"
    return f"{int(round(count))} / {int(round(whole))}"


def _tone_from_state(state: Optional[str]) -> str:
    token = (state or "").lower()
    if token in {"breakdown", "underperforming", "downtrend", "high_vol", "inverted", "falling_fast"}:
        return "danger"
    if token in {"watch", "mixed", "flat", "elevated_vol", "rising_fast", "stale", "lagging"}:
        return "caution"
    if token in {"stable", "uptrend", "outperforming", "normal_vol", "range_bound", "success", "healthy"}:
        return "positive"
    return "neutral"


def _metric(
    label: str,
    value: str,
    secondary: Optional[str] = None,
    state: Optional[str] = None,
    note: Optional[str] = None,
    tone: Optional[str] = None,
) -> Dict[str, str]:
    resolved_tone = tone or _tone_from_state(state)
    return {
        "label": label,
        "value": value,
        "secondary": secondary or "",
        "state": _labelize(state) if state else "",
        "note": note or "",
        "tone": resolved_tone,
    }


def _history_points(history_data: List[Dict[str, Any]], limit: int = 30) -> List[Dict[str, Any]]:
    points: List[Dict[str, Any]] = []
    newest_first = history_data[: max(0, int(limit))]
    for item in reversed(newest_first):
        score = _num(item.get("composite_score"))
        if score is None:
            continue
        label = str(item.get("date") or item.get("timestamp") or "")
        points.append({"label": label, "value": round(score, 2)})
    return points


def _asset_metric(label: str, asset_state: Dict[str, Any]) -> Dict[str, str]:
    if not asset_state or not asset_state.get("available"):
        return _metric(label, "--", secondary="No current proxy data", state="unavailable")
    return _metric(
        label=label,
        value=_format_number(asset_state.get("latest"), 2),
        secondary=f"20d {_format_percent(asset_state.get('return_20d'))}",
        state=str(asset_state.get("trend_state") or asset_state.get("state") or "neutral"),
        note=f"50DMA {_format_percent(asset_state.get('distance_50dma'))} | Vol {_labelize(asset_state.get('vol_state'))}",
    )


def _rate_metric(label: str, rate_state: Dict[str, Any]) -> Dict[str, str]:
    if not rate_state or not rate_state.get("available"):
        return _metric(label, "--", secondary="No current series", state="unavailable")
    return _metric(
        label=label,
        value=_format_number(rate_state.get("latest"), 2, "%"),
        secondary=f"20d {_format_signed_number(rate_state.get('change_20d_bps'), 0, ' bps')}",
        state=str(rate_state.get("state") or "neutral"),
        note=f"1Y z {_format_number(rate_state.get('z_score_1y'), 2)}",
    )


def _curve_metric(label: str, curve_state: Dict[str, Any]) -> Dict[str, str]:
    if not curve_state or not curve_state.get("available"):
        return _metric(label, "--", secondary="No curve data", state="unavailable")
    return _metric(
        label=label,
        value=_format_number(curve_state.get("latest_bps"), 0, " bps"),
        secondary=f"20d {_format_signed_number(curve_state.get('change_20d_bps'), 0, ' bps')}",
        state=str(curve_state.get("state") or "neutral"),
        note=f"1Y z {_format_number(curve_state.get('z_score_1y'), 2)}",
    )


def _macro_level_metric(label: str, series_state: Dict[str, Any], suffix: str = "") -> Dict[str, str]:
    if not series_state or not series_state.get("available"):
        return _metric(label, "--", secondary="No macro series", state="unavailable")
    return _metric(
        label=label,
        value=_format_number(series_state.get("latest"), 2, suffix),
        secondary=f"60d {_format_signed_number(series_state.get('change_60d'), 2)}",
        state=str(series_state.get("state") or "neutral"),
        note=f"1Y pct {_format_number(series_state.get('percentile_1y'), 0)}",
    )


def _relationship_metric(label: str, relationship_state: Dict[str, Any]) -> Dict[str, str]:
    if not relationship_state or not relationship_state.get("available"):
        return _metric(label, "--", secondary="No relative series", state="unavailable")
    return _metric(
        label=label,
        value=_format_percent(relationship_state.get("return_20d")),
        secondary=f"200DMA {_format_percent(relationship_state.get('distance_200dma'))}",
        state=str(relationship_state.get("state") or "neutral"),
        note=f"1Y z {_format_number(relationship_state.get('z_score_1y'), 2)}",
    )


def _trigger_items(trigger_states: Dict[str, Any]) -> List[Dict[str, str]]:
    items = []
    for key, label in TRIGGER_LABELS.items():
        active = bool(trigger_states.get(key))
        items.append(
            {
                "label": label,
                "status": "Active" if active else "Quiet",
                "tone": "danger" if active else "positive",
            }
        )
    return items


def _sector_leadership(sector_state: Dict[str, Any]) -> Dict[str, List[Dict[str, str]]]:
    rows = []
    for ticker, data in (sector_state or {}).items():
        rel = data.get("relative_to_spy") or {}
        abs_state = data.get("absolute") or {}
        rel_20d = _num(rel.get("return_20d"))
        if rel_20d is None:
            continue
        rows.append(
            {
                "ticker": ticker,
                "label": SECTOR_LABELS.get(ticker, ticker),
                "relative_return_20d": rel_20d,
                "absolute_return_20d": _format_percent(abs_state.get("return_20d")),
                "state": _labelize(rel.get("state")),
            }
        )

    leaders = sorted(rows, key=lambda row: row["relative_return_20d"], reverse=True)[:3]
    laggards = sorted(rows, key=lambda row: row["relative_return_20d"])[:3]

    def _clean(items: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        return [
            {
                "label": item["label"],
                "value": f"{item['relative_return_20d'] * 100:+.1f}% vs SPY",
                "detail": f"Absolute 20d {item['absolute_return_20d']} | {item['state']}",
            }
            for item in items
        ]

    return {"leaders": _clean(leaders), "laggards": _clean(laggards)}


def build_landing_payload(
    structural_snapshot: Dict[str, Any],
    preview_snapshot: Optional[Dict[str, Any]],
    history_data: List[Dict[str, Any]],
) -> Dict[str, Any]:
    context = structural_snapshot.get("market_context", {}) or {}
    macro = context.get("macro_rates", {}) or {}
    credit = context.get("credit_liquidity", {}) or {}
    safety = context.get("flight_to_safety", {}) or {}
    breadth = context.get("breadth_participation", {}) or {}
    sector_state = context.get("sector_state", {}) or {}
    volatility = context.get("volatility_stress", {}) or {}
    cross_asset = context.get("cross_asset_relationships", {}) or {}

    preview_components = (preview_snapshot or {}).get("component_states", {}) or {}
    preview_summary = {
        "available": bool(preview_snapshot),
        "assessment": (preview_snapshot or {}).get("preview_spillover_assessment") or "Preview unavailable",
        "session": ((preview_snapshot or {}).get("session_context") or {}).get("market_session", "unavailable"),
        "financials": _labelize((preview_components.get("xlf_spy") or {}).get("level")),
        "loans": _labelize((preview_components.get("leveraged_loans") or {}).get("level")),
        "jpy": _labelize((preview_components.get("jpy_risk") or {}).get("level")),
        "regional_banks": _labelize((preview_components.get("kre_spy") or {}).get("level")),
    }

    methodology = [
        "This page is presentation only. The data engine remains Macro Stress Monitor.",
        "All scores, component states, and market context come from stored Redis snapshots rather than page-level fetch logic.",
        "The terminal monitor remains available for deeper drilldown, but the landing page now carries the main dashboard role.",
    ]

    tabs = [
        {
            "id": "macro",
            "label": "Macro",
            "description": "Rates, curve, and macro backdrop from the structural snapshot.",
            "cards": [
                _rate_metric("2Y Yield", ((macro.get("rates") or {}).get("2y") or {})),
                _rate_metric("10Y Yield", ((macro.get("rates") or {}).get("10y") or {})),
                _rate_metric("30Y Yield", ((macro.get("rates") or {}).get("30y") or {})),
                _curve_metric("2s10s Curve", ((macro.get("curve_spreads") or {}).get("2s10s") or {})),
                _rate_metric("10Y Breakeven", (((macro.get("inflation_growth") or {}).get("10y_breakeven_inflation")) or {})),
                _macro_level_metric("Unemployment", (((macro.get("inflation_growth") or {}).get("unemployment_rate")) or {}), "%"),
                _macro_level_metric("Payrolls", (((macro.get("inflation_growth") or {}).get("nonfarm_payrolls")) or {})),
                _asset_metric("Dollar Proxy", (((macro.get("dollar_proxy") or {}).get("state")) or {})),
            ],
        },
        {
            "id": "stress",
            "label": "Market Stress",
            "description": "Credit, relative breakdowns, and volatility-sensitive proxies.",
            "cards": [
                _rate_metric("IG OAS", (credit.get("ig_oas") or {})),
                _rate_metric("HY OAS", (credit.get("hy_oas") or {})),
                _metric(
                    "HY - IG Gap",
                    _format_number(((credit.get("hy_minus_ig_gap") or {}).get("hy_ig_oas_gap")), 2),
                    secondary=f"Method {((credit.get('hy_minus_ig_gap') or {}).get('hy_ig_oas_gap_method') or 'unavailable')}",
                    state="watch",
                    note="Spread gap between high yield and investment grade.",
                    tone="caution",
                ),
                _asset_metric("Loan Proxy", (credit.get("loan_proxy") or {})),
                _relationship_metric("XLF vs SPY", (cross_asset.get("xlf_spy") or {})),
                _relationship_metric("KRE vs SPY", (cross_asset.get("kre_spy") or {})),
                _asset_metric("Equity Vol Proxy", (((volatility.get("vix_proxy") or {}).get("state")) or {})),
                _asset_metric("Bond Vol Proxy", (((volatility.get("move_proxy") or {}).get("state")) or {})),
            ],
        },
        {
            "id": "safety",
            "label": "Flight to Safety",
            "description": "Treasuries, gold, dollar, yen, and defensive rotation proxies.",
            "cards": [
                _asset_metric("TLT", (((safety.get("treasury_proxies") or {}).get("TLT")) or {})),
                _asset_metric("IEF", (((safety.get("treasury_proxies") or {}).get("IEF")) or {})),
                _asset_metric("Gold Proxy", (safety.get("gold_proxy") or {})),
                _asset_metric("Dollar Proxy", (safety.get("dollar_proxy") or {})),
                _asset_metric("JPY Proxy", (safety.get("jpy_proxy") or {})),
                _relationship_metric("Utilities vs Discretionary", (((safety.get("defensive_vs_cyclical") or {}).get("xlu_xly")) or {})),
                _relationship_metric("Staples vs Discretionary", (((safety.get("defensive_vs_cyclical") or {}).get("xlp_xly")) or {})),
                _relationship_metric("Gold vs SPY", (((safety.get("defensive_vs_cyclical") or {}).get("gld_spy")) or {})),
            ],
        },
        {
            "id": "breadth",
            "label": "Breadth",
            "description": "Participation and leadership based on tracked ETF breadth proxies.",
            "cards": [
                _metric(
                    "Above 50DMA",
                    _format_percent_plain(breadth.get("above_50dma_pct")),
                    secondary=_format_count(breadth.get("above_50dma_count"), breadth.get("tracked_count")),
                    state="positive" if (_num(breadth.get("above_50dma_pct")) or 0) >= 0.6 else "watch",
                    note="Tracked ETF basket above 50DMA.",
                    tone="positive" if (_num(breadth.get("above_50dma_pct")) or 0) >= 0.6 else "caution",
                ),
                _metric(
                    "Above 200DMA",
                    _format_percent_plain(breadth.get("above_200dma_pct")),
                    secondary=_format_count(breadth.get("above_200dma_count"), breadth.get("tracked_count")),
                    state="positive" if (_num(breadth.get("above_200dma_pct")) or 0) >= 0.6 else "watch",
                    note="Longer-term participation check.",
                    tone="positive" if (_num(breadth.get("above_200dma_pct")) or 0) >= 0.6 else "caution",
                ),
                _metric(
                    "Positive 20D Trend",
                    _format_percent_plain(breadth.get("positive_20d_trend_pct")),
                    secondary=_format_count(breadth.get("positive_20d_trend_count"), breadth.get("tracked_count")),
                    state="positive" if (_num(breadth.get("positive_20d_trend_pct")) or 0) >= 0.55 else "mixed",
                    note="Breadth proxy from tracked assets.",
                    tone="positive" if (_num(breadth.get("positive_20d_trend_pct")) or 0) >= 0.55 else "caution",
                ),
                _metric(
                    "1D Advancers",
                    _format_count(breadth.get("advancers_1d_count"), breadth.get("tracked_count")),
                    secondary=f"5D advancers {_format_count(breadth.get('advancers_5d_count'), breadth.get('tracked_count'))}",
                    state="mixed",
                    note="Short-horizon participation pulse.",
                    tone="neutral",
                ),
                _relationship_metric("Equal Weight vs Cap Weight", (breadth.get("equal_weight_vs_cap_weight") or {})),
            ],
        },
    ]

    leadership = _sector_leadership(sector_state)

    hero = {
        "title": "Bottom Sniffer Dashboard",
        "subtitle": "Powered by the Macro Stress Monitor engine",
        "score": int(round(_num(structural_snapshot.get("composite_score")) or 0)),
        "regime": str(structural_snapshot.get("regime_label") or structural_snapshot.get("regime") or "Unavailable"),
        "headline": str(structural_snapshot.get("headline_summary") or "No headline summary available."),
        "updated_at": _format_timestamp(structural_snapshot.get("computed_at_utc")),
        "confidence": str(structural_snapshot.get("confidence") or "N/A"),
        "data_quality": _format_percent_plain(((structural_snapshot.get("data_quality") or {}).get("score")), 0),
        "drivers": [str(item) for item in (structural_snapshot.get("primary_drivers") or [])][:3],
        "history": _history_points(history_data),
        "preview": preview_summary,
    }

    return {
        "hero": hero,
        "trigger_map": _trigger_items(structural_snapshot.get("trigger_states", {}) or {}),
        "leadership": leadership,
        "methodology": methodology,
        "tabs": tabs,
    }
