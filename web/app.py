"""
The Flask web application for the Macro Stress Monitor dashboard.
"""
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from flask import Flask, Response, jsonify, render_template

try:
    from ..services import reader, render
    from ..storage.redis_client import RedisClient
except ImportError:  # pragma: no cover - direct script compatibility
    from services import reader, render
    from storage.redis_client import RedisClient

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


COMPONENT_ORDER = [
    "ig_spreads",
    "hy_credit",
    "leveraged_loans",
    "xlf_spy",
    "kre_spy",
    "30y_yield",
    "jpy_risk",
]

COMPONENT_LABELS = {
    "ig_spreads": "Investment Grade Credit",
    "hy_credit": "High Yield Credit",
    "leveraged_loans": "Leveraged Loans",
    "xlf_spy": "Financials",
    "kre_spy": "Regional Banks",
    "30y_yield": "30Y Treasury Yield",
    "jpy_risk": "JPY Risk",
}

WHY_IT_MATTERS = {
    "ig_spreads": "Corporate bond stress. Rising spreads signal tighter liquidity.",
    "hy_credit": "Riskier corporate debt stress confirmation.",
    "leveraged_loans": "Lower-quality credit barometer.",
    "xlf_spy": "Financial sector leadership and risk appetite proxy.",
    "kre_spy": "Regional banking and local credit conditions.",
    "30y_yield": "Flight-to-safety or policy shock signal.",
    "jpy_risk": "Global deleveraging stress proxy.",
}


def _score_int(value: Any) -> int:
    if isinstance(value, (int, float)):
        return int(round(float(value)))
    return 0


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    normalized = ts.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _to_et(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if ZoneInfo is None:
        return dt.astimezone(timezone.utc)
    return dt.astimezone(ZoneInfo("America/New_York"))


def _format_et_time(ts: Optional[str]) -> Optional[str]:
    dt = _to_et(_parse_iso(ts))
    if dt is None:
        return None
    return dt.strftime("%H:%M ET")


def _environment_label(score: int) -> Dict[str, str]:
    if score <= 30:
        return {"label": "CALM", "color": "green"}
    if score <= 60:
        return {"label": "WATCH", "color": "yellow"}
    if score <= 80:
        return {"label": "ELEVATED", "color": "orange"}
    return {"label": "RISK-OFF", "color": "red"}


def _component_sentence(component: str, state_level: str) -> str:
    messages = {
        "ig_spreads": {
            "stable": "Investment-grade credit remains orderly with no broad stress signal.",
            "watch": "Investment-grade spreads are widening but not yet in a full breakdown.",
            "breakdown": "Investment-grade spreads are widening sharply, signaling tighter funding conditions.",
        },
        "hy_credit": {
            "stable": "High-yield credit is not confirming broad stress right now.",
            "watch": "High-yield spreads are rising and should be watched for confirmation.",
            "breakdown": "High-yield credit is under pressure, confirming tighter risk financing.",
        },
        "leveraged_loans": {
            "stable": "Loan markets remain above trend and are not signaling a credit break.",
            "watch": "Loan prices are weakening and risk appetite is softening.",
            "breakdown": "Loan prices are below trend, signaling weaker demand for lower-quality credit.",
        },
        "xlf_spy": {
            "stable": "Financials are tracking the broad market without a breakdown signal.",
            "watch": "Financials are lagging the broad market; leadership is narrowing.",
            "breakdown": "Financials are breaking down versus the broad market; risk appetite is weakening.",
        },
        "kre_spy": {
            "stable": "Regional banks are not flashing a sustained breakdown signal.",
            "watch": "Regional banks are lagging and need monitoring for credit transmission risk.",
            "breakdown": "Regional banks are in breakdown, raising local credit condition concerns.",
        },
        "30y_yield": {
            "stable": "Long-end rates are not in a sharp move regime.",
            "watch": "Long-end rates are moving enough to raise cross-asset sensitivity.",
            "breakdown": "Long-end rates are moving sharply, increasing cross-asset volatility risk.",
        },
        "jpy_risk": {
            "stable": "JPY is not signaling a confirmed liquidity shock.",
            "watch": "JPY volatility is elevated, but confirmation is not yet in place.",
            "breakdown": "JPY stress is confirmed, consistent with global deleveraging pressure.",
        },
    }
    return messages.get(component, {}).get(state_level, "No driver sentence available.")


def _regime_summary(trigger_states: Dict[str, bool], component_states: Dict[str, Dict[str, str]]) -> str:
    credit_stable = not (
        trigger_states.get("ig_widening")
        or trigger_states.get("hy_widening")
        or trigger_states.get("loans_below_200dma")
    )
    financials_breakdown = component_states.get("xlf_spy", {}).get("level") == "breakdown"
    jpy_confirmed = trigger_states.get("jpy_confirmed", False)

    if credit_stable and financials_breakdown and not jpy_confirmed:
        return "Credit orderly. Financials weak. No systemic stress detected."
    if credit_stable and not financials_breakdown and not jpy_confirmed:
        return "Credit and financials are stable. No systemic stress signal is active."
    if (trigger_states.get("ig_widening") or trigger_states.get("hy_widening")) and trigger_states.get("loans_below_200dma"):
        return "Credit stress is broadening across spreads and loans. Systemic risk is rising."
    if not credit_stable and not jpy_confirmed:
        return "Credit indicators are turning weaker. Liquidity confirmation is not in place."
    if not credit_stable and jpy_confirmed:
        return "Credit stress and liquidity confirmation are aligned. Systemic risk is rising."
    return "Signals are mixed. Monitor for credit confirmation."


def _spillover_panel(trigger_states: Dict[str, bool]) -> Dict[str, Any]:
    ig = bool(trigger_states.get("ig_widening"))
    hy = bool(trigger_states.get("hy_widening"))
    loans = bool(trigger_states.get("loans_below_200dma"))
    xlf_breakdown = bool(trigger_states.get("xlf_breakdown"))

    if (not ig) and (not hy) and (not loans):
        headline = "Contained to equities"
        detail = "Credit indicators remain stable while equity leadership is mixed."
        color = "green"
    elif loans and (not ig) and (not hy):
        headline = "Early spillover: watch credit"
        detail = "Loans are weakening first while IG and HY are not yet confirming."
        color = "orange"
    elif (ig or hy) and loans:
        headline = "Credit confirming risk-off"
        detail = "Spread widening and loan weakness are now aligned."
        color = "red"
    else:
        headline = "Early spillover: watch credit"
        detail = "Credit has started to turn but confirmation is partial."
        color = "orange" if xlf_breakdown else "yellow"

    return {
        "headline": headline,
        "detail": detail,
        "color": color,
        "checks": [
            {"label": "IG spreads widening", "triggered": ig},
            {"label": "Loans below 200DMA", "triggered": loans},
            {"label": "JPY confirmed", "triggered": bool(trigger_states.get("jpy_confirmed"))},
            {"label": "XLF/SPY breakdown", "triggered": xlf_breakdown},
        ],
    }


def _positioning_guidance(trigger_states: Dict[str, bool], environment_label: str) -> List[Dict[str, str]]:
    credit_stable = not (
        trigger_states.get("ig_widening")
        or trigger_states.get("hy_widening")
        or trigger_states.get("loans_below_200dma")
    )
    financials_breakdown = trigger_states.get("xlf_breakdown", False)
    jpy_confirmed = trigger_states.get("jpy_confirmed", False)

    if environment_label == "RISK-OFF":
        stances = {
            "Cyclicals": ("Defensive", "defensive", "Reduce cyclical beta until credit stabilizes."),
            "Banks": ("Underweight", "defensive", "Keep bank exposure defensive while stress is broad."),
            "High beta": ("Reduce", "defensive", "Cut high-beta risk while cross-asset stress is elevated."),
            "Defensive shift": ("Required", "defensive", "Prioritize quality, cash flow, and lower-volatility sleeves."),
        }
    elif environment_label == "ELEVATED" or not credit_stable:
        stances = {
            "Cyclicals": ("Selective", "caution", "Hold only higher-quality cyclicals with stronger balance sheets."),
            "Banks": ("Selective", "caution", "Favor better-capitalized financial exposures."),
            "High beta": ("Trim", "caution", "Reduce marginal high-beta risk until credit re-stabilizes."),
            "Defensive shift": ("Recommended", "caution", "Shift a portion of risk budget toward defensives."),
        }
    else:
        stances = {
            "Cyclicals": ("Safe", "positive", "Credit remains orderly; systemic pressure is limited."),
            "Banks": (
                "Selective" if financials_breakdown else "Normal",
                "caution" if financials_breakdown else "positive",
                "Use selectivity while financial leadership remains mixed."
                if financials_breakdown
                else "Financials are not signaling broad stress.",
            ),
            "High beta": (
                "Monitor" if financials_breakdown else "Normal",
                "caution" if financials_breakdown else "positive",
                "Keep tighter risk controls while equity leadership is uneven."
                if financials_breakdown
                else "No immediate high-beta de-risking signal from credit.",
            ),
            "Defensive shift": (
                "Not required" if credit_stable and not jpy_confirmed else "Consider",
                "positive" if credit_stable and not jpy_confirmed else "caution",
                "Systemic confirmation is not present."
                if credit_stable and not jpy_confirmed
                else "Keep a contingency defensive rotation plan ready.",
            ),
        }

    order = ["Cyclicals", "Banks", "High beta", "Defensive shift"]
    return [
        {
            "bucket": key,
            "stance": stances[key][0],
            "tone": stances[key][1],
            "note": stances[key][2],
        }
        for key in order
    ]


def _systemic_trigger_panel(trigger_states: Dict[str, bool]) -> List[Dict[str, Any]]:
    return [
        {
            "label": "IG OAS widening > threshold",
            "triggered": bool(trigger_states.get("ig_widening")),
            "rule": "z-score >= +1 or IG subscore >= 60",
        },
        {
            "label": "HY widening",
            "triggered": bool(trigger_states.get("hy_widening")),
            "rule": "z-score >= +1 or HY stress score >= 70",
        },
        {
            "label": "Loans below 200DMA",
            "triggered": bool(trigger_states.get("loans_below_200dma")),
            "rule": "BKLN below long-term trend",
        },
        {
            "label": "JPY confirmed",
            "triggered": bool(trigger_states.get("jpy_confirmed")),
            "rule": "JPY risk flag with confirmation",
        },
        {
            "label": "30Y sharp move",
            "triggered": bool(trigger_states.get("dgs30_sharp_move")),
            "rule": "abs(20d move) >= 25 bps",
        },
    ]


def _component_cards(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    indicators = snapshot.get("indicators", {})
    reasons = snapshot.get("component_reasons", {})
    states = snapshot.get("component_states", {})
    subscores = snapshot.get("component_subscores", {})

    cards: List[Dict[str, Any]] = []
    for component in COMPONENT_ORDER:
        state = states.get(component, {"level": "no_data", "text": "No Data"})
        subscore = subscores.get(component)
        cards.append(
            {
                "id": component,
                "label": COMPONENT_LABELS.get(component, component),
                "status": {
                    "level": state.get("level", "no_data"),
                    "text": state.get("text", "No Data"),
                },
                "driver": _component_sentence(component, state.get("level", "no_data")),
                "why_it_matters": WHY_IT_MATTERS.get(component, ""),
                "subscore": _score_int(subscore) if isinstance(subscore, (int, float)) else None,
                "reason_raw": reasons.get(component),
                "details": indicators.get(component, {}),
            }
        )
    return cards


def _parse_reasons(alert_meta: Dict[str, Any]) -> List[str]:
    reasons = alert_meta.get("reasons")
    if isinstance(reasons, list):
        return [str(reason) for reason in reasons if str(reason).strip()][:3]
    if isinstance(reasons, str) and reasons.strip():
        raw = reasons.replace(" AND ", "; ")
        return [part.strip() for part in raw.split(";") if part.strip()][:3]
    reason = alert_meta.get("reason")
    if isinstance(reason, str) and reason.strip():
        return [reason.strip()]
    return []


def _history_points(history_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized = []
    for item in history_data:
        score = _score_int(item.get("composite_score"))
        ts = item.get("timestamp")
        date = item.get("date")
        dt = _parse_iso(ts)
        if dt is not None:
            unix = int(dt.timestamp())
            time_key = unix
        else:
            time_key = date
        normalized.append(
            {
                "time": time_key,
                "value": score,
                "timestamp": ts,
                "date": date,
            }
        )

    # Sort oldest -> newest for chart rendering.
    normalized.sort(key=lambda row: row.get("timestamp") or row.get("date") or "")
    return normalized


def _integrity(snapshot_score: int, computed_at: str, history_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not history_data:
        return {
            "consistent": True,
            "badge": "consistent",
            "badge_text": "Consistent run",
            "history_last_score": None,
            "history_last_timestamp": None,
            "history_lag_minutes": None,
            "diff": None,
        }

    latest_history = history_data[0]
    history_score = _score_int(latest_history.get("composite_score"))
    history_ts = latest_history.get("timestamp")
    diff = abs(float(snapshot_score) - float(history_score))
    consistent = diff <= 0.5

    lag_minutes = None
    snap_dt = _parse_iso(computed_at)
    history_dt = _parse_iso(history_ts)
    if snap_dt and history_dt:
        lag_minutes = max(0, int((snap_dt - history_dt).total_seconds() / 60))

    return {
        "consistent": consistent,
        "badge": "consistent" if consistent else "lagging",
        "badge_text": "Consistent run" if consistent else "History lagging",
        "history_last_score": history_score,
        "history_last_timestamp": history_ts,
        "history_lag_minutes": lag_minutes,
        "diff": round(diff, 2),
    }


def _build_dashboard_payload(
    structural_snapshot: Dict[str, Any],
    preview_snapshot: Optional[Dict[str, Any]],
    last_alert: Optional[Dict[str, Any]],
    history_data: List[Dict[str, Any]],
) -> Dict[str, Any]:
    composite_score = _score_int(structural_snapshot.get("composite_score"))
    environment = _environment_label(composite_score)
    trigger_states = structural_snapshot.get("trigger_states", {})
    component_states = structural_snapshot.get("component_states", {})
    computed_at = structural_snapshot.get("computed_at_utc")
    integrity = _integrity(composite_score, computed_at, history_data)

    # Root-cause note:
    # 1) Banner score previously came from /api/latest -> Redis key msm:latest.composite_score.
    # 2) Chart score previously came from /api/history -> Redis key msm:history[0].composite_score.
    # 3) These were independent requests and independent keys, so a read could straddle writes.
    # 4) Composite scores are integers in scoring (no float/int rounding divergence).
    # 5) History is day-bucketed by date for charting context, so it can lag intraday state.
    # The dashboard now binds to one snapshot run_id and treats history as context only.
    alert_meta = last_alert or {}
    chart_points = _history_points(history_data)
    latest_history_ts = integrity.get("history_last_timestamp")

    structural_dt = _parse_iso(computed_at)
    preview_dt = _parse_iso(preview_snapshot.get("computed_at_utc")) if preview_snapshot else None
    preview_stale = bool((preview_snapshot or {}).get("is_stale")) or bool(preview_dt and structural_dt and preview_dt < structural_dt)
    preview_components = preview_snapshot.get("components", {}) if isinstance(preview_snapshot, dict) else {}
    preview_labels = {
        "leveraged_loans": "Loans",
        "xlf_spy": "Financials",
        "jpy_risk": "JPY",
        "kre_spy": "Regional Banks",
    }

    def _preview_item(component: str) -> Dict[str, str]:
        card = preview_components.get(component, {}) if isinstance(preview_components, dict) else {}
        state = card.get("state", {})
        return {
            "label": str(card.get("label") or preview_labels.get(component, component)),
            "state_text": str(state.get("text") or card.get("status") or "Unavailable"),
            "level": str(state.get("level") or "unavailable"),
        }

    if preview_snapshot:
        if preview_stale:
            badge_text = "Intraday Preview stale"
        else:
            et_label = _format_et_time(preview_snapshot.get("computed_at_utc")) or "--:-- ET"
            badge_text = f"Intraday Preview: ON (as of {et_label})"
    else:
        badge_text = "Intraday Preview: OFF"

    payload = {
        "run_id": structural_snapshot.get("run_id"),
        "computed_at_utc": computed_at,
        "environment": environment,
        "composite_score": composite_score,
        "regime_banner": {
            "label": environment["label"],
            "color": environment["color"],
            "summary": _regime_summary(trigger_states, component_states),
            "score": composite_score,
            "confidence": str(structural_snapshot.get("confidence", "N/A")).upper(),
            "reliable": bool(structural_snapshot.get("reliable", False)),
            "drivers": structural_snapshot.get("primary_drivers", []),
        },
        "spillover_risk": _spillover_panel(trigger_states),
        "positioning_guidance": _positioning_guidance(trigger_states, environment["label"]),
        "systemic_triggers": _systemic_trigger_panel(trigger_states),
        "component_cards": _component_cards(structural_snapshot),
        "intraday_preview": {
            "available": preview_snapshot is not None,
            "stale": preview_stale,
            "badge_text": badge_text,
            "computed_at_utc": preview_snapshot.get("computed_at_utc") if preview_snapshot else None,
            "assessment": preview_snapshot.get("preview_spillover_assessment") if preview_snapshot else None,
            "components": {
                "loans": _preview_item("leveraged_loans"),
                "financials": _preview_item("xlf_spy"),
                "jpy": _preview_item("jpy_risk"),
                "regional_banks": _preview_item("kre_spy"),
            },
        },
        "alert_display": {
            "timestamp": alert_meta.get("timestamp"),
            "score_at_alert": alert_meta.get("score_at_alert", alert_meta.get("composite_score")),
            "reasons": _parse_reasons(alert_meta),
            "cooldown_until": alert_meta.get("cooldown_until"),
        },
        "integrity": integrity,
        "chart": {
            "points": chart_points,
            "latest_marker_score": composite_score,
            "last_history_timestamp": latest_history_ts,
        },
        "as_of": {
            "banner": computed_at,
            "components": computed_at,
            "chart": latest_history_ts,
        },
    }
    return payload


def create_app():
    """Creates and configures the Flask application."""
    app = Flask(__name__)

    try:
        redis_client = RedisClient()
    except Exception as exc:
        print(f"ERROR: Could not connect to Redis. Dashboard data will be unavailable. {exc}")
        redis_client = None

    @app.route("/")
    def dashboard():
        return render_template("index.html")

    @app.route("/api/dashboard")
    def api_dashboard():
        if redis_client is None:
            return jsonify({"error": "Redis unavailable"}), 503
        structural_snapshot = redis_client.get_latest_structural_snapshot()
        if structural_snapshot is None:
            return jsonify({"error": "No structural run snapshot available"}), 404
        history_data = redis_client.get_history()
        preview_snapshot = redis_client.get_latest_preview_snapshot()
        payload = _build_dashboard_payload(
            structural_snapshot=structural_snapshot,
            preview_snapshot=preview_snapshot,
            last_alert=redis_client.get_last_alert(),
            history_data=history_data,
        )
        return jsonify(payload)

    @app.route("/api/latest")
    def api_latest():
        if redis_client is None:
            return jsonify({"error": "Redis unavailable"}), 503
        structural_snapshot = redis_client.get_latest_structural_snapshot()
        if structural_snapshot is None:
            return jsonify({"error": "No structural run snapshot available"}), 404
        history_data = redis_client.get_history()
        preview_snapshot = redis_client.get_latest_preview_snapshot()
        payload = _build_dashboard_payload(
            structural_snapshot=structural_snapshot,
            preview_snapshot=preview_snapshot,
            last_alert=redis_client.get_last_alert(),
            history_data=history_data,
        )
        return jsonify(payload)

    @app.route("/api/history")
    def api_history():
        if redis_client is None:
            return jsonify({"error": "Redis unavailable"}), 503
        history_data = redis_client.get_history()
        return jsonify(_history_points(history_data)), 200

    @app.route("/api/health")
    def api_health():
        if redis_client is None:
            return jsonify({"error": "Redis unavailable"}), 503
        return jsonify(reader.get_health_snapshot(redis_client)), 200

    @app.route("/api/macro/latest")
    def api_macro_latest():
        if redis_client is None:
            return jsonify({"error": "Redis unavailable"}), 503
        snapshot = reader.get_latest_structural_snapshot(redis_client)
        if snapshot is None:
            return jsonify({"error": "No structural run snapshot available"}), 404
        return jsonify(snapshot), 200

    @app.route("/api/macro/preview")
    def api_macro_preview():
        if redis_client is None:
            return jsonify({"error": "Redis unavailable"}), 503
        snapshot = reader.get_latest_preview_snapshot(redis_client)
        if snapshot is None:
            return jsonify({"error": "No preview run snapshot available"}), 404
        return jsonify(snapshot), 200

    @app.route("/api/macro/context")
    def api_macro_context():
        if redis_client is None:
            return jsonify({"error": "Redis unavailable"}), 503
        context = reader.get_latest_market_context(redis_client)
        if context is None:
            return jsonify({"error": "No market context available"}), 404
        return jsonify(context), 200

    # --- Render Layer API ---
    @app.route("/api/render/terminal")
    def api_render_terminal():
        if redis_client is None:
            return jsonify({"error": "Redis unavailable"}), 503
        health = reader.get_health_snapshot(redis_client) or {}
        struct = reader.get_latest_structural_snapshot(redis_client) or {}
        prev = reader.get_latest_preview_snapshot(redis_client) or {}
        ctx = reader.get_latest_market_context(redis_client) or {}
        payload = render.build_terminal_payload(health, struct, prev, ctx)
        return jsonify(payload), 200

    @app.route("/api/render/status")
    def api_render_status():
        if redis_client is None:
            return jsonify({"error": "Redis unavailable"}), 503
        health = reader.get_health_snapshot(redis_client) or {}
        struct = reader.get_latest_structural_snapshot(redis_client) or {}
        prev = reader.get_latest_preview_snapshot(redis_client) or {}
        return jsonify(render.build_status_strip(health, struct, prev)), 200

    @app.route("/api/render/structural")
    def api_render_structural():
        if redis_client is None:
            return jsonify({"error": "Redis unavailable"}), 503
        struct = reader.get_latest_structural_snapshot(redis_client) or {}
        return jsonify(render.build_structural_summary(struct)), 200

    @app.route("/api/render/preview")
    def api_render_preview():
        if redis_client is None:
            return jsonify({"error": "Redis unavailable"}), 503
        prev = reader.get_latest_preview_snapshot(redis_client) or {}
        return jsonify(render.build_preview_summary(prev)), 200

    @app.route("/api/render/health")
    def api_render_health_panel():
        if redis_client is None:
            return jsonify({"error": "Redis unavailable"}), 503
        health = reader.get_health_snapshot(redis_client) or {}
        struct = reader.get_latest_structural_snapshot(redis_client) or {}
        return jsonify(render.build_health_summary(health, struct)), 200

    @app.route("/api/render/context")
    def api_render_context():
        if redis_client is None:
            return jsonify({"error": "Redis unavailable"}), 503
        ctx = reader.get_latest_market_context(redis_client) or {}
        mc_data = ctx.get("market_context", ctx)
        return jsonify(render.build_market_context(mc_data)), 200

    @app.route("/favicon.ico")
    def favicon():
        return Response(status=204)

    return app


if __name__ == "__main__":
    app = create_app()
    host = os.getenv("MSM_WEB_HOST", "0.0.0.0")
    port = int(os.getenv("MSM_WEB_PORT", "5001"))
    app.run(debug=True, host=host, port=port)
