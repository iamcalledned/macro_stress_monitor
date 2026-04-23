"""
Render presentation layer.
Transforms raw foundation snapshots into UI-ready models, removing all business and formatting logic from the frontend.
Includes slots for future AI brain integrations.
"""

from typing import Any, Dict, List, Optional
import json

from .render_helpers import (
    format_time_et,
    format_num,
    get_env_color,
    get_env_text_color,
    class_from_state,
)

def _get_brain_hooks() -> Dict[str, Any]:
    return {
        "headline_slot": None,
        "key_points_slot": [],
        "alignment_slot": None,
        "risk_bias_slot": None,
        "interpretation_slot": None,
    }

def build_status_strip(health: Dict[str, Any], struct: Dict[str, Any], prev: Dict[str, Any]) -> Dict[str, Any]:
    # Construct Structural portion
    score = struct.get("composite_score", "--")
    regime_label = struct.get("regime_label", "UNKNOWN")
    
    # Construct flags
    flags = []
    if struct.get("is_stale"):
        flags.append("STRUC_STALE")
    anomaly = struct.get("anomaly_flags", {})
    if anomaly.get("extreme_move"):
        flags.append("EXT_MOVE")
    if anomaly.get("volatility_spike"):
        flags.append("VOL_SPIKE")
    if anomaly.get("multi_asset_divergence"):
        flags.append("DIVERGENCE")
        
    dq = struct.get("data_quality", {})
    if dq.get("critical_missing"):
        flags.append("MISSING_CRIT")
        
    flags_text = " ".join(flags) if flags else "OK"
    flags_class = "color-orange" if flags else "color-green"
    
    # Quality & Confidence
    sc = struct.get("state_confidence", {})
    conf = sc.get("confidence_level") or struct.get("confidence") or "N/A"
    completeness = dq.get("completeness_score")
    qual_text = f"{int(completeness * 100)}%" if completeness is not None else "N/A"
    
    # Preview
    if prev and not prev.get("error"):
        pa = prev.get("preview_spillover_assessment", "N/A")
        prev_class = "bg-green"
        if "Credit confirming" in pa:
            prev_class = "bg-red"
        elif "watch credit" in pa:
            prev_class = "bg-orange"
        
        prev_display = {
            "text": pa,
            "class_name": f"badge {prev_class}",
            "time_et": format_time_et(prev.get("computed_at_utc"))
        }
    else:
        prev_display = {
            "text": "OFF",
            "class_name": "badge bg-muted",
            "time_et": "--"
        }
        
    return {
        "structural": {
            "regime_text": regime_label,
            "regime_class": f"badge {get_env_color(score)}",
            "score_text": str(score),
            "score_class": get_env_text_color(score),
            "time_et": format_time_et(struct.get("computed_at_utc")),
            "quality_text": qual_text,
            "confidence_text": conf,
            "flags_text": flags_text,
            "flags_class": flags_class
        },
        "preview": prev_display,
        "brain_hooks": _get_brain_hooks()
    }


def build_structural_summary(struct: Dict[str, Any]) -> Dict[str, Any]:
    score = struct.get("composite_score", "--")
    regime = struct.get("regime") or struct.get("regime_label") or "--"
    
    # Extract drivers robustly
    drivers = struct.get("primary_drivers", [])
    ss = struct.get("signal_summary", {})
    if ss.get("primary_driver"):
        drivers = ss.get("primary_driver")
    elif isinstance(drivers, list):
        drivers = ", ".join(drivers)
    elif isinstance(drivers, dict):
        drivers = drivers.get("one_line") or json.dumps(drivers)
        
    drivers_text = str(drivers).upper() if isinstance(drivers, str) else "NONE"
    
    # Deltas
    d_text = "--"
    d_class = "val-neutral"
    delta = struct.get("delta", {})
    if delta.get("available"):
        chg = delta.get("score_change", 0)
        prefix = "+" if chg > 0 else ""
        d_text = f"{prefix}{chg}"
        if chg > 0:
            d_class = "val-negative"
        elif chg < 0:
            d_class = "val-positive"
            
    return {
        "score_text": str(score),
        "score_class": f"big-score {get_env_text_color(score)}",
        "regime_text": str(regime).upper(),
        "regime_class": f"summary-line {class_from_state(regime)}",
        "drivers_text": drivers_text,
        "delta_text": d_text,
        "delta_class": d_class,
        "brain_hooks": _get_brain_hooks()
    }


def build_preview_summary(prev: Dict[str, Any]) -> Dict[str, Any]:
    assessment = prev.get("preview_spillover_assessment", "--")
    
    rows = []
    pc = prev.get("component_statuses") or prev.get("components") or {}
    for k, v in pc.items():
        if isinstance(v, dict):
            text = v.get("status") or (v.get("state") or {}).get("text") or json.dumps(v)
        else:
            text = str(v)
        rows.append({"label": k, "value": text})
        
    session = (prev.get("session") or {}).get("market_session", "--")
    delta_text = "Updated" if (prev.get("delta") or {}).get("available") else "--"
    
    return {
        "assessment_text": assessment,
        "components": rows,
        "session_text": session,
        "delta_text": delta_text,
        "brain_hooks": _get_brain_hooks()
    }


def build_health_summary(health: Dict[str, Any], struct: Dict[str, Any]) -> Dict[str, Any]:
    s_age_min = round((health.get("structural_age_seconds") or 0) / 60)
    p_age_min = round((health.get("preview_age_seconds") or 0) / 60)
    
    s_stale = health.get("structural_stale", False)
    p_stale = health.get("preview_stale", False)
    
    exec_sec = (struct.get("execution") or {}).get("total_seconds")
    exec_text = f"{format_num(exec_sec, 2)}s" if exec_sec is not None else "--"
    
    dq = struct.get("data_quality", {})
    crit_missing = dq.get("critical_missing", False)
    crit_text = "YES" if crit_missing else "OK"
    non_crit_text = str(dq.get("noncritical_missing_count", 0))
    
    # Mini panels
    conf = (struct.get("state_confidence") or {}).get("confidence_level") or struct.get("confidence") or "N/A"
    conf_class = "val-positive" if "HIGH" in str(conf) else ("val-negative" if "LOW" in str(conf) else "val-warning")
    
    completeness = dq.get("completeness_score")
    qual_text = f"{int(completeness * 100)}%" if completeness is not None else "N/A"
    qual_class = "val-positive" if completeness and completeness > 0.9 else "val-warning"
    
    # Mini flags
    flgs = []
    if struct.get("is_stale"): flgs.append("STALE")
    anomaly = struct.get("anomaly_flags", {})
    if anomaly.get("extreme_move"): flgs.append("EXT_MOVE")
    if anomaly.get("volatility_spike"): flgs.append("VOL_SPIKE")
    if anomaly.get("multi_asset_divergence"): flgs.append("DIVERGENCE")
    
    if flgs:
        flags_html = "".join([f'<div class="color-red">⚠ {f}</div>' for f in flgs])
    else:
        flags_html = '<div class="color-green">OK</div>'
        
    return {
        "structural_age_text": f"{s_age_min}m",
        "structural_age_class": "color-red" if s_stale else "color-green",
        "preview_age_text": f"{p_age_min}m",
        "preview_age_class": "color-red" if p_stale else "color-green",
        "execution_text": exec_text,
        "critical_text": crit_text,
        "critical_class": "color-red" if crit_missing else "color-green",
        "noncritical_text": non_crit_text,
        "mini_conf_text": conf,
        "mini_conf_class": conf_class,
        "mini_qual_text": qual_text,
        "mini_qual_class": qual_class,
        "mini_flags_html": flags_html,
        "brain_hooks": _get_brain_hooks()
    }


def build_notable_changes(struct: Dict[str, Any]) -> Dict[str, Any]:
    rows = []
    delta = struct.get("delta", {})
    
    if delta.get("available"):
        if delta.get("regime_changed"):
            prev_r = delta.get("previous_regime", "UNKNOWN")
            curr_r = delta.get("current_regime", "UNKNOWN")
            rows.append({
                "text_html": f'+ REGIME: {prev_r} -> {curr_r}',
                "class_name": "val-warning",
                "style": "margin-bottom: 4px;"
            })
            
        stc = delta.get("component_state_changes", {})
        for k, v in stc.items():
            prev = v.get("previous", "UNKNOWN")
            curr = v.get("current", "UNKNOWN")
            curr_class = class_from_state(curr)
            rows.append({
                "text_html": f'+ {str(k).upper()}: <span class="color-muted">{prev}</span> -> <span class="{curr_class}">{curr}</span>',
                "class_name": "",
                "style": "margin-bottom: 2px;"
            })
            
        ssc = delta.get("component_subscore_changes", {})
        for k, v in ssc.items():
            prev = v.get("previous", "UNKNOWN")
            curr = v.get("current", "UNKNOWN")
            rows.append({
                "text_html": f'+ {str(k).upper()}: <span class="color-muted">{prev}</span> -> <span>{curr}</span>',
                "class_name": "",
                "style": "margin-bottom: 2px;"
            })
            
    if not rows:
        rows.append({
            "text_html": "NO MATERIAL CHANGE",
            "class_name": "color-muted",
            "style": ""
        })
        
    return {
        "rows": rows,
        "brain_hooks": _get_brain_hooks()
    }


def build_market_context(mc: Dict[str, Any]) -> List[Dict[str, Any]]:
    # Define sections
    sections_def = [
        {"id": "ctx-macro", "key": "macro_rates", "title": "MACRO / RATES", "headers": ["SERIES", "VALUE", "5D Δ", "20D Δ", "Z", "STATE"]},
        {"id": "ctx-credit", "key": "credit_liquidity", "title": "CREDIT / LIQ", "headers": ["SERIES", "VALUE", "Z", "STATE", "STRETCH"]},
        {"id": "ctx-equity", "key": "equity_index_state", "title": "EQUITY", "headers": ["ASSET", "5D Δ", "20D Δ", "vs 200DMA", "Z", "STATE", "STRETCH"]},
        {"id": "ctx-sectors", "key": "sector_state", "title": "SECTORS", "headers": ["SECTOR", "vs SPY 5D", "vs SPY 20D", "Z", "LEADERSHIP"]},
        {"id": "ctx-vol", "key": "volatility_stress", "title": "VOL / STRESS", "headers": ["ASSET", "REALIZED", "PCTILE", "STATE"]},
        {"id": "ctx-flight", "key": "flight_to_safety", "title": "SAFETY", "headers": ["ASSET", "STATE", "STRETCH", "DESC"]},
        {"id": "ctx-cross", "key": "cross_asset_relationships", "title": "CROSS ASSET", "headers": ["PAIR", "RATIO", "5D Δ", "Z", "STATE"]},
        {"id": "ctx-breadth", "key": "breadth_participation", "title": "BREADTH", "headers": ["METRIC", "VALUE", "PCT"]},
        {"id": "ctx-positioning", "key": "positioning_stretch", "title": "POSITIONING", "headers": ["ASSET", "RSI", "vs 200DMA", "STRETCH"]}
    ]
    
    result_sections = []
    
    for idx, sec in enumerate(sections_def):
        data = mc.get(sec["key"], {})
        
        # Check alerts for tab dots
        has_data = len(data) > 0
        j_str = json.dumps(data).lower()
        has_alert = any(w in j_str for w in ["triggered", "breakdown", "high_vol", "falling_fast"])
        
        if has_alert:
            tab_html = f'<span class="color-red">⚠</span> {sec["title"]}'
        elif has_data:
            tab_html = f'<span class="color-green">●</span> {sec["title"]}'
        else:
            tab_html = f'<span class="color-muted">○</span> {sec["title"]}'
            
        rows = []
        k = sec["key"]
        
        # Build normalized rows: [{"cells": [{"html": "...", "class": "..."}, ...]}, ...]
        if k == "macro_rates":
            def _add(name, v):
                if not v: return
                rows.append({"cells": [
                    {"html": name},
                    {"html": format_num(v.get("latest_bps", v.get("latest"))), "class": "num-cell"},
                    {"html": format_num(v.get("change_5d_bps", v.get("change_5d"))), "class": "num-cell"},
                    {"html": format_num(v.get("change_20d_bps", v.get("change_20d"))), "class": "num-cell"},
                    {"html": format_num(v.get("z_score_1y")), "class": "num-cell"},
                    {"html": v.get("state", "--"), "class": class_from_state(v.get("state"))}
                ]})
            for sk, sv in data.get("rates", {}).items(): _add(sk, sv)
            for sk, sv in data.get("curve_spreads", {}).items(): _add(sk, sv)
            for sk, sv in data.get("inflation_growth", {}).items(): _add(sk, sv)
            if data.get("dollar_proxy", {}).get("state"): _add("Dollar (UUP)", data.get("dollar_proxy")["state"])
            if data.get("real_rate_proxy", {}).get("state"): _add("Real Rate (10Y-BE)", data.get("real_rate_proxy")["state"])
            
        elif k == "credit_liquidity":
            def _add(name, v):
                if not v: return
                rows.append({"cells": [
                    {"html": name},
                    {"html": format_num(v.get("latest", v.get("latest_ratio", v.get("latest_bps")))), "class": "num-cell"},
                    {"html": format_num(v.get("z_score_1y", v.get("z_score"))), "class": "num-cell"},
                    {"html": v.get("state", "--"), "class": class_from_state(v.get("state"))},
                    {"html": v.get("stretch_state", "--"), "class": class_from_state(v.get("stretch_state"))}
                ]})
            if data.get("ig_oas"): _add("IG OAS", data.get("ig_oas"))
            if data.get("hy_oas"): _add("HY OAS", data.get("hy_oas"))
            if data.get("loan_proxy"): _add("Loan Proxy (BKLN)", data.get("loan_proxy"))
            for sk, sv in data.get("credit_etf_relationships", {}).items(): _add(sk, sv)
            for sk, sv in data.get("liquidity_sensitive_proxies", {}).items(): _add(sk, sv)
            
        elif k == "equity_index_state":
            for sk, v in data.items():
                if not v or not isinstance(v, dict): continue
                rows.append({"cells": [
                    {"html": sk},
                    {"html": format_num(v.get("return_5d", 0), is_percent=True), "class": "num-cell"},
                    {"html": format_num(v.get("return_20d", 0), is_percent=True), "class": "num-cell"},
                    {"html": format_num(v.get("distance_200dma", 0), is_percent=True), "class": "num-cell"},
                    {"html": format_num(v.get("z_score_1y")), "class": "num-cell"},
                    {"html": v.get("trend_state", "--"), "class": class_from_state(v.get("trend_state"))},
                    {"html": v.get("stretch_state", "--"), "class": class_from_state(v.get("stretch_state"))}
                ]})
                
        elif k == "sector_state":
            for sk, v in data.items():
                if not v or not isinstance(v, dict): continue
                rel = v.get("relative_to_spy", {})
                lead = "YES" if v.get("leadership_flag") else ("LAG" if v.get("laggard_flag") else "--")
                rows.append({"cells": [
                    {"html": sk},
                    {"html": format_num(rel.get("return_5d", 0), is_percent=True), "class": "num-cell"},
                    {"html": format_num(rel.get("return_20d", 0), is_percent=True), "class": "num-cell"},
                    {"html": format_num(rel.get("z_score_1y")), "class": "num-cell"},
                    {"html": lead, "class": class_from_state(lead)}
                ]})
                
        elif k == "volatility_stress":
            def _add(name, v):
                if not v: return
                rows.append({"cells": [
                    {"html": name},
                    {"html": format_num(v.get("realized_vol_20d")), "class": "num-cell"},
                    {"html": format_num(v.get("realized_vol_percentile_1y")), "class": "num-cell"},
                    {"html": v.get("vol_state", v.get("trend_state", "--")), "class": class_from_state(v.get("vol_state", v.get("trend_state")))}
                ]})
            if data.get("vix_proxy", {}).get("state"): _add("VIX Proxy (VIXY)", data.get("vix_proxy")["state"])
            if data.get("move_proxy", {}).get("state"): _add("MOVE Proxy (TLT Vol)", data.get("move_proxy")["state"])
            for sk, sv in data.get("realized_volatility", {}).items(): _add(sk, sv)
            for sk, sv in data.get("stress_flags", {}).items():
                text = "TRIGGERED" if sv else "OK"
                rows.append({"cells": [
                    {"html": sk},
                    {"html": "--", "class": "num-cell"},
                    {"html": "--", "class": "num-cell"},
                    {"html": text, "class": class_from_state(text)}
                ]})
                
        elif k == "flight_to_safety":
            def _add(name, v):
                if not v: return
                rows.append({"cells": [
                    {"html": name},
                    {"html": v.get("state", v.get("trend_state", "--")), "class": class_from_state(v.get("state", v.get("trend_state")))},
                    {"html": v.get("stretch_state", "--"), "class": class_from_state(v.get("stretch_state"))},
                    {"html": "--"}
                ]})
            for sk, sv in data.get("treasury_proxies", {}).items(): _add(sk, sv)
            if data.get("gold_proxy"): _add("Gold Proxy (GLD)", data.get("gold_proxy"))
            if data.get("dollar_proxy"): _add("Dollar Proxy (UUP)", data.get("dollar_proxy"))
            if data.get("jpy_proxy"): _add("JPY Proxy", data.get("jpy_proxy"))
            for sk, sv in data.get("defensive_vs_cyclical", {}).items(): _add(sk, sv)
                
        elif k == "cross_asset_relationships":
            for sk, v in data.items():
                if not v or not isinstance(v, dict): continue
                rows.append({"cells": [
                    {"html": sk},
                    {"html": format_num(v.get("latest_ratio")), "class": "num-cell"},
                    {"html": format_num(v.get("return_5d", 0), is_percent=True), "class": "num-cell"},
                    {"html": format_num(v.get("z_score_1y")), "class": "num-cell"},
                    {"html": v.get("state", "--"), "class": class_from_state(v.get("state"))}
                ]})
                
        elif k == "breadth_participation":
            def _add(name, v, pct):
                rows.append({"cells": [
                    {"html": name},
                    {"html": str(v), "class": "num-cell"},
                    {"html": str(pct), "class": "num-cell"}
                ]})
            if data.get("tracked_count") is not None: _add("Tracked ETFs", data.get("tracked_count"), "--")
            if data.get("above_50dma_count") is not None: _add("Above 50DMA", data.get("above_50dma_count"), format_num(data.get("above_50dma_pct", 0), is_percent=True))
            if data.get("above_200dma_count") is not None: _add("Above 200DMA", data.get("above_200dma_count"), format_num(data.get("above_200dma_pct", 0), is_percent=True))
            if data.get("positive_20d_trend_count") is not None: _add("Pos 20d Trend", data.get("positive_20d_trend_count"), format_num(data.get("positive_20d_trend_pct", 0), is_percent=True))
            if data.get("sectors_above_50dma_count") is not None: _add("Sectors Above 50DMA", data.get("sectors_above_50dma_count"), "--")
            if data.get("sectors_above_200dma_count") is not None: _add("Sectors Above 200DMA", data.get("sectors_above_200dma_count"), "--")
            
        elif k == "positioning_stretch":
            for sk, v in data.get("assets", {}).items():
                if not v: continue
                rows.append({"cells": [
                    {"html": sk},
                    {"html": format_num(v.get("rsi_14d")), "class": "num-cell"},
                    {"html": format_num(v.get("distance_200dma", 0), is_percent=True), "class": "num-cell"},
                    {"html": v.get("stretch_state", "--"), "class": class_from_state(v.get("stretch_state"))}
                ]})
            for sk, v in data.get("relationships", {}).items():
                if not v: continue
                rows.append({"cells": [
                    {"html": sk},
                    {"html": "--", "class": "num-cell"},
                    {"html": format_num(v.get("distance_50dma", 0), is_percent=True), "class": "num-cell"},
                    {"html": v.get("stretch_state", "--"), "class": class_from_state(v.get("stretch_state"))}
                ]})
                
        result_sections.append({
            "id": sec["id"],
            "title": sec["title"],
            "headers": sec["headers"],
            "tab_html": tab_html,
            "rows": rows,
            "is_active": idx == 0,
            "brain_hooks": _get_brain_hooks()
        })
        
    return result_sections


def build_terminal_payload(
    health: Dict[str, Any],
    struct: Dict[str, Any],
    prev: Dict[str, Any],
    context: Dict[str, Any]
) -> Dict[str, Any]:
    """Builds the composite payload for the entire terminal UI."""
    mc_data = context.get("market_context", context)
    return {
        "status_strip": build_status_strip(health, struct, prev),
        "structural_summary": build_structural_summary(struct),
        "preview_summary": build_preview_summary(prev),
        "health_summary": build_health_summary(health, struct),
        "notable_changes": build_notable_changes(struct),
        "market_context": build_market_context(mc_data),
        "audit": {
            "health": health,
            "struct": struct,
            "prev": prev,
            "context": context
        }
    }
