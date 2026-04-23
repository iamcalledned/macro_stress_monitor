"""
Render presentation layer.
Transforms raw foundation snapshots into explicitly normalized DOM bindings
and pre-rendered HTML blocks, removing all logic from the frontend.
Includes slots for future AI brain integrations.
"""

from typing import Any, Dict, List, Callable
import json

from .render_helpers import (
    format_time_et,
    format_num,
    get_env_color,
    get_env_text_color,
    class_from_state,
)

try:
    from .briefs import build_current_state_brief
    from .comparisons import compare_structural_vs_preview
    from .llm_briefs import get_cached_morning_brief, get_cached_evening_wrap
except ImportError:
    from services.briefs import build_current_state_brief
    from services.comparisons import compare_structural_vs_preview
    from services.llm_briefs import get_cached_morning_brief, get_cached_evening_wrap

def _get_brain_hooks() -> Dict[str, Any]:
    return {
        "headline_slot": None,
        "key_points_slot": [],
        "alignment_slot": None,
        "risk_bias_slot": None,
        "interpretation_slot": None,
    }


def _bind(id_name: str, text: Any = None, class_name: str = None, html: str = None) -> Dict[str, Any]:
    """Helper to generate a normalized binding payload."""
    b = {"id": id_name}
    if text is not None: b["text"] = str(text)
    if class_name is not None: b["class_name"] = str(class_name)
    if html is not None: b["html"] = str(html)
    return b


def build_status_strip_bindings(health: Dict[str, Any], struct: Dict[str, Any], prev: Dict[str, Any]) -> List[Dict[str, Any]]:
    b = []
    
    # Structural
    score = struct.get("composite_score", "--")
    regime_label = struct.get("regime_label", "UNKNOWN")
    b.append(_bind("ts-regime", text=regime_label, class_name=f"badge {get_env_color(score)}"))
    b.append(_bind("ts-score", text=score, class_name=get_env_text_color(score)))
    b.append(_bind("ts-struc-time", text=format_time_et(struct.get("computed_at_utc"))))
    
    dq = struct.get("data_quality", {})
    completeness = dq.get("completeness_score")
    qual_text = f"{int(completeness * 100)}%" if completeness is not None else "N/A"
    b.append(_bind("ts-quality", text=qual_text))
    
    sc = struct.get("state_confidence", {})
    conf = sc.get("confidence_level") or struct.get("confidence") or "N/A"
    b.append(_bind("ts-conf", text=conf))
    
    # Flags
    flags = []
    if struct.get("is_stale"): flags.append("STRUC_STALE")
    anomaly = struct.get("anomaly_flags", {})
    if anomaly.get("extreme_move"): flags.append("EXT_MOVE")
    if anomaly.get("volatility_spike"): flags.append("VOL_SPIKE")
    if anomaly.get("multi_asset_divergence"): flags.append("DIVERGENCE")
    if dq.get("critical_missing"): flags.append("MISSING_CRIT")
        
    b.append(_bind(
        "ts-flags", 
        text=" ".join(flags) if flags else "OK", 
        class_name="color-orange" if flags else "color-green"
    ))
    
    # Preview
    if prev and not prev.get("error"):
        pa = prev.get("preview_spillover_assessment", "N/A")
        prev_class = "bg-green"
        if "Credit confirming" in pa: prev_class = "bg-red"
        elif "watch credit" in pa: prev_class = "bg-orange"
        
        b.append(_bind("ts-preview", text=pa, class_name=f"badge {prev_class}"))
        b.append(_bind("ts-prev-time", text=format_time_et(prev.get("computed_at_utc"))))
    else:
        b.append(_bind("ts-preview", text="OFF", class_name="badge bg-muted"))
        b.append(_bind("ts-prev-time", text="--"))
        
    return b


def build_structural_summary_bindings(struct: Dict[str, Any]) -> List[Dict[str, Any]]:
    b = []
    score = struct.get("composite_score", "--")
    b.append(_bind("ss-score", text=score, class_name=f"big-score {get_env_text_color(score)}"))
    
    regime = struct.get("regime") or struct.get("regime_label") or "--"
    b.append(_bind("ss-regime", text=str(regime).upper(), class_name=f"summary-line {class_from_state(regime)}"))
    
    # Drivers
    drivers = struct.get("primary_drivers", [])
    ss = struct.get("signal_summary", {})
    if ss.get("primary_driver"): drivers = ss.get("primary_driver")
    elif isinstance(drivers, list): drivers = ", ".join(drivers)
    elif isinstance(drivers, dict): drivers = drivers.get("one_line") or json.dumps(drivers)
    b.append(_bind("ss-drivers", text=str(drivers).upper() if isinstance(drivers, str) else "NONE"))
    
    # Deltas
    delta = struct.get("delta", {})
    if delta.get("available"):
        chg = delta.get("score_change", 0)
        prefix = "+" if chg > 0 else ""
        d_class = "val-negative" if chg > 0 else ("val-positive" if chg < 0 else "val-neutral")
        b.append(_bind("ss-delta", text=f"{prefix}{chg}", class_name=d_class))
    else:
        b.append(_bind("ss-delta", text="--", class_name="val-neutral"))
        
    return b


def build_preview_summary_bindings(prev: Dict[str, Any]) -> List[Dict[str, Any]]:
    b = []
    b.append(_bind("ps-assessment", text=prev.get("preview_spillover_assessment", "--")))
    
    session = (prev.get("session") or {}).get("market_session", "--")
    b.append(_bind("ps-session", text=session))
    
    delta_text = "Updated" if (prev.get("delta") or {}).get("available") else "--"
    b.append(_bind("ps-delta", text=delta_text))
    
    rows = []
    pc = prev.get("component_statuses") or prev.get("components") or {}
    for k, v in pc.items():
        text = (v.get("status") or (v.get("state") or {}).get("text") or json.dumps(v)) if isinstance(v, dict) else str(v)
        rows.append(f"<tr><td>{k}</td><td>{text}</td></tr>")
        
    b.append(_bind("ps-components", html="".join(rows) if rows else "<tr><td colspan='2'>No components</td></tr>"))
    return b


def build_health_summary_bindings(health: Dict[str, Any], struct: Dict[str, Any]) -> List[Dict[str, Any]]:
    b = []
    s_age = round((health.get("structural_age_seconds") or 0) / 60)
    p_age = round((health.get("preview_age_seconds") or 0) / 60)
    
    b.append(_bind("hl-struc-age", text=f"{s_age}m", class_name="color-red" if health.get("structural_stale") else "color-green"))
    b.append(_bind("hl-prev-age", text=f"{p_age}m", class_name="color-red" if health.get("preview_stale") else "color-green"))
    
    exec_sec = (struct.get("execution") or {}).get("total_seconds")
    b.append(_bind("hl-exec", text=f"{format_num(exec_sec, 2)}s" if exec_sec is not None else "--"))
    
    dq = struct.get("data_quality", {})
    crit_missing = dq.get("critical_missing", False)
    b.append(_bind("hl-missing-crit", text="YES" if crit_missing else "OK", class_name="color-red" if crit_missing else "color-green"))
    b.append(_bind("hl-missing-noncrit", text=str(dq.get("noncritical_missing_count", 0))))
    
    # Mini panels
    conf = (struct.get("state_confidence") or {}).get("confidence_level") or struct.get("confidence") or "N/A"
    conf_class = "val-positive" if "HIGH" in str(conf) else ("val-negative" if "LOW" in str(conf) else "val-warning")
    b.append(_bind("mini-conf", text=conf, class_name=conf_class))
    
    completeness = dq.get("completeness_score")
    qual_class = "val-positive" if completeness and completeness > 0.9 else "val-warning"
    b.append(_bind("mini-qual", text=f"{int(completeness * 100)}%" if completeness is not None else "N/A", class_name=qual_class))
    
    # Mini flags
    flgs = []
    if struct.get("is_stale"): flgs.append("STALE")
    anomaly = struct.get("anomaly_flags", {})
    if anomaly.get("extreme_move"): flgs.append("EXT_MOVE")
    if anomaly.get("volatility_spike"): flgs.append("VOL_SPIKE")
    if anomaly.get("multi_asset_divergence"): flgs.append("DIVERGENCE")
    
    flags_html = "".join([f'<div class="color-red">⚠ {f}</div>' for f in flgs]) if flgs else '<div class="color-green">OK</div>'
    b.append(_bind("mini-flags", html=flags_html))
    return b


def build_notable_changes_bindings(struct: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    delta = struct.get("delta", {})
    
    if delta.get("available"):
        if delta.get("regime_changed"):
            rows.append(f'<li class="val-warning" style="margin-bottom: 4px;">+ REGIME: {delta.get("previous_regime")} -> {delta.get("current_regime")}</li>')
            
        stc = delta.get("component_state_changes", {})
        for k, v in stc.items():
            curr = v.get("current", "UNKNOWN")
            rows.append(f'<li style="margin-bottom: 2px;">+ {str(k).upper()}: <span class="color-muted">{v.get("previous")}</span> -> <span class="{class_from_state(curr)}">{curr}</span></li>')
            
        ssc = delta.get("component_subscore_changes", {})
        for k, v in ssc.items():
            rows.append(f'<li style="margin-bottom: 2px;">+ {str(k).upper()}: <span class="color-muted">{v.get("previous")}</span> -> <span>{v.get("current")}</span></li>')
            
    b = [_bind("delta-list", html="".join(rows) if rows else "<li class='color-muted'>NO MATERIAL CHANGE</li>")]
    return b


# --- Centralized Market Context Adapters ---

def _build_row(cells: List[Dict[str, str]]) -> str:
    html = "<tr>"
    for c in cells:
        cls = f' class="{c.get("class")}"' if c.get("class") else ""
        html += f"<td{cls}>{c.get('html')}</td>"
    return html + "</tr>"

def _cell(html: Any, class_name: str = None) -> Dict[str, str]:
    return {"html": str(html), "class": class_name}

def _adapter_macro_rates(data: Dict) -> str:
    rows = []
    def _add(name, v):
        if v: rows.append(_build_row([
            _cell(name),
            _cell(format_num(v.get("latest_bps", v.get("latest"))), "num-cell"),
            _cell(format_num(v.get("change_5d_bps", v.get("change_5d"))), "num-cell"),
            _cell(format_num(v.get("change_20d_bps", v.get("change_20d"))), "num-cell"),
            _cell(format_num(v.get("z_score_1y")), "num-cell"),
            _cell(v.get("state", "--"), class_from_state(v.get("state")))
        ]))
    for sk, sv in data.get("rates", {}).items(): _add(sk, sv)
    for sk, sv in data.get("curve_spreads", {}).items(): _add(sk, sv)
    for sk, sv in data.get("inflation_growth", {}).items(): _add(sk, sv)
    _add("Dollar (UUP)", data.get("dollar_proxy", {}).get("state"))
    _add("Real Rate (10Y-BE)", data.get("real_rate_proxy", {}).get("state"))
    return "".join(rows)

def _adapter_credit_liquidity(data: Dict) -> str:
    rows = []
    def _add(name, v):
        if v: rows.append(_build_row([
            _cell(name),
            _cell(format_num(v.get("latest", v.get("latest_ratio", v.get("latest_bps")))), "num-cell"),
            _cell(format_num(v.get("z_score_1y", v.get("z_score"))), "num-cell"),
            _cell(v.get("state", "--"), class_from_state(v.get("state"))),
            _cell(v.get("stretch_state", "--"), class_from_state(v.get("stretch_state")))
        ]))
    _add("IG OAS", data.get("ig_oas"))
    _add("HY OAS", data.get("hy_oas"))
    _add("Loan Proxy (BKLN)", data.get("loan_proxy"))
    for sk, sv in data.get("credit_etf_relationships", {}).items(): _add(sk, sv)
    for sk, sv in data.get("liquidity_sensitive_proxies", {}).items(): _add(sk, sv)
    return "".join(rows)

def _adapter_equity_index(data: Dict) -> str:
    rows = []
    for sk, v in data.items():
        if isinstance(v, dict): rows.append(_build_row([
            _cell(sk),
            _cell(format_num(v.get("return_5d", 0), is_percent=True), "num-cell"),
            _cell(format_num(v.get("return_20d", 0), is_percent=True), "num-cell"),
            _cell(format_num(v.get("distance_200dma", 0), is_percent=True), "num-cell"),
            _cell(format_num(v.get("z_score_1y")), "num-cell"),
            _cell(v.get("trend_state", "--"), class_from_state(v.get("trend_state"))),
            _cell(v.get("stretch_state", "--"), class_from_state(v.get("stretch_state")))
        ]))
    return "".join(rows)

def _adapter_sectors(data: Dict) -> str:
    rows = []
    for sk, v in data.items():
        if isinstance(v, dict):
            rel = v.get("relative_to_spy", {})
            lead = "YES" if v.get("leadership_flag") else ("LAG" if v.get("laggard_flag") else "--")
            rows.append(_build_row([
                _cell(sk),
                _cell(format_num(rel.get("return_5d", 0), is_percent=True), "num-cell"),
                _cell(format_num(rel.get("return_20d", 0), is_percent=True), "num-cell"),
                _cell(format_num(rel.get("z_score_1y")), "num-cell"),
                _cell(lead, class_from_state(lead))
            ]))
    return "".join(rows)

def _adapter_volatility(data: Dict) -> str:
    rows = []
    def _add(name, v):
        if v: rows.append(_build_row([
            _cell(name),
            _cell(format_num(v.get("realized_vol_20d")), "num-cell"),
            _cell(format_num(v.get("realized_vol_percentile_1y")), "num-cell"),
            _cell(v.get("vol_state", v.get("trend_state", "--")), class_from_state(v.get("vol_state", v.get("trend_state"))))
        ]))
    _add("VIX Proxy (VIXY)", data.get("vix_proxy", {}).get("state"))
    _add("MOVE Proxy (TLT Vol)", data.get("move_proxy", {}).get("state"))
    for sk, sv in data.get("realized_volatility", {}).items(): _add(sk, sv)
    for sk, sv in data.get("stress_flags", {}).items():
        state = "TRIGGERED" if sv else "OK"
        rows.append(_build_row([_cell(sk), _cell("--", "num-cell"), _cell("--", "num-cell"), _cell(state, class_from_state(state))]))
    return "".join(rows)

def _adapter_safety(data: Dict) -> str:
    rows = []
    def _add(name, v):
        if v: rows.append(_build_row([
            _cell(name),
            _cell(v.get("state", v.get("trend_state", "--")), class_from_state(v.get("state", v.get("trend_state")))),
            _cell(v.get("stretch_state", "--"), class_from_state(v.get("stretch_state"))),
            _cell("--")
        ]))
    for sk, sv in data.get("treasury_proxies", {}).items(): _add(sk, sv)
    _add("Gold Proxy (GLD)", data.get("gold_proxy"))
    _add("Dollar Proxy (UUP)", data.get("dollar_proxy"))
    _add("JPY Proxy", data.get("jpy_proxy"))
    for sk, sv in data.get("defensive_vs_cyclical", {}).items(): _add(sk, sv)
    return "".join(rows)

def _adapter_cross_asset(data: Dict) -> str:
    rows = []
    for sk, v in data.items():
        if isinstance(v, dict): rows.append(_build_row([
            _cell(sk),
            _cell(format_num(v.get("latest_ratio")), "num-cell"),
            _cell(format_num(v.get("return_5d", 0), is_percent=True), "num-cell"),
            _cell(format_num(v.get("z_score_1y")), "num-cell"),
            _cell(v.get("state", "--"), class_from_state(v.get("state")))
        ]))
    return "".join(rows)

def _adapter_breadth(data: Dict) -> str:
    rows = []
    def _add(name, v, pct):
        rows.append(_build_row([_cell(name), _cell(str(v), "num-cell"), _cell(str(pct), "num-cell")]))
    if data.get("tracked_count") is not None: _add("Tracked ETFs", data.get("tracked_count"), "--")
    if data.get("above_50dma_count") is not None: _add("Above 50DMA", data.get("above_50dma_count"), format_num(data.get("above_50dma_pct", 0), is_percent=True))
    if data.get("above_200dma_count") is not None: _add("Above 200DMA", data.get("above_200dma_count"), format_num(data.get("above_200dma_pct", 0), is_percent=True))
    if data.get("positive_20d_trend_count") is not None: _add("Pos 20d Trend", data.get("positive_20d_trend_count"), format_num(data.get("positive_20d_trend_pct", 0), is_percent=True))
    if data.get("sectors_above_50dma_count") is not None: _add("Sectors Above 50DMA", data.get("sectors_above_50dma_count"), "--")
    if data.get("sectors_above_200dma_count") is not None: _add("Sectors Above 200DMA", data.get("sectors_above_200dma_count"), "--")
    return "".join(rows)

def _adapter_positioning(data: Dict) -> str:
    rows = []
    for sk, v in data.get("assets", {}).items():
        if v: rows.append(_build_row([
            _cell(sk),
            _cell(format_num(v.get("rsi_14d")), "num-cell"),
            _cell(format_num(v.get("distance_200dma", 0), is_percent=True), "num-cell"),
            _cell(v.get("stretch_state", "--"), class_from_state(v.get("stretch_state")))
        ]))
    for sk, v in data.get("relationships", {}).items():
        if v: rows.append(_build_row([
            _cell(sk),
            _cell("--", "num-cell"),
            _cell(format_num(v.get("distance_50dma", 0), is_percent=True), "num-cell"),
            _cell(v.get("stretch_state", "--"), class_from_state(v.get("stretch_state")))
        ]))
    return "".join(rows)

# Registry maps section keys to (Title, Headers, AdapterFunc)
CONTEXT_REGISTRY = [
    ("ctx-macro", "macro_rates", "MACRO / RATES", ["SERIES", "VALUE", "5D Δ", "20D Δ", "Z", "STATE"], _adapter_macro_rates),
    ("ctx-credit", "credit_liquidity", "CREDIT / LIQ", ["SERIES", "VALUE", "Z", "STATE", "STRETCH"], _adapter_credit_liquidity),
    ("ctx-equity", "equity_index_state", "EQUITY", ["ASSET", "5D Δ", "20D Δ", "vs 200DMA", "Z", "STATE", "STRETCH"], _adapter_equity_index),
    ("ctx-sectors", "sector_state", "SECTORS", ["SECTOR", "vs SPY 5D", "vs SPY 20D", "Z", "LEADERSHIP"], _adapter_sectors),
    ("ctx-vol", "volatility_stress", "VOL / STRESS", ["ASSET", "REALIZED", "PCTILE", "STATE"], _adapter_volatility),
    ("ctx-flight", "flight_to_safety", "SAFETY", ["ASSET", "STATE", "STRETCH", "DESC"], _adapter_safety),
    ("ctx-cross", "cross_asset_relationships", "CROSS ASSET", ["PAIR", "RATIO", "5D Δ", "Z", "STATE"], _adapter_cross_asset),
    ("ctx-breadth", "breadth_participation", "BREADTH", ["METRIC", "VALUE", "PCT"], _adapter_breadth),
    ("ctx-positioning", "positioning_stretch", "POSITIONING", ["ASSET", "RSI", "vs 200DMA", "STRETCH"], _adapter_positioning)
]

def build_market_context_bindings(mc: Dict[str, Any]) -> List[Dict[str, Any]]:
    tabs_html = ""
    panels_html = ""
    
    for idx, (id_name, key, title, headers, adapter) in enumerate(CONTEXT_REGISTRY):
        data = mc.get(key, {})
        has_data = len(data) > 0
        j_str = json.dumps(data).lower()
        has_alert = any(w in j_str for w in ["triggered", "breakdown", "high_vol", "falling_fast"])
        
        dot = f'<span class="color-red">⚠</span>' if has_alert else (f'<span class="color-green">●</span>' if has_data else f'<span class="color-muted">○</span>')
        active = "active" if idx == 0 else ""
        tabs_html += f'<button class="tab-btn {active}" data-target="{id_name}">{dot} {title}</button>'
        
        pHtml = f'<div class="tab-pane {active}" id="{id_name}">'
        pHtml += '<table class="dense-table data-table"><thead><tr>' + "".join([f"<th>{h}</th>" for h in headers]) + "</tr></thead><tbody>"
        
        rows_html = adapter(data)
        if not rows_html:
            rows_html = f"<tr><td colspan='{len(headers)}'>No data available</td></tr>"
            
        panels_html += pHtml + rows_html + "</tbody></table></div>"

    if not mc:
        panels_html = "<div class='panel-content'>Context data unavailable.</div>"

    return [
        _bind("context-tabs", html=tabs_html),
        _bind("context-content", html=panels_html)
    ]

def build_brief_bindings(health: Dict[str, Any], struct: Dict[str, Any], prev: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
    b = []
    mc_data = context.get("market_context", context)
    brief = build_current_state_brief(health, struct, prev, mc_data)
    
    if brief.get("error"):
        b.append(_bind("brief-headline", text="Brief unavailable: " + brief["error"]))
        return b
        
    b.append(_bind("brief-headline", text=brief.get("headline", "--")))
    
    # Risks
    risks_html = ""
    for r in brief.get("top_risks", []) + brief.get("watch_items", []):
        sev_color = "color-red" if r.get("severity") in ("warning", "critical") else "color-orange"
        risks_html += f'<li style="margin-bottom: 4px;"><span class="{sev_color}">■</span> <b>{r.get("title", "")}</b>: <span class="color-muted">{r.get("reason", "")}</span></li>'
    if not risks_html: risks_html = "<li>No immediate risks identified</li>"
    b.append(_bind("brief-risks", html=risks_html))
    
    # Supports
    supp_html = ""
    for s in brief.get("top_supports", []):
        supp_html += f'<li style="margin-bottom: 4px;"><span class="color-green">■</span> <b>{s.get("title", "")}</b>: <span class="color-muted">{s.get("reason", "")}</span></li>'
    if not supp_html: supp_html = "<li>No distinct supports identified</li>"
    b.append(_bind("brief-supports", html=supp_html))
    
    # Caveats & Alignment
    cav_html = ""
    comp = compare_structural_vs_preview(struct, prev)
    align_color = "color-green" if comp.get("alignment") == "aligned" else ("color-red" if comp.get("alignment") == "diverging" else "color-orange")
    cav_html += f'<li style="margin-bottom: 4px;"><span class="{align_color}">■</span> <b>Alignment</b>: <span class="color-muted">{comp.get("alignment", "unknown").upper()}</span></li>'
    
    for c in brief.get("caveats", []):
        cav_html += f'<li style="margin-bottom: 4px;"><span class="color-orange">⚠</span> <span class="color-muted">{c}</span></li>'
    b.append(_bind("brief-caveats", html=cav_html))
    
    return b

def _format_llm_brief_html(brief: Dict[str, Any]) -> str:
    if "error" in brief:
        return f'<div class="color-red">Error: {brief["error"]}</div>'
    
    html = ""
    sections = brief.get("sections", {})
    if "overall" in sections:
        html += f'<div style="margin-bottom: 8px;">{sections["overall"]}</div>'
        
    for key in ["what_matters", "risks", "supports", "watch_next", "caveats"]:
        items = sections.get(key, [])
        if items:
            title = key.replace("_", " ").upper()
            html += f'<div style="color: var(--text-main); font-weight: bold; margin-top: 6px;">{title}</div>'
            html += '<ul style="margin: 2px 0 8px 0; padding-left: 16px;">'
            for item in items:
                html += f'<li>{item}</li>'
            html += '</ul>'
            
    if "structural_vs_preview" in sections:
        html += f'<div style="color: var(--text-main); font-weight: bold; margin-top: 6px;">ALIGNMENT</div>'
        html += f'<div style="margin-bottom: 8px;">{sections["structural_vs_preview"]}</div>'
        
    if not html:
        html = f'<pre style="white-space: pre-wrap;">{brief.get("raw_text", "Empty response.")}</pre>'
        
    return html

def build_llm_intelligence_bindings() -> List[Dict[str, Any]]:
    b = []
    
    # Morning Brief
    mb = get_cached_morning_brief()
    if mb:
        b.append(_bind("llm-morning-content", html=_format_llm_brief_html(mb)))
        b.append(_bind("llm-morning-meta", text=f"Generated: {mb.get('generated_at_utc', '--')} | Struct: {mb.get('source_run_ids', {}).get('structural', '--')[:8]}"))
    else:
        b.append(_bind("llm-morning-content", text="No cached morning brief. Click [GENERATE] to build one."))
        
    # Evening Wrap
    ew = get_cached_evening_wrap()
    if ew:
        b.append(_bind("llm-evening-content", html=_format_llm_brief_html(ew)))
        b.append(_bind("llm-evening-meta", text=f"Generated: {ew.get('generated_at_utc', '--')} | Struct: {ew.get('source_run_ids', {}).get('structural', '--')[:8]}"))
    else:
        b.append(_bind("llm-evening-content", text="No cached evening wrap. Click [GENERATE] to build one."))
        
    return b

def build_terminal_payload(
    health: Dict[str, Any],
    struct: Dict[str, Any],
    prev: Dict[str, Any],
    context: Dict[str, Any]
) -> Dict[str, Any]:
    """Builds the composite normalized binding payload for the entire terminal UI."""
    bindings = []
    bindings.extend(build_status_strip_bindings(health, struct, prev))
    bindings.extend(build_structural_summary_bindings(struct))
    bindings.extend(build_preview_summary_bindings(prev))
    bindings.extend(build_health_summary_bindings(health, struct))
    bindings.extend(build_notable_changes_bindings(struct))
    
    mc_data = context.get("market_context", context)
    bindings.extend(build_market_context_bindings(mc_data))
    bindings.extend(build_brief_bindings(health, struct, prev, context))
    bindings.extend(build_llm_intelligence_bindings())
    
    return {
        "bindings": bindings,
        "audit": {
            "health": health,
            "struct": struct,
            "prev": prev,
            "context": context
        },
        "brain_hooks": _get_brain_hooks()
    }
