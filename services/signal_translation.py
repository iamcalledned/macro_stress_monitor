"""
Signal Translation Layer.
Converts internal deterministic system jargon into human-readable plain English 
before passing data to the LLM layer.
"""

import json
from typing import Dict, Any, List

JARGON_DICTIONARY = {
    "structural calm": "Longer-term market conditions still look stable",
    "preview divergence": "Short-term stress signals are triggering before the slower-moving structural picture has confirmed it",
    "preview diverges from structural": "Short-term stress signals are weakening before the slower-moving structural picture has confirmed it",
    "multi-asset divergence": "Different parts of the market are no longer moving together, which is often an early sign that stress is building",
    "volatility spike": "Options markets are pricing in sudden, sharp movements",
    "credit confirmation": "Credit markets are confirming the stress seen in equities",
    "lack of confirmation": "Other asset classes are not yet confirming the move",
    "localized stress": "The weakness is showing up in specific areas, not across the whole market yet",
    "broadening stress": "More parts of the market are starting to show the same weakness",
    "defensive rotation": "Investors are moving money into safer, defensive sectors",
    "breadth deterioration": "Fewer stocks are participating in the broader market move",
    "anomaly flags": "Unusual market movements were detected",
    "stale data": "Some input data is older than usual",
    "proxy-based measures": "Using alternative data sources to estimate conditions",
    "spillover stress": "Stress from one asset class is bleeding into others",
    "trend transition": "The primary market trend appears to be shifting",
    "structural backdrop": "The long-term market environment",
    "preview conditions": "Short-term market signals"
}

def translate_jargon(text: str) -> str:
    """Translates internal jargon terms to plain English if found in text."""
    if not text:
        return ""
    
    text_lower = text.lower()
    
    # Simple replace logic for known phrases
    for jargon, translation in JARGON_DICTIONARY.items():
        if jargon in text_lower:
            # We use a case-insensitive replacement to keep the rest of the text intact
            import re
            text = re.sub(re.escape(jargon), translation, text, flags=re.IGNORECASE)
            
    return text

def _clean_list(items: Any) -> List[str]:
    """Normalize a possible list of strings / dicts into concise strings and translate."""
    if not items:
        return []

    cleaned: List[str] = []
    for item in items:
        if item is None:
            continue
        if isinstance(item, str):
            text = item.strip()
            if text:
                cleaned.append(translate_jargon(text))
        elif isinstance(item, dict):
            title = str(item.get("title", "")).strip()
            reason = str(item.get("reason", "")).strip()
            severity = str(item.get("severity", "")).strip()

            if title and reason:
                text = f"{title}: {reason}"
            elif title:
                text = title
            elif reason:
                text = reason
            else:
                text = json.dumps(item, sort_keys=True)

            if severity:
                text = f"[{severity}] {text}"
            cleaned.append(translate_jargon(text))
        else:
            cleaned.append(translate_jargon(str(item).strip()))

    return [x for x in cleaned if x]

def _safe_get(d: Dict[str, Any], *keys: str, default: Any = None) -> Any:
    """Nested dict getter."""
    cur: Any = d
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
        if cur is None:
            return default
    return cur

def build_human_meaning_packet(
    brief_dict: Dict[str, Any],
    health: Dict[str, Any],
    comparison: Dict[str, Any],
    mode: str,
) -> Dict[str, Any]:
    """
    Build a deterministic, plain-English LLM-friendly packet.
    Translates internal signals into human meaning.
    """
    dominant_factors = _clean_list(brief_dict.get("dominant_factors"))
    secondary_factors = _clean_list(brief_dict.get("secondary_factors"))
    top_risks = _clean_list(brief_dict.get("top_risks"))
    top_supports = _clean_list(brief_dict.get("top_supports"))
    watch_items = _clean_list(brief_dict.get("watch_items"))
    caveats = _clean_list(brief_dict.get("caveats"))
    anomalies = _clean_list(brief_dict.get("anomalies"))
    contradictions = _clean_list(brief_dict.get("contradictions"))
    what_changed = _clean_list(brief_dict.get("what_changed"))
    why_it_matters = _clean_list(brief_dict.get("why_it_matters"))

    structural_state = (
        brief_dict.get("summary_state")
        or brief_dict.get("structural_regime")
        or _safe_get(comparison, "structural_regime")
        or "UNKNOWN"
    )
    preview_state = (
        brief_dict.get("preview_assessment")
        or _safe_get(comparison, "preview_assessment")
        or "UNKNOWN"
    )
    alignment = _safe_get(comparison, "alignment", default="unavailable")

    confirming_signals = _clean_list(_safe_get(comparison, "confirming_signals", default=[]))
    non_confirming_signals = _clean_list(_safe_get(comparison, "non_confirming_signals", default=[]))
    divergence_points = _clean_list(_safe_get(comparison, "divergence_points", default=[]))
    compare_caveats = _clean_list(_safe_get(comparison, "caveats", default=[]))
    compare_why = _clean_list(_safe_get(comparison, "why_it_matters", default=[]))

    structural_stale = bool(health.get("structural_stale", False))
    preview_stale = bool(health.get("preview_stale", False))
    structural_age = health.get("structural_age_seconds")
    preview_age = health.get("preview_age_seconds")

    missing_critical = (
        health.get("missing_critical")
        or _safe_get(health, "data_quality", "missing_critical")
        or False
    )
    missing_noncritical_count = (
        health.get("missing_noncritical_count")
        or _safe_get(health, "data_quality", "missing_noncritical_count")
        or 0
    )

    confidence = (
        brief_dict.get("confidence")
        or _safe_get(brief_dict, "state_confidence", "score")
        or _safe_get(brief_dict, "state_confidence", "label")
        or health.get("confidence")
        or "UNKNOWN"
    )

    dominant_signal = dominant_factors[0] if dominant_factors else "No dominant factor identified"
    dominant_signal_reason = why_it_matters[0] if why_it_matters else ""

    what_is_breaking: List[str] = []
    what_is_holding_up: List[str] = []
    what_is_not_confirmed: List[str] = []

    what_is_breaking.extend(top_risks[:3])
    what_is_breaking.extend(anomalies[:2])
    what_is_breaking.extend(divergence_points[:2])

    what_is_holding_up.extend(top_supports[:3])
    what_is_holding_up.extend(confirming_signals[:2])

    what_is_not_confirmed.extend(non_confirming_signals[:3])

    if alignment == "diverging":
        broadening_or_localized = "localized"
    elif alignment in {"aligned", "weakly_aligned"}:
        broadening_or_localized = "broadening_or_confirming"
    else:
        broadening_or_localized = "unclear"

    most_important_tension = ""
    if alignment == "diverging":
        most_important_tension = (
            "Intraday conditions are weaker than the structural backdrop, so early stress is appearing "
            "before the slower-moving structural framework confirms it."
        )
    elif alignment == "aligned":
        most_important_tension = (
            "Intraday and structural conditions are pointing in the same direction, which increases confidence "
            "that the current regime is real rather than noise."
        )
    elif alignment == "weakly_aligned":
        most_important_tension = (
            "Some signals are lining up, but the confirmation is incomplete."
        )
    else:
        most_important_tension = "Alignment between structural and preview conditions is unclear."

    freshness = {
        "structural_stale": structural_stale,
        "preview_stale": preview_stale,
        "structural_age_seconds": structural_age,
        "preview_age_seconds": preview_age,
    }

    quality_and_limits: List[str] = []
    if structural_stale:
        quality_and_limits.append("Structural data is stale.")
    if preview_stale:
        quality_and_limits.append("Preview data is stale.")
    if missing_critical:
        quality_and_limits.append("Critical input data is missing.")
    if missing_noncritical_count:
        quality_and_limits.append(f"{missing_noncritical_count} noncritical inputs are missing.")

    quality_and_limits.extend(caveats[:3])
    quality_and_limits.extend(compare_caveats[:2])

    # Deduplicate while preserving order
    def _dedupe(seq: List[str]) -> List[str]:
        seen = set()
        out = []
        for item in seq:
            if not item:
                continue
            if item not in seen:
                seen.add(item)
                out.append(item)
        return out

    packet = {
        "mode": mode,
        "structural_state": translate_jargon(str(structural_state)),
        "preview_state": translate_jargon(str(preview_state)),
        "alignment": alignment,
        "dominant_signal": dominant_signal,
        "dominant_signal_reason": dominant_signal_reason,
        "dominant_factors": dominant_factors[:4],
        "secondary_factors": secondary_factors[:4],
        "what_is_breaking": _dedupe(what_is_breaking)[:5],
        "what_is_holding_up": _dedupe(what_is_holding_up)[:5],
        "what_is_not_confirmed": _dedupe(what_is_not_confirmed)[:5],
        "broadening_or_localized": broadening_or_localized,
        "most_important_tension": most_important_tension,
        "watchpoints": watch_items[:5],
        "anomalies": anomalies[:5],
        "contradictions": contradictions[:4],
        "what_changed": what_changed[:5],
        "why_this_matters_points": _dedupe(why_it_matters + compare_why)[:5],
        "quality_and_limits": _dedupe(quality_and_limits)[:5],
        "freshness": freshness,
        "confidence": confidence,
        "comparison_summary": {
            "confirming_signals": confirming_signals[:4],
            "non_confirming_signals": non_confirming_signals[:4],
            "divergence_points": divergence_points[:4],
        },
    }

    return packet
