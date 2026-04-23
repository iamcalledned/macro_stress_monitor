"""
Prompt Builder Layer.
Constructs strict prompts from structured deterministic outputs.
"""

import json
from typing import Dict, Any, Tuple, List


def _clean_list(items: Any) -> List[str]:
    """Normalize a possible list of strings / dicts into concise strings."""
    if not items:
        return []

    cleaned: List[str] = []
    for item in items:
        if item is None:
            continue
        if isinstance(item, str):
            text = item.strip()
            if text:
                cleaned.append(text)
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
            cleaned.append(text)
        else:
            cleaned.append(str(item).strip())

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


def _build_brief_packet(
    brief_dict: Dict[str, Any],
    health: Dict[str, Any],
    comparison: Dict[str, Any],
    mode: str,
) -> Dict[str, Any]:
    """
    Build a deterministic, LLM-friendly packet.

    The goal is to give the model:
    - one dominant signal
    - clear distinction between what is breaking vs holding
    - what is not yet confirmed
    - why the setup matters
    - concise caveats
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
        "structural_state": structural_state,
        "preview_state": preview_state,
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


def get_system_prompt() -> str:
    return """You are generating internal market briefs from structured system outputs.

Your job is not to merely restate fields.
Your job is to produce a useful internal analyst note.

Rules:
- Use only the supplied facts
- Do NOT reference any asset, metric, ratio, or condition that is not explicitly present in the input
- Do NOT invent missing data
- Do NOT provide trade recommendations
- Do NOT restate obvious fields already visible in the UI unless needed for context
- Do NOT simply paraphrase the same point across multiple sections
- Start with the most important signal, not the background state
- Rank signals: dominant first, supporting second
- Distinguish clearly between:
  - what is happening
  - what is breaking
  - what is holding up
  - what is not yet confirmed
  - why it matters
  - what to watch next
- Explain why the dominant signals matter in plain English
- Use direct language
- Avoid generic finance filler
- Avoid weak phrasing such as:
  - 'suggesting potential'
  - 'may indicate'
  - 'could imply'
  - 'markets are digesting'
  - 'sentiment is mixed'
- If there are no meaningful supports, say so briefly and move on
- Mention stale, missing, or proxy-based caveats when relevant
- Be concise and structured
- Keep each section distinct and non-repetitive
- YOU MUST output pure JSON matching the requested schema. No markdown wrapping or conversational text outside the JSON object.

Morning Brief:
- Focus on the setup entering the day
- Emphasize what matters now, risks, supports, why it matters, alignment, and what to watch next

Evening Wrap:
- Focus on what confirmed, what failed to confirm, what changed, and what matters next
- Emphasize follow-through or non-confirmation

Always:
- Highlight structural vs preview divergence clearly
- Explain consequences, not just observations
- State whether stress looks localized, broadening, or unclear
- Sound like an internal desk note, not a retail newsletter

OUTPUT JSON SCHEMA:
{
  "headline": "A sharp, one-sentence summary of the current setup and main tension.",
  "sections": {
    "what_matters": ["Bullet point 1", "Bullet point 2"],
    "risks": ["Risk point 1", "Risk point 2"],
    "supports_or_confirmations": ["Support point 1", "Support point 2"],
    "why_this_matters": ["Explanation point 1", "Explanation point 2"],
    "watch_next": ["Item 1 to watch", "Item 2 to watch"],
    "alignment": "A short explanation of whether intraday preview confirms or diverges from the structural backdrop.",
    "caveats": ["Caveat 1", "Caveat 2"]
  }
}
"""


def build_morning_brief_prompt(
    brief_dict: Dict[str, Any],
    health: Dict[str, Any],
    comparison: Dict[str, Any],
) -> Tuple[str, str]:
    """Build the payload for the Morning Brief."""
    system = get_system_prompt()
    packet = _build_brief_packet(
        brief_dict=brief_dict,
        health=health,
        comparison=comparison,
        mode="morning_brief",
    )

    user = f"""Generate the MORNING BRIEF.

Start from dominant_signal and dominant_factors. Everything else is supporting context.

Write each section so it contributes something distinct:
- what_matters = dominant setup right now
- risks = what could worsen or spread
- supports_or_confirmations = what is holding up or confirming stability
- why_this_matters = practical consequence of the setup
- watch_next = next confirmation or invalidation points
- alignment = structural vs preview relationship
- caveats = confidence limits, stale data, missing data, or proxy limitations

Do NOT mention any asset, ratio, or metric not explicitly present in the packet.
Do NOT repeat the same sentence across sections.
If supports are weak or absent, say so briefly.

Structured Brief Packet:
{json.dumps(packet, indent=2)}
"""
    return system, user


def build_evening_wrap_prompt(
    brief_dict: Dict[str, Any],
    health: Dict[str, Any],
    comparison: Dict[str, Any],
) -> Tuple[str, str]:
    """Build the payload for the Evening Wrap."""
    system = get_system_prompt()
    packet = _build_brief_packet(
        brief_dict=brief_dict,
        health=health,
        comparison=comparison,
        mode="evening_wrap",
    )

    user = f"""Generate the EVENING WRAP.

Start from dominant_signal and what_changed. Everything else is supporting context.

Write each section so it contributes something distinct:
- what_matters = what defined the day by the close
- risks = what remains fragile or unresolved
- supports_or_confirmations = what actually held up or confirmed
- why_this_matters = what today's outcome means in practical terms
- watch_next = next confirmation or failure points
- alignment = whether preview and structural conditions ended up aligned or still diverging
- caveats = confidence limits, stale data, missing data, or proxy limitations

Emphasize:
- what confirmed
- what failed to confirm
- whether stress stayed localized or started broadening

Do NOT mention any asset, ratio, or metric not explicitly present in the packet.
Do NOT repeat the same sentence across sections.
If supports are weak or absent, say so briefly.

Structured Brief Packet:
{json.dumps(packet, indent=2)}
"""
    return system, user