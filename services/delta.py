"""
Compact delta helpers for comparing comparable Macro Stress Monitor snapshots.
"""
from typing import Any, Dict, Optional


def _score(value: Any) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _regime(snapshot: Dict[str, Any]) -> Optional[str]:
    value = snapshot.get("regime_label", snapshot.get("regime"))
    return str(value) if value is not None else None


def _state_level(state: Any) -> Optional[str]:
    if isinstance(state, dict):
        value = state.get("level")
        return str(value) if value is not None else None
    return None


def _component_state_changes(current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Dict[str, Optional[str]]]:
    current_states = current.get("component_states") or {}
    previous_states = previous.get("component_states") or {}
    changes: Dict[str, Dict[str, Optional[str]]] = {}

    for component in sorted(set(current_states) | set(previous_states)):
        current_level = _state_level(current_states.get(component))
        previous_level = _state_level(previous_states.get(component))
        if current_level != previous_level:
            changes[component] = {
                "from": previous_level,
                "to": current_level,
            }
    return changes


def _component_subscore_changes(current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, float]:
    current_scores = current.get("component_subscores") or {}
    previous_scores = previous.get("component_subscores") or {}
    changes: Dict[str, float] = {}

    for component in sorted(set(current_scores) | set(previous_scores)):
        current_score = _score(current_scores.get(component))
        previous_score = _score(previous_scores.get(component))
        if current_score is None or previous_score is None:
            continue
        diff = round(current_score - previous_score, 2)
        if diff:
            changes[component] = diff
    return changes


def compute_structural_delta(current: Dict[str, Any], previous: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Computes a compact change summary versus the prior structural run."""
    if not previous:
        return {
            "available": False,
            "reason": "no_previous_structural_snapshot",
        }

    current_score = _score(current.get("composite_score"))
    previous_score = _score(previous.get("composite_score"))
    score_change = None
    if current_score is not None and previous_score is not None:
        score_change = round(current_score - previous_score, 2)

    current_regime = _regime(current)
    previous_regime = _regime(previous)
    current_drivers = set(current.get("primary_drivers") or [])
    previous_drivers = set(previous.get("primary_drivers") or [])

    return {
        "available": True,
        "previous_run_id": previous.get("run_id"),
        "previous_computed_at_utc": previous.get("computed_at_utc"),
        "score_change": score_change,
        "regime_changed": current_regime != previous_regime,
        "previous_regime": previous_regime,
        "current_regime": current_regime,
        "component_state_changes": _component_state_changes(current, previous),
        "component_subscore_changes": _component_subscore_changes(current, previous),
        "primary_drivers_added": sorted(current_drivers - previous_drivers),
        "primary_drivers_removed": sorted(previous_drivers - current_drivers),
    }


def compute_preview_delta(current: Dict[str, Any], previous: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Computes a compact change summary versus the prior preview run."""
    if not previous:
        return {
            "available": False,
            "reason": "no_previous_preview_snapshot",
        }

    current_assessment = current.get("preview_spillover_assessment")
    previous_assessment = previous.get("preview_spillover_assessment")
    return {
        "available": True,
        "previous_run_id": previous.get("run_id"),
        "previous_computed_at_utc": previous.get("computed_at_utc"),
        "assessment_changed": current_assessment != previous_assessment,
        "previous_assessment": previous_assessment,
        "current_assessment": current_assessment,
        "component_state_changes": _component_state_changes(current, previous),
        "component_subscore_changes": _component_subscore_changes(current, previous),
    }
