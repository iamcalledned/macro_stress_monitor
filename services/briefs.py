"""
Master Brief Builder.
Coordinates deterministic interpretation modules to produce structured briefs.
"""
from typing import Dict, Any

try:
    from .prioritization import rank_dominant_factors, get_top_risks, get_top_supports
    from .caveats import build_caveat_summary
    from .alerts import build_alert_summary
    from .comparisons import compare_structural_vs_preview
except ImportError:
    from services.prioritization import rank_dominant_factors, get_top_risks, get_top_supports
    from services.caveats import build_caveat_summary
    from services.alerts import build_alert_summary
    from services.comparisons import compare_structural_vs_preview

def build_headline(struct: Dict[str, Any], prev: Dict[str, Any], alerts: Dict[str, Any], comparison: Dict[str, Any]) -> str:
    """Deterministically generates a factual headline based on state and comparison."""
    comp_headline = comparison.get("headline", "")
    
    if any(a.get("severity") == "critical" for a in alerts.get("items", [])):
        return f"SYSTEM CRITICAL: {alerts['items'][0]['title']}."
        
    return comp_headline

def build_current_state_brief(health: Dict[str, Any], struct: Dict[str, Any], prev: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    """Generates the master deterministic interpretation object."""
    if not struct:
        return {"error": "Structural foundation snapshot is missing."}
        
    mc = context.get("market_context", context)
    
    dominant_factors = rank_dominant_factors(struct, mc)
    top_risks = get_top_risks(struct, mc)
    top_supports = get_top_supports(struct, mc)
    caveats = build_caveat_summary(health, struct, prev)
    alerts = build_alert_summary(health, struct, prev, mc)
    comparison = compare_structural_vs_preview(struct, prev)
    
    headline = build_headline(struct, prev, alerts, comparison)
    
    return {
        "headline": headline,
        "summary_state": str(struct.get("regime") or struct.get("regime_label", "UNKNOWN")).upper(),
        "top_risks": top_risks,
        "top_supports": top_supports,
        "watch_items": alerts.get("items", []),
        "contradictions": comparison.get("divergence_points", []),
        "caveats": caveats,
        "dominant_factors": dominant_factors,
        "secondary_factors": [],
        "anomalies": [f for f in struct.get("anomaly_flags", {}).keys() if struct["anomaly_flags"][f]],
        "freshness": {
            "structural_stale": health.get("structural_stale", False),
            "preview_stale": health.get("preview_stale", False)
        },
        "confidence": struct.get("state_confidence", {}),
        "data_quality": struct.get("data_quality", {})
    }
