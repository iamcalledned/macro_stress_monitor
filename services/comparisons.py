"""
Comparisons Layer.
Deterministically compares structural snapshots against intraday preview signals.
"""
from typing import Dict, Any

def compare_structural_vs_preview(struct: Dict[str, Any], prev: Dict[str, Any]) -> Dict[str, Any]:
    """Generates a structured comparison of structural vs preview state."""
    if not struct or not prev:
        return {
            "alignment": "unavailable",
            "headline": "Comparison unavailable due to missing data.",
            "confirming_signals": [],
            "non_confirming_signals": [],
            "divergence_points": [],
            "why_it_matters": [],
            "caveats": ["Missing structural or preview snapshot."]
        }
        
    s_regime = str(struct.get("regime") or struct.get("regime_label") or "UNKNOWN").upper()
    p_assessment = str(prev.get("preview_spillover_assessment", "UNKNOWN"))
    
    alignment = "aligned"
    headline = "Preview confirms structural regime."
    divergence_points = []
    
    # Simple deterministic heuristic mapping
    is_struct_calm = "CALM" in s_regime
    is_struct_stress = "RISK-OFF" in s_regime or "ELEVATED" in s_regime
    is_prev_stress = "Credit confirming" in p_assessment or "watch credit" in p_assessment.lower()
    
    if is_struct_calm and is_prev_stress:
        alignment = "diverging"
        headline = "Preview diverging: intraday stress building despite structural calm."
        divergence_points.append("Intraday credit/equity weakness not yet reflected in structural foundation.")
    elif is_struct_stress and not is_prev_stress:
        alignment = "weakly_aligned"
        headline = "Preview shows intraday calm, but structural stress remains."
        divergence_points.append("Intraday session is quiet, but core structural foundation remains broken.")
    elif is_struct_stress and is_prev_stress:
        headline = "Preview confirms ongoing structural stress."
    elif is_struct_calm and not is_prev_stress:
        headline = "Preview confirms ongoing structural calm."
        
    return {
        "alignment": alignment,
        "headline": headline,
        "confirming_signals": ["Intraday aligns with structural bias."] if alignment == "aligned" else [],
        "non_confirming_signals": [],
        "divergence_points": divergence_points,
        "why_it_matters": [
            "Divergence between structural and preview indicates shifting market sentiment or trend transition."
        ] if alignment == "diverging" else [],
        "caveats": [],
        "preview_assessment": p_assessment,
        "structural_regime": s_regime
    }
