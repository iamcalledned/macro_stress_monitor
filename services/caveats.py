"""
Caveat / Trust Layer.
Deterministically surfaces data quality issues, staleness, and low confidence.
"""
from typing import Dict, Any, List

def build_caveat_summary(health: Dict[str, Any], struct: Dict[str, Any], prev: Dict[str, Any]) -> List[str]:
    """Generates a list of factual caveats regarding data integrity."""
    caveats = []
    
    # Staleness
    if health.get("structural_stale") or struct.get("is_stale"):
        caveats.append("Structural snapshot is stale. Market state may have drifted.")
    if health.get("preview_stale") or (prev and prev.get("is_stale")):
        caveats.append("Intraday preview is stale. Intraday signals are unavailable.")
        
    # Missing Data
    dq = struct.get("data_quality", {})
    if dq.get("critical_missing"):
        caveats.append("Critical data points are missing from the foundation. Score reliability is reduced.")
    missing_count = dq.get("noncritical_missing_count", 0)
    if missing_count > 0:
        caveats.append(f"{missing_count} non-critical components are missing.")
        
    # Confidence
    conf = (struct.get("state_confidence") or {}).get("confidence_level") or struct.get("confidence") or "UNKNOWN"
    if "LOW" in str(conf).upper():
        caveats.append("Overall state confidence is LOW due to conflicting cross-asset signals.")
        
    return caveats
