"""
Alert / Watch-Item Layer.
Surfaces reflex-style alerts based on current state parameters.
"""
from typing import Dict, Any, List

def build_watch_items(struct: Dict[str, Any], prev: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, str]]:
    """Identifies watch items based on market context."""
    items = []
    mc = context.get("market_context", context)
    
    # Financials under pressure
    sectors = mc.get("sector_state", {})
    xlf = sectors.get("XLF", {})
    if isinstance(xlf, dict) and xlf.get("laggard_flag"):
        items.append({
            "severity": "watch",
            "title": "Financials Lagging",
            "reason": "XLF is a relative laggard to SPY, indicating potential systemic pressure.",
            "source": "context"
        })
        
    # Credit deterioration
    credit = mc.get("credit_liquidity", {})
    hy = credit.get("hy_oas", {})
    if isinstance(hy, dict) and "ELEVATED" in str(hy.get("state", "")).upper():
        items.append({
            "severity": "watch",
            "title": "High Yield Spreads Elevated",
            "reason": "HY credit spreads are widening, indicating decreasing risk appetite.",
            "source": "context"
        })
        
    # Defensive rotation
    defensive = mc.get("flight_to_safety", {}).get("defensive_vs_cyclical", {})
    xlu_xly = defensive.get("XLU/XLY")
    if isinstance(xlu_xly, dict) and "FLIGHT" in str(xlu_xly.get("state", "")).upper():
        items.append({
            "severity": "watch",
            "title": "Defensive Rotation Strengthening",
            "reason": "Utilities outperforming Consumer Discretionary strongly.",
            "source": "context"
        })
        
    return items

def build_alert_summary(health: Dict[str, Any], struct: Dict[str, Any], prev: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, List[Dict[str, str]]]:
    """Consolidates all system and market watch items into a structured alerts object."""
    alerts = []
    
    # System level alerts
    if health.get("structural_stale"):
        alerts.append({
            "severity": "critical",
            "title": "Structural Data Stale",
            "reason": "The system has not successfully completed a structural run within the required timeframe.",
            "source": "health"
        })
        
    anomaly = struct.get("anomaly_flags", {})
    if anomaly.get("extreme_move"):
        alerts.append({
            "severity": "warning",
            "title": "Extreme Move Anomaly",
            "reason": "Structural run detected an extreme standard deviation move in core assets.",
            "source": "structural"
        })
        
    alerts.extend(build_watch_items(struct, prev, context))
    
    return {"items": alerts}
