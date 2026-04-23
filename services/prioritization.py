"""
Prioritization Engine.
Deterministically ranks factors, risks, and supports based on foundation snapshots.
"""
from typing import Dict, Any, List

def rank_dominant_factors(struct: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Identifies the most extreme assets or factors currently driving the market."""
    factors = []
    
    # Check for extreme Z-scores across all context sections
    mc = context.get("market_context", context)
    for section, data in mc.items():
        if isinstance(data, dict):
            for asset, metrics in data.items():
                if isinstance(metrics, dict):
                    z = metrics.get("z_score_1y") or metrics.get("z_score")
                    if isinstance(z, (int, float)) and abs(z) >= 2.0:
                        factors.append({
                            "factor": asset,
                            "metric": f"Z-Score {z:.1f}",
                            "significance": "dominant" if abs(z) > 2.5 else "secondary",
                            "state": metrics.get("state", "UNKNOWN")
                        })
                        
    # Sort by absolute z-score
    factors.sort(key=lambda x: abs(float(x["metric"].split(" ")[1])), reverse=True)
    return factors[:5]

def get_top_risks(struct: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Surfaces top risks based on breakdown states and anomaly flags."""
    risks = []
    
    anomaly = struct.get("anomaly_flags", {})
    if anomaly.get("extreme_move"):
        risks.append({"title": "Extreme Move Detected", "severity": "warning", "reason": "Anomaly flag triggered"})
    if anomaly.get("volatility_spike"):
        risks.append({"title": "Volatility Spike", "severity": "watch", "reason": "Anomaly flag triggered"})
    if anomaly.get("multi_asset_divergence"):
        risks.append({"title": "Multi-Asset Divergence", "severity": "watch", "reason": "Cross-asset relationships broken"})
        
    mc = context.get("market_context", context)
    
    # Check credit stress
    credit = mc.get("credit_liquidity", {})
    for k, v in credit.items():
        if isinstance(v, dict) and "STRESS" in str(v.get("state", "")).upper():
            risks.append({"title": f"Credit Stress: {k}", "severity": "warning", "reason": "Credit state triggered"})
            
    # Check Volatility breakdowns
    vol = mc.get("volatility_stress", {})
    for k, v in vol.get("stress_flags", {}).items():
        if v:
            risks.append({"title": f"Vol Trigger: {k}", "severity": "watch", "reason": "Stress flag active"})
            
    # Check Equity breakdowns
    eq = mc.get("equity_index_state", {})
    for k, v in eq.items():
        if isinstance(v, dict) and v.get("trend_state") == "BREAKDOWN":
            risks.append({"title": f"Equity Breakdown: {k}", "severity": "warning", "reason": "Trend breakdown"})
            
    return risks[:5]

def get_top_supports(struct: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Surfaces confirming positive signals."""
    supports = []
    mc = context.get("market_context", context)
    
    # Check strong credit
    credit = mc.get("credit_liquidity", {})
    if str(credit.get("ig_oas", {}).get("state", "")).upper() == "CALM":
        supports.append({"title": "IG Credit Calm", "strength": "strong", "reason": "Investment grade spreads remain contained"})
        
    # Check breadth
    breadth = mc.get("breadth_participation", {})
    if breadth.get("above_200dma_pct", 0) > 0.7:
        supports.append({"title": "Strong Breadth", "strength": "strong", "reason": f"{int(breadth['above_200dma_pct']*100)}% of assets above 200DMA"})
        
    return supports[:5]
