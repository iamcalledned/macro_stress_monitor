"""
Calculates subscores for each indicator and the final composite score.
"""
from typing import Dict, Any, Optional, Tuple
import numpy as np


IG_SPREAD_THRESHOLDS = {
    0.0: 10,
    1.0: 40,
    1.5: 70,
    2.0: 90,
}

LEVERAGED_LOAN_VOL_THRESHOLDS = {
    0.0: 5,
    1.0: 40,
    1.5: 70,
    2.0: 90,
}

RELATIVE_STRENGTH_THRESHOLDS = {
    0.0: 10,
    1.0: 40,
    1.5: 70,
    2.0: 90,
}

COMPOSITE_WEIGHTS = {
    "ig_spreads": 0.25,
    "leveraged_loans": 0.20,
    "xlf_spy": 0.20,
    "30y_yield": 0.20,
    "jpy_risk": 0.15,
}

REGIME_THRESHOLDS = {
    "risk_off_min": 81,
    "elevated_min": 61,
    "watch_min": 31,
}

def _normalize_z_score(z: float, thresholds: Dict[float, int]) -> int:
    """Normalizes a z-score to a 0-100 score based on thresholds."""
    # Sort thresholds by z-score value
    sorted_thresholds = sorted(thresholds.items())
    for z_thresh, score in sorted_thresholds:
        if z < z_thresh:
            return score
    # Return the highest score if z-score exceeds all thresholds
    return sorted_thresholds[-1][1] if sorted_thresholds else 0

def _clip_score(value: float, min_val: int = 0, max_val: int = 100) -> int:
    """Clips a score to be within a [min, max] range."""
    return int(np.clip(value, min_val, max_val))

def calculate_ig_spread_subscore(indicator_data: Optional[Dict[str, Any]]) -> Tuple[int, str]:
    """Calculates the subscore for the IG Spread indicator."""
    if indicator_data is None:
        return 0, "Data Missing"
    
    z_score = indicator_data["z_score_1y"]
    # Higher z-score (wider spread or falling proxy) is higher risk
    score = _normalize_z_score(z_score, IG_SPREAD_THRESHOLDS)
    if indicator_data.get("ig_data_quality") == "proxy_disagreement":
        score = min(score, 60)

    status = "Calm"
    if score >= 80: status = "Risk"
    elif score >= 50: status = "Elevated"
    
    return score, status

def calculate_leveraged_loan_subscore(indicator_data: Optional[Dict[str, Any]]) -> Tuple[int, str]:
    """Calculates the subscore for the Leveraged Loan indicator."""
    if indicator_data is None:
        return 0, "Data Missing"

    # Score 1: Price vs 200DMA (lower is worse)
    price_vs_ma = indicator_data["price_vs_200dma"]
    score1 = _clip_score(-price_vs_ma * 1000) # e.g., -5% -> 50

    # Score 2: 30-day drawdown (lower is worse)
    drawdown = indicator_data["drawdown_30d"]
    score2 = _clip_score(-drawdown * 1500) # e.g., -5% -> 75

    # Score 3: Volatility Z-score (higher is worse)
    vol_z = indicator_data["volatility_z_score"]
    score3 = _normalize_z_score(vol_z, LEVERAGED_LOAN_VOL_THRESHOLDS)

    # Average the scores
    final_score = _clip_score((score1 + score2 + score3) / 3)
    
    status = "Calm"
    if final_score >= 80: status = "Risk"
    elif final_score >= 50: status = "Elevated"
    
    return final_score, status
    
def calculate_xlf_spy_subscore(indicator_data: Optional[Dict[str, Any]]) -> Tuple[int, str]:
    """Calculates the subscore for the XLF/SPY relative strength."""
    if indicator_data is None:
        return 0, "Data Missing"

    # We use negative z-score because a falling ratio is a sign of stress
    z_score = -indicator_data["z_score_1y"]
    score = _normalize_z_score(z_score, RELATIVE_STRENGTH_THRESHOLDS)

    # If the ratio breaks below its 200DMA, add a penalty
    if indicator_data["breakdown_flag"]:
        score = _clip_score(score + 15)

    status = "Calm"
    if score >= 80: status = "Risk"
    elif score >= 50: status = "Elevated"

    return score, status

def calculate_30y_yield_subscore(indicator_data: Optional[Dict[str, Any]]) -> Tuple[int, str]:
    """
    Calculates the subscore for the 30Y Treasury Yield.
    Risk is high if yields are falling sharply (risk-off flight to safety) or
    if they are rising very fast (inflation/policy panic).
    """
    if indicator_data is None:
        return 0, "Data Missing"
    
    z_score = indicator_data["z_score_1y"]
    change_20d_bps = indicator_data["dgs30_20d_bps"]
    is_below_200dma = indicator_data["is_below_200dma"]

    # Explicitly directional and asymmetric: falling yields imply risk-off duration bid;
    # rising yields imply policy/inflation shock, with lower impact/cap on systemic stress.
    score = 15
    if change_20d_bps <= -25:
        severity = min(abs(change_20d_bps) - 25.0, 40.0)
        score = max(score, 70 + severity * 0.6)  # ramps quickly to ~94
    elif change_20d_bps >= 25:
        severity = min(change_20d_bps - 25.0, 50.0)
        score = max(score, 35 + severity * 0.3)  # ramps slowly, lower cap
        score = min(score, 70)

    # Level z-score with explicit direction:
    # low yield z-score is risk-off supportive; high z-score is policy shock only.
    if z_score <= -1.0:
        score = max(score, 55 + min(abs(z_score) - 1.0, 1.5) * 18)
    elif z_score >= 1.0:
        score = max(score, 30 + min(z_score - 1.0, 1.5) * 12)
        score = min(score, 75)

    # Below 200DMA is only a modest additive penalty.
    if is_below_200dma:
        score += 8

    score = _clip_score(score)
    status = "Calm"
    if score >= 80: status = "Risk"
    elif score >= 50: status = "Elevated"
    
    return score, status
    
def calculate_jpy_risk_subscore(indicator_data: Optional[Dict[str, Any]]) -> Tuple[int, str]:
    """Calculates the subscore for JPY Spike Risk."""
    if indicator_data is None:
        return 0, "Data Missing"
        
    vol_percentile = indicator_data["vol_percentile_1y"]
    move_5d = indicator_data["move_5d_pct"]
    
    # Score from volatility (0-100)
    score1 = _clip_score(vol_percentile)
    
    # Score from price move (JPY strengthening = USDJPY falling = negative move)
    score2 = _clip_score(-move_5d * 25) # e.g., -2% move -> 50 score
    
    # Average them
    score = _clip_score((score1 + score2) / 2)
    
    # If the main risk flag hits, the score should be high
    if indicator_data["risk_flag"]:
        score = max(score, 90)

    if not indicator_data.get("jpy_confirmed", False):
        score = min(score, 70)

    status = "Calm"
    if score >= 80: status = "Risk"
    elif score >= 50: status = "Elevated"

    return score, status

def get_composite_score(subscores: Dict[str, int]) -> Tuple[int, str]:
    """
    Calculates the final composite score from all subscores.
    """
    composite_score = 0.0
    total_weight = 0.0

    for name, score in subscores.items():
        if score > 0: # Only include components with data
            composite_score += score * COMPOSITE_WEIGHTS[name]
            total_weight += COMPOSITE_WEIGHTS[name]
    
    if total_weight == 0:
        return 0, "Unknown"

    # Re-normalize in case some data was missing
    final_score = _clip_score(composite_score / total_weight)

    if final_score >= REGIME_THRESHOLDS["risk_off_min"]:
        regime = "Risk-Off"
    elif final_score >= REGIME_THRESHOLDS["elevated_min"]:
        regime = "Elevated"
    elif final_score >= REGIME_THRESHOLDS["watch_min"]:
        regime = "Watch"
    else:
        regime = "Calm"
        
    return final_score, regime


def get_scoring_config() -> Dict[str, Any]:
    """Returns scoring settings that materially affect run results."""
    return {
        "thresholds": {
            "ig_spreads_z_score": IG_SPREAD_THRESHOLDS,
            "leveraged_loan_volatility_z_score": LEVERAGED_LOAN_VOL_THRESHOLDS,
            "relative_strength_negative_z_score": RELATIVE_STRENGTH_THRESHOLDS,
            "regime": REGIME_THRESHOLDS,
            "xlf_spy_breakdown_penalty": 15,
            "jpy_unconfirmed_cap": 70,
            "jpy_risk_flag_floor": 90,
            "alert_composite_min": 70,
            "alert_high_subscore_min": 80,
        },
        "weights": COMPOSITE_WEIGHTS,
        "component_rules": {
            "30y_yield": {
                "risk_off_move_bps_20d": -25,
                "policy_shock_move_bps_20d": 25,
                "below_200dma_penalty": 8,
            },
            "jpy_risk": {
                "sharp_drop_5d_pct": -3.0,
                "vol_spike_percentile": 90.0,
            },
        },
    }
