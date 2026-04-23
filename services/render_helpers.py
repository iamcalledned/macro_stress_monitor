"""
Helper utilities for the presentation/render layer.
Formats foundation numeric and string states into display-ready elements.
"""

from datetime import datetime, timezone
from typing import Any, Optional
import math

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None

def format_time_et(iso_string: Optional[str]) -> str:
    if not iso_string:
        return "--"
    try:
        normalized = iso_string.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if ZoneInfo:
            dt = dt.astimezone(ZoneInfo("America/New_York"))
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.strftime("%H:%M:%S ET")
    except ValueError:
        return iso_string

def format_num(val: Any, decimals: int = 2, is_percent: bool = False) -> str:
    if val is None:
        return "--"
    try:
        fval = float(val)
        if math.isnan(fval):
            return str(val)
        
        if is_percent:
            fval = fval * 100
        
        # Determine format string
        fmt_str = f"{{:.{decimals}f}}"
        res = fmt_str.format(fval)
        
        if is_percent:
            res += "%"
            
        return res
    except (ValueError, TypeError):
        return str(val)

def get_env_color(score: Any) -> str:
    try:
        s = float(score)
        if s <= 30: return "bg-green"
        if s <= 60: return "bg-yellow"
        if s <= 80: return "bg-orange"
        return "bg-red"
    except (ValueError, TypeError):
        return "bg-muted"

def get_env_text_color(score: Any) -> str:
    try:
        s = float(score)
        if s <= 30: return "color-green"
        if s <= 60: return "color-yellow"
        if s <= 80: return "color-orange"
        return "color-red"
    except (ValueError, TypeError):
        return "color-muted"

def class_from_state(state: Any) -> str:
    if state is None:
        return ""
    s = str(state).lower()
    
    # Exact matches for positive first to catch specific flags
    if s == "yes":
        return "val-positive"
        
    positive_words = ["ok", "calm", "normal", "stable", "safe", "positive", "uptrend", "leadership", "outperforming"]
    warning_words = ["elevated", "watch", "mixed", "lag", "range", "flat"]
    negative_words = ["breakdown", "high", "triggered", "fast", "risk", "stress", "underperforming", "downtrend", "overbought", "oversold"]
    muted_words = ["unavailable", "insufficient", "--"]
    
    if any(w in s for w in positive_words):
        return "val-positive state-cell"
    if any(w in s for w in warning_words):
        return "val-warning state-cell"
    if any(w in s for w in negative_words):
        return "val-negative state-cell"
    if any(w in s for w in muted_words):
        return "color-muted state-cell"
    
    return "val-neutral state-cell"
