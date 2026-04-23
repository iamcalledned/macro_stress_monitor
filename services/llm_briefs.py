"""
Summary Generation Service.
Orchestrates LLM brief generation and formatting.
"""
from datetime import datetime, timezone
from typing import Dict, Any, Optional

try:
    from .llm_client import generate_structured_summary
    from .brief_prompts import build_morning_brief_prompt, build_evening_wrap_prompt
    from .briefs import build_current_state_brief
    from .comparisons import compare_structural_vs_preview
    from ..storage.redis_client import RedisClient
except ImportError:
    from services.llm_client import generate_structured_summary
    from services.brief_prompts import build_morning_brief_prompt, build_evening_wrap_prompt
    from services.briefs import build_current_state_brief
    from services.comparisons import compare_structural_vs_preview
    from storage.redis_client import RedisClient

def _get_redis() -> RedisClient:
    return RedisClient()

def _build_base_context(redis_client: RedisClient) -> Dict[str, Any]:
    """Gathers the underlying deterministic data."""
    try:
        from .reader import get_latest_structural_snapshot, get_latest_preview_snapshot, get_health_snapshot, get_latest_market_context
    except ImportError:
        from services.reader import get_latest_structural_snapshot, get_latest_preview_snapshot, get_health_snapshot, get_latest_market_context
        
    health = get_health_snapshot(redis_client) or {}
    struct = get_latest_structural_snapshot(redis_client) or {}
    prev = get_latest_preview_snapshot(redis_client) or {}
    ctx = get_latest_market_context(redis_client) or {}
    
    mc_data = ctx.get("market_context", ctx)
    deterministic_brief = build_current_state_brief(health, struct, prev, mc_data)
    comparison = compare_structural_vs_preview(struct, prev)
    
    return {
        "health": health,
        "struct": struct,
        "prev": prev,
        "brief": deterministic_brief,
        "comparison": comparison
    }

def _format_payload(mode: str, struct: Dict[str, Any], prev: Dict[str, Any], llm_result: Dict[str, Any]) -> Dict[str, Any]:
    """Formats the LLM result into the target UI payload structure."""
    
    now = datetime.now(timezone.utc).isoformat()
    
    base = {
        "mode": mode,
        "generated_at_utc": now,
        "source_run_ids": {
            "structural": struct.get("run_id", "unknown"),
            "preview": prev.get("run_id", "unknown")
        },
        "llm_meta": llm_result.get("meta", {})
    }
    
    if not llm_result.get("success"):
        base["error"] = llm_result.get("error", "Unknown LLM error.")
        base["raw_text"] = llm_result.get("raw_text", "")
        return base
        
    parsed = llm_result.get("parsed", {})
    base["headline"] = parsed.get("headline", "Summary unavailable.")
    base["sections"] = parsed.get("sections", {})
    base["raw_text"] = llm_result.get("text", "")
    
    return base

def generate_morning_brief() -> Dict[str, Any]:
    redis = _get_redis()
    ctx = _build_base_context(redis)
    
    system_prompt, user_prompt = build_morning_brief_prompt(
        ctx["brief"], ctx["health"], ctx["comparison"]
    )
    
    result = generate_structured_summary(system_prompt, user_prompt)
    payload = _format_payload("morning_brief", ctx["struct"], ctx["prev"], result)
    
    if payload.get("llm_meta", {}).get("success", True) and "error" not in payload:
        redis.redis.set("msm:brief:morning:latest", __import__("json").dumps(payload))
        
    return payload

def generate_evening_wrap() -> Dict[str, Any]:
    redis = _get_redis()
    ctx = _build_base_context(redis)
    
    system_prompt, user_prompt = build_evening_wrap_prompt(
        ctx["brief"], ctx["health"], ctx["comparison"]
    )
    
    result = generate_structured_summary(system_prompt, user_prompt)
    payload = _format_payload("evening_wrap", ctx["struct"], ctx["prev"], result)
    
    if payload.get("llm_meta", {}).get("success", True) and "error" not in payload:
        redis.redis.set("msm:brief:evening:latest", __import__("json").dumps(payload))
        
    return payload

def get_cached_morning_brief() -> Optional[Dict[str, Any]]:
    redis = _get_redis()
    val = redis.redis.get("msm:brief:morning:latest")
    if val:
        return __import__("json").loads(val)
    return None

def get_cached_evening_wrap() -> Optional[Dict[str, Any]]:
    redis = _get_redis()
    val = redis.redis.get("msm:brief:evening:latest")
    if val:
        return __import__("json").loads(val)
    return None
