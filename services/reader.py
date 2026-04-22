"""
Read helpers for Macro Stress Monitor Redis data.
"""
from typing import Any, Dict, List, Optional

try:
    from .health import build_health_snapshot
    from ..storage.redis_client import RedisClient
except ImportError:  # pragma: no cover - direct script compatibility
    from services.health import build_health_snapshot
    from storage.redis_client import RedisClient


def _client(redis_client: Optional[RedisClient] = None) -> RedisClient:
    return redis_client or RedisClient()


def get_latest_structural_snapshot(redis_client: Optional[RedisClient] = None) -> Optional[Dict[str, Any]]:
    """Returns the latest structural run snapshot."""
    return _client(redis_client).get_latest_structural_snapshot()


def get_latest_preview_snapshot(redis_client: Optional[RedisClient] = None) -> Optional[Dict[str, Any]]:
    """Returns the latest preview run snapshot."""
    return _client(redis_client).get_latest_preview_snapshot()


def get_structural_history(limit: int = 30, redis_client: Optional[RedisClient] = None) -> List[Dict[str, Any]]:
    """Returns structural history entries, newest first."""
    history = _client(redis_client).get_history()
    return history[: max(0, int(limit))]


def get_structural_snapshot(run_id: str, redis_client: Optional[RedisClient] = None) -> Optional[Dict[str, Any]]:
    """Returns a structural snapshot by run id."""
    return _client(redis_client).get_structural_run_snapshot(run_id)


def get_preview_snapshot(run_id: str, redis_client: Optional[RedisClient] = None) -> Optional[Dict[str, Any]]:
    """Returns a preview snapshot by run id."""
    return _client(redis_client).get_preview_run_snapshot(run_id)


def get_health_snapshot(redis_client: Optional[RedisClient] = None) -> Dict[str, Any]:
    """Returns stored health, or builds a current health view if none exists."""
    redis = _client(redis_client)
    stored = redis.get_health_snapshot()
    return stored if stored is not None else build_health_snapshot(redis)


def get_latest_market_context(redis_client: Optional[RedisClient] = None) -> Optional[Dict[str, Any]]:
    """Returns the latest structural market context package."""
    snapshot = get_latest_structural_snapshot(redis_client)
    if not snapshot:
        return None
    context = snapshot.get("market_context")
    return context if isinstance(context, dict) else None
