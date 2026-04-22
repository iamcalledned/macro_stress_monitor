"""
Freshness and health helpers for Macro Stress Monitor snapshots.
"""
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional


ENGINE_VERSION = os.getenv("MSM_ENGINE_VERSION", "2026.04.22")
SNAPSHOT_SCHEMA_VERSION = "2.0"
PREVIEW_RULE_VERSION = "1.0"
STRUCTURAL_STALE_AFTER_SECONDS = int(os.getenv("MSM_STRUCTURAL_STALE_AFTER_SECONDS", str(36 * 60 * 60)))
PREVIEW_STALE_AFTER_SECONDS = int(os.getenv("MSM_PREVIEW_STALE_AFTER_SECONDS", str(8 * 60 * 60)))


def utc_now() -> datetime:
    """Returns the current UTC time."""
    return datetime.now(timezone.utc)


def parse_iso_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parses ISO timestamps and normalizes naive values to UTC."""
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def age_seconds(computed_at_utc: Optional[str], now: Optional[datetime] = None) -> Optional[int]:
    """Returns snapshot age in seconds, or None if the timestamp is invalid."""
    dt = parse_iso_datetime(computed_at_utc)
    if dt is None:
        return None
    current = now or utc_now()
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return max(0, int((current.astimezone(timezone.utc) - dt).total_seconds()))


def freshness_for_snapshot(
    snapshot: Optional[Dict[str, Any]],
    mode: str,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Evaluates centralized staleness for a structural or preview snapshot."""
    if not snapshot:
        return {
            "age_seconds": None,
            "is_stale": True,
            "stale_reason": f"missing_{mode}_snapshot",
            "stale_after_seconds": _stale_after_seconds(mode),
        }

    computed_at = snapshot.get("computed_at_utc")
    age = age_seconds(computed_at, now=now)
    stale_after = _stale_after_seconds(mode)
    if age is None:
        return {
            "age_seconds": None,
            "is_stale": True,
            "stale_reason": "invalid_computed_at_utc",
            "stale_after_seconds": stale_after,
        }

    is_stale = age > stale_after
    return {
        "age_seconds": age,
        "is_stale": is_stale,
        "stale_reason": f"older_than_{stale_after}_seconds" if is_stale else None,
        "stale_after_seconds": stale_after,
    }


def _stale_after_seconds(mode: str) -> int:
    if mode == "preview":
        return PREVIEW_STALE_AFTER_SECONDS
    return STRUCTURAL_STALE_AFTER_SECONDS


def snapshot_meta(extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Builds metadata included in each expanded snapshot."""
    meta = {
        "engine_version": ENGINE_VERSION,
        "schema_version": SNAPSHOT_SCHEMA_VERSION,
    }
    if extra:
        meta.update(extra)
    return meta


def build_health_snapshot(redis_client: Any, now: Optional[datetime] = None) -> Dict[str, Any]:
    """Builds the compact system health record from current Redis pointers."""
    current = now or utc_now()
    previous_health = redis_client.get_health_snapshot() or {}
    structural_run_id = redis_client.get_latest_structural_run_id()
    preview_run_id = redis_client.get_latest_preview_run_id()
    structural_snapshot = redis_client.get_latest_structural_snapshot()
    preview_snapshot = redis_client.get_latest_preview_snapshot()
    structural_freshness = freshness_for_snapshot(structural_snapshot, "structural", now=now)
    preview_freshness = freshness_for_snapshot(preview_snapshot, "preview", now=now)

    structural_status = (structural_snapshot or {}).get("run_status", {}).get("status")
    preview_status = (preview_snapshot or {}).get("run_status", {}).get("status")

    last_successful_structural = previous_health.get("last_successful_structural_run")
    if structural_status in {"success", "partial"}:
        last_successful_structural = {
            "run_id": structural_run_id,
            "computed_at_utc": (structural_snapshot or {}).get("computed_at_utc"),
        }

    last_successful_preview = previous_health.get("last_successful_preview_run")
    if preview_status in {"success", "partial"}:
        last_successful_preview = {
            "run_id": preview_run_id,
            "computed_at_utc": (preview_snapshot or {}).get("computed_at_utc"),
        }

    return {
        "computed_at_utc": current.isoformat(),
        "meta": snapshot_meta({"record_type": "system_health"}),
        "latest_structural_run_id": structural_run_id,
        "latest_structural_computed_at_utc": (structural_snapshot or {}).get("computed_at_utc"),
        "structural_age_seconds": structural_freshness["age_seconds"],
        "structural_stale": structural_freshness["is_stale"],
        "structural_stale_reason": structural_freshness["stale_reason"],
        "latest_preview_run_id": preview_run_id,
        "latest_preview_computed_at_utc": (preview_snapshot or {}).get("computed_at_utc"),
        "preview_age_seconds": preview_freshness["age_seconds"],
        "preview_stale": preview_freshness["is_stale"],
        "preview_stale_reason": preview_freshness["stale_reason"],
        "last_successful_structural_run": last_successful_structural,
        "last_successful_preview_run": last_successful_preview,
        "last_failed_run": previous_health.get("last_failed_run"),
    }
