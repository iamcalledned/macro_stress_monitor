"""
Interface for interacting with the Redis data store.
"""
import os
import json
from datetime import date, datetime
import redis
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv

load_dotenv()


def _json_default(value: Any) -> Any:
    """Converts common scientific Python scalars to JSON-safe values."""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return str(value)


def _json_dumps(payload: Dict[str, Any]) -> str:
    return json.dumps(payload, default=_json_default)


class RedisClient:
    """A client to handle all Redis operations for the application."""
    
    LATEST_KEY = "msm:latest"
    HISTORY_KEY = "msm:history"
    ALERT_KEY = "msm:alerts:last"
    LATEST_RUN_ID_KEY = "msm:latest_run_id"  # Legacy key kept for compatibility.
    LATEST_STRUCTURAL_RUN_ID_KEY = "msm:latest_structural_run_id"
    LATEST_PREVIEW_RUN_ID_KEY = "msm:latest_preview_run_id"
    HEALTH_KEY = "msm:health"
    RUN_SNAPSHOT_PREFIX = "msm:run:"
    HISTORY_RETENTION = 400 # Keep last 400 data points

    def __init__(self, redis_url: Optional[str] = None):
        """
        Initializes the Redis client.
        
        Args:
            redis_url: The Redis connection URL. Defaults to env var REDIS_URL.
        """
        url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        try:
            self.client = redis.from_url(url, decode_responses=True)
            self.client.ping()
            print("Successfully connected to Redis.")
        except redis.exceptions.ConnectionError as e:
            print(f"ERROR: Could not connect to Redis at {url}. Please ensure Redis is running.")
            raise e

    def get_latest(self) -> Optional[Dict[str, Any]]:
        """Retrieves the latest data object."""
        latest_json = self.client.get(self.LATEST_KEY)
        return json.loads(latest_json) if latest_json else None

    def get_history(self) -> List[Dict[str, Any]]:
        """Retrieves the entire history list."""
        history_json = self.client.lrange(self.HISTORY_KEY, 0, -1)
        return [json.loads(item) for item in history_json]

    def get_last_alert(self) -> Optional[Dict[str, Any]]:
        """Retrieves the most recently stored alert object."""
        alert_json = self.client.get(self.ALERT_KEY)
        return json.loads(alert_json) if alert_json else None

    def get_latest_run_id(self) -> Optional[str]:
        """Gets the latest structural dashboard run id."""
        return self.get_latest_structural_run_id()

    def get_latest_structural_run_id(self) -> Optional[str]:
        """Gets the latest structural run id."""
        structural_id = self.client.get(self.LATEST_STRUCTURAL_RUN_ID_KEY)
        if structural_id:
            return structural_id
        return self.client.get(self.LATEST_RUN_ID_KEY)

    def get_latest_preview_run_id(self) -> Optional[str]:
        """Gets the latest intraday preview run id."""
        return self.client.get(self.LATEST_PREVIEW_RUN_ID_KEY)

    def _snapshot_key(self, run_id: str) -> str:
        return f"{self.RUN_SNAPSHOT_PREFIX}{run_id}:snapshot"

    def _preview_snapshot_key(self, run_id: str) -> str:
        return f"{self.RUN_SNAPSHOT_PREFIX}{run_id}:preview_snapshot"

    def get_run_snapshot(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Gets a structural run snapshot by id."""
        raw = self.client.get(self._snapshot_key(run_id))
        return json.loads(raw) if raw else None

    def get_preview_run_snapshot(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Gets a preview run snapshot by id."""
        raw = self.client.get(self._preview_snapshot_key(run_id))
        return json.loads(raw) if raw else None

    def get_structural_run_snapshot(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Gets a structural run snapshot by id."""
        return self.get_run_snapshot(run_id)

    def get_latest_snapshot(self) -> Optional[Dict[str, Any]]:
        """Gets the latest structural run snapshot."""
        run_id = self.get_latest_structural_run_id()
        if not run_id:
            return None
        return self.get_run_snapshot(run_id)

    def get_latest_structural_snapshot(self) -> Optional[Dict[str, Any]]:
        """Gets the latest structural run snapshot."""
        return self.get_latest_snapshot()

    def get_latest_preview_snapshot(self) -> Optional[Dict[str, Any]]:
        """Gets the latest intraday preview snapshot."""
        run_id = self.get_latest_preview_run_id()
        if not run_id:
            return None
        return self.get_preview_run_snapshot(run_id)

    def get_health_snapshot(self) -> Optional[Dict[str, Any]]:
        """Gets the compact system health record."""
        raw = self.client.get(self.HEALTH_KEY)
        return json.loads(raw) if raw else None

    def write_data(self, data: Dict[str, Any], date_str: str):
        """
        Writes the latest data point and updates the history.
        This operation is idempotent for a given day.

        Args:
            data: The complete data object for the latest update.
            date_str: The date string (e.g., 'YYYY-MM-DD') for the data point.
        """
        # Write the latest data
        self.client.set(self.LATEST_KEY, _json_dumps(data))

        # Check if the last history entry is for the same day
        last_entry_json = self.client.lindex(self.HISTORY_KEY, 0)
        if last_entry_json:
            last_entry = json.loads(last_entry_json)
            if last_entry.get("date") == date_str:
                # It's an update for the same day, so replace it
                print(f"Updating history for today: {date_str}")
                self.client.lset(self.HISTORY_KEY, 0, _json_dumps(data))
                return

        # It's a new day, add to the front of the list
        print(f"Adding new history entry for: {date_str}")
        self.client.lpush(self.HISTORY_KEY, _json_dumps(data))
        
        # Trim the history to the desired retention length
        self.client.ltrim(self.HISTORY_KEY, 0, self.HISTORY_RETENTION - 1)

    def write_alert(self, alert_data: Dict[str, Any]):
        """
        Writes an alert to the alert key.
        
        Args:
            alert_data: The data for the alert to be stored.
        """
        self.client.set(self.ALERT_KEY, _json_dumps(alert_data))
        if alert_data.get("active", True):
            print(f"ALERT: Fired new alert: {alert_data.get('reason')}")
        else:
            print(f"ALERT: Cleared alert state at {alert_data.get('cleared_at')}")

    def write_run_snapshot(self, run_id: str, snapshot: Dict[str, Any]):
        """Compatibility alias for writing a structural run snapshot."""
        self.write_structural_run_snapshot(run_id=run_id, snapshot=snapshot)

    def write_structural_run_snapshot(self, run_id: str, snapshot: Dict[str, Any]):
        """
        Writes a structural run snapshot and advances the structural run pointer.

        Args:
            run_id: Unique run id.
            snapshot: Snapshot payload for one update run.
        """
        key = self._snapshot_key(run_id)
        pipe = self.client.pipeline()
        pipe.set(key, _json_dumps(snapshot))
        pipe.set(self.LATEST_STRUCTURAL_RUN_ID_KEY, run_id)
        pipe.set(self.LATEST_RUN_ID_KEY, run_id)
        pipe.execute()

    def write_preview_run_snapshot(self, run_id: str, snapshot: Dict[str, Any]):
        """
        Writes a preview run snapshot and advances the preview run pointer.

        Args:
            run_id: Unique run id.
            snapshot: Preview snapshot payload for one preview update run.
        """
        key = self._preview_snapshot_key(run_id)
        pipe = self.client.pipeline()
        pipe.set(key, _json_dumps(snapshot))
        pipe.set(self.LATEST_PREVIEW_RUN_ID_KEY, run_id)
        pipe.execute()

    def write_health_snapshot(self, health: Dict[str, Any]):
        """Writes the compact system health record."""
        self.client.set(self.HEALTH_KEY, _json_dumps(health))
