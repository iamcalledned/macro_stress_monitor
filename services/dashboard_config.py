"""
Config helpers for landing, basket, and quote watchlist pages.
"""
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional


DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "web" / "dashboard_config.json"


def _config_path(explicit_path: Optional[str] = None) -> Path:
    raw = explicit_path or os.getenv("MSM_DASHBOARD_CONFIG_PATH")
    return Path(raw).expanduser() if raw else DEFAULT_CONFIG_PATH


def load_dashboard_config(explicit_path: Optional[str] = None) -> Dict[str, Any]:
    path = _config_path(explicit_path)
    if not path.exists():
        return {
            "dashboard_name": "Bottom Sniffer Dashboard",
            "quote_watchlist": [],
            "baskets": [],
            "categories": [],
        }

    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if not isinstance(payload, dict):
        raise ValueError(f"Dashboard config must be a JSON object: {path}")

    payload.setdefault("dashboard_name", "Bottom Sniffer Dashboard")
    payload.setdefault("quote_watchlist", [])
    payload.setdefault("baskets", [])
    payload.setdefault("categories", [])
    payload["_config_path"] = str(path)
    return payload


def get_baskets(config: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    payload = config or load_dashboard_config()
    baskets = payload.get("baskets", [])
    return baskets if isinstance(baskets, list) else []


def get_basket(basket_name: str, config: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
    target = str(basket_name).strip().lower()
    for basket in get_baskets(config):
        if str(basket.get("name", "")).strip().lower() == target:
            return basket
    return None


def get_quote_symbols(config: Optional[Dict[str, Any]] = None) -> List[str]:
    payload = config or load_dashboard_config()
    symbols: List[str] = []

    for item in payload.get("quote_watchlist", []):
        symbol = str(item).strip().upper()
        if symbol:
            symbols.append(symbol)

    for basket in get_baskets(payload):
        for stock in basket.get("stocks", []):
            symbol = str(stock.get("ticker", "")).strip().upper()
            if symbol:
                symbols.append(symbol)

    return list(dict.fromkeys(symbols))
