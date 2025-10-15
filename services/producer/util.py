from __future__ import annotations

import json
import os
import pathlib
from datetime import datetime, timezone
from typing import Dict, Any, List

import orjson

# Minimal JSON logger to stdout (structured for ingestion)
def _log(level: str, payload: Dict[str, Any]) -> None:
    rec = {"level": level, "ts": datetime.now(tz=timezone.utc).isoformat()}
    rec.update(payload)
    print(orjson.dumps(rec).decode("utf-8"), flush=True)

class logger:
    @staticmethod
    def info(p: Dict[str, Any]) -> None: _log("INFO", p)
    @staticmethod
    def warning(p: Dict[str, Any]) -> None: _log("WARN", p)
    @staticmethod
    def error(p: Dict[str, Any]) -> None: _log("ERROR", p)
    @staticmethod
    def debug(p: Dict[str, Any]) -> None:
        if os.environ.get("DEBUG", "0") == "1":
            _log("DEBUG", p)

def json_dumps(obj: Any) -> str:
    return orjson.dumps(obj).decode("utf-8")


# Schema loader tolerant of container/repo layouts
def load_trade_schema() -> Dict[str, Any]:
    # Prefer explicit path
    explicit = os.environ.get("TRADE_SCHEMA_PATH")

    here = pathlib.Path(__file__).resolve()
    candidates: List[str] = []

    if explicit:
        candidates.append(explicit)

    # Common in-container paths given your layout (Dockerfile copies ./services/producer -> /app)
    candidates.append(str(here.parent / "schemas" / "trade.schema.json"))   # /app/schemas/...
    candidates.append("/app/schemas/trade.schema.json")                     # absolute fallback

    # Walk up safely (covers other layouts) — no IndexError
    for p in here.parents:
        candidates.append(str(p / "schemas" / "trade.schema.json"))

    # Pick the first that exists
    for path in candidates:
        if os.path.exists(path):
            logger.debug({"msg": "schema_path_selected", "path": path})
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)

    raise FileNotFoundError("trade.schema.json not found. Tried:\n" + "\n".join(candidates))


# Normalize a single Finnhub 'trade' message line into list of minimal dicts
def normalize_finnhub_trade_msg(raw_line: str) -> List[Dict[str, Any]]:
    """
    Input example:
    {"type":"trade","data":[{"p":172.1,"s":"AAPL","t":1700000000123,"v":50,"c":["@"]}]}
    Returns list of dicts: [{"symbol": "...", "price": ..., "volume": ..., "event_ts": ...}, ...]
    """
    doc = json.loads(raw_line)
    if "type" in doc and doc["type"] == "trade" and "data" in doc:
        out: List[Dict[str, Any]] = []
        for it in doc["data"]:
            # Some messages may be partial; guard required fields
            if not all(k in it for k in ("p", "s", "t", "v")):
                continue
            out.append(
                {
                    "symbol": str(it["s"]).upper(),
                    "price": float(it["p"]),
                    "volume": float(it["v"]),
                    "event_ts": int(it["t"]),  # ms epoch
                    "conditions": it.get("c", []),
                }
            )
        return out
    # Ignore other message types like {"ping":1} or {"type":"error",...}
    return []


def build_trade_record(symbol: str, price: float, volume: float, event_ts_ms: int, source: str) -> Dict[str, Any]:
    ingest_ts = datetime.now(tz=timezone.utc).isoformat()
    key = f"{symbol}|{event_ts_ms}|{price}|{volume}"
    return {
        "type": "trade",
        "source": source,
        "ingest_ts": ingest_ts,
        "event_ts": int(event_ts_ms),
        "symbol": symbol,
        "price": float(price),
        "volume": float(volume),
        "conditions": [],
        "key": key,
    }
