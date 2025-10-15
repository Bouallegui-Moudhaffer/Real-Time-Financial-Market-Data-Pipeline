"""
Placeholder data access functions (Step 1). Real Cassandra logic arrives in Step 7.
"""

from typing import List, Dict
from datetime import datetime, timezone

def get_latest_price(symbol: str) -> Dict:
    # Stubbed response
    return {"t": datetime.now(tz=timezone.utc).isoformat(), "c": 123.45}

def list_symbols(config_symbols: str) -> List[str]:
    return [s.strip().upper() for s in config_symbols.split(",") if s.strip()]
