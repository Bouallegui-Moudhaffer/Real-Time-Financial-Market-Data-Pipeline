from __future__ import annotations

import os
import socket
import time
from datetime import date, datetime
from typing import List, Optional

from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import RoundRobinPolicy
from cassandra.query import PreparedStatement, dict_factory
from cassandra import ConsistencyLevel
from cassandra import OperationTimedOut

# ---- Config
RAW_CONTACTS = os.getenv("CASSANDRA_CONTACT_POINTS", "cassandra")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "market_ks")
DNS_TIMEOUT_SECS = int(os.getenv("CASSANDRA_DNS_TIMEOUT", "60"))
DNS_POLL_INTERVAL = float(os.getenv("CASSANDRA_DNS_INTERVAL", "1.0"))

app = FastAPI(title="rtmkt-read-api", version="1.0.0")

# ---- Cassandra (singleton)
cluster: Cluster | None = None
session = None

ps_ohlcv_1s: PreparedStatement | None = None
ps_ohlcv_1m: PreparedStatement | None = None
ps_ind_1m: PreparedStatement | None = None


def _clean_contact_points(raw: str) -> List[str]:
    hosts: List[str] = []
    for token in (raw or "").split(","):
        h = token.strip()
        if not h:
            continue
        if h.startswith("[") and "]" in h:
            right = h.split("]", 1)[1]
            if right.startswith(":"):
                h = h.split("]", 1)[0] + "]"
        else:
            if ":" in h:
                h = h.split(":", 1)[0]
        hosts.append(h)
    return hosts


def _wait_for_dns(hosts: List[str], timeout_s: int, interval_s: float) -> None:
    start = time.time()
    unresolved = set(hosts)
    while unresolved and (time.time() - start) < timeout_s:
        still_unresolved = set()
        for h in unresolved:
            try:
                socket.getaddrinfo(h, None)
            except OSError:
                still_unresolved.add(h)
        if not still_unresolved:
            return
        time.sleep(interval_s)
        unresolved = still_unresolved
    if unresolved:
        raise RuntimeError(f"Unresolvable contact points after {timeout_s}s: {sorted(unresolved)}")


def _ts(x) -> Optional[str]:
    if x is None:
        return None
    if isinstance(x, datetime):
        return x.replace(tzinfo=None).isoformat(timespec="seconds") + "Z"
    return str(x)


class OHLCV(BaseModel):
    symbol: str
    bucket_date: str
    window_start: str
    window_end: Optional[str] = None
    open: float
    high: float
    low: float
    close: float
    volume: float
    trade_count: Optional[int] = None
    vwap: Optional[float] = None
    pct_change_1m: Optional[float] = None
    last_event_ts: Optional[str] = None
    src: Optional[str] = None


class Indicator1m(BaseModel):
    symbol: str
    bucket_date: str
    window_start: str
    window_end: Optional[str] = None
    vwap_1m: Optional[float] = None
    vol_1m: Optional[float] = None


@app.on_event("startup")
def on_startup():
    global cluster, session, ps_ohlcv_1s, ps_ohlcv_1m, ps_ind_1m

    contact_points = _clean_contact_points(RAW_CONTACTS)
    if not contact_points:
        raise RuntimeError(
            "CASSANDRA_CONTACT_POINTS resolved to an empty list. "
            "Set it to a comma-separated list of hostnames (no ports)."
        )

    _wait_for_dns(contact_points, timeout_s=DNS_TIMEOUT_SECS, interval_s=DNS_POLL_INTERVAL)

    # Put row_factory into the execution profile (don’t set session.row_factory directly)
    profile = ExecutionProfile(
        load_balancing_policy=RoundRobinPolicy(),
        row_factory=dict_factory,
    )
    cluster = Cluster(
        contact_points=contact_points,
        port=CASSANDRA_PORT,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
    )
    session = cluster.connect(KEYSPACE)

    # Prepared statements
    ps_ohlcv_1s = session.prepare(
        """
        SELECT symbol, bucket_date, window_start, window_end,
               open, high, low, close, volume, trade_count,
               vwap, last_event_ts, src
        FROM ohlcv_1s
        WHERE symbol=? AND bucket_date=?
        ORDER BY window_start DESC
        LIMIT ?
        """
    )
    ps_ohlcv_1s.consistency_level = ConsistencyLevel.ONE

    ps_ohlcv_1m = session.prepare(
        """
        SELECT symbol, bucket_date, window_start, window_end,
               open, high, low, close, volume, trade_count,
               vwap, pct_change_1m, last_event_ts, src
        FROM ohlcv_1m
        WHERE symbol=? AND bucket_date=?
        ORDER BY window_start DESC
        LIMIT ?
        """
    )
    ps_ohlcv_1m.consistency_level = ConsistencyLevel.ONE

    ps_ind_1m = session.prepare(
        """
        SELECT symbol, bucket_date, window_start, window_end,
               vwap_1m, vol_1m
        FROM indicators_1m
        WHERE symbol=? AND bucket_date=?
        ORDER BY window_start DESC
        LIMIT ?
        """
    )
    ps_ind_1m.consistency_level = ConsistencyLevel.ONE


@app.on_event("shutdown")
def on_shutdown():
    global session, cluster
    if session:
        session.shutdown()
    if cluster:
        cluster.shutdown()


@app.get("/healthz")
def healthz():
    return {
        "status": "ok",
        "contact_points": _clean_contact_points(RAW_CONTACTS),
        "port": CASSANDRA_PORT,
        "keyspace": KEYSPACE,
    }


def _parse_date(d: Optional[str]) -> date:
    if not d:
        return datetime.utcnow().date()
    try:
        return datetime.strptime(d, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")


def _row_to_ohlcv(r) -> OHLCV:
    return OHLCV(
        symbol=r["symbol"],
        bucket_date=str(r["bucket_date"]),
        window_start=_ts(r["window_start"]),
        window_end=_ts(r.get("window_end")),
        open=float(r["open"]),
        high=float(r["high"]),
        low=float(r["low"]),
        close=float(r["close"]),
        volume=float(r["volume"]),
        trade_count=int(r["trade_count"]) if r.get("trade_count") is not None else None,
        vwap=float(r["vwap"]) if r.get("vwap") is not None else None,
        pct_change_1m=float(r["pct_change_1m"]) if r.get("pct_change_1m") is not None else None,
        last_event_ts=_ts(r.get("last_event_ts")),
        src=r.get("src"),
    )


def _row_to_ind(r) -> Indicator1m:
    return Indicator1m(
        symbol=r["symbol"],
        bucket_date=str(r["bucket_date"]),
        window_start=_ts(r["window_start"]),
        window_end=_ts(r.get("window_end")),
        vwap_1m=float(r["vwap_1m"]) if r.get("vwap_1m") is not None else None,
        vol_1m=float(r["vol_1m"]) if r.get("vol_1m") is not None else None,
    )


@app.get("/ohlcv/1s", response_model=List[OHLCV])
def ohlcv_1s(
    symbol: str = Query(..., min_length=1),
    date_str: Optional[str] = Query(None, alias="date"),
    limit: int = Query(100, ge=1, le=5000),
):
    bdate = _parse_date(date_str)
    rows = session.execute(ps_ohlcv_1s, [symbol.upper(), bdate, limit])
    return [_row_to_ohlcv(r) for r in rows]


@app.get("/ohlcv/1m", response_model=List[OHLCV])
def ohlcv_1m(
    symbol: str = Query(..., min_length=1),
    date_str: Optional[str] = Query(None, alias="date"),
    limit: int = Query(100, ge=1, le=5000),
):
    bdate = _parse_date(date_str)
    rows = session.execute(ps_ohlcv_1m, [symbol.upper(), bdate, limit])
    return [_row_to_ohlcv(r) for r in rows]


@app.get("/indicators/1m", response_model=List[Indicator1m])
def indicators_1m(
    symbol: str = Query(..., min_length=1),
    date_str: Optional[str] = Query(None, alias="date"),
    limit: int = Query(100, ge=1, le=5000),
):
    bdate = _parse_date(date_str)
    rows = session.execute(ps_ind_1m, [symbol.upper(), bdate, limit])
    return [_row_to_ind(r) for r in rows]
