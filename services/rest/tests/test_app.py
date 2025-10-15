from fastapi.testclient import TestClient
from services.rest.main import app

client = TestClient(app)

def test_health_ok():
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"

def test_symbols_default():
    resp = client.get("/symbols")
    assert resp.status_code == 200
    data = resp.json()
    assert "symbols" in data
    assert set(data["symbols"]) >= {"AAPL", "MSFT", "SPY"}
