from services.producer.util import normalize_finnhub_trade_msg

def test_normalize_single_trade():
    line = '{"type":"trade","data":[{"p":100.5,"s":"AAPL","t":1700000000001,"v":10,"c":["@"]}]}'
    out = normalize_finnhub_trade_msg(line)
    assert len(out) == 1
    rec = out[0]
    assert rec["symbol"] == "AAPL"
    assert rec["price"] == 100.5
    assert rec["volume"] == 10
    assert rec["event_ts"] == 1700000000001

def test_normalize_ignores_non_trade():
    assert normalize_finnhub_trade_msg('{"ping":1}') == []
    assert normalize_finnhub_trade_msg('{"type":"error","msg":"rate limit"}') == []
