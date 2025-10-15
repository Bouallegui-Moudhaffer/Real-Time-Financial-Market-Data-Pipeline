from services.spark.job_ohlcv import ohlcv_from_trades

def test_ohlcv_from_trades_basic():
    trades = [(10.0, 1.0), (11.0, 2.0), (9.5, 1.0), (10.5, 3.0)]
    res = ohlcv_from_trades(trades)
    assert res["open"] == 10.0
    assert res["high"] == 11.0
    assert res["low"] == 9.5
    assert res["close"] == 10.5
    assert res["volume"] == 7.0
    assert abs(res["vwap"] - ((10*1 + 11*2 + 9.5*1 + 10.5*3)/7)) < 1e-9
