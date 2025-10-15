import os
from services.producer.util import load_trade_schema, build_trade_record
from jsonschema import validate

def test_trade_schema_valid_sample():
    schema = load_trade_schema()
    trade = build_trade_record("MSFT", 400.12, 50, 1700000000000, source="replay")
    validate(instance=trade, schema=schema)

def test_trade_schema_rejects_negative_price():
    schema = load_trade_schema()
    trade = build_trade_record("MSFT", -1.0, 50, 1700000000000, source="replay")
    trade["price"] = -1.0
    import pytest
    with pytest.raises(Exception):
        validate(instance=trade, schema=schema)
