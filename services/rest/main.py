from fastapi import FastAPI, Query
from pydantic_settings import BaseSettings
from .models import HealthResponse, PricePoint
from .crud import get_latest_price, list_symbols

class Settings(BaseSettings):
    symbols: str = "AAPL,MSFT,SPY"
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

app = FastAPI(title="rtmkt REST (skeleton)")

@app.get("/health", response_model=HealthResponse)
def health():
    return {"status": "ok"}

@app.get("/symbols")
def symbols():
    settings = Settings()
    return {"symbols": list_symbols(settings.symbols)}

@app.get("/price/latest", response_model=PricePoint)
def price_latest(symbol: str = Query(..., description="Ticker symbol, e.g., AAPL")):
    return get_latest_price(symbol)
