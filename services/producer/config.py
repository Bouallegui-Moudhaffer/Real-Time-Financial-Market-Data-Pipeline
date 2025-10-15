from pydantic_settings import BaseSettings
from pydantic import Field

class Settings(BaseSettings):
    finnhub_api_token: str = Field(default="")
    symbols: str = Field(default="AAPL,MSFT,SPY")

    kafka_brokers: str = Field(default="localhost:9092")
    kafka_client_id: str = Field(default="rtmkt-producer")
    kafka_security_protocol: str = Field(default="PLAINTEXT")
    kafka_sasl_mechanism: str = Field(default="")
    kafka_sasl_username: str = Field(default="")
    kafka_sasl_password: str = Field(default="")

    topic_trades_raw: str = Field(default="market.trades.raw")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        env_prefix = ""
        case_sensitive = False
        extra="ignore"
