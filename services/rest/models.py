from pydantic import BaseModel, Field

class HealthResponse(BaseModel):
    status: str = Field(default="ok")

class PricePoint(BaseModel):
    t: str  # ISO8601
    c: float
