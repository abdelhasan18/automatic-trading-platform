from pydantic import BaseModel
from typing import Literal, Optional

class PriceTick(BaseModel):
    symbol: str
    price: float
    timestamp: str
    ingestion_ts: float = 0.0

class Signal(BaseModel):
    signal_id: str
    symbol: str
    action: Literal["BUY", "SELL"]
    confidence: float
    price_at_signal: float
    ingestion_ts: float

class Order(BaseModel):
    order_id: str
    symbol: str
    action: Literal["BUY", "SELL"]
    quantity: int
    price_at_order: float
    ingestion_ts: float

class Fill(BaseModel):
    fill_id: str
    symbol: str
    action: Literal["BUY", "SELL"]
    quantity_filled: int
    fill_price: float
    ingestion_ts: float