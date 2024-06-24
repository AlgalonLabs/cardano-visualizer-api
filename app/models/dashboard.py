from pydantic import BaseModel


class CardanoData(BaseModel):
    price: float
    market_cap: float
    volume_24h: float
    percent_change_24h: float
