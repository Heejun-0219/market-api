from pydantic import BaseModel
from datetime import datetime
from typing import Dict, List, Optional

class MarketData(BaseModel):
    exchange_rates: Dict[str, float]
    market_indices: Dict[str, float]
    timestamp: datetime