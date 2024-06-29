from typing import Optional, List

from pydantic import BaseModel


class BalanceHistoryPoint(BaseModel):
    time: str
    balance: str

class AddressDetails(BaseModel):
    id: str
    transactions: int
    balance: str
    value: str
    stake_address: Optional[str]
    total_stake: str
    pool_name: Optional[str]
    reward_balance: str
    highest_balance: str
    lowest_balance: str
    balance_history: List[BalanceHistoryPoint]
    utxos: List[dict]
    recent_transactions: List[dict]