from datetime import datetime
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


class UTXOInfo(BaseModel):
    address: str
    stake_address: Optional[str]
    amount: float
    utxo_hash: str
    utxo_index: int


class TransactionDetails(BaseModel):
    hash: str
    created_at: datetime
    total_output: float
    fee: float
    block_number: Optional[int]
    slot: Optional[int]
    absolute_slot: Optional[int]
    inputs: List[UTXOInfo]
    outputs: List[UTXOInfo]

    class Config:
        from_attributes = True
