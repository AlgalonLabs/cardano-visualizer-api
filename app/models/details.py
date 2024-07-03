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


class InputUTXOInfo(BaseModel):
    address: str
    stake_address: Optional[str] = None
    amount: float
    utxo_hash: str
    utxo_index: int


class OutputUTXOInfo(BaseModel):
    address: str
    stake_address: Optional[str] = None
    amount: float


class TransactionSummary(BaseModel):
    address: str
    net_amount: float
    tokens_sent: int
    tokens_received: int


class TransactionDetails(BaseModel):
    hash: str
    created_at: str
    total_output: float
    fees: float
    block_no: Optional[int] = None
    slot_no: Optional[int] = None
    absolute_slot_no: Optional[int] = None
    summary: List[TransactionSummary]
    inputs: List[InputUTXOInfo]
    outputs: List[OutputUTXOInfo]

    class Config:
        from_attributes = True
