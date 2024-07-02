from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel


@dataclass
class InputUTXO:
    tx_id: int
    tx_out_id: int
    tx_out_index: int
    stake_address_id: int
    consuming_tx_hash: str
    creating_tx_hash: str
    block_hash: str
    block_index: int
    consuming_timestamp: datetime
    creating_timestamp: datetime
    input_address: str
    input_value: int
    stake_address: Optional[str] = None
    asset_policy: Optional[str] = None
    asset_name: Optional[str] = None
    asset_quantity: Optional[int] = None


@dataclass
class OutputUTXO:
    tx_id: int
    tx_out_index: int
    stake_address_id: int
    consuming_tx_hash: str
    creating_tx_hash: str
    block_hash: str
    block_index: int
    fee: int
    consuming_timestamp: datetime
    creating_timestamp: datetime
    output_address: str
    output_value: int
    stake_address: Optional[str] = None
    asset_policy: Optional[str] = None
    asset_name: Optional[str] = None
    asset_quantity: Optional[int] = None


@dataclass
class Transaction:
    fee: int = 0.0
    block_hash: str = ""
    block_index: int = 0
    inputs: List[InputUTXO] = field(default_factory=list)
    outputs: List[OutputUTXO] = field(default_factory=list)


@dataclass
class ProcessedTransaction:
    tx_hash: str
    input_address: str
    input_stake_address: Optional[str]
    output_address: str
    output_stake_address: Optional[str]
    output_value: int
    actual_sent: int
    timestamp: datetime
    asset_policy: Optional[str]
    asset_name: Optional[str]
    asset_quantity: Optional[int]


class TransactionResponse(BaseModel):
    tx_hash: str
    block_no: str
    epoch_no: int
    slot_no: int
    timestamp: str
    absolute_slot_no: int
    fees: float
    total_output: float
    input_addresses: List[str]
    output_addresses: List[str]
    status: str


class TransactionsResponse(BaseModel):
    transactions: List[TransactionResponse]
    total_count: int
