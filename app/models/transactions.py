from dataclasses import dataclass
from typing import List, Optional


@dataclass
class InputUTXO:
    address: str
    value: int
    stake_address: Optional[str] = None


@dataclass
class OutputUTXO:
    address: str
    value: int
    timestamp: str
    asset_policy: Optional[str] = None
    asset_name: Optional[str] = None
    asset_quantity: Optional[int] = None
    stake_address: Optional[str] = None


@dataclass
class Transaction:
    inputs: List[InputUTXO]
    outputs: List[OutputUTXO]


@dataclass
class ProcessedTransaction:
    tx_hash: str
    input_address: str
    input_stake_address: Optional[str]
    output_address: str
    output_stake_address: Optional[str]
    output_value: int
    actual_sent: int
    timestamp: str
    asset_policy: Optional[str]
    asset_name: Optional[str]
    asset_quantity: Optional[int]
