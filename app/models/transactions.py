from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional


@dataclass
class InputUTXO:
    tx_id: int
    tx_out_id: int
    tx_out_index: int
    stake_address_id: int
    consuming_tx_hash: bytes
    creating_tx_hash: bytes
    consuming_timestamp: datetime
    creating_timestamp: datetime
    input_address: str
    input_value: int
    stake_address: Optional[str] = None
    asset_policy: Optional[str] = None
    asset_name: Optional[str] = None
    asset_quantity: Optional[int] = None

    def consuming_tx_hash_tex(self) -> str:
        return self.consuming_tx_hash.hex()

    def creating_tx_hash_tex(self) -> str:
        return self.creating_tx_hash.hex()


@dataclass
class OutputUTXO:
    tx_id: int
    tx_out_index: int
    stake_address_id: int
    consuming_tx_hash: bytes
    creating_tx_hash: bytes
    fee: int
    consuming_timestamp: datetime
    creating_timestamp: datetime
    output_address: str
    output_value: int
    stake_address: Optional[str] = None
    asset_policy: Optional[str] = None
    asset_name: Optional[str] = None
    asset_quantity: Optional[int] = None

    def consuming_tx_hash_tex(self) -> str:
        return self.consuming_tx_hash.hex()

    def creating_tx_hash_tex(self) -> str:
        return self.creating_tx_hash.hex()


@dataclass
class Transaction:
    fee: int = 0
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
