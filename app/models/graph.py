from typing import List, Dict, Any, Optional, Union

from pydantic import BaseModel


class AddressNode(BaseModel):
    id: str
    type: str
    label: str


class TransactionNode(BaseModel):
    id: str
    type: str
    tx_hash: str
    timestamp: str
    value: int
    asset_policy: Optional[str]
    asset_name: Optional[str]
    asset_quantity: Optional[int]


class StakeAddressNode(BaseModel):
    id: str
    type: str
    label: str


Node = Union[AddressNode, TransactionNode, StakeAddressNode]


class Edge(BaseModel):
    from_address: str
    to_address: str
    type: str


class GraphData(BaseModel):
    nodes: List[Dict[str, Any]]
    edges: List[Edge]


class AddressDetails(BaseModel):
    address: str
    utxos: List[Any]
    transactions: List[Any]


class TransactionDetails(BaseModel):
    transaction: Any
    relationships: List[Any]
    nodes: List[Any]


class AssetDetails(BaseModel):
    asset: Any
    transactions: List[Any]


class BlockDetails(BaseModel):
    block: Any
    transactions: List[Any]
    epoch: Any


class EpochDetails(BaseModel):
    epoch: Any
    block_count: int
    tx_count: int
    total_size: int
