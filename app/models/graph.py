from typing import List, Any, Optional, Union

from pydantic import BaseModel, Field


# Base Models
class BaseNode(BaseModel):
    id: str
    type: str


class BaseEdge(BaseModel):
    from_address: str
    to_address: str
    type: str


class GraphData(BaseModel):
    nodes: List[BaseNode]
    edges: List[BaseEdge]


class AddressNode(BaseNode):
    label: str


class TransactionNode(BaseNode):
    hash: str = Field(..., alias="tx_hash")
    timestamp: str
    value: int
    asset_policy: Optional[str] = None
    asset_name: Optional[str] = None
    asset_quantity: Optional[int] = None
    fee: Optional[float] = None
    valid_contract: Optional[bool] = None
    script_size: Optional[int] = None


class StakeAddressNode(BaseNode):
    label: str


class BlockNode(BaseNode):
    hash: str
    block_no: int
    epoch_no: int
    slot_no: int
    time: str
    tx_count: int
    size: int


class UTXONode(BaseNode):
    id: str
    type: str = "UTXO"
    value: int
    asset_policy: Optional[str] = None
    asset_name: Optional[str] = None
    asset_quantity: Optional[int] = None


class EpochNode(BaseNode):
    no: int
    start_time: str
    fees: float
    out_sum: float
    end_time: str


# Union type for all possible node types
Node = Union[AddressNode, TransactionNode, StakeAddressNode, BlockNode, EpochNode]


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


# List Models
class PaginatedList(BaseModel):
    total_count: int


class Blocks(PaginatedList):
    blocks: List[Any]


class Epochs(PaginatedList):
    epochs: List[Any]
