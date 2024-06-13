from typing import TypedDict, List, Dict, Any, Optional, Union


class AddressNode(TypedDict):
    id: str
    type: str
    label: str


class TransactionNode(TypedDict):
    id: str
    type: str
    tx_hash: str
    timestamp: str
    value: int
    asset_policy: Optional[str]
    asset_name: Optional[str]
    asset_quantity: Optional[int]


class StakeAddressNode(TypedDict):
    id: str
    type: str
    label: str


Node = Union[AddressNode, TransactionNode, StakeAddressNode]


class Edge(TypedDict):
    from_address: str
    to_address: str
    type: str


class GraphData(TypedDict):
    nodes: List[Dict[str, Any]]
    edges: List[Edge]
