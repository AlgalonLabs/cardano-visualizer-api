from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Depends
from neo4j import Driver

from app.db.db_neo4j import get_graph_by_asset, get_graph_by_address, get_address_details, get_transaction_details, \
    get_asset_details, get_block_details, get_epoch_details, get_blocks, get_epochs
from app.models.graph import GraphData, AddressDetails, TransactionDetails, AssetDetails, BlockDetails, EpochDetails
from app.routers.dependencies import get_neo4j_driver

router = APIRouter()


@router.get("/graph/asset/{asset_id}", response_model=GraphData)
def api_get_graph_by_asset(asset_id: str, start_time: Optional[str] = None, end_time: Optional[str] = None,
                           driver: Driver = Depends(get_neo4j_driver)) -> GraphData:
    return get_graph_by_asset(driver, asset_id, start_time, end_time)


@router.get("/graph/address/{address}", response_model=GraphData)
def api_get_graph_by_address(address: str, start_time: Optional[str] = None, end_time: Optional[str] = None,
                             driver: Driver = Depends(get_neo4j_driver)) -> GraphData:
    return get_graph_by_address(driver, address, start_time, end_time)


@router.get("/address/{address_hash}", response_model=AddressDetails)
def api_get_address_details(address_hash: str, driver: Driver = Depends(get_neo4j_driver)) -> AddressDetails:
    return get_address_details(driver, address_hash)


@router.get("/transaction/{transaction_hash}", response_model=TransactionDetails)
def api_get_transaction_details(transaction_hash: str,
                                driver: Driver = Depends(get_neo4j_driver)) -> TransactionDetails:
    return get_transaction_details(driver, transaction_hash)


@router.get("/asset/{asset_id}", response_model=AssetDetails)
def api_get_asset_details(asset_id: str, driver: Driver = Depends(get_neo4j_driver)) -> AssetDetails:
    return get_asset_details(driver, asset_id)


@router.get("/block/{block_hash}", response_model=BlockDetails)
def api_get_block_details(block_hash: str, driver: Driver = Depends(get_neo4j_driver)) -> BlockDetails:
    return get_block_details(driver, block_hash)


@router.get("/epoch/{epoch_no}", response_model=EpochDetails)
def api_get_epoch_details(epoch_no: int, driver: Driver = Depends(get_neo4j_driver)) -> EpochDetails:
    return get_epoch_details(driver, epoch_no)


@router.get("/blocks", response_model=List[Dict[str, Any]])
def api_get_blocks(skip: int = 0, limit: int = 100, driver: Driver = Depends(get_neo4j_driver)) -> List[Dict[str, Any]]:
    return get_blocks(driver, skip, limit)


@router.get("/epochs", response_model=List[Dict[str, Any]])
def api_get_epochs(skip: int = 0, limit: int = 100, driver: Driver = Depends(get_neo4j_driver)) -> List[Dict[str, Any]]:
    return get_epochs(driver, skip, limit)
