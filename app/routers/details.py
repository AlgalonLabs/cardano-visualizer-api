from fastapi import APIRouter, Depends
from neo4j import Driver

from app.db.graph.address import get_address_details
from app.db.graph.asset import get_asset_details
from app.db.graph.block import get_block_details
from app.db.graph.epoch import get_epoch_details
from app.db.graph.transaction import get_transaction_details
from app.models.details import AddressDetails
from app.models.graph import TransactionDetails, AssetDetails, BlockDetails, EpochDetails
from app.routers.dependencies import get_neo4j_driver

router = APIRouter()


@router.get("/addresses/{address_hash}", response_model=AddressDetails)
def api_get_address_details(address_hash: str, driver: Driver = Depends(get_neo4j_driver)) -> AddressDetails:
    return get_address_details(driver, address_hash)


@router.get("/transaction/{transaction_hash}", response_model=TransactionDetails)
def api_get_transaction_details(transaction_hash: str,
                                driver: Driver = Depends(get_neo4j_driver)) -> TransactionDetails:
    return get_transaction_details(driver, transaction_hash)


@router.get("/asset/{asset_id}", response_model=AssetDetails)
def api_get_asset_details(asset_id: str, driver: Driver = Depends(get_neo4j_driver)) -> AssetDetails:
    return get_asset_details(driver, asset_id)


@router.get("/blocks/{block_hash}", response_model=BlockDetails)
def api_get_block_details(block_hash: str, driver: Driver = Depends(get_neo4j_driver)) -> BlockDetails:
    return get_block_details(driver, block_hash)


@router.get("/epochs/{epoch_no}", response_model=EpochDetails)
def api_get_epoch_details(epoch_no: int, driver: Driver = Depends(get_neo4j_driver)) -> EpochDetails:
    return get_epoch_details(driver, epoch_no)
