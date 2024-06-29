from typing import Optional

from fastapi import APIRouter, Depends, Query
from neo4j import Driver

from app.db.graph.address import get_graph_by_address
from app.db.graph.asset import get_graph_by_asset
from app.db.graph.block import get_graph_by_block_hash, get_blocks
from app.db.graph.epoch import get_epochs
from app.models.graph import GraphData, Blocks, Epochs
from app.routers.dependencies import get_neo4j_driver

router = APIRouter()


@router.get("/graph/asset/{asset_id}", response_model=GraphData)
def api_get_graph_by_asset(asset_id: str, start_time: Optional[str] = None, end_time: Optional[str] = None,
                           driver: Driver = Depends(get_neo4j_driver)) -> GraphData:
    return get_graph_by_asset(driver, asset_id, start_time, end_time)


@router.get("/graph/addresses/{address}", response_model=GraphData)
def api_get_graph_by_address(address: str, start_time: Optional[str] = None, end_time: Optional[str] = None,
                             driver: Driver = Depends(get_neo4j_driver)) -> GraphData:
    return get_graph_by_address(driver, address, start_time, end_time)


@router.get("/graph/blocks/{block_hash}", response_model=GraphData)
def api_get_graph_by_block_hash(block_hash: str, driver: Driver = Depends(get_neo4j_driver)) -> GraphData:
    return get_graph_by_block_hash(driver, block_hash, 1)


@router.get("/blocks", response_model=Blocks)
def api_get_blocks(skip: int = Query(0, alias='skip'), limit: int = Query(10, alias='limit'),
                   driver: Driver = Depends(get_neo4j_driver)) -> Blocks:
    return get_blocks(driver, skip, limit)


@router.get("/epochs", response_model=Epochs)
def api_get_epochs(skip: int = Query(0, alias='skip'), limit: int = Query(10, alias='limit'),
                   driver: Driver = Depends(get_neo4j_driver)) -> Epochs:
    return get_epochs(driver, skip, limit)
