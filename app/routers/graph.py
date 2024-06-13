from typing import Optional

from fastapi import APIRouter, Depends
from neo4j import Driver

from app.db.db_neo4j import get_graph_by_asset, get_graph_by_address
from app.models.graph import GraphData
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
