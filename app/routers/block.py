from fastapi import APIRouter, Depends
from neo4j import Driver

from app.db.graph.block import get_block_details
from app.models.graph import BlockDetails
from app.routers.dependencies import get_neo4j_driver

router = APIRouter()


@router.get("/blocks/{block_hash}", response_model=BlockDetails)
def api_get_block_details(block_hash: str, driver: Driver = Depends(get_neo4j_driver)) -> BlockDetails:
    return get_block_details(driver, block_hash)
