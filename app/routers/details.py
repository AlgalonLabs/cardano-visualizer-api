from fastapi import APIRouter, Depends
from neo4j import Driver

from app.db.graph.asset import get_asset_details
from app.db.graph.epoch import get_epoch_details
from app.models.graph import AssetDetails, EpochDetails
from app.routers.dependencies import get_neo4j_driver

router = APIRouter()


@router.get("/asset/{asset_id}", response_model=AssetDetails)
def api_get_asset_details(asset_id: str, driver: Driver = Depends(get_neo4j_driver)) -> AssetDetails:
    return get_asset_details(driver, asset_id)
