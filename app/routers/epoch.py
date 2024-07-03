from fastapi import Depends, APIRouter
from neo4j import Driver

from app.db.graph.epoch import get_epoch_details
from app.models.graph import EpochDetails
from app.routers.dependencies import get_neo4j_driver

router = APIRouter()


@router.get("/epochs/{epoch_no}", response_model=EpochDetails)
def api_get_epoch_details(epoch_no: int, driver: Driver = Depends(get_neo4j_driver)) -> EpochDetails:
    return get_epoch_details(driver, epoch_no)
