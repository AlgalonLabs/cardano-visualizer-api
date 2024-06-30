from typing import Dict

from fastapi import APIRouter, Depends
from neo4j import Driver

from app.routers.dependencies import get_neo4j_driver

router = APIRouter()


@router.get("/api/v1/stakes/{stake_address}")
async def get_stake_info(
        stake_address: str,
        driver: Driver = Depends(get_neo4j_driver)
) -> Dict:
    # For now, returning an empty object as requested
    return {}
