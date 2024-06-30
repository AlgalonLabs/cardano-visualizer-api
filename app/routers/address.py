from enum import Enum
from typing import Dict, List

from fastapi import APIRouter, Depends, Query
from neo4j import Driver

from app.db.graph.address import get_address_details
from app.models.details import AddressDetails
from app.routers.dependencies import get_neo4j_driver

router = APIRouter()


class TimePeriod(str, Enum):
    ONE_DAY = "ONE_DAY"
    ONE_MONTH = "ONE_MONTH"
    ONE_YEAR = "ONE_YEAR"


@router.get("/api/v1/addresses/analytics/{address}/{time_period}")
async def get_address_analytics(
        address: str,
        time_period: TimePeriod,
        driver: Driver = Depends(get_neo4j_driver)
) -> Dict[str, List[Dict[str, float]]]:
    query = """
    MATCH (a:Address {address: $address})-[:OWNS]->(u:UTXO)
    WHERE u.timestamp >= datetime() - duration($duration)
    WITH a, u.timestamp AS timestamp, sum(u.value) AS balance
    ORDER BY timestamp
    RETURN timestamp, balance
    """

    duration_map = {
        TimePeriod.ONE_DAY: "P1D",
        TimePeriod.ONE_MONTH: "P1M",
        TimePeriod.ONE_YEAR: "P1Y"
    }

    with driver.session() as session:
        result = session.run(query, {"address": address, "duration": duration_map[time_period]})
        data = [{"timestamp": record["timestamp"], "balance": record["balance"]} for record in result]

    return {"analytics": data}


@router.get("/api/v1/addresses/{address}/txs")
async def get_address_transactions(
        address: str,
        page: int = Query(0, ge=0),
        size: int = Query(50, ge=1, le=100),
        sort: str = Query("timestamp,desc"),
        driver: Driver = Depends(get_neo4j_driver)
) -> Dict[str, List[Dict]]:
    query = """
    MATCH (a:Address {address: $address})-[:OWNS]->(u:UTXO)-[:INPUT|OUTPUT]-(t:Transaction)
    WITH DISTINCT t
    ORDER BY t.timestamp DESC
    SKIP $skip
    LIMIT $limit
    RETURN t.tx_hash AS tx_hash, t.timestamp AS timestamp, t.fee AS fee,
           [(u:UTXO)-[:INPUT]->(t) | {address: u.address, value: u.value}] AS inputs,
           [(t)-[:OUTPUT]->(u:UTXO) | {address: u.address, value: u.value}] AS outputs
    """

    with driver.session() as session:
        result = session.run(query, {
            "address": address,
            "skip": page * size,
            "limit": size
        })
        transactions = [dict(record) for record in result]

    return {"transactions": transactions}


@router.get("/api/v1/addresses/{address}/tokens")
async def get_address_tokens(
        address: str,
        display_name: str = Query(None),
        page: int = Query(0, ge=0),
        size: int = Query(50, ge=1, le=100),
        driver: Driver = Depends(get_neo4j_driver)
) -> Dict[str, List[Dict]]:
    query = """
    MATCH (a:Address {address: $address})-[:OWNS]->(u:UTXO)
    WHERE NOT (u)-[:INPUT]->(:Transaction)
      AND u.asset_policy IS NOT NULL
      AND ($display_name IS NULL OR u.asset_name CONTAINS $display_name)
    WITH u.asset_policy AS policy, u.asset_name AS name, sum(u.asset_quantity) AS quantity
    ORDER BY quantity DESC
    SKIP $skip
    LIMIT $limit
    RETURN policy, name, quantity
    """

    with driver.session() as session:
        result = session.run(query, {
            "address": address,
            "display_name": display_name,
            "skip": page * size,
            "limit": size
        })
        tokens = [dict(record) for record in result]

    return {"tokens": tokens}


@router.get("/addresses/{address_hash}", response_model=AddressDetails)
def api_get_address_details(address_hash: str, driver: Driver = Depends(get_neo4j_driver)) -> AddressDetails:
    return get_address_details(driver, address_hash)
