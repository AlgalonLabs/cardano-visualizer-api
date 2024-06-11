import datetime
from typing import Optional
from fastapi import APIRouter
from app.neo4j_connection import neo4j_driver

router = APIRouter()

@router.get("/graph/asset/{asset_id}")
def get_graph_by_asset(asset_id: str, start_time: Optional[str] = None, end_time: Optional[str] = None):
    query = """
    MATCH (a:Address)-[r]->(b:Address)
    WHERE r.asset_id = $asset_id
    """
    if start_time or end_time:
        query += " AND"
    if start_time:
        query += " r.timestamp >= $start_time"
    if start_time and end_time:
        query += " AND"
    if end_time:
        query += " r.timestamp <= $end_time"
    query += " RETURN a.address, b.address, r.tx_hash, r.value, r.timestamp"

    with neo4j_driver.session() as session:
        result = session.run(query, asset_id=asset_id, start_time=start_time, end_time=end_time)
        transactions = [{"from": record["a.address"], "to": record["b.address"], "tx_hash": record["r.tx_hash"],
                         "value": record["r.value"], "timestamp": record["r.timestamp"]} for record in result]

    return {"asset_id": asset_id, "transactions": transactions}


def parse_timestamp(ts: str) -> str:
    # Assuming the input is in the format 'YYYY-MM-DD'
    return datetime.datetime.strptime(ts, '%Y-%m-%d').isoformat()

@router.get("/graph/address/{address}")
def get_graph_by_address(address: str, start_time: Optional[str] = None, end_time: Optional[str] = None):
    query = """
    MATCH (a:Address {address: $address})-[r]->(b:Address)
    RETURN a.address AS from, b.address AS to, type(r) AS tx_hash, r.value AS value, r.timestamp AS timestamp
    UNION
    MATCH (b:Address)-[r]->(a:Address {address: $address})
    RETURN b.address AS from, a.address AS to, type(r) AS tx_hash, r.value AS value, r.timestamp AS timestamp
    """

    params = {
        'address': address,
        'start_time': start_time,
        'end_time': end_time
    }

    with neo4j_driver.session() as session:
        result = session.run(query, parameters=params)
        transactions = [
            {
                "from": record["from"],
                "to": record["to"],
                "tx_hash": record["tx_hash"],
                "value": record["value"] / 1000000,
                "timestamp": record["timestamp"].strftime('%Y-%m-%dT%H:%M:%S')
            }
            for record in result
        ]

    return {"address": address, "transactions": transactions}