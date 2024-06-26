from typing import Optional, List

from neo4j import Driver

from app.db.graph.db_neo4j import parse_timestamp
from app.models.graph import GraphData, BaseNode, BaseEdge, AddressNode, TransactionNode, StakeAddressNode, \
    AddressDetails


def get_graph_by_address(driver: Driver, address: str, start_time: Optional[str] = None,
                         end_time: Optional[str] = None) -> GraphData:
    nodes: List[BaseNode] = []
    edges: List[BaseEdge] = []

    query = """
    MATCH (a:Address {address: $address})-[r:INPUT_TRANSACTION]->(t:Transaction)
    OPTIONAL MATCH (t)-[s:OUTPUT_TRANSACTION]->(b:Address)
    RETURN a.address AS from, b.address AS to, t.tx_hash AS tx_hash, t.output_value AS value, t.timestamp AS timestamp,
           t.asset_policy AS asset_policy, t.asset_name AS asset_name, t.asset_quantity AS asset_quantity
    UNION
    MATCH (b:Address)-[r:OUTPUT_TRANSACTION]->(t:Transaction)-[s:INPUT_TRANSACTION]->(a:Address {address: $address})
    RETURN b.address AS from, a.address AS to, t.tx_hash AS tx_hash, t.output_value AS value, t.timestamp AS timestamp,
           t.asset_policy AS asset_policy, t.asset_name AS asset_name, t.asset_quantity AS asset_quantity
    """
    if start_time or end_time:
        query = """
        MATCH (a:Address {address: $address})-[r:INPUT_TRANSACTION]->(t:Transaction)
        OPTIONAL MATCH (t)-[s:OUTPUT_TRANSACTION]->(b:Address)
        WHERE
        """
        if start_time:
            query += " t.timestamp >= datetime($start_time)"
        if start_time and end_time:
            query += " AND"
        if end_time:
            query += " t.timestamp <= datetime($end_time)"
        query += """
        RETURN a.address AS from, b.address AS to, t.tx_hash AS tx_hash, t.output_value AS value, t.timestamp AS timestamp,
               t.asset_policy AS asset_policy, t.asset_name AS asset_name, t.asset_quantity AS asset_quantity
        UNION
        MATCH (b:Address)-[r:OUTPUT_TRANSACTION]->(t:Transaction)-[s:INPUT_TRANSACTION]->(a:Address {address: $address})
        WHERE
        """
        if start_time:
            query += " t.timestamp >= datetime($start_time)"
        if start_time and end_time:
            query += " AND"
        if end_time:
            query += " t.timestamp <= datetime($end_time)"
        query += """
        RETURN b.address AS from, a.address AS to, t.tx_hash AS tx_hash, t.output_value AS value, t.timestamp AS timestamp,
               t.asset_policy AS asset_policy, t.asset_name AS asset_name, t.asset_quantity AS asset_quantity
        """

    params = {'address': address}
    if start_time:
        params['start_time'] = parse_timestamp(start_time)
    if end_time:
        params['end_time'] = parse_timestamp(end_time)

    with driver.session() as session:
        result = session.run(query, params)
        for record in result:
            from_address = record["from"]
            to_address = record["to"]
            tx_hash = record["tx_hash"]

            if not any(node["id"] == from_address for node in nodes):
                nodes.append(AddressNode(id=from_address, type="Address", label=from_address))

            if not any(node["id"] == tx_hash for node in nodes):
                nodes.append(TransactionNode(
                    id=tx_hash, type="Transaction", tx_hash=tx_hash,
                    timestamp=record["timestamp"].isoformat(), value=record["value"],
                    asset_policy=record["asset_policy"], asset_name=record["asset_name"],
                    asset_quantity=record["asset_quantity"]
                ))

            if not any(node["id"] == to_address for node in nodes):
                nodes.append(AddressNode(id=to_address, type="Address", label=to_address))

            edges.append(BaseEdge(from_address=from_address, to_address=tx_hash, type="INPUT_TRANSACTION"))
            edges.append(BaseEdge(from_address=tx_hash, to_address=to_address, type="OUTPUT_TRANSACTION"))

    stake_query = """
    MATCH (a:Address {address: $address})-[:STAKE]->(s:StakeAddress)
    RETURN a.address AS address, s.address AS stake_address
    """

    with driver.session() as session:
        result = session.run(stake_query, params)
        for record in result:
            if not any(node["id"] == record["stake_address"] for node in nodes):
                nodes.append(
                    StakeAddressNode(id=record["stake_address"], type="StakeAddress", label=record["stake_address"]))

            edges.append(BaseEdge(from_address=record["address"], to_address=record["stake_address"], type="STAKE"))

    return GraphData(nodes=nodes, edges=edges)


def get_address_details(driver: Driver, address_hash: str) -> AddressDetails:
    query = """
    MATCH (a:Address {address: $address_hash})-[:OWNS]->(u:UTXO)
    OPTIONAL MATCH (u)-[:INPUT]->(t:Transaction)
    RETURN a.address AS address, collect(distinct u) AS utxos, collect(distinct t) AS transactions
    """
    with driver.session() as session:
        result = session.run(query, {"address_hash": address_hash})
        record = result.single()
        if record:
            return {
                "address": record["address"],
                "utxos": record["utxos"],
                "transactions": record["transactions"]
            }
        return {}

