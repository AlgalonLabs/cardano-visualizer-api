from typing import Optional, List

from neo4j import Driver
from pydantic import ValidationError

from app.db.graph.db_neo4j import serialize_node, serialize_value
from app.models.graph import BaseEdge, AddressNode, TransactionNode, AddressDetails, BaseNode, UTXONode, \
    StakeAddressNode, GraphData


def get_graph_by_address(driver: Driver, address: str, start_time: Optional[str] = None,
                         end_time: Optional[str] = None) -> GraphData:
    nodes: List[BaseNode] = []
    edges: List[BaseEdge] = []

    query = """
    MATCH (a:Address {address: $address})
    OPTIONAL MATCH (a)-[:OWNS]->(u:UTXO)-[:INPUT]->(t:Transaction)
    OPTIONAL MATCH (t)-[:OUTPUT]->(u2:UTXO)<-[:OWNS]-(b:Address)
    WHERE ($start_time IS NULL OR t.timestamp >= datetime($start_time))
      AND ($end_time IS NULL OR t.timestamp <= datetime($end_time))
    RETURN a.address AS address, u.utxo_hash AS input_utxo_hash, u.index AS input_utxo_index, u.value AS input_value,
           u.asset_policy AS input_asset_policy, u.asset_name AS input_asset_name, u.asset_quantity AS input_asset_quantity,
           t.tx_hash AS tx_hash, t.timestamp AS timestamp, t.fee AS fee,
           b.address AS other_address, u2.utxo_hash AS output_utxo_hash, u2.index AS output_utxo_index, u2.value AS output_value,
           u2.asset_policy AS output_asset_policy, u2.asset_name AS output_asset_name, u2.asset_quantity AS output_asset_quantity
    UNION
    MATCH (b:Address)-[:OWNS]->(u:UTXO)<-[:OUTPUT]-(t:Transaction)<-[:INPUT]-(u2:UTXO)<-[:OWNS]-(a:Address {address: $address})
    WHERE ($start_time IS NULL OR t.timestamp >= datetime($start_time))
      AND ($end_time IS NULL OR t.timestamp <= datetime($end_time))
    RETURN a.address AS address, u2.utxo_hash AS input_utxo_hash, u2.index AS input_utxo_index, u2.value AS input_value,
           u2.asset_policy AS input_asset_policy, u2.asset_name AS input_asset_name, u2.asset_quantity AS input_asset_quantity,
           t.tx_hash AS tx_hash, t.timestamp AS timestamp, t.fee AS fee,
           b.address AS other_address, u.utxo_hash AS output_utxo_hash, u.index AS output_utxo_index, u.value AS output_value,
           u.asset_policy AS output_asset_policy, u.asset_name AS output_asset_name, u.asset_quantity AS output_asset_quantity
    """

    params = {'address': address, 'start_time': start_time, 'end_time': end_time}

    with driver.session() as session:
        result = session.run(query, params)
        for record in result:
            address = serialize_value(record["address"])
            input_utxo_hash = f"{serialize_value(record["input_utxo_hash"])}_{record["input_utxo_index"]}"
            tx_hash = serialize_value(record["tx_hash"])
            other_address = serialize_value(record["other_address"])
            output_utxo_hash = f"{serialize_value(record["output_utxo_hash"])}_{record["output_utxo_index"]}"

            if not any(node.id == address for node in nodes):
                nodes.append(AddressNode(id=address, type="Address", label=address))
            if other_address and not any(node.id == other_address for node in nodes):
                nodes.append(AddressNode(id=other_address, type="Address", label=other_address))

            if input_utxo_hash and not any(node.id == input_utxo_hash for node in nodes):
                nodes.append(UTXONode(
                    id=input_utxo_hash,
                    type="UTXO",
                    value=int(record["input_value"] or 0),
                    asset_policy=serialize_value(record["input_asset_policy"]),
                    asset_name=serialize_value(record["input_asset_name"]),
                    asset_quantity=int(record["input_asset_quantity"] or 0)
                ))
                edges.append(BaseEdge(from_address=address, to_address=input_utxo_hash, type="OWNS"))

            if output_utxo_hash and not any(node.id == output_utxo_hash for node in nodes):
                nodes.append(UTXONode(
                    id=output_utxo_hash,
                    type="UTXO",
                    value=int(record["output_value"] or 0),
                    asset_policy=serialize_value(record["output_asset_policy"]),
                    asset_name=serialize_value(record["output_asset_name"]),
                    asset_quantity=int(record["output_asset_quantity"] or 0)
                ))
                if other_address:
                    edges.append(BaseEdge(from_address=other_address, to_address=output_utxo_hash, type="OWNS"))

            if not any(node.id == tx_hash for node in nodes):
                nodes.append(TransactionNode(
                    id=tx_hash,
                    type="Transaction",
                    tx_hash=tx_hash,
                    timestamp=record["timestamp"].isoformat() if record["timestamp"] else None,
                    fee=float(record["fee"] or 0),
                    value=int(record["output_value"] or 0)
                ))

            # Add edges
            if input_utxo_hash:
                edges.append(BaseEdge(from_address=input_utxo_hash, to_address=tx_hash, type="INPUT"))
            if output_utxo_hash:
                edges.append(BaseEdge(from_address=tx_hash, to_address=output_utxo_hash, type="OUTPUT"))

    stake_query = """
    MATCH (a:Address {address: $address})-[:STAKE]->(s:StakeAddress)
    RETURN s.address AS stake_address
    """

    with driver.session() as session:
        result = session.run(stake_query, params)
        for record in result:
            stake_address = serialize_value(record["stake_address"])
            if stake_address and not any(node.id == stake_address for node in nodes):
                nodes.append(StakeAddressNode(id=stake_address, type="StakeAddress", label=stake_address))
                edges.append(BaseEdge(from_address=address, to_address=stake_address, type="STAKE"))

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
            try:
                return AddressDetails(
                    address=serialize_value(record["address"]),
                    utxos=[serialize_node(utxo) for utxo in record["utxos"]],
                    transactions=[serialize_node(transaction) for transaction in record["transactions"]]
                )
            except ValidationError as e:
                print(f"Validation error: {e}")
                # You might want to log this error or handle it in some way
                return AddressDetails(address=address_hash, utxos=[], transactions=[])
        return AddressDetails(address=address_hash, utxos=[], transactions=[])
