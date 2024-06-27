import binascii
from typing import Optional, List

from neo4j import Driver
from pydantic import ValidationError

from app.db.graph.db_neo4j import serialize_node, serialize_value
from app.models.graph import GraphData, BaseEdge, AddressNode, TransactionNode, StakeAddressNode, \
    AddressDetails, BlockNode, BaseNode


def get_graph_by_address(driver: Driver, address: str, start_time: Optional[str] = None,
                         end_time: Optional[str] = None) -> GraphData:
    nodes: List[BaseNode] = []
    edges: List[BaseEdge] = []

    query = """
    MATCH (a:Address {address: $address})-[:OWNS]->(u:UTXO)-[:INPUT]->(t:Transaction)
    OPTIONAL MATCH (t)-[:OUTPUT]->(u2:UTXO)-[:OWNS]->(b:Address)
    OPTIONAL MATCH (t)-[:CONTAINED_BY]->(block:Block)
    WHERE ($start_time IS NULL OR t.timestamp >= datetime($start_time))
      AND ($end_time IS NULL OR t.timestamp <= datetime($end_time))
    RETURN a.address AS from_address, b.address AS to_address, t.tx_hash AS tx_hash, 
           t.timestamp AS timestamp, t.fee AS fee,
           u.value AS input_value, u2.value AS output_value, 
           u.asset_policy AS asset_policy, u.asset_name AS asset_name, u.asset_quantity AS asset_quantity,
           block.hash AS block_hash, block.block_no AS block_no, block.epoch_no AS epoch_no,
           block.slot_no AS slot_no, block.time AS block_time, block.tx_count AS tx_count, block.size AS block_size
    UNION
    MATCH (b:Address)-[:OWNS]->(u:UTXO)<-[:OUTPUT]-(t:Transaction)<-[:INPUT]-(u2:UTXO)-[:OWNS]->(a:Address {address: $address})
    OPTIONAL MATCH (t)-[:CONTAINED_BY]->(block:Block)
    WHERE ($start_time IS NULL OR t.timestamp >= datetime($start_time))
      AND ($end_time IS NULL OR t.timestamp <= datetime($end_time))
    RETURN b.address AS from_address, a.address AS to_address, t.tx_hash AS tx_hash, 
           t.timestamp AS timestamp, t.fee AS fee,
           u2.value AS input_value, u.value AS output_value, 
           u.asset_policy AS asset_policy, u.asset_name AS asset_name, u.asset_quantity AS asset_quantity,
           block.hash AS block_hash, block.block_no AS block_no, block.epoch_no AS epoch_no,
           block.slot_no AS slot_no, block.time AS block_time, block.tx_count AS tx_count, block.size AS block_size
    """

    params = {'address': address, 'start_time': start_time, 'end_time': end_time}

    with driver.session() as session:
        result = session.run(query, params)
        for record in result:
            from_address = record["from_address"]
            to_address = record["to_address"]

            if isinstance(record["tx_hash"], bytes):
                tx_hash = binascii.hexlify(record["tx_hash"]).decode('ascii')
            else:
                tx_hash = record["tx_hash"]

            if from_address and not any(node.id == from_address for node in nodes):
                nodes.append(AddressNode(id=from_address, type="Address", label=from_address))

            if not any(node.id == tx_hash for node in nodes):
                nodes.append(TransactionNode(
                    id=tx_hash,
                    type="Transaction",
                    tx_hash=tx_hash,
                    timestamp=record["timestamp"].isoformat() if record["timestamp"] else None,
                    value=int(record["output_value"] or 0),  # Use 0 if None
                    fee=float(record["fee"] or 0),  # Use 0 if None
                    asset_policy=record["asset_policy"],
                    asset_name=record["asset_name"],
                    asset_quantity=int(record["asset_quantity"] or 0)  # Use 0 if None
                ))

            if to_address and not any(node.id == to_address for node in nodes):
                nodes.append(AddressNode(id=to_address, type="Address", label=to_address))

            if record["block_hash"] and not any(node.id == record["block_hash"] for node in nodes):
                nodes.append(BlockNode(
                    id=record["block_hash"],
                    type="Block",
                    hash=record["block_hash"],
                    block_no=record["block_no"],
                    epoch_no=record["epoch_no"],
                    slot_no=record["slot_no"],
                    time=record["block_time"].isoformat() if record["block_time"] else None,
                    tx_count=record["tx_count"],
                    size=record["block_size"]
                ))

            if from_address and tx_hash:
                edges.append(BaseEdge(from_address=from_address, to_address=tx_hash, type="INPUT"))
            if tx_hash and to_address:
                edges.append(BaseEdge(from_address=tx_hash, to_address=to_address, type="OUTPUT"))
            if tx_hash and record["block_hash"]:
                edges.append(BaseEdge(from_address=tx_hash, to_address=record["block_hash"], type="CONTAINED_BY"))

    stake_query = """
    MATCH (a:Address {address: $address})-[:STAKE]->(s:StakeAddress)
    RETURN a.address AS address, s.address AS stake_address
    """

    with driver.session() as session:
        result = session.run(stake_query, params)
        for record in result:
            if not any(node.id == record["stake_address"] for node in nodes):
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
