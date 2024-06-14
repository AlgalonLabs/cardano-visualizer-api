import logging
from datetime import datetime
from typing import List, Dict
from typing import Optional

from neo4j import Driver

from app.db.connections import connect_neo4j
from app.models.graph import Edge, GraphData, Node, AddressNode, TransactionNode, StakeAddressNode
from app.models.transactions import Transaction


def clear_neo4j_database():
    logging.info("Clearing all data in Neo4j...")
    driver = connect_neo4j()
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    driver.close()


def insert_utxos_into_neo4j(transactions: Dict[str, Transaction], batch_size: int = 5000):
    logging.info("Inserting data into Neo4j...")
    driver = connect_neo4j()

    with driver.session() as session:
        # Create constraints to ensure uniqueness of addresses, transactions, stake addresses, and UTXOs
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address) REQUIRE a.address IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (t:Transaction) REQUIRE t.tx_hash IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (s:StakeAddress) REQUIRE s.address IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (u:UTXO) REQUIRE u.utxo_id IS UNIQUE")

        def process_batch(batch_to_process: Dict[str, Transaction]):
            for tx_hash, tx in batch_to_process.items():

                timestamp = tx.outputs[0].timestamp  # Assuming the timestamp is consistent across outputs

                logging.debug(f'Inserting transaction: {tx_hash}')
                session.run(
                    """
                    MERGE (t:Transaction {tx_hash: $tx_hash})
                    ON CREATE SET t.timestamp = datetime($timestamp)
                    """,
                    {
                        "tx_hash": tx_hash,
                        "timestamp": timestamp.isoformat()
                    }
                )

                for input_utxo in tx.inputs:
                    utxo_id = f"{input_utxo.tx_hash_hex()}_{input_utxo.input_address}"
                    session.run(
                        """
                        MERGE (u:UTXO {utxo_id: $utxo_id})
                        ON CREATE SET u.value = $value,
                                      u.asset_policy = $asset_policy,
                                      u.asset_name = $asset_name,
                                      u.asset_quantity = $asset_quantity
                        """,
                        {
                            "utxo_id": utxo_id,
                            "value": int(input_utxo.input_value) / 1000000,
                            "asset_policy": input_utxo.asset_policy,
                            "asset_name": input_utxo.asset_name,
                            "asset_quantity": input_utxo.asset_quantity
                        }
                    )
                    session.run(
                        "MERGE (a:Address {address: $address})",
                        {"address": input_utxo.input_address}
                    )
                    session.run(
                        """
                        MATCH (a:Address {address: $input_address})
                        MATCH (u:UTXO {utxo_id: $utxo_id})
                        MERGE (a)-[:OWNS]->(u)
                        """,
                        {
                            "input_address": input_utxo.input_address,
                            "utxo_id": utxo_id
                        }
                    )
                    session.run(
                        """
                        MATCH (u:UTXO {utxo_id: $utxo_id})
                        MATCH (t:Transaction {tx_hash: $tx_hash})
                        MERGE (u)-[:INPUT]->(t)
                        """,
                        {
                            "utxo_id": utxo_id,
                            "tx_hash": tx_hash
                        }
                    )

                    if input_utxo.stake_address:
                        session.run(
                            "MERGE (s:StakeAddress {address: $address})",
                            {"address": input_utxo.stake_address}
                        )
                        session.run(
                            """
                            MATCH (a:Address {address: $input_address})
                            MATCH (s:StakeAddress {address: $stake_address})
                            MERGE (a)-[r:STAKE]->(s)
                            """,
                            {
                                "input_address": input_utxo.input_address,
                                "stake_address": input_utxo.stake_address
                            }
                        )

                for output_utxo in tx.outputs:
                    utxo_id = f"{output_utxo.tx_hash_hex()}_{output_utxo.output_address}"
                    session.run(
                        """
                        MERGE (u:UTXO {utxo_id: $utxo_id})
                        ON CREATE SET u.value = $value,
                                      u.asset_policy = $asset_policy,
                                      u.asset_name = $asset_name,
                                      u.asset_quantity = $asset_quantity
                        """,
                        {
                            "utxo_id": utxo_id,
                            "value": int(output_utxo.output_value) / 1000000,
                            "asset_policy": output_utxo.asset_policy,
                            "asset_name": output_utxo.asset_name,
                            "asset_quantity": output_utxo.asset_quantity
                        }
                    )
                    session.run(
                        "MERGE (b:Address {address: $address})",
                        {"address": output_utxo.output_address}
                    )
                    session.run(
                        """
                        MATCH (t:Transaction {tx_hash: $tx_hash})
                        MATCH (u:UTXO {utxo_id: $utxo_id})
                        MERGE (t)-[:OUTPUT]->(u)
                        """,
                        {
                            "tx_hash": tx_hash,
                            "utxo_id": utxo_id
                        }
                    )
                    session.run(
                        """
                        MATCH (u:UTXO {utxo_id: $utxo_id})
                        MATCH (b:Address {address: $address})
                        MERGE (b)-[:OWNS]->(u)
                        """,
                        {
                            "utxo_id": utxo_id,
                            "address": output_utxo.output_address
                        }
                    )

                    if output_utxo.stake_address:
                        session.run(
                            "MERGE (s:StakeAddress {address: $address})",
                            {"address": output_utxo.stake_address}
                        )
                        session.run(
                            """
                            MATCH (b:Address {address: $output_address})
                            MATCH (s:StakeAddress {address: $stake_address})
                            MERGE (b)-[r:STAKE]->(s)
                            """,
                            {
                                "output_address": output_utxo.output_address,
                                "stake_address": output_utxo.stake_address
                            }
                        )

        total_batches = (len(transactions) + batch_size - 1) // batch_size  # Calculate total number of batches

        for i in range(total_batches):
            start_index = i * batch_size
            end_index = min(start_index + batch_size, len(transactions))
            batch = {k: transactions[k] for k in list(transactions)[start_index:end_index]}

            logging.info(f"Processing batch {i + 1}/{total_batches}")
            process_batch(batch)
            logging.info(f"Processed batch of size {len(batch)}")

    driver.close()


def parse_timestamp(ts: str) -> str:
    return datetime.datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S').isoformat()


def get_graph_by_asset(driver: Driver, asset_id: str, start_time: Optional[str] = None,
                       end_time: Optional[str] = None) -> GraphData:
    nodes: List[Node] = []
    edges: List[Edge] = []

    query = """
    MATCH (a:Address)-[r:INPUT_TRANSACTION]->(t:Transaction {asset_id: $asset_id})-[s:OUTPUT_TRANSACTION]->(b:Address)
    """
    if start_time:
        query += " WHERE t.timestamp >= datetime($start_time)"
    if end_time:
        query += " AND t.timestamp <= datetime($end_time)"
    query += """
    RETURN a.address AS from, b.address AS to, t.tx_hash AS tx_hash, t.output_value AS value, t.timestamp AS timestamp,
           t.asset_policy AS asset_policy, t.asset_name AS asset_name, t.asset_quantity AS asset_quantity
    """

    params = {'asset_id': asset_id}
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

            edges.append(Edge(from_address=from_address, to_address=tx_hash, type="INPUT_TRANSACTION"))
            edges.append(Edge(from_address=tx_hash, to_address=to_address, type="OUTPUT_TRANSACTION"))

    stake_query = """
    MATCH (a:Address)-[:STAKE]->(s:StakeAddress)
    RETURN a.address AS address, s.address AS stake_address
    """

    with driver.session() as session:
        result = session.run(stake_query)
        for record in result:
            if not any(node["id"] == record["stake_address"] for node in nodes):
                nodes.append(
                    StakeAddressNode(id=record["stake_address"], type="StakeAddress", label=record["stake_address"]))

            edges.append(Edge(from_address=record["address"], to_address=record["stake_address"], type="STAKE"))

    return GraphData(nodes=nodes, edges=edges)


def get_graph_by_address(driver: Driver, address: str, start_time: Optional[str] = None,
                         end_time: Optional[str] = None) -> GraphData:
    nodes: List[Node] = []
    edges: List[Edge] = []

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

            edges.append(Edge(from_address=from_address, to_address=tx_hash, type="INPUT_TRANSACTION"))
            edges.append(Edge(from_address=tx_hash, to_address=to_address, type="OUTPUT_TRANSACTION"))

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

            edges.append(Edge(from_address=record["address"], to_address=record["stake_address"], type="STAKE"))

    return GraphData(nodes=nodes, edges=edges)
