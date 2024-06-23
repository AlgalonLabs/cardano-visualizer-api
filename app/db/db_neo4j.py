import logging
from datetime import datetime
from typing import List, Dict, Any
from typing import Optional

from neo4j import Driver

from app.db.connections import connect_neo4j
from app.db.models.base import Epoch, Block
from app.models.graph import Edge, GraphData, Node, AddressNode, TransactionNode, StakeAddressNode
from app.models.transactions import Transaction
from app.utils.currency_converter import CurrencyConverter


def clear_neo4j_database():
    logging.info("Performing a clean-up of the graph database")
    driver = connect_neo4j()
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    driver.close()


def insert_epochs(driver: Driver, epochs: List[Epoch]):
    """
    Insert epochs into graph.
    :param driver:
    :param epochs:
    :return:
    """
    with driver.session() as session:
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (e:Epoch) REQUIRE e.no IS UNIQUE")

        logging.info(f"Inserting {len(epochs)} epochs into graph")
        epoch_data = [
            {
                "no": epoch.no,
                "out_sum": CurrencyConverter.lovelace_to_ada(epoch.out_sum),
                "fees": CurrencyConverter.lovelace_to_ada(epoch.fees),
                "start_time": epoch.start_time.isoformat(),
                "end_time": epoch.end_time.isoformat()
            }
            for epoch in epochs
        ]

        result = session.run(
            """
            UNWIND $epoch_data AS data
            MERGE (e:Epoch {no: data.no})
            ON CREATE SET e.out_sum = data.out_sum,
                          e.fees = data.fees,
                          e.start_time = datetime(data.start_time),
                          e.end_time = datetime(data.end_time)
            """,
            {"epoch_data": epoch_data}
        )
        summary = result.consume()
        logging.info(
            f"Inserted {summary.counters.nodes_created} nodes, {summary.counters.nodes_deleted} nodes deleted.")

        # Create relationships between consecutive epochs
        result = session.run(
            """
            MATCH (e1:Epoch), (e2:Epoch)
            WHERE e1.no = e2.no - 1
            MERGE (e1)-[:HAS_SUCCESSOR]->(e2)
            """
        )

        summary = result.consume()
        logging.info(f"Created {summary.counters.relationships_created} HAS_SUCCESSOR relationships.")


def insert_blocks(driver: Driver, blocks: List[Block]):
    """
    Insert blocks into graph.
    :param driver:
    :param blocks: List of blocks with their properties.
    """
    with driver.session() as session:
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (b:Block) REQUIRE b.hash IS UNIQUE;")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (e:Epoch) REQUIRE e.no IS UNIQUE;")

        blocks_data = [
            {
                "hash": block.hash.hex(),
                "block_id": block.id,
                "epoch_no": block.epoch_no,
                "slot_no": block.slot_no,
                "epoch_slot_no": block.epoch_slot_no,
                "block_no": block.block_no,
                "previous_id": block.previous_id,
                "slot_leader_id": block.slot_leader_id,
                "size": block.size,
                "time": block.time.isoformat(),
                "tx_count": block.tx_count,
                "proto_major": block.proto_major,
                "proto_minor": block.proto_minor,
                "vrf_key": block.vrf_key,
                "op_cert": block.op_cert,
                "op_cert_counter": block.op_cert_counter
            }
            for block in blocks
        ]

        logging.info(f"Inserting {len(blocks)} blocks into graph")

        result = session.run(
            """
            UNWIND $blocks_data AS block
            MERGE (b:Block {hash: block.hash})
            ON CREATE SET b.id = block.block_id,
                          b.epoch_no = block.epoch_no,
                          b.slot_no = block.slot_no,
                          b.epoch_slot_no = block.epoch_slot_no,
                          b.block_no = block.block_no,
                          b.previous_id = block.previous_id,
                          b.slot_leader_id = block.slot_leader_id,
                          b.size = block.size,
                          b.time = datetime(block.time),
                          b.tx_count = block.tx_count,
                          b.proto_major = block.proto_major,
                          b.proto_minor = block.proto_minor,
                          b.vrf_key = block.vrf_key,
                          b.op_cert = block.op_cert,
                          b.op_cert_counter = block.op_cert_counter
            ON MATCH SET b.id = block.block_id,
                         b.epoch_no = block.epoch_no,
                         b.slot_no = block.slot_no,
                         b.epoch_slot_no = block.epoch_slot_no,
                         b.block_no = block.block_no,
                         b.previous_id = block.previous_id,
                         b.slot_leader_id = block.slot_leader_id,
                         b.size = block.size,
                         b.time = datetime(block.time),
                         b.tx_count = block.tx_count,
                         b.proto_major = block.proto_major,
                         b.proto_minor = block.proto_minor,
                         b.vrf_key = block.vrf_key,
                         b.op_cert = block.op_cert,
                         b.op_cert_counter = block.op_cert_counter

            WITH block
            MATCH (b:Block {hash: block.hash})
            MATCH (b2:Block) WHERE b2.id = block.previous_id
            MERGE (b)-[:HAS_PREVIOUS_BLOCK]->(b2)

            WITH block
            MATCH (e:Epoch {no: block.epoch_no})
            MATCH (b:Block {hash: block.hash})
            MERGE (e)-[:HAS_BLOCK]->(b)
            """,
            {"blocks_data": blocks_data}
        )
        summary = result.consume()
        logging.info(f"Inserted {summary.counters.nodes_created} block nodes, {summary.counters.nodes_deleted} block "
                     f"nodes deleted. Created {summary.counters.relationships_created} relationships.")


def insert_utxos(driver: Driver, transactions: Dict[str, Transaction], batch_size: int = 1000):
    with driver.session() as session:
        # Create constraints to ensure uniqueness of addresses, transactions, stake addresses, and UTXOs
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address) REQUIRE a.address IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (t:Transaction) REQUIRE t.tx_hash IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (s:StakeAddress) REQUIRE s.address IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (u:UTXO) REQUIRE (u.utxo_hash, u.index) IS UNIQUE")

        def process_batch(batch_to_process: Dict[str, Transaction]):
            for tx_hash, tx in batch_to_process.items():

                if tx.outputs:
                    timestamp = tx.outputs[0].creating_timestamp  # Assuming the timestamp is consistent across outputs
                elif tx.inputs:
                    timestamp = tx.inputs[0].consuming_timestamp  # Assuming the timestamp is consistent across inputs
                else:
                    logging.warning(f"Transaction {tx_hash} has no inputs or outputs")
                    continue

                logging.debug(f'Inserting transaction: {tx_hash}')
                session.run(
                    """
                    MERGE (t:Transaction {tx_hash: $tx_hash})
                    ON CREATE SET t.timestamp = datetime($timestamp),
                                  t.fee = $fee
                    """,
                    {
                        "tx_hash": tx_hash,
                        "timestamp": timestamp,
                        "fee": int(tx.fee) / 1000000,
                        "block_index": tx.block_index
                    }
                )

                session.run(
                    """
                    MATCH (t:Transaction {tx_hash: $tx_hash})
                    MATCH (b:Block {hash: $block_hash})
                    MERGE (b)-[:CONTAINS]->(t)
                    MERGE (t)-[:CONTAINED_BY]->(b)
                    """,
                    {
                        "tx_hash": tx_hash,
                        "block_hash": tx.block_hash
                    }
                )

                for input_utxo in tx.inputs:
                    utxo_hash = input_utxo.creating_tx_hash
                    session.run(
                        """
                        MERGE (u:UTXO {utxo_hash: $utxo_hash, index: $index})
                        ON CREATE SET u.value = $value,
                                      u.asset_policy = $asset_policy,
                                      u.asset_name = $asset_name,
                                      u.asset_quantity = $asset_quantity,
                                      u.timestamp = datetime($timestamp)
                        """,
                        {
                            "utxo_hash": utxo_hash,
                            "index": input_utxo.tx_out_index,
                            "value": int(input_utxo.input_value) / 1000000,
                            "asset_policy": input_utxo.asset_policy,
                            "asset_name": input_utxo.asset_name,
                            "asset_quantity": input_utxo.asset_quantity,
                            "timestamp": input_utxo.creating_timestamp
                        }
                    )
                    session.run(
                        "MERGE (a:Address {address: $address})",
                        {"address": input_utxo.input_address}
                    )
                    session.run(
                        """
                        MATCH (a:Address {address: $input_address})
                        MATCH (u:UTXO {utxo_hash: $utxo_hash, index: $index})
                        MERGE (a)-[:OWNS]->(u)
                        """,
                        {
                            "input_address": input_utxo.input_address,
                            "utxo_hash": utxo_hash,
                            "index": input_utxo.tx_out_index
                        }
                    )
                    session.run(
                        """
                        MATCH (u:UTXO {utxo_hash: $utxo_hash, index: $index})
                        MATCH (t:Transaction {tx_hash: $tx_hash})
                        MERGE (u)-[:INPUT]->(t)
                        """,
                        {
                            "utxo_hash": utxo_hash,
                            "index": input_utxo.tx_out_index,
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
                            MATCH (a:Address {address: $address})
                            MATCH (s:StakeAddress {address: $stake_address})
                            MERGE (a)-[:STAKE]->(s)
                            MERGE (s)-[:STAKE_OF]->(a)
                            """,
                            {
                                "address": input_utxo.input_address,
                                "stake_address": input_utxo.stake_address
                            }
                        )

                for output_utxo in tx.outputs:
                    utxo_hash = output_utxo.creating_tx_hash
                    session.run(
                        """
                        MERGE (u:UTXO {utxo_hash: $utxo_hash, index: $index})
                        ON CREATE SET u.value = $value,
                                      u.asset_policy = $asset_policy,
                                      u.asset_name = $asset_name,
                                      u.asset_quantity = $asset_quantity,
                                      u.timestamp = datetime($timestamp)
                        """,
                        {
                            "utxo_hash": utxo_hash,
                            "index": output_utxo.tx_out_index,
                            "value": int(output_utxo.output_value) / 1000000,
                            "asset_policy": output_utxo.asset_policy,
                            "asset_name": output_utxo.asset_name,
                            "asset_quantity": output_utxo.asset_quantity,
                            "timestamp": output_utxo.consuming_timestamp
                        }
                    )
                    session.run(
                        "MERGE (b:Address {address: $address})",
                        {"address": output_utxo.output_address}
                    )
                    session.run(
                        """
                        MATCH (t:Transaction {tx_hash: $tx_hash})
                        MATCH (u:UTXO {utxo_hash: $utxo_hash, index: $index})
                        MERGE (t)-[:OUTPUT]->(u)
                        """,
                        {
                            "tx_hash": tx_hash,
                            "utxo_hash": utxo_hash,
                            "index": output_utxo.tx_out_index
                        }
                    )
                    session.run(
                        """
                        MATCH (u:UTXO {utxo_hash: $utxo_hash, index: $index})
                        MATCH (b:Address {address: $address})
                        MERGE (b)-[:OWNS]->(u)
                        """,
                        {
                            "utxo_hash": utxo_hash,
                            "index": output_utxo.tx_out_index,
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
                            MATCH (a:Address {address: $address})
                            MATCH (s:StakeAddress {address: $stake_address})
                            MERGE (a)-[:STAKE]->(s)
                            MERGE (s)-[:STAKE_OF]->(a)
                            """,
                            {
                                "address": output_utxo.output_address,
                                "stake_address": output_utxo.stake_address
                            }
                        )

        total_batches = (len(transactions) + batch_size - 1) // batch_size

        for i in range(total_batches):
            start_index = i * batch_size
            end_index = min(start_index + batch_size, len(transactions))
            batch = {k: transactions[k] for k in list(transactions)[start_index:end_index]}

            logging.info(f"Processing batch {i + 1}/{total_batches}")
            process_batch(batch)
            logging.info(f"Processed batch of size {len(batch)}")


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


def get_address_details(driver: Driver, address_hash: str) -> Dict[str, Any]:
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


def get_transaction_details(driver: Driver, transaction_hash: str) -> Dict[str, Any]:
    query = """
    MATCH (t:Transaction {tx_hash: $transaction_hash})-[r]->(n)
    RETURN t, collect(r), collect(n)
    """
    with driver.session() as session:
        result = session.run(query, {"transaction_hash": transaction_hash})
        record = result.single()
        if record:
            return {
                "transaction": record["t"],
                "relationships": record["collect(r)"],
                "nodes": record["collect(n)"]
            }
        return {}


def get_asset_details(driver: Driver, asset_id: str) -> Dict[str, Any]:
    query = """
    MATCH (a:Asset {asset_id: $asset_id})-[:USED_IN]->(t:Transaction)
    RETURN a, collect(t) AS transactions
    """
    with driver.session() as session:
        result = session.run(query, {"asset_id": asset_id})
        record = result.single()
        if record:
            return {
                "asset": record["a"],
                "transactions": record["transactions"]
            }
        return {}


def get_block_details(driver: Driver, block_hash: str) -> Dict[str, Any]:
    query = """
    MATCH (b:Block {hash: $block_hash})-[:CONTAINS]->(t:Transaction)
    MATCH (b)-[:HAS_BLOCK]->(e:Epoch)
    RETURN b, collect(t) AS transactions, e
    """
    with driver.session() as session:
        result = session.run(query, {"block_hash": block_hash})
        record = result.single()
        if record:
            return {
                "block": record["b"],
                "transactions": record["transactions"],
                "epoch": record["e"]
            }
        return {}


def get_epoch_details(driver: Driver, epoch_no: int) -> Dict[str, Any]:
    query = """
    MATCH (e:Epoch {no: $epoch_no})-[:HAS_BLOCK]->(b:Block)
    RETURN e, count(b) AS block_count, sum(b.tx_count) AS tx_count, sum(b.size) AS total_size
    """
    with driver.session() as session:
        result = session.run(query, {"epoch_no": epoch_no})
        record = result.single()
        if record:
            return {
                "epoch": record["e"],
                "block_count": record["block_count"],
                "tx_count": record["tx_count"],
                "total_size": record["total_size"]
            }
        return {}


def get_blocks(driver: Driver, skip: int, limit: int) -> List[Dict[str, Any]]:
    query = """
    MATCH (b:Block)
    RETURN {
        hash: b.hash,
        block_id: b.id,
        epoch_no: b.epoch_no,
        slot_no: b.slot_no,
        epoch_slot_no: b.epoch_slot_no,
        block_no: b.block_no,
        previous_id: b.previous_id,
        slot_leader_id: b.slot_leader_id,
        size: b.size,
        time: toString(b.time),
        tx_count: b.tx_count,
        proto_major: b.proto_major,
        proto_minor: b.proto_minor,
        vrf_key: b.vrf_key,
        op_cert: b.op_cert,
        op_cert_counter: b.op_cert_counter
    } AS block
    SKIP $skip
    LIMIT $limit
    """
    with driver.session() as session:
        result = session.run(query, {"skip": skip, "limit": limit})
        blocks = [record["block"] for record in result]
        return blocks


def get_epochs(driver: Driver, skip: int, limit: int) -> List[Dict[str, Any]]:
    query = """
    MATCH (e:Epoch)
    OPTIONAL MATCH (e)-[r:HAS_BLOCK]->(b:Block)
    WITH e, COUNT(r) AS block_count
    RETURN {
        no: e.no,
        out_sum: e.out_sum,
        fees: e.fees,
        start_time: toString(e.start_time),
        end_time: toString(e.end_time),
        block_count: block_count
    } AS epoch
    SKIP $skip
    LIMIT $limit
    """
    with driver.session() as session:
        result = session.run(query, {"skip": skip, "limit": limit})
        epochs = [record["epoch"] for record in result]
        return epochs