import logging
from typing import List, Dict, Any

from neo4j import Driver

from app.db.graph.db_neo4j import serialize_node
from app.db.models.base import Block
from app.models.graph import GraphData, BaseNode, BaseEdge, BlockNode, TransactionNode, EpochNode, Blocks


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


def get_graph_by_block_hash(driver: Driver, block_hash: str, depth: int = 1) -> GraphData:
    nodes: List[BaseNode] = []
    edges: List[BaseEdge] = []

    query = """
    MATCH (b:Block {hash: $block_hash})
    OPTIONAL MATCH (b)-[:CONTAINS]->(t:Transaction)
    OPTIONAL MATCH (e:Epoch)-[:HAS_BLOCK]->(b)
    OPTIONAL MATCH path = (b)-[:HAS_PREVIOUS_BLOCK*1..{depth}]->(prev:Block)
    WITH b, collect(t) AS transactions, e, collect(nodes(path)) AS prev_blocks
    RETURN b, transactions, e, prev_blocks
    """

    with driver.session() as session:
        result = session.run(query, {"block_hash": block_hash, "depth": depth})
        record = result.single()

        if record:
            main_block = record["b"]
            transactions = record["transactions"]
            epoch = record["e"]
            prev_blocks = [block for path in record["prev_blocks"] for block in path]

            # Add main block node
            nodes.append(BlockNode(id=main_block["hash"], type="Block", **serialize_node(main_block)))

            # Add transaction nodes
            for tx in transactions:
                nodes.append(TransactionNode(id=tx["hash"], type="Transaction", **serialize_node(tx)))
                edges.append(BaseEdge(from_address=main_block["hash"], to_address=tx["hash"], type="CONTAINS"))

            # Add epoch node
            if epoch:
                nodes.append(EpochNode(id=f"epoch_{epoch['no']}", type="Epoch", **serialize_node(epoch)))
                edges.append(
                    BaseEdge(from_address=f"epoch_{epoch['no']}", to_address=main_block["hash"], type="HAS_BLOCK"))

            # Add previous block nodes and edges
            for prev_block in prev_blocks:
                if not any(node.id == prev_block["hash"] for node in nodes):
                    nodes.append(BlockNode(id=prev_block["hash"], type="Block", **serialize_node(prev_block)))
                if prev_block["hash"] != main_block["hash"]:
                    edges.append(
                        BaseEdge(from_address=main_block["hash"], to_address=prev_block["hash"],
                                 type="HAS_PREVIOUS_BLOCK"))
                    main_block = prev_block  # Update main_block for the next iteration

    return GraphData(nodes=nodes, edges=edges)


def get_block_details(driver: Driver, block_hash: str) -> Dict[str, Any]:
    query = """
    MATCH (b:Block {hash: $block_hash})
    OPTIONAL MATCH (b)-[:CONTAINS]->(t:Transaction)
    OPTIONAL MATCH (e:Epoch)-[:HAS_BLOCK]->(b)
    RETURN b, collect(t) AS transactions, e
    """
    with driver.session() as session:
        result = session.run(query, {"block_hash": block_hash})
        record = result.single()
        if record:
            return {
                "block": serialize_node(record.get("b")),
                "transactions": [serialize_node(tx) for tx in record.get("transactions", [])],
                "epoch": serialize_node(record.get("e"))
            }
        return {"block": {}, "transactions": [], "epoch": {}}


def get_blocks(driver: Driver, skip: int, limit: int) -> Blocks:
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
    ORDER BY b.block_no DESC
    SKIP $skip
    LIMIT $limit
    """

    query_count = "MATCH (e:Block) RETURN COUNT(e) AS total_count"

    with driver.session() as session:
        total_count_result = session.run(query_count)
        total_count = total_count_result.single()["total_count"]

        result = session.run(query, {"skip": skip, "limit": limit})
        blocks = [record["block"] for record in result]

    return {"blocks": blocks, "total_count": total_count}
