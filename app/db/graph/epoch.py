import logging

from neo4j import Driver

from app.db.graph.db_neo4j import serialize_node
from app.models.graph import Epochs, EpochDetails
from app.utils.currency_converter import CurrencyConverter


def get_epoch_details(driver: Driver, epoch_no: int) -> EpochDetails:
    query = """
    MATCH (e:Epoch {no: $epoch_no})
    OPTIONAL MATCH (e)-[:HAS_BLOCK]->(b:Block)
    RETURN e, count(b) AS block_count, sum(b.tx_count) AS tx_count, sum(b.size) AS total_size
    """
    with driver.session() as session:
        result = session.run(query, {"epoch_no": epoch_no})
        record = result.single()
        if record:
            return {
                "epoch": serialize_node(record["e"]),
                "block_count": record["block_count"],
                "tx_count": record["tx_count"],
                "total_size": record["total_size"]
            }
        return {"epoch": {}, "block_count": 0, "tx_count": 0, "total_size": 0}


def get_epochs(driver: Driver, skip: int, limit: int) -> Epochs:
    query_data = """
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
    ORDER BY e.no DESC
    SKIP $skip
    LIMIT $limit
    """

    query_count = "MATCH (e:Epoch) RETURN COUNT(e) AS total_count"

    with driver.session() as session:
        total_count_result = session.run(query_count)
        total_count = total_count_result.single()["total_count"]

        result = session.run(query_data, {"skip": skip, "limit": limit})
        epochs = [record["epoch"] for record in result]

    return {"epochs": epochs, "total_count": total_count}


def insert_epochs(driver: Driver, epochs: Epochs):
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
