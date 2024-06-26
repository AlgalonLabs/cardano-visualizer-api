from neo4j import Driver

from app.models.graph import TransactionDetails


def get_transaction_details(driver: Driver, transaction_hash: str) -> TransactionDetails:
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
