from neo4j import Driver

from app.db.graph.db_neo4j import serialize_node
from app.models.details import TransactionDetails


def get_transaction_details(driver: Driver, transaction_hash: str) -> TransactionDetails:
    query = """
    MATCH (t:Transaction {tx_hash: $transaction_hash})
    MATCH (input:UTXO)-[:INPUT]->(t)
    MATCH (t)-[:OUTPUT]->(output:UTXO)
    MATCH (input)<-[:OWNS]-(inputAddress:Address)
    MATCH (output)<-[:OWNS]-(outputAddress:Address)
    MATCH (t)-[:CONTAINED_BY]->(b:Block)
    OPTIONAL MATCH (inputAddress)-[:STAKE]->(inputStake:StakeAddress)
    OPTIONAL MATCH (outputAddress)-[:STAKE]->(outputStake:StakeAddress)
    RETURN t, 
           collect(DISTINCT {utxo: input, address: inputAddress, stake: inputStake}) AS inputs,
           collect(DISTINCT {utxo: output, address: outputAddress, stake: outputStake}) AS outputs,
           b
    """
    with driver.session() as session:
        result = session.run(query, {"transaction_hash": transaction_hash})
        record = result.single()
        if record:
            transaction = serialize_node(record["t"])
            inputs = [serialize_node(utxo_input) for utxo_input in record["inputs"]]
            outputs = [serialize_node(output) for output in record["outputs"]]
            block = serialize_node(record["b"]) if record["b"] else None

            return {
                "hash": transaction["tx_hash"],
                "created_at": transaction["timestamp"],
                "total_output": sum(output["utxo"]["value"] for output in outputs),
                "fee": transaction["fee"],
                "block_no": block.get("block_no") if block else None,
                "slot_no": block.get("slot_no") if block else None,
                "absolute_slot_no": block.get("absolute_slot") if block else None,
                "inputs": [{
                    "address": utxo_input["address"]["address"],
                    "stake_address": utxo_input["stake"]["address"] if utxo_input["stake"] else None,
                    "amount": utxo_input["utxo"]["value"],
                    "utxo_hash": utxo_input["utxo"]["utxo_hash"],
                    "utxo_index": utxo_input["utxo"]["index"]
                } for utxo_input in inputs],
                "outputs": [{
                    "address": output["address"]["address"],
                    "stake_address": output["stake"]["address"] if output["stake"] else None,
                    "amount": output["utxo"]["value"]
                } for output in outputs]
            }
        return None
