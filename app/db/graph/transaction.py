from neo4j import Driver

from app.models.details import TransactionDetails


def get_transaction_details(driver: Driver, transaction_hash: str) -> TransactionDetails:
    query = """
    MATCH (t:Transaction {tx_hash: $transaction_hash})
    OPTIONAL MATCH (input:UTXO)-[:INPUT]->(t)
    OPTIONAL MATCH (t)-[:OUTPUT]->(output:UTXO)
    OPTIONAL MATCH (input)<-[:OWNS]-(inputAddress:Address)
    OPTIONAL MATCH (output)<-[:OWNS]-(outputAddress:Address)
    OPTIONAL MATCH (inputAddress)-[:STAKE]->(inputStake:StakeAddress)
    OPTIONAL MATCH (outputAddress)-[:STAKE]->(outputStake:StakeAddress)
    OPTIONAL MATCH (t)-[:CONTAINED_BY]->(b:Block)
    RETURN t, 
           collect(DISTINCT {utxo: input, address: inputAddress, stake: inputStake}) AS inputs,
           collect(DISTINCT {utxo: output, address: outputAddress, stake: outputStake}) AS outputs,
           b
    """
    with driver.session() as session:
        result = session.run(query, {"transaction_hash": transaction_hash})
        record = result.single()
        if record:
            transaction = record["t"]
            utxo_inputs = record["inputs"]
            utxo_outputs = record["outputs"]
            block = record["b"]

            return TransactionDetails(
                hash=transaction["tx_hash"],
                created_at=transaction["timestamp"],
                total_output=sum(output["utxo"]["value"] for output in utxo_outputs),
                fee=transaction["fee"],
                block_number=block["block_no"] if block else None,
                slot=block["slot_no"] if block else None,
                absolute_slot=block["absolute_slot"] if block else None,
                inputs=[{
                    "address": utxo_input["address"]["address"],
                    "stake_address": utxo_input["stake"]["address"] if utxo_input["stake"] else None,
                    "amount": utxo_input["utxo"]["value"],
                    "utxo_hash": utxo_input["utxo"]["utxo_hash"],
                    "utxo_index": utxo_input["utxo"]["index"]
                } for utxo_input in utxo_inputs],
                outputs=[{
                    "address": utxo_output["address"]["address"],
                    "stake_address": utxo_output["stake"]["address"] if utxo_output["stake"] else None,
                    "amount": utxo_output["utxo"]["value"]
                } for utxo_output in utxo_outputs]
            )
        return None
