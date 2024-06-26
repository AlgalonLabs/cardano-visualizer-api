import logging
from typing import Dict

from neo4j import Driver

from app.models.transactions import Transaction


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

