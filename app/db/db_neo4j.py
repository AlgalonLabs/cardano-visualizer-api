import logging
from typing import List, Dict, Any

from app.db.connections import connect_neo4j
from app.models.transactions import ProcessedTransaction


def clear_neo4j_database():
    """
    Clear all data in Neo4j database
    """
    logging.info("Clearing all data in Neo4j...")
    driver = connect_neo4j()

    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")

    driver.close()


def insert_into_neo4j(transactions: List[ProcessedTransaction], batch_size: int = 1000):
    """
    Insert Addresses, StakeAddresses, and Transactions into Neo4j

    :param transactions: List of UTXOs to insert
    :param batch_size: Batch size for inserting data
    """
    logging.info("Inserting data into Neo4j...")
    driver = connect_neo4j()

    with driver.session() as session:
        # Create constraints to ensure uniqueness of addresses, transactions, and stake addresses
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address) REQUIRE a.address IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (t:Transaction) REQUIRE t.tx_hash IS UNIQUE")
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (s:StakeAddress) REQUIRE s.address IS UNIQUE")

        def process_batch(batch_to_process: List[Dict[str, Any]]):
            for transaction in batch_to_process:
                input_address = transaction.get("input_address")
                output_address = transaction.get("output_address")
                tx_hash = transaction["tx_hash"]
                output_value = transaction["output_value"]
                actual_sent = transaction["actual_sent"]
                timestamp = transaction["timestamp"]
                asset_policy = transaction.get("asset_policy")
                asset_name = transaction.get("asset_name")
                asset_quantity = transaction.get("asset_quantity")
                input_stake_address = transaction.get("input_stake_address")
                output_stake_address = transaction.get("output_stake_address")
                tx_hash_str = tx_hash.hex()

                logging.debug(f'Inserting transaction: {tx_hash_str}')
                session.run(
                    """
                    MERGE (t:Transaction {tx_hash: $tx_hash})
                    ON CREATE SET t.output_value = $output_value,
                                  t.actual_sent = $actual_sent,
                                  t.timestamp = $timestamp,
                                  t.asset_policy = $asset_policy,
                                  t.asset_name = $asset_name,
                                  t.asset_quantity = $asset_quantity
                    """,
                    {
                        "tx_hash": tx_hash_str,
                        "output_value": int(output_value),
                        "actual_sent": int(actual_sent),
                        "timestamp": timestamp,
                        "asset_policy": asset_policy,
                        "asset_name": asset_name,
                        "asset_quantity": int(asset_quantity) if asset_quantity else None
                    }
                )

                if input_address:
                    logging.debug(f'Inserting input address: {input_address}')
                    session.run(
                        "MERGE (a:Address {address: $address})",
                        {"address": input_address}
                    )
                    session.run(
                        """
                        MATCH (a:Address {address: $input_address})
                        MATCH (t:Transaction {tx_hash: $tx_hash})
                        MERGE (a)-[r:HAS_INPUT_TRANSACTION]->(t)
                        """,
                        {
                            "input_address": input_address,
                            "tx_hash": tx_hash_str
                        }
                    )

                if output_address:
                    logging.debug(f'Inserting output address: {output_address}')
                    session.run(
                        "MERGE (b:Address {address: $address})",
                        {"address": output_address}
                    )
                    session.run(
                        """
                        MATCH (b:Address {address: $output_address})
                        MATCH (t:Transaction {tx_hash: $tx_hash})
                        MERGE (t)-[r:HAS_OUTPUT_TRANSACTION]->(b)
                        """,
                        {
                            "output_address": output_address,
                            "tx_hash": tx_hash_str
                        }
                    )

                if input_stake_address:
                    logging.debug(f'Inserting input stake address: {input_stake_address}')
                    session.run(
                        "MERGE (s:StakeAddress {address: $address})",
                        {"address": input_stake_address}
                    )
                    session.run(
                        """
                        MATCH (a:Address {address: $input_address})
                        MATCH (s:StakeAddress {address: $stake_address})
                        MERGE (a)-[r:STAKE]->(s)
                        """,
                        {
                            "input_address": input_address,
                            "stake_address": input_stake_address
                        }
                    )

                if output_stake_address:
                    logging.debug(f'Inserting output stake address: {output_stake_address}')
                    session.run(
                        "MERGE (s:StakeAddress {address: $address})",
                        {"address": output_stake_address}
                    )
                    session.run(
                        """
                        MATCH (b:Address {address: $output_address})
                        MATCH (s:StakeAddress {address: $stake_address})
                        MERGE (b)-[r:STAKE]->(s)
                        """,
                        {
                            "output_address": output_address,
                            "stake_address": output_stake_address
                        }
                    )

        total_batches = (len(transactions) + batch_size - 1) // batch_size  # Calculate total number of batches

        for i in range(total_batches):
            start_index = i * batch_size
            end_index = min(start_index + batch_size, len(transactions))
            batch = transactions[start_index:end_index]

            logging.info(f"Processing batch {i + 1}/{total_batches}")
            process_batch(batch)
            logging.info(f"Processed batch of size {len(batch)}")

    driver.close()
