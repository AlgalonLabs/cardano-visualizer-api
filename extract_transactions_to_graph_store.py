import logging
from typing import List, Dict, Any, Tuple

import psycopg2
from neo4j import GraphDatabase, exceptions


# Connect to PostgreSQL
def connect_postgres():
    return psycopg2.connect(
        dbname="cexplorer",
        user="postgres",
        password="v8hlDV0yMAHHlIurYupj",
        host="localhost"
    )


# Connect to Neo4j
def connect_neo4j():
    uri = "bolt://localhost:7687"
    return GraphDatabase.driver(uri, auth=("matt", "Topspin3)"))


def extract_utxos():
    logging.info('Extracting UTXO data from PostgreSQL...')
    conn = connect_postgres()
    cur = conn.cursor()

    query = """
    WITH const AS (
        SELECT to_timestamp('2023-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') AS effective_time_
    )
    SELECT
        generating_tx.hash AS tx_hash,
        generating_tx.id AS tx_id,
        tx_out.address AS output_address,
        tx_out.value AS output_value,
        generating_block.time AS timestamp,
        ma_tx_out.quantity AS asset_quantity,
        multi_asset.policy AS asset_policy,
        multi_asset.name AS asset_name,
        input_tx_out.address AS input_address,
        input_tx_out.value AS input_value
    FROM const
        CROSS JOIN tx_out
        INNER JOIN tx AS generating_tx ON generating_tx.id = tx_out.tx_id
        INNER JOIN block AS generating_block ON generating_block.id = generating_tx.block_id
        LEFT JOIN tx_in ON tx_in.tx_out_id = tx_out.tx_id AND tx_in.tx_out_index = tx_out.index
        LEFT JOIN tx_out AS input_tx_out ON input_tx_out.tx_id = tx_in.tx_in_id
        LEFT JOIN ma_tx_out ON ma_tx_out.tx_out_id = tx_out.id
        LEFT JOIN multi_asset ON multi_asset.id = ma_tx_out.ident
    WHERE (
        const.effective_time_ < '2023-12-16 21:44:00'
        OR generating_block.epoch_no IS NOT NULL
    )
    AND const.effective_time_ >= generating_block.time
    AND (
        const.effective_time_ <= CURRENT_TIMESTAMP
        OR tx_in.id IS NULL
    );
    """

    cur.execute(query)
    utxos = cur.fetchall()

    print("Number of UTXOs fetched: ", len(utxos))

    cur.close()
    conn.close()
    return utxos


def process_utxos(utxos: List[Tuple[str, str, str, int, int, str, str, str, int]]) -> List[Dict[str, Any]]:
    transactions = {}

    for utxo in utxos:
        tx_hash, tx_id, output_address, output_value, timestamp, asset_quantity, asset_policy, asset_name, input_address, input_value = utxo

        if tx_hash not in transactions:
            transactions[tx_hash] = {
                'inputs': [],
                'outputs': []
            }

        if input_address:
            transactions[tx_hash]['inputs'].append({
                'address': input_address,
                'value': input_value
            })

        transactions[tx_hash]['outputs'].append({
            'address': output_address,
            'value': output_value,
            'timestamp': timestamp,
            'asset_policy': asset_policy,
            'asset_name': asset_name,
            'asset_quantity': asset_quantity
        })

    processed_utxos = []

    for tx_hash, tx in transactions.items():
        inputs = tx['inputs']
        outputs = tx['outputs']

        for input_utxo in inputs:
            input_address = input_utxo['address']
            input_value = input_utxo['value']

            for output_utxo in outputs:
                output_address = output_utxo['address']
                output_value = output_utxo['value']
                timestamp = output_utxo['timestamp']
                asset_policy = output_utxo['asset_policy']
                asset_name = output_utxo['asset_name']
                asset_quantity = output_utxo['asset_quantity']

                # Calculate the actual sent amount
                actual_sent = input_value - sum([out['value'] for out in outputs if out['address'] == input_address])

                processed_utxos.append({
                    "tx_hash": tx_hash,
                    "input_address": input_address,
                    "output_address": output_address,
                    "output_value": output_value,
                    "actual_sent": actual_sent,
                    "timestamp": timestamp,
                    "asset_policy": asset_policy,
                    "asset_name": asset_name,
                    "asset_quantity": asset_quantity
                })

    return processed_utxos


def insert_into_neo4j(utxos: List[Dict[str, Any]]):
    logging.info("Inserting data into Neo4j...")
    driver = connect_neo4j()

    with driver.session() as session:
        # Clear existing data
        logging.info('Clearing existing data in Neo4j...')
        session.run("MATCH (n) DETACH DELETE n")

        # Create a constraint to ensure uniqueness of addresses
        session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Address) REQUIRE a.address IS UNIQUE")

        print("Number of UTXOs to process: ", len(utxos))
        for utxo in utxos:
            input_address = utxo["input_address"]
            output_address = utxo["output_address"]
            tx_hash = utxo["tx_hash"]
            output_value = utxo["output_value"]
            actual_sent = utxo["actual_sent"]
            timestamp = utxo["timestamp"]
            asset_policy = utxo["asset_policy"]
            asset_name = utxo["asset_name"]
            asset_quantity = utxo["asset_quantity"]

            if input_address:  # If input address exists, proceed
                logging.info(f'Inserting address: {input_address}')
                session.run(
                    "MERGE (a:Address {address: $address})",
                    {"address": input_address}
                )

            logging.info(f'Inserting address: {output_address}')
            session.run(
                "MERGE (b:Address {address: $address})",
                {"address": output_address}
            )

            # Handle multi-asset relationships
            if asset_policy and asset_name:
                relationship_query = f"""
                    MATCH (a:Address {{address: $input_address}})
                    MATCH (b:Address {{address: $output_address}})
                    MERGE (a)-[r:`{tx_hash}` {{
                        output_value: $output_value,
                        actual_sent: $actual_sent,
                        timestamp: $timestamp,
                        asset_policy: $asset_policy,
                        asset_name: $asset_name,
                        asset_quantity: $asset_quantity
                    }}]->(b)
                """
                params = {
                    "input_address": input_address,
                    "output_address": output_address,
                    "output_value": int(output_value),
                    "actual_sent": int(actual_sent),
                    "timestamp": timestamp,
                    "asset_policy": asset_policy,
                    "asset_name": asset_name,
                    "asset_quantity": int(asset_quantity)
                }
            else:
                relationship_query = f"""
                    MATCH (a:Address {{address: $input_address}})
                    MATCH (b:Address {{address: $output_address}})
                    MERGE (a)-[r:`{tx_hash}` {{
                        output_value: $output_value,
                        actual_sent: $actual_sent,
                        timestamp: $timestamp
                    }}]->(b)
                """
                params = {
                    "input_address": input_address,
                    "output_address": output_address,
                    "output_value": int(output_value),
                    "actual_sent": int(actual_sent),
                    "timestamp": timestamp
                }

            logging.info(
                f'Inserting relationship: {input_address} -> {output_address} with tx_hash: {tx_hash}, output_value: {output_value}, actual_sent: {actual_sent}, timestamp: {timestamp}')
            session.run(relationship_query, params)


def main():
    logging.basicConfig(level=logging.INFO)
    utxos = extract_utxos()
    processed_utxos = process_utxos(utxos)
    insert_into_neo4j(processed_utxos)


# Insert data into Neo4j
# def insert_into_neo4j(utxos):
#     logging.info("Inserting data into Neo4j...")
#     driver = connect_neo4j()
#
#     with driver.session() as session:
#         # Clear existing data
#         logging.info('Clearing existing data in Neo4j...')
#         session.run("MATCH (n) DETACH DELETE n")
#
#         # Create a constraint to ensure uniqueness of addresses
#         for utxo in utxos:
#             address = utxo[0]
#             tx_hash = utxo[3].tobytes().hex()  # Convert memory view to hex string
#
#             logging.info(f'Inserting address: {address}')
#             session.run(
#                 "MERGE (a:Address {address: $address})",
#                 {"address": address}
#             )
#
#             # Collect all addresses related to the same transaction hash
#             tx_hash_addresses = [(u[0], u[1], u[2]) for u in utxos if u[3].tobytes().hex() == tx_hash]
#             for i in range(len(tx_hash_addresses)):
#                 for j in range(i + 1, len(tx_hash_addresses)):
#                     address1, lovelace1, timestamp1 = tx_hash_addresses[i]
#                     address2, lovelace2, timestamp2 = tx_hash_addresses[j]
#                     logging.info(
#                         f'Inserting relationship: {address1} -> {address2} with tx_hash: {tx_hash}, ADA: {lovelace1}, timestamp: {timestamp1}')
#                     session.run(
#                         f"""
#                         MATCH (a:Address {{address: $address1}})
#                         MATCH (b:Address {{address: $address2}})
#                         MERGE (a)-[r:`{tx_hash}` {{amount: $value, timestamp: $timestamp}}]->(b)
#                         """,
#                         {"address1": address1, "address2": address2, "value": int(lovelace1), "timestamp": timestamp1}
#                     )
#
#     driver.close()


if __name__ == "__main__":
    main()
