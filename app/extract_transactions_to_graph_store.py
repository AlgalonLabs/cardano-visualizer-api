import datetime
import logging

from app.db.db_neo4j import insert_utxos, clear_neo4j_database, insert_blocks
from app.db.db_postgres import fetch_input_utxos, fetch_output_utxos, fetch_blocks
from app.utils.utxo_processor import process_utxos


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(asctime)s - %(message)s")

    # Clear existing data
    clear_neo4j_database()

    start = datetime.datetime(2021, 1, 1)
    end = datetime.datetime(2021, 1, 2)
    for i in range(0, 20):
        try:
            start_string = start.isoformat()
            logging.info(f"Day {start_string}: Fetching blocks")
            blocks = fetch_blocks(start, end)
            logging.info(f"Day {start_string}: Fetched {len(blocks)} blocks")

            logging.info(f"Day {start_string}: Inserting blocks into Neo4j")
            insert_blocks(blocks)
            logging.info(f"Day {start_string}: Inserted {len(blocks)} blocks into Neo4j")
            start, end = end, end + datetime.timedelta(days=1)
        except Exception as e:
            logging.error(f"Day {i + 1}: Error processing blocks from {start} to {end}: {e}", exc_info=True)

    start = datetime.datetime(2021, 1, 1)
    end = datetime.datetime(2021, 1, 2)
    for i in range(0, 365):
        try:
            logging.info(f"Day {start_string}: Processing UTXOs from {start} to {end}")
            inputs = fetch_input_utxos(start, end)
            logging.info(f"Day {start_string}: Fetched {len(inputs)} input UTXOs")

            logging.info(f"Day {start_string}: Fetching output UTXOs")
            outputs = fetch_output_utxos(start, end)
            logging.info(f"Day {start_string}: Fetched {len(outputs)} output UTXOs")

            logging.info(f"Day {start_string}: Processing UTXOs")
            processed_utxos = process_utxos(inputs, outputs)
            logging.info(f"Day {start_string}: Processed {len(processed_utxos)} UTXOs")

            logging.info(f"Day {start_string}: Inserting UTXOs into Neo4j")
            insert_utxos(processed_utxos)
            logging.info(f"Day {start_string}: Inserted {len(processed_utxos)} UTXOs into Neo4j")

            # Iterate 1 day at a time
            start, end = end, end + datetime.timedelta(days=1)
        except Exception as e:
            logging.error(f"Day {i + 1}: Error processing UTXOs from {start} to {end}: {e}", exc_info=True)

    logging.info("Finished processing all UTXOs")


if __name__ == "__main__":
    main()
