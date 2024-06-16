import logging
import datetime

from app.db.db_neo4j import insert_utxos_into_neo4j, clear_neo4j_database
from app.db.db_postgres import fetch_input_utxos, fetch_output_utxos
from app.utils.utxo_processor import process_utxos


def main():
    logging.basicConfig(level=logging.INFO)
    start = datetime.datetime(2021, 1, 1)
    end = datetime.datetime(2021, 1, 7)

    # Clear existing data
    clear_neo4j_database()

    # Iterate over 52 weeks
    for i in range(0, 52):
        try:
            logging.info(f"Week {i}: Processing UTXOs from {start} to {end}")

            logging.info(f"Week {i}: Fetching input UTXOs")
            inputs = fetch_input_utxos(start, end)
            logging.info(f"Week {i}: Fetched {len(inputs)} input UTXOs")

            logging.info(f"Week {i}: Fetching output UTXOs")
            outputs = fetch_output_utxos(start, end)
            logging.info(f"Week {i}: Fetched {len(outputs)} output UTXOs")

            logging.info(f"Week {i}: Processing UTXOs")
            processed_utxos = process_utxos(inputs, outputs)
            logging.info(f"Week {i}: Processed {len(processed_utxos)} UTXOs")

            logging.info(f"Week {i}: Inserting UTXOs into Neo4j")
            insert_utxos_into_neo4j(processed_utxos)
            logging.info(f"Week {i}: Inserted {len(processed_utxos)} UTXOs into Neo4j")

            # Iterate 1 week at a time
            start, end = end, end + datetime.timedelta(weeks=1)
        except Exception as e:
            logging.error(f"Week {i}: Error processing UTXOs from {start} to {end}: {e}", exc_info=True)

    logging.info("Finished processing all UTXOs")

if __name__ == "__main__":
    main()
