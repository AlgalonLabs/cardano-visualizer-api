import logging
from datetime import datetime

from app.db.db_neo4j import insert_into_neo4j
from app.db.db_postgres import fetch_input_utxos, fetch_output_utxos
from app.utils.utxo_processor import process_utxos


def main():
    logging.basicConfig(level=logging.INFO)
    start = datetime(2021, 1, 1)
    end = datetime(2021, 1, 7)

    # Iterate over 52 weeks
    for i in range(0, 52):
        logging.info(f"Processing UTXOs for week {i}")
        inputs = fetch_input_utxos(start, end)
        outputs = fetch_output_utxos(start, end)

        # Process UTXOs
        processed_utxos = process_utxos(inputs, outputs)
        logging.info(f"Processed {len(processed_utxos)} UTXOs")

        # Insert into Neo4j
        insert_into_neo4j(processed_utxos)
        logging.info(f"Inserted {len(processed_utxos)} UTXOs into Neo4j")

        # Iterate 1 week at a time
        start, end = end, end + datetime.timedelta(weeks=1)

    logging.info("Finished processing UTXOs")


if __name__ == "__main__":
    main()
