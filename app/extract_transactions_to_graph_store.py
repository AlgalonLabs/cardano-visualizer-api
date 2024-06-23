import datetime
import logging

from sqlalchemy.orm import sessionmaker

from app.db.connections import connect_postgres, connect_neo4j
from app.db.db_neo4j import clear_neo4j_database, insert_blocks, insert_epochs
from app.db.db_postgres import fetch_blocks, fetch_epochs


def main():
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] - %(asctime)s - %(message)s")

    Session = sessionmaker(bind=connect_postgres())
    driver = connect_neo4j()
    # Clear existing data
    # clear_neo4j_database()
    # start = datetime.datetime(2018, 9, 23)
    # end = datetime.datetime(2024, 5, 4)
    # Process epochs
    # with Session() as session:
    #     try:
    #         epochs = fetch_epochs(session, start.isoformat(), end.isoformat())
    #         insert_epochs(driver, epochs)
    #     except Exception as e:
    #         logging.error(f"Error processing epochs from {start} to {end}: {e}", exc_info=True)
    #
    start = datetime.datetime(2018, 4, 30)
    end = start + datetime.timedelta(days=1)

    # Process blocks
    for i in range(0, 2200):
        with Session() as session:
            try:
                blocks = fetch_blocks(session, start.isoformat(), end.isoformat())
                insert_blocks(driver, blocks)
            except Exception as e:
                logging.error(f"Day {i + 1}: Error processing blocks from {start} to {end}: {e}", exc_info=True)

        start = end
        end = start + datetime.timedelta(days=1)

    # start = datetime.datetime(2021, 1, 1)
    # end = datetime.datetime(2021, 1, 2)
    # for i in range(0, 365):
    #     try:
    #         logging.info(f"Day {start_string}: Processing UTXOs from {start} to {end}")
    #         inputs = fetch_input_utxos(start, end)
    #         logging.info(f"Day {start_string}: Fetched {len(inputs)} input UTXOs")
    #
    #         logging.info(f"Day {start_string}: Fetching output UTXOs")
    #         outputs = fetch_output_utxos(start, end)
    #         logging.info(f"Day {start_string}: Fetched {len(outputs)} output UTXOs")
    #
    #         logging.info(f"Day {start_string}: Processing UTXOs")
    #         processed_utxos = process_utxos(inputs, outputs)
    #         logging.info(f"Day {start_string}: Processed {len(processed_utxos)} UTXOs")
    #
    #         logging.info(f"Day {start_string}: Inserting UTXOs into Neo4j")
    #         insert_utxos(processed_utxos)
    #         logging.info(f"Day {start_string}: Inserted {len(processed_utxos)} UTXOs into Neo4j")
    #
    #         # Iterate 1 day at a time
    #         start, end = end, end + datetime.timedelta(days=1)
    #     except Exception as e:
    #         logging.error(f"Day {i + 1}: Error processing UTXOs from {start} to {end}: {e}", exc_info=True)
    # logging.info("Finished processing all UTXOs")


if __name__ == "__main__":
    main()
