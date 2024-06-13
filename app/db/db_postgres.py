import logging
from typing import List, Dict, Any

from app.db.connections import connect_postgres


def fetch_input_utxos(start: str, end: str) -> List[Dict[str, Any]]:
    logging.info(f"Fetching input UTXOs for the given time range: {start} - {end}")
    query = f"""
    SELECT
        tx_in.tx_in_id AS tx_id,
        tx.hash AS tx_hash,
        tx_out.id AS tx_out_id,
        tx_out.index AS tx_out_index,
        tx_out.address AS input_address,
        tx_out.value AS input_value,
        generating_block.time AS timestamp,
        ma_tx_out.quantity AS asset_quantity,
        multi_asset.policy AS asset_policy,
        multi_asset.name AS asset_name,
        tx_out.stake_address_id AS stake_address_id,
        stake_address.view AS stake_address
    FROM
        tx_in
        INNER JOIN tx_out ON tx_in.tx_out_id = tx_out.tx_id AND tx_in.tx_out_index = tx_out.index
        INNER JOIN tx ON tx.id = tx_in.tx_in_id
        INNER JOIN block AS generating_block ON generating_block.id = tx.block_id
        LEFT JOIN ma_tx_out ON ma_tx_out.tx_out_id = tx_out.id
        LEFT JOIN multi_asset ON multi_asset.id = ma_tx_out.ident
        LEFT JOIN stake_address ON stake_address.id = tx_out.stake_address_id
    WHERE
        generating_block.time >= %s
        AND generating_block.time <= %s
    """
    data = (start, end)
    return execute_query(query, data)


def fetch_output_utxos(start, end) -> List[Dict[str, Any]]:
    query = f"""
    SELECT
        tx.id AS tx_id,
        tx.hash AS tx_hash,
        tx_out.index AS tx_out_index,
        tx_out.address AS output_address,
        tx_out.value AS output_value,
        generating_block.time AS timestamp,
        ma_tx_out.quantity AS asset_quantity,
        multi_asset.policy AS asset_policy,
        multi_asset.name AS asset_name,
        tx_out.stake_address_id AS stake_address_id,
        stake_address.view AS stake_address
    FROM
        tx
        INNER JOIN tx_out ON tx_out.tx_id = tx.id
        INNER JOIN block AS generating_block ON generating_block.id = tx.block_id
        LEFT JOIN ma_tx_out ON ma_tx_out.tx_out_id = tx_out.id
        LEFT JOIN multi_asset ON multi_asset.id = ma_tx_out.ident
        LEFT JOIN stake_address ON stake_address.id = tx_out.stake_address_id
    WHERE
        generating_block.time >= %s
        AND generating_block.time <= %s
    """
    data = (start, end)
    return execute_query(query, data)


def execute_query(query: str, data) -> List[Dict[str, Any]]:
    logging.info('Executing query on PostgreSQL...')
    conn = connect_postgres()
    cur = conn.cursor()
    cur.execute(query, data)
    rows = cur.fetchall()
    logging.info('Number of rows fetched: %s', len(rows))
    cur.close()
    conn.close()

    utxos = [dict(zip([desc[0] for desc in cur.description], row)) for row in rows]
    return utxos
