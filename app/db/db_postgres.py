import logging
from typing import List, Dict, Any

from sqlalchemy.orm import Session
from sqlalchemy.sql.operators import and_

from app.db.connections import connect_postgres
from app.db.models.base import Block, Epoch


def fetch_blocks(session: Session, start_time: str, end_time: str) -> List[Block]:
    """
    Fetch blocks from Postgres for a specified time range.
    :param session: SQLAlchemy session object.
    :param start_time: Start time of the range in ISO format.
    :param end_time: End time of the range in ISO format.
    :return: List of Block ORM objects.
    """
    logging.info(f"Fetching blocks for the given time range: {start_time} - {end_time}")

    blocks = session.query(Block).filter(
        and_(Block.time >= start_time, Block.time <= end_time)
    ).all()

    return blocks


def fetch_epochs(session: Session, start_time: str, end_time: str) -> List[Dict[str, Any]]:
    """
    Fetch epochs from Postgres for a specified time range.
    :param session: SQLAlchemy session object.
    :param start_time: Start time of the range in ISO format.
    :param end_time: End time of the range in ISO format.
    :return: List of epoch ORM objects.
    """
    logging.info(f"Fetching epochs for the given time range: {start_time} - {end_time}")
    epochs = session.query(Epoch).filter(
        and_(Epoch.time >= start_time, Epoch.time <= end_time)
    ).all()

    return epochs


def fetch_input_utxos(start: str, end: str) -> List[Dict[str, Any]]:
    logging.info(f"Fetching input UTXOs for the given time range: {start} - {end}")
    query = f"""
    SELECT tx_in.tx_in_id                     AS tx_id,
           encode(consuming_tx.hash, 'hex')   AS consuming_tx_hash, -- Transaction hash consuming the input UTXO
           encode(creating_tx.hash, 'hex')    AS creating_tx_hash,  -- Transaction hash creating the input UTXO
           encode(creating_block.hash, 'hex') AS block_hash,
           creating_tx.block_index            AS block_index,
           tx_out.id                          AS tx_out_id,
           tx_out.index                       AS tx_out_index,
           tx_out.address                     AS input_address,
           tx_out.value                       AS input_value,
           creating_block.time                AS creating_timestamp,
           consuming_block.time               AS consuming_timestamp,
           ma_tx_out.quantity                 AS asset_quantity,
           multi_asset.policy                 AS asset_policy,
           multi_asset.name                   AS asset_name,
           tx_out.stake_address_id            AS stake_address_id,
           stake_address.view                 AS stake_address
    FROM tx_in
             INNER JOIN tx_out ON tx_in.tx_out_id = tx_out.tx_id AND tx_in.tx_out_index = tx_out.index
             INNER JOIN tx AS consuming_tx ON consuming_tx.id = tx_in.tx_in_id -- Transaction consuming the UTXO
             INNER JOIN tx AS creating_tx ON creating_tx.id = tx_out.tx_id -- Transaction creating the UTXO
             INNER JOIN block as consuming_block ON consuming_block.id = consuming_tx.block_id
             INNER JOIN block AS creating_block ON creating_block.id = creating_tx.block_id
             LEFT JOIN ma_tx_out ON ma_tx_out.tx_out_id = tx_out.id
             LEFT JOIN multi_asset ON multi_asset.id = ma_tx_out.ident
             LEFT JOIN stake_address ON stake_address.id = tx_out.stake_address_id
    WHERE consuming_block.time >= %s
      AND consuming_block.time <= %s
    """
    data = (start, end)
    return execute_query(query, data)


def fetch_output_utxos(start, end) -> List[Dict[str, Any]]:
    query = f"""
    SELECT creating_tx.id                     AS tx_id,
           encode(creating_tx.hash, 'hex')    AS creating_tx_hash,  -- Transaction hash creating the output UTXO
           encode(consuming_tx.hash, 'hex')   AS consuming_tx_hash, -- Transaction hash consuming the output UTXO
           encode(creating_block.hash, 'hex') AS block_hash,
           creating_tx.block_index            AS block_index,
           creating_tx.fee                    AS fee,
           tx_out.index                       AS tx_out_index,
           tx_out.address                     AS output_address,
           tx_out.value                       AS output_value,
           creating_block.time                AS creating_timestamp,
           consuming_block.time               AS consuming_timestamp,
           ma_tx_out.quantity                 AS asset_quantity,
           multi_asset.policy                 AS asset_policy,
           multi_asset.name                   AS asset_name,
           tx_out.stake_address_id            AS stake_address_id,
           stake_address.view                 AS stake_address
    FROM tx_out
             LEFT JOIN tx_in
                       ON tx_in.tx_out_id = tx_out.tx_id AND tx_in.tx_out_index = tx_out.index -- Transaction consuming the UTXO
             INNER JOIN tx AS creating_tx ON creating_tx.id = tx_out.tx_id -- Transaction creating the UTXO
             LEFT JOIN tx AS consuming_tx ON consuming_tx.id = tx_in.tx_in_id
             INNER JOIN block AS creating_block ON creating_block.id = creating_tx.block_id
             INNER JOIN block as consuming_block ON consuming_block.id = consuming_tx.block_id
             LEFT JOIN ma_tx_out ON ma_tx_out.tx_out_id = tx_out.id
             LEFT JOIN multi_asset ON multi_asset.id = ma_tx_out.ident
             LEFT JOIN stake_address ON stake_address.id = tx_out.stake_address_id
    WHERE creating_block.time >= %s
      AND creating_block.time <= %s
    """
    data = (start, end)
    return execute_query(query, data)


def execute_query(query: str, data) -> List[Dict[str, Any]]:
    conn = connect_postgres()
    cur = conn.cursor()
    cur.execute(query, data)
    rows = cur.fetchall()
    logging.info('Number of rows fetched: %s', len(rows))
    cur.close()
    conn.close()

    utxos = [dict(zip([desc[0] for desc in cur.description], row)) for row in rows]
    return utxos
