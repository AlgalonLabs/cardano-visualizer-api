import logging
from typing import List

from sqlalchemy import select, func
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql.operators import and_

from app.db.models.base import Block, Epoch, TransactionIn, Transaction, TransactionOut, StakeAddress
from app.models.transactions import InputUTXO, OutputUTXO


def fetch_blocks(session: Session, start_time: str, end_time: str) -> List[Block]:
    """
    Fetch blocks from Postgres for a specified time range.
    :param session: SQLAlchemy session object.
    :param start_time: Start time of the range in ISO format.
    :param end_time: End time of the range in ISO format.
    :return: List of Block ORM objects.
    """
    logging.info(f"Fetching blocks between: {start_time} - {end_time}")

    blocks = session.query(Block).filter(
        and_(Block.time >= start_time, Block.time <= end_time)
    ).all()

    logging.info(f"Fetched: {len(blocks)} blocks between {start_time} - {end_time}")

    return blocks


def fetch_epochs(session: Session, start_time: str, end_time: str) -> List[Epoch]:
    """
    Fetch epochs from Postgres for a specified time range.
    :param session: SQLAlchemy session object.
    :param start_time: Start time of the range in ISO format.
    :param end_time: End time of the range in ISO format.
    :return: List of epoch ORM objects.
    """
    logging.info(f"Fetching epochs between: {start_time} - {end_time}")
    epochs = session.query(Epoch).filter(
        and_(Epoch.start_time >= start_time, Epoch.end_time <= end_time)
    ).order_by(Epoch.no).all()

    logging.info(f"Fetched: {len(epochs)} epochs between {start_time} - {end_time}")

    return epochs


def fetch_input_utxos(session: Session, start: str, end: str) -> List[InputUTXO]:
    logging.info(f"Fetching input UTXOs between: {start} - {end}")

    CreatingTransaction = aliased(Transaction)
    ConsumingTransaction = aliased(Transaction)
    CreatingBlock = aliased(Block)
    ConsumingBlock = aliased(Block)

    stmt = (
        select(
            TransactionIn.tx_in_id.label('tx_id'),
            func.encode(ConsumingTransaction.hash, 'hex').label('consuming_tx_hash'),
            func.encode(CreatingTransaction.hash, 'hex').label('creating_tx_hash'),
            func.encode(CreatingBlock.hash, 'hex').label('block_hash'),
            CreatingTransaction.block_index,
            TransactionOut.id.label('tx_out_id'),
            TransactionOut.index.label('tx_out_index'),
            TransactionOut.address.label('input_address'),
            TransactionOut.value.label('input_value'),
            CreatingBlock.time.label('creating_timestamp'),
            ConsumingBlock.time.label('consuming_timestamp'),
            TransactionOut.stake_address_id.label('stake_address_id'),
            StakeAddress.view.label('stake_address')
        )
        .select_from(TransactionIn)
        .join(ConsumingTransaction, ConsumingTransaction.id == TransactionIn.tx_in_id)
        .join(ConsumingBlock, ConsumingBlock.id == ConsumingTransaction.block_id)
        .join(TransactionOut,
              (TransactionIn.tx_out_id == TransactionOut.tx_id) & (TransactionIn.tx_out_index == TransactionOut.index))
        .join(CreatingTransaction, CreatingTransaction.id == TransactionOut.tx_id)
        .join(CreatingBlock, CreatingBlock.id == CreatingTransaction.block_id)
        .outerjoin(StakeAddress, StakeAddress.id == TransactionOut.stake_address_id)
        .where(ConsumingBlock.time >= start, ConsumingBlock.time <= end)
    )

    result = session.execute(stmt)
    rows = result.fetchall()
    logging.info('Number of rows fetched: %s', len(rows))

    return [InputUTXO(**row._asdict()) for row in rows]


# def fetch_input_utxos(start: str, end: str) -> List[Dict[str, Any]]:
#     logging.info(f"Fetching input UTXOs between: {start} - {end}")
#     query = f"""
#     SELECT tx_in.tx_in_id                     AS tx_id,
#            encode(consuming_tx.hash, 'hex')   AS consuming_tx_hash, -- Transaction hash consuming the input UTXO
#            encode(creating_tx.hash, 'hex')    AS creating_tx_hash,  -- Transaction hash creating the input UTXO
#            encode(creating_block.hash, 'hex') AS block_hash,
#            creating_tx.block_index            AS block_index,
#            tx_out.id                          AS tx_out_id,
#            tx_out.index                       AS tx_out_index,
#            tx_out.address                     AS input_address,
#            tx_out.value                       AS input_value,
#            creating_block.time                AS creating_timestamp,
#            consuming_block.time               AS consuming_timestamp,
#            ma_tx_out.quantity                 AS asset_quantity,
#            multi_asset.policy                 AS asset_policy,
#            multi_asset.name                   AS asset_name,
#            tx_out.stake_address_id            AS stake_address_id,
#            stake_address.view                 AS stake_address
#     FROM tx_in
#              INNER JOIN tx_out ON tx_in.tx_out_id = tx_out.tx_id AND tx_in.tx_out_index = tx_out.index
#              INNER JOIN tx AS consuming_tx ON consuming_tx.id = tx_in.tx_in_id -- Transaction consuming the UTXO
#              INNER JOIN tx AS creating_tx ON creating_tx.id = tx_out.tx_id -- Transaction creating the UTXO
#              INNER JOIN block as consuming_block ON consuming_block.id = consuming_tx.block_id
#              INNER JOIN block AS creating_block ON creating_block.id = creating_tx.block_id
#              LEFT JOIN ma_tx_out ON ma_tx_out.tx_out_id = tx_out.id
#              LEFT JOIN multi_asset ON multi_asset.id = ma_tx_out.ident
#              LEFT JOIN stake_address ON stake_address.id = tx_out.stake_address_id
#     WHERE consuming_block.time >= %s
#       AND consuming_block.time <= %s
#     """
#     data = (start, end)
#     return execute_query(query, data)


def fetch_output_utxos(session: Session, start: str, end: str) -> List[OutputUTXO]:
    logging.info(f"Fetching output UTXOs between: {start} - {end}")

    CreatingTransaction = aliased(Transaction)
    ConsumingTransaction = aliased(Transaction)
    CreatingBlock = aliased(Block)
    ConsumingBlock = aliased(Block)

    stmt = (
        select(
            CreatingTransaction.id.label('tx_id'),
            func.encode(CreatingTransaction.hash, 'hex').label('creating_tx_hash'),
            func.encode(ConsumingTransaction.hash, 'hex').label('consuming_tx_hash'),
            func.encode(CreatingBlock.hash, 'hex').label('block_hash'),
            CreatingTransaction.block_index,
            CreatingTransaction.fee,
            TransactionOut.index.label('tx_out_index'),
            TransactionOut.address.label('output_address'),
            TransactionOut.value.label('output_value'),
            CreatingBlock.time.label('creating_timestamp'),
            ConsumingBlock.time.label('consuming_timestamp'),
            TransactionOut.stake_address_id.label('stake_address_id'),
            StakeAddress.view.label('stake_address')
        )
        .select_from(TransactionOut)
        .join(CreatingTransaction, CreatingTransaction.id == TransactionOut.tx_id)
        .join(CreatingBlock, CreatingBlock.id == CreatingTransaction.block_id)
        .outerjoin(TransactionIn,
                   (TransactionIn.tx_out_id == TransactionOut.tx_id) &
                   (TransactionIn.tx_out_index == TransactionOut.index))
        .outerjoin(ConsumingTransaction, ConsumingTransaction.id == TransactionIn.tx_in_id)
        .outerjoin(ConsumingBlock, ConsumingBlock.id == ConsumingTransaction.block_id)
        .outerjoin(StakeAddress, StakeAddress.id == TransactionOut.stake_address_id)
        .where(CreatingBlock.time >= start, CreatingBlock.time <= end)
    )

    result = session.execute(stmt)
    rows = result.fetchall()
    logging.info('Number of rows fetched: %s', len(rows))

    return [OutputUTXO(**row._asdict()) for row in rows]

# def fetch_output_utxos(start, end) -> List[Dict[str, Any]]:
#     query = f"""
#     SELECT creating_tx.id                     AS tx_id,
#            encode(creating_tx.hash, 'hex')    AS creating_tx_hash,  -- Transaction hash creating the output UTXO
#            encode(consuming_tx.hash, 'hex')   AS consuming_tx_hash, -- Transaction hash consuming the output UTXO
#            encode(creating_block.hash, 'hex') AS block_hash,
#            creating_tx.block_index            AS block_index,
#            creating_tx.fee                    AS fee,
#            tx_out.index                       AS tx_out_index,
#            tx_out.address                     AS output_address,
#            tx_out.value                       AS output_value,
#            creating_block.time                AS creating_timestamp,
#            consuming_block.time               AS consuming_timestamp,
#            ma_tx_out.quantity                 AS asset_quantity,
#            multi_asset.policy                 AS asset_policy,
#            multi_asset.name                   AS asset_name,
#            tx_out.stake_address_id            AS stake_address_id,
#            stake_address.view                 AS stake_address
#     FROM tx_out
#              LEFT JOIN tx_in
#                        ON tx_in.tx_out_id = tx_out.tx_id AND tx_in.tx_out_index = tx_out.index -- Transaction consuming the UTXO
#              INNER JOIN tx AS creating_tx ON creating_tx.id = tx_out.tx_id -- Transaction creating the UTXO
#              LEFT JOIN tx AS consuming_tx ON consuming_tx.id = tx_in.tx_in_id
#              INNER JOIN block AS creating_block ON creating_block.id = creating_tx.block_id
#              INNER JOIN block as consuming_block ON consuming_block.id = consuming_tx.block_id
#              LEFT JOIN ma_tx_out ON ma_tx_out.tx_out_id = tx_out.id
#              LEFT JOIN multi_asset ON multi_asset.id = ma_tx_out.ident
#              LEFT JOIN stake_address ON stake_address.id = tx_out.stake_address_id
#     WHERE creating_block.time >= %s
#       AND creating_block.time <= %s
#     """
#     data = (start, end)
#     return execute_query(query, data)


# def execute_query(query: str, data) -> List[Dict[str, Any]]:
#     conn = connect_postgres()
#     cur = conn.cursor()
#     cur.execute(query, data)
#     rows = cur.fetchall()
#     logging.info('Number of rows fetched: %s', len(rows))
#     cur.close()
#     conn.close()
#
#     utxos = [dict(zip([desc[0] for desc in cur.description], row)) for row in rows]
#     return utxos
