from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from neo4j import Driver

from app.db.graph.db_neo4j import serialize_node
from app.db.graph.transaction import get_transaction_details
from app.models.details import TransactionDetails
from app.models.transactions import TransactionsResponse, TransactionResponse
from app.routers.dependencies import get_neo4j_driver

router = APIRouter()


@router.get("/transactions/{transaction_hash}", response_model=TransactionDetails)
def api_get_transaction_details(transaction_hash: str,
                                driver: Driver = Depends(get_neo4j_driver)) -> TransactionDetails:
    transaction_details = get_transaction_details(driver, transaction_hash)
    if transaction_details is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return transaction_details


@router.get("/transactions/{transaction_hash}/utxos")
def get_transaction_utxos(transaction_hash: str, driver: Driver = Depends(get_neo4j_driver)):
    query = """
    MATCH (t:Transaction {tx_hash: $transaction_hash})
    OPTIONAL MATCH (input:UTXO)-[:INPUT]->(t)
    OPTIONAL MATCH (t)-[:OUTPUT]->(output:UTXO)
    RETURN collect(DISTINCT input) AS inputs, collect(DISTINCT output) AS outputs
    """
    with driver.session() as session:
        result = session.run(query, {"transaction_hash": transaction_hash})
        record = result.single()
        if record:
            return {
                "inputs": [serialize_node(utxo) for utxo in record["inputs"]],
                "outputs": [serialize_node(utxo) for utxo in record["outputs"]]
            }
        return {"inputs": [], "outputs": []}


@router.get("/transactions/{transaction_hash}/signatories")
def get_transaction_signatories(transaction_hash: str, driver: Driver = Depends(get_neo4j_driver)):
    query = """
    MATCH (t:Transaction {tx_hash: $transaction_hash})
    OPTIONAL MATCH (t)<-[:SIGNED]-(a:Address)
    RETURN collect(DISTINCT a) AS signatories
    """
    with driver.session() as session:
        result = session.run(query, {"transaction_hash": transaction_hash})
        record = result.single()
        if record:
            return {"signatories": [dict(address) for address in record["signatories"]]}
        return {"signatories": []}


@router.get("/transactions", response_model=TransactionsResponse)
async def get_transactions(
        driver: Driver = Depends(get_neo4j_driver),
        page: int = Query(1, ge=1),
        page_size: int = Query(20, ge=1, le=100),
        sort_by: str = Query("timestamp", regex="^(fee|total_output|slot_no|timestamp)$"),
        sort_order: str = Query("DESC", regex="^(ASC|DESC)$"),
        tx_hash_filter: Optional[str] = Query(None)
):
    query = """
    MATCH (t:Transaction)
    WHERE $tx_hash_filter IS NULL OR t.tx_hash CONTAINS $tx_hash_filter
    WITH t
    MATCH (input:UTXO)-[:INPUT]->(t)
    MATCH (t)-[:OUTPUT]->(output:UTXO)
    MATCH (input)<-[:OWNS]-(inputAddress:Address)
    MATCH (output)<-[:OWNS]-(outputAddress:Address)
    MATCH (t)-[:CONTAINED_BY]->(b:Block)<-[:HAS_BLOCK]-(e:Epoch)
    WITH t, b, e, 
         collect(DISTINCT {address: inputAddress.address, utxo: input}) AS inputs,
         collect(DISTINCT {address: outputAddress.address, utxo: output}) AS outputs
    ORDER BY
        CASE WHEN $sort_order = "ASC" THEN t[$sort_by] ELSE null END ASC,
        CASE WHEN $sort_order = "DESC" THEN t[$sort_by] ELSE null END DESC
    SKIP $skip
    LIMIT $limit
    RETURN 
        t.tx_hash AS tx_hash,
        t.timestamp AS timestamp,
        b.block_no AS block_no,
        b.hash AS block_hash,
        e.no AS epoch_no,
        b.slot_no AS slot_no,
        b.epoch_slot_no AS absolute_slot_no,
        t.fee AS fees,
        reduce(s = 0, output IN outputs | s + output.utxo.value) AS total_output,
        [input IN inputs | input.address] AS input_addresses,
        [output IN outputs | output.address] AS output_addresses
    """

    params = {
        "tx_hash_filter": tx_hash_filter,
        "skip": (page - 1) * page_size,
        "limit": page_size,
        "sort_by": sort_by,
        "sort_order": sort_order
    }

    query_count = "MATCH (t:Transaction) RETURN COUNT(t) AS total_count"

    with driver.session() as session:
        result = session.run(query, params)

        transactions = []
        for record in result:
            record = serialize_node(record)
            transactions.append(TransactionResponse(
                tx_hash=record["tx_hash"],
                timestamp=record["timestamp"],
                block_no=str(record["block_no"]),
                block_hash=record["block_hash"],
                epoch_no=record["epoch_no"],
                slot_no=record["slot_no"],
                absolute_slot_no=record["absolute_slot_no"],
                fees=record["fees"],
                total_output=record["total_output"],
                input_addresses=record["input_addresses"],
                output_addresses=record["output_addresses"],
                status="SUCCESS",
            ))

        total_count_result = session.run(query_count)
        total_count = total_count_result.single()["total_count"]

    return TransactionsResponse(transactions=transactions, total_count=total_count)
