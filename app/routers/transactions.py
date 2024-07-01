from fastapi import APIRouter, Depends
from neo4j import Driver

from app.db.graph.transaction import get_transaction_details
from app.models.details import TransactionDetails
from app.routers.dependencies import get_neo4j_driver

router = APIRouter()


@router.get("/transactions/{transaction_hash}", response_model=TransactionDetails)
def api_get_transaction_details(transaction_hash: str,
                                driver: Driver = Depends(get_neo4j_driver)) -> TransactionDetails:
    return get_transaction_details(driver, transaction_hash)


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
                "inputs": [dict(utxo) for utxo in record["inputs"]],
                "outputs": [dict(utxo) for utxo in record["outputs"]]
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
