import logging
from typing import List, Dict, Any

from app.models.transactions import InputUTXO, OutputUTXO, Transaction


def process_utxos(inputs: List[InputUTXO], outputs: List[OutputUTXO]) -> Dict[str, Transaction]:
    return group_transactions(inputs, outputs)


def group_transactions(inputs: List[InputUTXO], outputs: List[OutputUTXO]) -> Dict[str, Transaction]:
    transactions = {}

    logging.info(f"Processing {len(inputs)} inputs")
    for utxo in inputs:
        tx_hash = utxo.consuming_tx_hash

        if tx_hash not in transactions:
            transactions[tx_hash] = Transaction(inputs=[], outputs=[], block_index=utxo.block_index,
                                                block_hash=utxo.block_hash)

        transactions[tx_hash].inputs.append(utxo)

    logging.info(f"Processing {len(outputs)} outputs")
    for utxo in outputs:
        tx_hash = utxo.creating_tx_hash

        if tx_hash not in transactions:
            transactions[tx_hash] = Transaction(inputs=[], outputs=[], block_index=utxo.block_index,
                                                block_hash=utxo.block_hash)

        transactions[tx_hash].fee = utxo.fee
        transactions[tx_hash].outputs.append(utxo)

    logging.info(f"Grouped {len(transactions)} unique transactions")
    return transactions


def calculate_actual_sent(input_value: int, input_address: str, outputs: List[Dict[str, Any]]) -> int:
    output_sum = sum([out['value'] for out in outputs if out['address'] == input_address])
    actual_sent = abs(input_value - output_sum)
    return actual_sent
