import logging
from typing import List, Dict, Any

from app.models.transactions import InputUTXO, OutputUTXO, Transaction


def process_utxos(inputs: List[Dict], outputs: List[Dict]) -> Dict[str, Transaction]:
    inputs = [InputUTXO(**utxo) for utxo in inputs]
    outputs = [OutputUTXO(**utxo) for utxo in outputs]
    return group_transactions(inputs, outputs)


def group_transactions(inputs: List[InputUTXO], outputs: List[OutputUTXO]) -> Dict[str, Transaction]:
    transactions = {}

    logging.info(f"Processing {len(inputs)} inputs")
    for utxo in inputs:
        tx_hash = utxo.tx_hash_hex()

        if tx_hash not in transactions:
            transactions[tx_hash] = Transaction(inputs=[], outputs=[])

        transactions[tx_hash].inputs.append(utxo)

    logging.info(f"Processing {len(outputs)} outputs")
    for utxo in outputs:
        tx_hash = utxo.tx_hash_hex()

        if tx_hash not in transactions:
            transactions[tx_hash] = Transaction(inputs=[], outputs=[])

        transactions[tx_hash].outputs.append(utxo)

    logging.info(f"Grouped {len(transactions)} unique transactions")
    return transactions


# def group_transactions(inputs: List[InputUTXO], outputs: List[OutputUTXO]) -> Dict[str, Transaction]:
#     transactions = {}
#
#     logging.info(f"Processing {len(inputs)} inputs")
#     for utxo in inputs:
#         tx_hash = utxo['tx_hash']
#         tx_hash = tx_hash.hex()
#         input_data = {
#             'address': utxo['input_address'],
#             'value': utxo['input_value'],
#             'stake_address': utxo.get('stake_address')
#         }
#
#         if tx_hash not in transactions:
#             transactions[tx_hash] = {'inputs': [], 'outputs': []}
#
#         transactions[tx_hash]['inputs'].append(input_data)
#
#     logging.info(f"Processing {len(outputs)} outputs")
#     for utxo in outputs:
#         tx_hash = utxo['tx_hash']
#         output_data = {
#             'address': utxo['output_address'],
#             'value': utxo['output_value'],
#             'timestamp': utxo['timestamp'],
#             'asset_policy': utxo.get('asset_policy'),
#             'asset_name': utxo.get('asset_name'),
#             'asset_quantity': utxo.get('asset_quantity'),
#             'stake_address': utxo.get('stake_address')
#         }
#
#         if tx_hash not in transactions:
#             transactions[tx_hash] = {'inputs': [], 'outputs': []}
#
#         transactions[tx_hash]['outputs'].append(output_data)
#
#     logging.info(f"Grouped {len(transactions)} unique transactions")
#     return transactions


# def process_transactions(transactions: Dict[str, Transaction]) -> List[ProcessedTransaction]:
#     processed_utxos = []
#     logging.info(f"Processing {len(transactions)} transactions")
#
#     for tx_hash, tx in transactions.items():
#         inputs = tx['inputs']
#         outputs = tx['outputs']
#
#         for input_utxo in inputs:
#             input_address = input_utxo['address']
#             input_value = input_utxo['value']
#             input_stake_address = input_utxo.get('stake_address')
#
#             for output_utxo in outputs:
#                 output_address = output_utxo['address']
#                 output_value = output_utxo['value']
#                 timestamp = output_utxo['timestamp']
#                 asset_policy = output_utxo.get('asset_policy')
#                 asset_name = output_utxo.get('asset_name')
#                 asset_quantity = output_utxo.get('asset_quantity')
#                 output_stake_address = output_utxo.get('stake_address')
#
#                 actual_sent = calculate_actual_sent(input_value, input_address, outputs)
#
#                 logging.debug(f"Processing UTXO: {input_address} -> {output_address}")
#                 processed_utxos.append({
#                     "tx_hash": tx_hash,
#                     "input_address": input_address,
#                     "input_stake_address": input_stake_address,
#                     "output_address": output_address,
#                     "output_stake_address": output_stake_address,
#                     "output_value": output_value,
#                     "actual_sent": actual_sent,
#                     "timestamp": timestamp,
#                     "asset_policy": asset_policy,
#                     "asset_name": asset_name,
#                     "asset_quantity": asset_quantity
#                 })
#
#     return processed_utxos


def calculate_actual_sent(input_value: int, input_address: str, outputs: List[Dict[str, Any]]) -> int:
    output_sum = sum([out['value'] for out in outputs if out['address'] == input_address])
    actual_sent = abs(input_value - output_sum)
    return actual_sent
