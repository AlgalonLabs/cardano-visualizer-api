import logging
from typing import List, Dict, Any


def process_utxos(inputs: List[Dict[str, Any]], outputs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    transactions = {}

    logging.log(logging.INFO, f"Processing {len(inputs)} inputs")
    for utxo in inputs:
        tx_hash = utxo['tx_hash']
        input_address = utxo['input_address']
        input_value = utxo['input_value']
        stake_address = utxo.get('stake_address')

        if tx_hash not in transactions:
            transactions[tx_hash] = {
                'inputs': [],
                'outputs': []
            }

        transactions[tx_hash]['inputs'].append({
            'address': input_address,
            'value': input_value,
            'stake_address': stake_address
        })

    logging.info(f"Processing {len(outputs)} outputs")
    for utxo in outputs:
        tx_hash = utxo['tx_hash']
        output_address = utxo['output_address']
        output_value = utxo['output_value']
        timestamp = utxo['timestamp']
        asset_policy = utxo.get('asset_policy')
        asset_name = utxo.get('asset_name')
        asset_quantity = utxo.get('asset_quantity')
        stake_address = utxo.get('stake_address')

        if tx_hash not in transactions:
            transactions[tx_hash] = {
                'inputs': [],
                'outputs': []
            }

        transactions[tx_hash]['outputs'].append({
            'address': output_address,
            'value': output_value,
            'timestamp': timestamp,
            'asset_policy': asset_policy,
            'asset_name': asset_name,
            'asset_quantity': asset_quantity,
            'stake_address': stake_address
        })

    processed_utxos = []

    logging.info(f"Processing {len(transactions)} transactions")
    for tx_hash, tx in transactions.items():
        inputs = tx['inputs']
        outputs = tx['outputs']

        for input_utxo in inputs:
            input_address = input_utxo['address']
            input_value = input_utxo['value']
            input_stake_address = input_utxo.get('stake_address')

            for output_utxo in outputs:
                output_address = output_utxo['address']
                output_value = output_utxo['value']
                timestamp = output_utxo['timestamp']
                asset_policy = output_utxo.get('asset_policy')
                asset_name = output_utxo.get('asset_name')
                asset_quantity = output_utxo.get('asset_quantity')
                output_stake_address = output_utxo.get('stake_address')

                # Calculate the actual sent amount
                actual_sent = input_value - sum([out['value'] for out in outputs if out['address'] == input_address])

                logging.debug(f"Processing UTXO: {input_address} -> {output_address}")
                processed_utxos.append({
                    "tx_hash": tx_hash,
                    "input_address": input_address,
                    "input_stake_address": input_stake_address,
                    "output_address": output_address,
                    "output_stake_address": output_stake_address,
                    "output_value": output_value,
                    "actual_sent": actual_sent,
                    "timestamp": timestamp,
                    "asset_policy": asset_policy,
                    "asset_name": asset_name,
                    "asset_quantity": asset_quantity
                })

    return processed_utxos
