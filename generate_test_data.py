# generate_test_data.py

import pandas as pd
import numpy as np
import random
import os

def generate_address():
    return '0x' + ''.join(random.choices('0123456789abcdef', k=40))

def generate_hash():
    return '0x' + ''.join(random.choices('0123456789abcdef', k=64))

def generate_blockchain_data(start_block, end_block, transactions_per_block=2, logs_per_transaction=3):
    num_blocks = end_block - start_block + 1
    num_transactions = num_blocks * transactions_per_block
    num_logs = num_transactions * logs_per_transaction
    
    # Generate transactions
    transactions = pd.DataFrame({
        'id': [generate_hash() for _ in range(num_transactions)],
        'transactionIndex': np.repeat(np.arange(transactions_per_block), num_blocks),
        'blockNumber': np.repeat(np.arange(start_block, end_block + 1), transactions_per_block),
        'from': [generate_address() for _ in range(num_transactions)],
        'to': [generate_address() for _ in range(num_transactions)],
        'hash': [generate_hash() for _ in range(num_transactions)]
    })

    # Generate logs
    logs = pd.DataFrame({
        'id': [generate_hash() for _ in range(num_logs)],
        'logIndex': np.tile(np.arange(logs_per_transaction), num_transactions),
        'transactionIndex': np.repeat(transactions['transactionIndex'], logs_per_transaction),
        'blockHeight': np.repeat(transactions['blockNumber'], logs_per_transaction),
        'transactionId': np.repeat(transactions['id'], logs_per_transaction),
        'address': [generate_address() for _ in range(num_logs)],
        'data': [generate_hash() for _ in range(num_logs)],
        'topics0': [generate_hash() for _ in range(num_logs)],
        'topics1': [generate_hash() for _ in range(num_logs)],
        'topics2': [generate_hash() for _ in range(num_logs)],
        'topics3': [generate_hash() for _ in range(num_logs)]
    })

    return transactions, logs

def save_data(transactions, logs, node_id):
    if not os.path.exists('test_data'):
        os.makedirs('test_data')
    
    transactions.to_parquet(f'test_data/transactions_node{node_id}.parquet', index=False)
    logs.to_parquet(f'test_data/logs_node{node_id}.parquet', index=False)

if __name__ == '__main__':
    # Generate data for node 1
    transactions1, logs1 = generate_blockchain_data(1, 25000)
    save_data(transactions1, logs1, 1)
    print(f"Data for node 1 generated and saved. Transactions: {len(transactions1)}, Logs: {len(logs1)}")

    # Generate data for node 2
    transactions2, logs2 = generate_blockchain_data(25001, 50000)
    save_data(transactions2, logs2, 2)
    print(f"Data for node 2 generated and saved. Transactions: {len(transactions2)}, Logs: {len(logs2)}")