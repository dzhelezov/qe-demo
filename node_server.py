from flask import Flask, request, jsonify
from flask_cors import CORS
import pandas as pd
import argparse
import os

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

blockchain_data = None
args = None

def load_blockchain_data(node_id):
    transactions = pd.read_parquet(f'test_data/transactions_node{node_id}.parquet')
    logs = pd.read_parquet(f'test_data/logs_node{node_id}.parquet')
    return {'transactions': transactions, 'logs': logs}

@app.before_request
def initialize_data():
    global blockchain_data, args
    if blockchain_data is None:
        print(f"Loading blockchain data for node {args.node_id}...")
        blockchain_data = load_blockchain_data(args.node_id)
        print(f"Data loaded. Transactions: {len(blockchain_data['transactions'])}, Logs: {len(blockchain_data['logs'])}")

@app.route('/query', methods=['GET'])
def query():
    print(f"Data: Transactions: {len(blockchain_data['transactions'])}, Logs: {len(blockchain_data['logs'])}")

    table = request.args.get('table', '').lower()
    if table.startswith('logs'):
        table = 'logs'
    elif table.startswith('transactions'):
        table = 'transactions'
    
    from_block = int(request.args.get('from_block', 1))
    to_block = int(request.args.get('to_block', 50000))
    
    if table == 'logs':
        result = blockchain_data['logs'][(blockchain_data['logs']['blockHeight'] >= from_block) & 
                                         (blockchain_data['logs']['blockHeight'] <= to_block)]
    elif table == 'transactions':
        result = blockchain_data['transactions'][(blockchain_data['transactions']['blockNumber'] >= from_block) & 
                                                 (blockchain_data['transactions']['blockNumber'] <= to_block)]
    else:
        return jsonify({"error": f"Invalid table specified: {table}"}), 400

    print(f"Got {len(result)} recordds")
    return jsonify(result.to_dict(orient='records'))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run the node server.')
    parser.add_argument('--port', type=int, required=True, help='Port to run the server on')
    parser.add_argument('--node_id', type=int, required=True, help='Node ID (1 or 2)')
    args = parser.parse_args()
    
    if not os.path.exists(f'test_data/transactions_node{args.node_id}.parquet') or \
       not os.path.exists(f'test_data/logs_node{args.node_id}.parquet'):
        print(f"Test data for node {args.node_id} not found. Please run generate_test_data.py first.")
        exit(1)
    
    print(f"Starting server for node {args.node_id} on port {args.port}...")
    app.run(port=args.port, debug=True, use_reloader=False)