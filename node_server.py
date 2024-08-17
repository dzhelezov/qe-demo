from flask import Flask, request, jsonify
import pandas as pd
import argparse

app = Flask(__name__)

# Mock data
data = pd.DataFrame({
    'blockHeight': range(1, 1000001),
    'account': ['0x' + str(i).zfill(40) for i in range(1, 1000001)],
    'value': [i * 10 for i in range(1, 1000001)]
})

@app.route('/query', methods=['GET'])
def query():
    from_block = int(request.args.get('from_block', 1))
    to_block = int(request.args.get('to_block', 1000000))
    account = request.args.get('account', None)
    
    result = data[(data['blockHeight'] >= from_block) & (data['blockHeight'] <= to_block)]
    if account:
        result = result[result['account'].str.startswith(account)]
    
    return jsonify(result.to_dict(orient='records'))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run the node server.')
    parser.add_argument('--port', type=int, default=5000, help='Port to run the server on')
    args = parser.parse_args()
    
    app.run(port=args.port)