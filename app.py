from flask import Flask, request, jsonify, render_template
from query_engine import DistributedQueryEngine

app = Flask(__name__)
engine = DistributedQueryEngine(['http://localhost:5000', 'http://localhost:5001'])  # Add all your nodes

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/execute_query', methods=['POST'])
def execute_query():
    query = request.form['query']
    result = engine.execute_query(query)
    return jsonify(result.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(port=8000)