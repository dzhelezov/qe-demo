from flask import Flask, request, jsonify, render_template
from query_engine import DistributedQueryEngine

app = Flask(__name__)
engine = DistributedQueryEngine(['http://localhost:5000', 'http://localhost:5001'])

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/execute_query', methods=['POST'])
def execute_query():
    query = request.form['query']
    result = engine.execute_query(query)
    return jsonify(result.to_dict(orient='records'))

@app.route('/explain_query', methods=['POST'])
def explain_query():
    query = request.form['query']
    explanation = engine.explain_plan(query)
    return jsonify({"explanation": explanation})

if __name__ == '__main__':
    app.run(port=8000)