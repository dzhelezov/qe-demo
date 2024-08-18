from flask import Flask, request, jsonify, render_template
from query_engine import DistributedQueryEngine

app = Flask(__name__)
engine = DistributedQueryEngine(['http://localhost:4999', 'http://localhost:5001'])

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/execute_query', methods=['POST'])
def execute_query():
    query = request.form['query']
    result, error = engine.execute_query(query)
    if error:
        return jsonify({"error": error}), 400
    return jsonify({
        "result": result.to_dict(orient='records'),
        "debug_info": {
            "registered_tables": engine.con.execute("SHOW TABLES").fetchall(),
            "table_sizes": {table: len(engine.con.execute(f"SELECT * FROM {table}").fetchdf()) for table in engine.con.execute("SHOW TABLES").fetchdf()['name']}
        }
    })

@app.route('/explain_query', methods=['POST'])
def explain_query():
    query = request.form['query']
    explanation = engine.explain_plan(query)
    return jsonify({"explanation": explanation})

if __name__ == '__main__':
    app.run(port=8000, debug=True)