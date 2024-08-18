import requests
import duckdb
import pandas as pd
from requests.exceptions import RequestException
import sqlparse

from sqlparse.sql import IdentifierList, Identifier, Where, Comparison
from sqlparse.tokens import Keyword, Whitespace, DML, Comparison as ComparisonToken

class QueryParser:
    def __init__(self):
        self.from_block = 1
        self.to_block = float('inf')
        self.tables = set()
        self.joins = []
        self.where_conditions = []

    def parse(self, sql_query):
        parsed = sqlparse.parse(sql_query)[0]
        self._extract_tables(parsed)
        self._extract_joins(parsed)
        self._extract_where(parsed)
        return self

    def _extract_tables(self, parsed):
        from_seen = False
        for token in parsed.tokens:
            if from_seen:
                if isinstance(token, IdentifierList):
                    for identifier in token.get_identifiers():
                        self.tables.add(str(identifier).split()[0])  # Get the table name without alias
                elif isinstance(token, Identifier):
                    self.tables.add(str(token).split()[0])  # Get the table name without alias
                elif token.ttype is sqlparse.tokens.Keyword:
                    break
            elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
                from_seen = True

    def _extract_joins(self, parsed):
        join_keywords = ['JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'FULL JOIN']
        for token in parsed.tokens:
            if token.ttype is Keyword and token.value.upper() in join_keywords:
                self.joins.append(str(token.parent))

    def _extract_where(self, parsed):
        where_clause = next((token for token in parsed.tokens if isinstance(token, Where)), None)
        if where_clause:
            self._parse_where_clause(where_clause)

    def _parse_where_clause(self, where_clause):
        for token in where_clause.tokens:
            if isinstance(token, Comparison):
                self.where_conditions.append(str(token))
                left = str(token.left).strip()
                right = str(token.right).strip()
                operator = next((t.value for t in token.tokens if t.ttype is ComparisonToken), None)
                
                if operator and ('blockHeight' in left or 'blockNumber' in left):
                    if '>=' in operator:
                        self.from_block = max(self.from_block, int(right))
                    elif '<=' in operator:
                        self.to_block = min(self.to_block, int(right))

    def get_query_info(self):
        return {
            'from_block': self.from_block,
            'to_block': self.to_block,
            #'tables': list(self.tables),
            'tables': ['logs', 'transactions'],
            'joins': self.joins,
            'where_conditions': self.where_conditions
        }

class DistributedQueryEngine:
    def __init__(self, nodes):
        self.nodes = nodes
        self.con = duckdb.connect(':memory:')
        self.node_ranges = {
            nodes[0]: (1, 25000),
            nodes[1]: (25001, 50000)
        }

    def execute_plan(self, plan):
        all_data = {table: [] for table in set(step['table'] for step in plan)}
        for step in plan:
            try:
                params = {
                    'table': step['table'],
                    'from_block': step['from_block'],
                    'to_block': step['to_block']
                }
                response = requests.get(f"{step['node']}/query", params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                if isinstance(data, list):
                    all_data[step['table']].extend(data)
                else:
                    print(f"Unexpected response format from node {step['node']}: {data}")
            except RequestException as e:
                print(f"Error fetching data from node {step['node']}: {str(e)}")
        
        return {table: pd.DataFrame(data) for table, data in all_data.items()}

    def execute_query(self, sql_query):
        parser = QueryParser()
        query_info = parser.parse(sql_query).get_query_info()
        
        try:
            plan = self.plan_query(query_info)
            data = self.execute_plan(plan)
            
            if all(df.empty for df in data.values()):
                return pd.DataFrame(), "No data found for the given query"

            # Append tables in DuckDB only if data is not empty and the table is not already registered
            for table, df in data.items():
                if not df.empty:
                    self.con.register(table, df)
                    print(f"Registered table '{table}' with {len(df)} rows.")

            # List registered tables for debugging
            tables = self.con.execute("SHOW TABLES").fetchall()
            print("Registered tables:", [table[0] for table in tables])

            # Execute the query
            result = self.con.execute(sql_query).fetchdf()
            return result, None
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            return pd.DataFrame(), f"Error executing query: {str(e)}"


    def plan_query(self, query_info):
        plan = []
        for node, (start, end) in self.node_ranges.items():
            if query_info['from_block'] <= end and query_info['to_block'] >= start:
                node_from = max(query_info['from_block'], start)
                node_to = min(query_info['to_block'], end)
                for table in query_info['tables']:
                    plan.append({
                        'node': node,
                        'table': table,
                        'from_block': node_from,
                        'to_block': node_to,
                        'where_conditions': query_info['where_conditions']
                    })
        return plan
    
    def explain_plan(self, sql_query):
        parser = QueryParser()
        query_info = parser.parse(sql_query).get_query_info()
        plan = self.plan_query(query_info)
        
        explanation = ["Query Execution Plan:"]
        for step in plan:
            explanation.append(f"- Scan {step['table']} on node {step['node']}:")
            explanation.append(f"  Range: blocks {step['from_block']} to {step['to_block']}")
            if step['where_conditions']:
                explanation.append(f"  Conditions: {', '.join(step['where_conditions'])}")
        
        if query_info['joins']:
            explanation.append("Joins:")
            for join in query_info['joins']:
                explanation.append(f"- {join}")
        
        return "\n".join(explanation)