import requests
import duckdb
import pandas as pd
from requests.exceptions import RequestException
import sqlparse

from sqlparse.sql import Where, Comparison, Identifier
from sqlparse.tokens import Keyword, Whitespace, Comparison as ComparisonToken

class QueryParser:
    def __init__(self):
        self.from_block = 1
        self.to_block = float('inf')
        self.account = None
        self.select_columns = []
        self.group_by = []
        self.order_by = []
        self.limit = None

    def parse(self, sql_query):
        parsed = sqlparse.parse(sql_query)[0]
        self._extract_select(parsed)
        self._extract_where(parsed)
        self._extract_group_by(parsed)
        self._extract_order_by(parsed)
        self._extract_limit(parsed)
        return self

    def _extract_select(self, parsed):
        select_tokens = [token for token in parsed.tokens if isinstance(token, sqlparse.sql.IdentifierList)]
        if select_tokens:
            self.select_columns = [str(col).strip() for col in select_tokens[0].get_identifiers()]
        else:
            # Handle case where there's only one column
            select_token = next((token for token in parsed.tokens if token.ttype == sqlparse.tokens.DML and token.value.upper() == 'SELECT'), None)
            if select_token:
                next_token = parsed.token_next(parsed.token_index(select_token))[1]
                if isinstance(next_token, sqlparse.sql.Identifier):
                    self.select_columns = [str(next_token)]

    def _extract_where(self, parsed):
        where_clause = next((token for token in parsed.tokens if isinstance(token, Where)), None)
        if where_clause:
            self._parse_where_clause(where_clause)

    def _parse_where_clause(self, where_clause):
        comparison_tokens = [token for token in where_clause.tokens if isinstance(token, Comparison)]
        for token in comparison_tokens:
            self._parse_comparison(token)

    def _parse_comparison(self, comparison):
        left = str(comparison.left).strip().lower()
        right = str(comparison.right).strip("'\"")
        operator = None
        for token in comparison.tokens:
            if token.ttype in (Keyword, ComparisonToken):
                operator = str(token)
                break
        if operator:
            self._apply_condition(left, operator, right)

    def _apply_condition(self, column, operator, value):
        if 'blockheight' in column:
            try:
                if '>=' in operator:
                    self.from_block = int(value)
                elif '<=' in operator:
                    self.to_block = int(value)
            except ValueError:
                print(f"Warning: Could not parse '{value}' as an integer for blockHeight")
        elif 'account' in column and '=' in operator:
            self.account = value

    def _extract_group_by(self, parsed):
        group_by_idx = next((i for i, token in enumerate(parsed.tokens) if token.value.upper() == 'GROUP BY'), -1)
        if group_by_idx != -1:
            group_by_token = parsed.tokens[group_by_idx + 2]  # Skip whitespace
            if isinstance(group_by_token, sqlparse.sql.IdentifierList):
                self.group_by = [str(col).strip() for col in group_by_token.get_identifiers()]
            else:
                self.group_by = [str(group_by_token).strip()]

    def _extract_order_by(self, parsed):
        order_by_idx = next((i for i, token in enumerate(parsed.tokens) if token.value.upper() == 'ORDER BY'), -1)
        if order_by_idx != -1:
            order_by_token = parsed.tokens[order_by_idx + 2]  # Skip whitespace
            if isinstance(order_by_token, sqlparse.sql.IdentifierList):
                self.order_by = [str(col).strip() for col in order_by_token.get_identifiers()]
            else:
                self.order_by = [str(order_by_token).strip()]

    def _extract_limit(self, parsed):
        limit_idx = next((i for i, token in enumerate(parsed.tokens) if token.value.upper() == 'LIMIT'), -1)
        if limit_idx != -1:
            self.limit = int(str(parsed.tokens[limit_idx + 2]).strip())

    def get_query_info(self):
        return {
            'from_block': self.from_block,
            'to_block': self.to_block,
            'account': self.account,
            'select_columns': self.select_columns,
            'group_by': self.group_by,
            'order_by': self.order_by,
            'limit': self.limit
        }

class DistributedQueryEngine:
    def __init__(self, nodes):
        self.nodes = nodes
        self.con = duckdb.connect(':memory:')
        # Assume each node has a specific block range (for demo purposes)
        self.node_ranges = {
            nodes[0]: (1, 500000),
            nodes[1]: (500001, 1000000)
        }

    def execute_query(self, sql_query):
        parser = QueryParser()
        query_info = parser.parse(sql_query).get_query_info()
        
        try:
            # Plan the query
            plan = self.plan_query(query_info)
            
            # Execute the plan
            data = self.execute_plan(plan)
            
            if data.empty:
                return pd.DataFrame()  # Return empty DataFrame if no data
            
            self.con.register('temp_table', data)
            result = self.con.execute(sql_query).fetchdf()
            return result
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            return pd.DataFrame()  # Return empty DataFrame on error

    def plan_query(self, query_info):
        plan = []
        for node, (start, end) in self.node_ranges.items():
            if query_info['from_block'] <= end and query_info['to_block'] >= start:
                node_from = max(query_info['from_block'], start)
                node_to = min(query_info['to_block'], end)
                plan.append({
                    'node': node,
                    'from_block': node_from,
                    'to_block': node_to,
                    'account': query_info['account']
                })
        return plan

    def execute_plan(self, plan):
        all_data = []
        for step in plan:
            try:
                params = {
                    'from_block': step['from_block'],
                    'to_block': step['to_block']
                }
                if step['account']:
                    params['account'] = step['account']
                
                response = requests.get(f"{step['node']}/query", params=params, timeout=10)
                response.raise_for_status()
                data = response.json()
                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    print(f"Unexpected response format from node {step['node']}: {data}")
            except RequestException as e:
                print(f"Error fetching data from node {step['node']}: {str(e)}")
            except ValueError as e:
                print(f"Error parsing JSON from node {step['node']}: {str(e)}")
        
        if not all_data:
            print("No data retrieved from any node")
        
        return pd.DataFrame(all_data)

    def explain_plan(self, sql_query):
        parser = QueryParser()
        query_info = parser.parse(sql_query).get_query_info()
        plan = self.plan_query(query_info)
        
        explanation = ["Query Execution Plan:"]
        for step in plan:
            explanation.append(f"- Scan node {step['node']}:")
            explanation.append(f"  Range: blocks {step['from_block']} to {step['to_block']}")
            if step['account']:
                explanation.append(f"  Predicate: account = {step['account']}")
        
        return "\n".join(explanation)