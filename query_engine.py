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

    def execute_query(self, sql_query):
        parser = QueryParser()
        query_info = parser.parse(sql_query).get_query_info()
        
        try:
            data = self.fetch_data(query_info['from_block'], query_info['to_block'], query_info['account'])
            
            if data.empty:
                return pd.DataFrame()  # Return empty DataFrame if no data
            
            self.con.register('temp_table', data)
            result = self.con.execute(sql_query).fetchdf()
            return result
        except Exception as e:
            print(f"Error executing query: {str(e)}")
            return pd.DataFrame()  # Return empty DataFrame on error

    def fetch_data(self, from_block, to_block, account=None):
        all_data = []
        for node in self.nodes:
            params = {'from_block': from_block, 'to_block': to_block}
            if account:
                params['account'] = account
            try:
                response = requests.get(f"{node}/query", params=params, timeout=10)
                response.raise_for_status()  # Raise an exception for bad status codes
                data = response.json()
                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    print(f"Unexpected response format from node {node}: {data}")
            except RequestException as e:
                print(f"Error fetching data from node {node}: {str(e)}")
            except ValueError as e:
                print(f"Error parsing JSON from node {node}: {str(e)}")
        
        if not all_data:
            print("No data retrieved from any node")
        
        return pd.DataFrame(all_data)