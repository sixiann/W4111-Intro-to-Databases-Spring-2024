from typing import Any, Dict, List, Tuple, Union

import pymysql

# Type definitions
# Key-value pairs
KV = Dict[str, Any]
# A Query consists of a string (possibly with placeholders) and a list of values to be put in the placeholders
Query = Tuple[str, List]

class DB:
	def __init__(self, host: str, port: int, user: str, password: str, database: str):
		conn = pymysql.connect(
			host=host,
			port=port,
			user=user,
			password=password,
			database=database,
			cursorclass=pymysql.cursors.DictCursor,
			autocommit=True,
		)
		self.conn = conn

	def get_cursor(self):
		return self.conn.cursor()

	def execute_query(self, query: str, args: List, ret_result: bool) -> Union[List[KV], int]:
		"""Executes a query.

		:param query: A query string, possibly containing %s placeholders
		:param args: A list containing the values for the %s placeholders
		:param ret_result: If True, execute_query returns a list of dicts, each representing a returned
							row from the table. If False, the number of rows affected is returned. Note
							that the length of the list of dicts is not necessarily equal to the number
							of rows affected.
		:returns: a list of dicts or a number, depending on ret_result
		"""
		cur = self.get_cursor()
		count = cur.execute(query, args=args)
		if ret_result:
			return cur.fetchall()
		else:
			return count


	# TODO: all methods below


	@staticmethod
	def build_select_query(table: str, columns: List[str], filters: KV) -> Query:
		"""Builds a query that selects rows. See db_test for examples.

		:param table: The table to be selected from
		:param columns: The attributes to select. If empty, then selects all columns.
		:param filters: Key-value pairs that the rows from table must satisfy
		:returns: A query string and any placeholder arguments
		"""

		# Start building the SELECT part of the query
		if rows:
			select_clause = f"SELECT {', '.join(rows)}"
		else:
			select_clause = "SELECT *"

		# Add the FROM part of the query
		from_clause = f"FROM {table}"

		# Initialize an empty list for arguments
		args = []

		# Build the WHERE part of the query if there are any filters
		if filters:
			where_clauses = []
			for key, value in filters.items():
				# Assume all filters use equality for simplicity
				where_clauses.append(f"{key} = %s")
				args.append(value)
			where_clause = "WHERE " + " AND ".join(where_clauses)
		else:
			where_clause = ""

		# Combine all parts of the query
		query = f"{select_clause} {from_clause} {where_clause}"

		if query[-1] == " ":
			query = query[:-1]
		return query, args



	def select(self, table: str, columns: List[str], filters: KV) -> List[KV]:
		"""Runs a select statement. You should use build_select_query and execute_query.

		:param table: The table to be selected from
		:param columns: The attributes to select. If empty, then selects all columns.
		:param filters: Key-value pairs that the rows to be selected must satisfy
		:returns: The selected rows
		"""
		query, args = self.build_select_query(table, rows, filters)
		rows = self.execute_query(query, args, True)
		return rows

	@staticmethod
	def build_insert_query(table: str, values: KV) -> Query:
		"""Builds a query that inserts a row. See db_test for examples.

		:param table: The table to be inserted into
		:param values: Key-value pairs that represent the values to be inserted
		:returns: A query string and any placeholder arguments
		"""
		insert_clause = f"INSERT INTO {table} "

		args = []

		keys = "("
		vals = "("

		for k, v in values.items():
			args.append(v)
			keys += k + ", "
			vals += "%s, "

		keys = keys[:-2] + ")"
		vals = vals[:-2] + ")"

		insert_clause += keys + " VALUES " + vals

		return insert_clause, args



	def insert(self, table: str, values: KV) -> int:
		"""Runs an insert statement. You should use build_insert_query and execute_query.

		:param table: The table to be inserted into
		:param values: Key-value pairs that represent the values to be inserted
		:returns: The number of rows affected
		"""
		query, args = self.build_insert_query(table, values)
		n_rows = self.execute_query(query, args, False)

		return n_rows

	@staticmethod
	def build_update_query(table: str, values: KV, filters: KV) -> Query:
		"""Builds a query that updates rows. See db_test for examples.

		:param table: The table to be updated
		:param values: Key-value pairs that represent the new values
		:param filters: Key-value pairs that the rows from table must satisfy
		:returns: A query string and any placeholder arguments
		"""
		clauses = [f"UPDATE {table}"]
		args = []

		set_clauses = []
		for key, value in values.items():
			set_clauses.append(f"{key} = %s")
			args.append(value)
		clauses.append("SET " + ", ".join(set_clauses))

		if filters:
			where_clauses = []
			for key, value in filters.items():
				where_clauses.append(f"{key} = %s")
				args.append(value)
			clauses.append("WHERE " + " AND ".join(where_clauses))

		return " ".join(clauses), args



	def update(self, table: str, values: KV, filters: KV) -> int:
		"""Runs an update statement. You should use build_update_query and execute_query.

		:param table: The table to be updated
		:param values: Key-value pairs that represent the new values
		:param filters: Key-value pairs that the rows to be updated must satisfy
		:returns: The number of rows affected
		"""
		query, args = self.build_update_query(table, values, filters)
		n_rows = self.execute_query(query, args, False)
		return n_rows



	@staticmethod
	def build_delete_query(table: str, filters: KV) -> Query:
		"""Builds a query that deletes rows. See db_test for examples.

		:param table: The table to be deleted from
		:param filters: Key-value pairs that the rows to be deleted must satisfy
		:returns: A query string and any placeholder arguments
		"""
		args = []
		clauses = [f"DELETE FROM {table}"]

		if filters:
			where_clauses = []
			for key, value in filters.items():
				where_clauses.append(f"{key} = %s")
				args.append(value)
			clauses.append("WHERE " + " AND ".join(where_clauses))

		return " ".join(clauses), args




	def delete(self, table: str, filters: KV) -> int:
		"""Runs a delete statement. You should use build_delete_query and execute_query.

		:param table: The table to be deleted from
		:param filters: Key-value pairs that the rows to be deleted must satisfy
		:returns: The number of rows affected
		"""
		query, args = self.build_delete_query(table, filters)
		n_rows = self.execute_query(query, args, False)
		return n_rows
