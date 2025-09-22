from __future__ import annotations
from typing import Dict, Iterable, List, Tuple
import os
import oracledb
import pandas as pd
import numpy as np
from .schema_mapper import clean_table_or_field_name
import datetime
from pandas._libs.tslibs.nattype import NaTType
ORACLE_DATE_FORMAT = "ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
class OracleLoader:
	def __init__(self,  conn: str, username: str, password: str):
		# # Initialize Oracle client
		# if lib_dir and os.path.isdir(lib_dir):
		# 	oracledb.init_oracle_client(lib_dir=lib_dir)
		self.dsn = conn
		self.username = username
		self.password = password

	def _connect(self):
		return oracledb.connect(user=self.username, password=self.password, dsn=self.dsn)

	def exec(self, sql: str):
		with self._connect() as conn:
			with conn.cursor() as cur:
				cur.execute(sql)
				conn.commit()

	def test_connection(self):
		with self._connect() as conn:
			with conn.cursor() as cur:
				cur.execute("SELECT 1 FROM DUAL")
				cur.fetchone()
		

	def maybe_drop(self, schema: str, table: str):
		schema = clean_table_or_field_name(schema)
		table = clean_table_or_field_name(table)
		try:
			self.exec(f"DROP TABLE {schema}.{table}")
		except Exception:
			pass

	def create_table(self, ddl: str):
		with self._connect() as conn:
			with conn.cursor() as cur:
				cur.execute("ALTER SESSION SET NLS_LENGTH_SEMANTICS=CHAR")
				cur.execute(ddl)
			conn.commit()

	def truncate_table(self, schema: str, table: str):
		schema = clean_table_or_field_name(schema)
		table = clean_table_or_field_name(table)
		self.exec(f"TRUNCATE TABLE {schema}.{table}")

	def bulk_insert(self, schema: str, table: str, dataframes: Iterable[pd.DataFrame]) -> Tuple[int, int]:
		schema = clean_table_or_field_name(schema)
		table = clean_table_or_field_name(table)
		rows_read = 0
		rows_inserted = 0
		with self._connect() as conn:
			with conn.cursor() as cur:
				# Ensure Oracle parses bound date/time strings consistently
				cur.execute(ORACLE_DATE_FORMAT)
				for df in dataframes:
					if df is None or df.empty:
						continue
					rows_read += len(df)
					# Prepare insert
					columns = [clean_table_or_field_name(c) for c in df.columns]
					placeholders = ",".join([":" + str(i+1) for i in range(len(columns))])
					sql = f"INSERT INTO {schema}.{table} (" + ",".join(columns) + ") VALUES (" + placeholders + ")"
					# Convert DataFrame rows to tuples; handle NaN -> None
					records = [tuple(convert_value(v) for v in row)
							for row in df.itertuples(index=False, name=None)
					]
					cur.executemany(sql, records)
					rows_inserted += cur.rowcount if cur.rowcount is not None else len(records)
				conn.commit()
		return rows_read, rows_inserted

def convert_value(v):
    # Treat NaT/NaN as NULL
    if v is pd.NaT or pd.isna(v):
        return None
    if isinstance(v, str) and v.strip() == "":
        return None

	# Handle numpy datetime64
    if isinstance(v, (np.datetime64,)):
        return pd.to_datetime(v).to_pydatetime()

    # Handle pandas Timestamps
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
		
    return str(v)


