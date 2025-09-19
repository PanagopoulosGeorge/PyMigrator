from __future__ import annotations
from typing import Dict, List
from .schema_mapper import clean_table_or_field_name, map_type_to_oracle


def create_table_statement_for_oracle(meta: Dict, schema: str, target_table: str, db_type: str) -> str:
	cols: List[str] = []
	for col in meta["columns"]:
		col_name = clean_table_or_field_name(col["name"])
		col_type = map_type_to_oracle(col, db_type)
		cols.append(f"\t{col_name} {col_type}")
	cols.append("\tparser_error NVARCHAR2(2000)")
	cols_sql = ",\n".join(cols)
	table_name = clean_table_or_field_name(target_table)
	schema_name = clean_table_or_field_name(schema)
	return f"CREATE TABLE {schema_name}.{table_name} (\n{cols_sql}\n)"
