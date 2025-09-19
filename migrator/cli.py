from __future__ import annotations
import argparse
import sys
from typing import List, Optional

from .config import load_config
from .log import setup_logger
from .connectors.factory import create_connector
from .schema_mapper import clean_table_or_field_name
from .ddl_generator import create_table_statement_for_oracle
from .loader import OracleLoader

def migrate_table(config_path: str, table_arg: Optional[str], mode: str, dry_run: bool):
	cfg = load_config(config_path)
	logger = setup_logger()
	oracle = cfg["oracle"]
	sources = cfg["source"]

	# Connector via factory (supports future types)
	conn = create_connector(sources)

	# Build Oracle loader
	loader = OracleLoader(
		#lib_dir=oracle["lib_dir"],
		conn=oracle["conn"],
		username=oracle["username"],
		password=oracle["password"],
	)

	selected = []
	for t in sources.get("tables", []):
		if table_arg and clean_table_or_field_name(t.get("target_table", "")) != clean_table_or_field_name(table_arg):
			continue
		selected.append(t)
	if not selected:
		raise SystemExit("No tables matched selection")

	report = []
	for entry in selected:
		path = entry["path"]
		schema = entry.get("schema", oracle.get("username"))
		target_table = entry["target_table"]
		drop_before_load = bool(entry.get("drop_before_load", False))

		logger.info("Processing table: path=%s schema=%s target=%s", path, schema, target_table)
		meta = conn.get_table_metadata(path)
		ddl = create_table_statement_for_oracle(meta, schema, target_table, db_type=sources.get('type'))

		logger.info("Generated DDL:\n%s", ddl)

		if dry_run:
			# Sample first N rows
			n = 10
			it = conn.stream_rows(path, chunksize=n)
			try:
				sample = next(it)
				logger.info("Sample rows (up to %d):\n%s", n, sample.head(n).to_string(index=False))
			except StopIteration:
				logger.info("No rows available in source")
			report.append({"table": target_table, "rows_read": 0, "rows_inserted": 0})
			continue

		# Execute DDL actions
		if drop_before_load:
			loader.maybe_drop(schema, target_table)
		if mode in ("create", "replace"):
			loader.create_table(ddl)
		elif mode == "truncate":
			loader.truncate_table(schema, target_table)

		# Load data
		rows_read, rows_inserted = loader.bulk_insert(schema, target_table, conn.stream_rows(path))
		logger.info("Load completed: read=%d inserted=%d", rows_read, rows_inserted)
		report.append({"table": target_table, "rows_read": rows_read, "rows_inserted": rows_inserted})

	# Summary
	logger.info("Summary report:")
	for r in report:
		logger.info("%s: read=%d inserted=%d", r["table"], r["rows_read"], r["rows_inserted"])

def main(argv: Optional[List[str]] = None):
	p = argparse.ArgumentParser(description="Migrate DBF/Paradox to Oracle")
	p.add_argument("--config", required=True, help="Path to YAML config")
	p.add_argument("--table", help="Target table name to migrate")
	p.add_argument("--dry-run", action="store_true", help="Preview DDL and sample rows")
	p.add_argument("--mode", choices=["append", "create", "truncate", "drop", "replace"], default="append", help="DDL/load mode")
	p.add_argument("--test-connection", action="store_true", help="Test Oracle connection and exit")
	args = p.parse_args(argv)

	if args.test_connection:
		cfg = load_config(args.config)
		logger = setup_logger()
		oracle = cfg["oracle"]
		loader = OracleLoader(conn=oracle["conn"], username=oracle["username"], password=oracle["password"])
		try:
			loader.test_connection()
			logger.info("Oracle connection successful.")
			sys.exit(0)
		except Exception as e:
			logger.exception("Oracle connection failed: %s", e)
			sys.exit(2)

	migrate_table(args.config, args.table, args.mode, args.dry_run)

if __name__ == "__main__":
	main()
