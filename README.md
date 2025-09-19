# PyMigrator

**PyMigrator** is a Python CLI tool designed to migrate legacy data sources—such as DBF files and paradox databases—into Oracle. It offers pluggable connectors, schema mapping, Oracle DDL generation, bulk loading, and structured logging to streamline complex migration workflows.

## Why Use PyMigrator?

- Config-driven migration runner supporting multiple source types via connectors.
- Handles real migration challenges: naming conventions, type mapping, dry-run previews, DDL modes, and centralized logging.
- Extensible architecture with a clean CLI interface.

## Who Is This For?

- **Data engineers and DBAs** tasked with migrating legacy systems to Oracle.
- **Developers** needing a configurable, extensible tool to automate data migration workflows.
- **Organizations** looking to modernize legacy data sources with minimal manual effort.
- Anyone dealing with **DBF or Paradox data sources** requiring reliable, repeatable migration pipelines.

## Quick Start

```bash
# Create and activate a virtual environment (Windows)
python -m venv .venv && .venv\Scripts\activate

# Or on Unix/macOS
python3 -m venv .venv && source .venv/bin/activate

pip install -r requirements.txt

# Show CLI help
python -m migrator.cli -h

# Test Oracle connectivity (no data moved)
python -m migrator.cli --config migrate.example.yml --test-connection

# Dry-run a single table (prints DDL + sample rows, no writes)
python -m migrator.cli --config migrate.example.yml --table TBL_NAME --dry-run

# Perform a load (append mode by default)
python -m migrator.cli --config migrate.example.yml --table TBL_NAME --mode append

Supported modes: `append` (default), `create`, `truncate`, `drop`, `replace`.

## Demo (dry-run preview)

Command:

```bash
python -m migrator.cli --config migrate.example.yml --table TBL_NAME --dry-run
```



All runs emit structured logs under `logs/` with timestamps.

## Architecture (at a glance)

```text
YAML config --> Connector Factory --> Source Connector (dbf, paradox, ...)
                                   |-- get_table_metadata()
                                   |-- stream_rows()
                                      |
                                      v
                              Schema Mapper + DDL Generator
                                      |
                                      v
                                  Oracle Loader
                                      |
                                      v
                                Oracle (DDL + Data)

[cli] parses flags -> loads config -> picks tables -> orchestrates steps -> logs summary
```

Key files:
- `migrator/cli.py`: CLI, argument parsing, run orchestration, summary reporting
- `migrator/connectors/factory.py`: chooses connector based on `source.type`
- `migrator/connectors/dbf.py`: DBF connector (`get_table_metadata`, `stream_rows`)
- `migrator/connectors/parsers.py`: field/type parsing utilities
- `migrator/ddl_generator.py`: builds Oracle `CREATE TABLE` statements
- `migrator/loader.py`: Oracle loader (create/truncate/drop, bulk insert, test connection)
- `migrator/schema_mapper.py`: cleans/normalizes table/column names
- `migrator/log.py`: logger setup (writes to `logs/`)

## Configuration

See `migrate.example.yml`:

```yaml
oracle:
  conn: "localhost:1521/orclpdb"
  username: "SRC_SCHEMA_USER"
  password: "SRC_SCHEMA_PASS"

source:
  type: dbf
  root_dir: "C:\\dumps\\sftp\\FOR_PROD"  # optional base dir
  tables:
    - path: "C:\\dumps\\sftp\\EKTELESH\\TBL_NAME.DBF"
      target_table: "TBL_NAME"
      schema: "TARGET_SCHEMA"
      drop_before_load: true
```

- **oracle.conn**: `host:port/service` for Oracle.
- **source.type**: currently `dbf` implemented; `paradox` scaffolded via connector pattern.
- **tables[].path**: full path to source file (for DBF).
- **tables[].target_table**: Oracle table name to create/load.
- **tables[].schema**: Oracle schema (defaults to `oracle.username` if omitted).
- **tables[].drop_before_load**: drop table before DDL/data when true.

Select a single table by `--table` (matched after name cleaning), otherwise all listed tables are processed.

## CLI options

```text
--config <path>          Path to YAML config (required)
--table <name>           Only migrate the named target table
--dry-run                Preview DDL and sample rows; no writes
--mode <append|create>
--test-connection        Validate Oracle connectivity and exit
```

## Development

```bash
python -m venv .venv && .venv\Scripts\activate
pip install -r requirements.txt
python -m migrator.cli --config migrate.example.yml --dry-run
```

Logs are written under `logs/` with timestamps (see existing samples for format).

## Roadmap

- Added a single source DBMS (DBASE - legacy system)
- Implement `paradox` connector (metadata + streaming)

## Limitations

- Currently focused on DBF -> Oracle path
- Assumes Oracle client/driver availability per `requirements.txt`
- Limited type inference for some edge-case legacy fields
- No retry/backoff or resumable bulk load yet

## License

This project is licensed under the [MIT License](https://opensource.org/licenses/MIT). 

Please review the LICENSE file.

## Share / Feedback

If this helps or you have connector ideas (Sybase, CSV, ODBC), please open an issue or PR.