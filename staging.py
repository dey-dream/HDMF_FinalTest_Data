from __future__ import annotations

import argparse
import sqlite3
import uuid

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from parser import iter_records
from schemas import RISK_01_FILES, TABLE_SCHEMAS


DEFAULT_DATABASE = "hdmf_migration_output.db"


METADATA_COLUMNS = [
    "_stage_seq",
    "_transaction_seq",
    "_is_processed",
    "_raw_id",
    "_cdc_op",
    "_source_user",
    "_source_ts",
    "_ingested_at",
    "_source_file",
    "_batch_id",
]


def quoted(identifier: str) -> str:
    safe_identifier = identifier.replace('"', '""')
    return f'"{safe_identifier}"'


def staging_table_name(config: dict[str, Any]) -> str:
    return f"stg_{config['base_table']}"


def create_staging_table(
    connection: sqlite3.Connection,
    config: dict[str, Any],
) -> None:

    table_name = staging_table_name(config)

    source_definitions = [
        f"{quoted(column_name)} TEXT"
        for column_name in config["columns"]
    ]

    metadata_definitions = [
        f'{quoted("_stage_seq")} INTEGER NOT NULL',
        f'{quoted("_transaction_seq")} INTEGER',
        f'{quoted("_is_processed")} INTEGER NOT NULL DEFAULT 0',
        f'{quoted("_raw_id")} TEXT PRIMARY KEY',
        f'{quoted("_cdc_op")} TEXT NOT NULL',
        f'{quoted("_source_user")} TEXT',
        f'{quoted("_source_ts")} TEXT',
        f'{quoted("_ingested_at")} TEXT NOT NULL',
        f'{quoted("_source_file")} TEXT NOT NULL',
        f'{quoted("_batch_id")} TEXT NOT NULL',
        (
            f'CHECK ({quoted("_is_processed")} '
            f'IN (0, 1))'
        ),
    ]

    all_definitions = (
        source_definitions
        + metadata_definitions
    )

    create_sql = (
        f"CREATE TABLE IF NOT EXISTS "
        f"{quoted(table_name)} ("
        f"{', '.join(all_definitions)}"
        f")"
    )

    connection.execute(create_sql)

    index_name = (
        f"idx_{table_name}_batch_stage"
    )

    connection.execute(
        f"CREATE INDEX IF NOT EXISTS "
        f"{quoted(index_name)} "
        f"ON {quoted(table_name)} "
        f"({quoted('_batch_id')}, "
        f"{quoted('_stage_seq')})"
    )


def load_staging_table(
    connection: sqlite3.Connection,
    file_path: Path,
    config: dict[str, Any],
    batch_id: str,
) -> int:

    table_name = staging_table_name(config)

    parsed_records = list(
        iter_records(
            file_path,
            config,
        )
    )

    ingested_at = datetime.now(
        timezone.utc
    ).isoformat()

    all_columns = (
        list(config["columns"])
        + METADATA_COLUMNS
    )

    placeholders = ", ".join(
        "?"
        for _ in all_columns
    )

    insert_sql = (
        f"INSERT INTO {quoted(table_name)} "
        f"("
        f"{', '.join(quoted(column) for column in all_columns)}"
        f") "
        f"VALUES ({placeholders})"
    )

    rows_to_insert = []

    for record in parsed_records:
        stage_seq = int(
            record["_stage_seq"]
        )

        enriched_record = {
            **record,
            "_is_processed": 0,
            "_raw_id": (
                f"{config['base_table']}_"
                f"{batch_id}_"
                f"{stage_seq}"
            ),
            "_ingested_at": ingested_at,
            "_source_file": file_path.name,
            "_batch_id": batch_id,
        }

        row_values = [
            enriched_record.get(column)
            for column in all_columns
        ]

        rows_to_insert.append(
            row_values
        )

    connection.executemany(
        insert_sql,
        rows_to_insert,
    )

    return len(rows_to_insert)


def load_all_staging_tables(
    data_directory: Path,
    database_path: Path,
    batch_id: str,
) -> None:

    connection = sqlite3.connect(
        database_path
    )

    connection.row_factory = sqlite3.Row

    loaded_tables = 0
    loaded_records = 0
    skipped_files = 0

    try:
        print("HDMF SQLite Staging Load")
        print("=" * 90)
        print(f"Data directory: {data_directory.resolve()}")
        print(f"Database:       {database_path.resolve()}")
        print(f"Batch ID:       {batch_id}")
        print("=" * 90)

        for filename, config in TABLE_SCHEMAS.items():
            file_path = (
                data_directory
                / filename
            )

            if not file_path.exists():
                raise FileNotFoundError(
                    f"Source file not found: "
                    f"{file_path}"
                )

            create_staging_table(
                connection,
                config,
            )

            staged_count = load_staging_table(
                connection,
                file_path,
                config,
                batch_id,
            )

            connection.commit()

            loaded_tables += 1
            loaded_records += staged_count

            print(
                f"[LOADED] "
                f"{staging_table_name(config):<40} "
                f"Rows: {staged_count}"
            )

        for filename in sorted(RISK_01_FILES):
            file_path = (
                data_directory
                / filename
            )

            if not file_path.exists():
                print(
                    f"[MISSING] {filename}"
                )

                continue

            if file_path.stat().st_size == 0:
                skipped_files += 1

                print(
                    f"[SKIPPED] "
                    f"{filename:<40} "
                    f"RISK-01"
                )

            else:
                print(
                    f"[WARNING] "
                    f"{filename:<40} "
                    f"Expected an empty RISK-01 file, "
                    f"but the file contains data."
                )

        print("=" * 90)
        print("Staging Load Summary")
        print(f"Loaded tables:   {loaded_tables}")
        print(f"Loaded records:  {loaded_records}")
        print(f"Skipped files:   {skipped_files}")
        print(f"Database output: {database_path}")

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def parse_arguments() -> argparse.Namespace:
    argument_parser = argparse.ArgumentParser(
        description=(
            "Parse HDMF TRL files and load "
            "them into SQLite staging tables."
        )
    )

    argument_parser.add_argument(
        "data_directory",
        nargs="?",
        default=".",
        help=(
            "Directory containing the TRL files. "
            "Default: current directory"
        ),
    )

    argument_parser.add_argument(
        "--db",
        default=DEFAULT_DATABASE,
        help=(
            "SQLite output database path. "
            f"Default: {DEFAULT_DATABASE}"
        ),
    )

    argument_parser.add_argument(
        "--batch-id",
        default=None,
        help=(
            "Optional batch identifier. "
            "A UUID is generated when omitted."
        ),
    )

    argument_parser.add_argument(
        "--reset",
        action="store_true",
        help=(
            "Delete the existing output database "
            "before loading."
        ),
    )

    return argument_parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    data_directory = Path(
        arguments.data_directory
    )

    database_path = Path(
        arguments.db
    )

    batch_id = (
        arguments.batch_id
        or uuid.uuid4().hex
    )

    if not data_directory.exists():
        raise FileNotFoundError(
            f"Data directory does not exist: "
            f"{data_directory}"
        )

    if arguments.reset and database_path.exists():
        database_path.unlink()

        print(
            f"Removed existing database: "
            f"{database_path}"
        )

    load_all_staging_tables(
        data_directory=data_directory,
        database_path=database_path,
        batch_id=batch_id,
    )


if __name__ == "__main__":
    main()
