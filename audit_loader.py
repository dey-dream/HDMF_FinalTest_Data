from __future__ import annotations

import argparse
import sqlite3

from pathlib import Path
from typing import Any

from schemas import TABLE_SCHEMAS


AUDIT_METADATA_COLUMNS = [
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


def audit_table_name(config: dict[str, Any]) -> str:
    return f"audit_{config['base_table']}"


def create_audit_table(
    connection: sqlite3.Connection,
    config: dict[str, Any],
) -> None:

    table_name = audit_table_name(config)

    source_definitions = [
        f"{quoted(column_name)} TEXT"
        for column_name in config["columns"]
    ]

    metadata_definitions = [
        f'{quoted("_raw_id")} TEXT NOT NULL',
        f'{quoted("_cdc_op")} TEXT NOT NULL',
        f'{quoted("_source_user")} TEXT',
        f'{quoted("_source_ts")} TEXT',
        f'{quoted("_ingested_at")} TEXT NOT NULL',
        f'{quoted("_source_file")} TEXT NOT NULL',
        f'{quoted("_batch_id")} TEXT NOT NULL',
    ]

    primary_key_columns = ", ".join(
        quoted(column_name)
        for column_name in config["primary_key"]
    )

    all_definitions = (
        source_definitions
        + metadata_definitions
        + [
            f"UNIQUE ({primary_key_columns})"
        ]
    )

    create_sql = (
        f"CREATE TABLE IF NOT EXISTS "
        f"{quoted(table_name)} ("
        f"{', '.join(all_definitions)}"
        f")"
    )

    connection.execute(create_sql)


def resolve_batch_id(
    connection: sqlite3.Connection,
    staging_table: str,
    requested_batch_id: str | None,
) -> str:

    if requested_batch_id:
        return requested_batch_id

    rows = connection.execute(
        f"""
        SELECT DISTINCT "_batch_id"
        FROM {quoted(staging_table)}
        WHERE "_is_processed" = 0
        ORDER BY "_batch_id"
        """
    ).fetchall()

    batch_ids = [
        row[0]
        for row in rows
    ]

    if not batch_ids:
        raise ValueError(
            f"No unprocessed batch found in {staging_table}."
        )

    if len(batch_ids) > 1:
        raise ValueError(
            "Multiple unprocessed batches were found. "
            "Use --batch-id to select one batch."
        )

    return batch_ids[0]


def load_audit_table(
    connection: sqlite3.Connection,
    config: dict[str, Any],
    batch_id: str,
) -> tuple[int, int]:

    staging_table = staging_table_name(config)
    target_table = audit_table_name(config)

    create_audit_table(
        connection,
        config,
    )

    select_columns = (
        list(config["columns"])
        + AUDIT_METADATA_COLUMNS
    )

    select_sql = (
        f"SELECT "
        f"{', '.join(quoted(column) for column in select_columns)} "
        f"FROM {quoted(staging_table)} "
        f"WHERE {quoted('_batch_id')} = ? "
        f"AND {quoted('_is_processed')} = 0 "
        f"ORDER BY {quoted('_stage_seq')}"
    )

    staging_rows = connection.execute(
        select_sql,
        (batch_id,),
    ).fetchall()

    if not staging_rows:
        raise ValueError(
            f"No unprocessed rows found in {staging_table} "
            f"for batch {batch_id}."
        )

    placeholders = ", ".join(
        "?"
        for _ in select_columns
    )

    conflict_columns = ", ".join(
        quoted(column)
        for column in config["primary_key"]
    )

    update_columns = [
        column
        for column in select_columns
        if column not in config["primary_key"]
    ]

    update_assignments = ", ".join(
        (
            f"{quoted(column)} = "
            f"excluded.{quoted(column)}"
        )
        for column in update_columns
    )

    insert_sql = (
        f"INSERT INTO {quoted(target_table)} "
        f"("
        f"{', '.join(quoted(column) for column in select_columns)}"
        f") "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_columns}) "
        f"DO UPDATE SET {update_assignments}"
    )

    rows_to_insert = [
        tuple(row[column] for column in select_columns)
        for row in staging_rows
    ]

    connection.executemany(
        insert_sql,
        rows_to_insert,
    )

    connection.execute(
        f"""
        UPDATE {quoted(staging_table)}
        SET {quoted("_is_processed")} = 1
        WHERE {quoted("_batch_id")} = ?
          AND {quoted("_is_processed")} = 0
        """,
        (batch_id,),
    )

    final_row_count = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM {quoted(target_table)}
        WHERE {quoted("_batch_id")} = ?
        """,
        (batch_id,),
    ).fetchone()[0]

    return (
        len(staging_rows),
        final_row_count,
    )


def run_audit_loader(
    database_path: Path,
    requested_batch_id: str | None,
) -> None:

    connection = sqlite3.connect(
        database_path
    )

    connection.row_factory = sqlite3.Row

    try:
        print("HDMF Audit Table Load")
        print("=" * 80)
        print(f"Database: {database_path.resolve()}")

        audit_configs = [
            config
            for config in TABLE_SCHEMAS.values()
            if config["table_type"] == "audit"
        ]

        if not audit_configs:
            raise ValueError(
                "No audit-table configuration was found."
            )

        for config in audit_configs:
            staging_table = staging_table_name(config)

            batch_id = resolve_batch_id(
                connection,
                staging_table,
                requested_batch_id,
            )

            staged_count, audit_count = load_audit_table(
                connection,
                config,
                batch_id,
            )

            print(f"Batch ID:       {batch_id}")
            print(f"Staging table:  {staging_table}")
            print(f"Audit table:    {audit_table_name(config)}")
            print(f"Rows processed: {staged_count}")
            print(f"Audit rows:     {audit_count}")

        connection.commit()

        print("=" * 80)
        print("Audit load completed successfully.")

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Load HDMF audit records from SQLite "
            "staging tables into audit tables."
        )
    )

    parser.add_argument(
        "--db",
        default="hdmf_migration_output.db",
        help="SQLite database path.",
    )

    parser.add_argument(
        "--batch-id",
        default=None,
        help=(
            "Batch ID to process. When omitted, "
            "the single unprocessed batch is used."
        ),
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    database_path = Path(
        arguments.db
    )

    if not database_path.exists():
        raise FileNotFoundError(
            f"Database does not exist: {database_path}"
        )

    run_audit_loader(
        database_path=database_path,
        requested_batch_id=arguments.batch_id,
    )


if __name__ == "__main__":
    main()
