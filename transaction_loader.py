from __future__ import annotations

import argparse
import sqlite3

from pathlib import Path
from typing import Any

from schemas import TABLE_SCHEMAS


ACTIVE_END_DATE = "9999-12-31T23:59:59"

SOURCE_METADATA_COLUMNS = [
    "_raw_id",
    "_cdc_op",
    "_source_user",
    "_source_ts",
    "_ingested_at",
    "_source_file",
    "_batch_id",
]

SCD_COLUMNS = [
    "scd_key",
    "scd_version",
    "eff_start_date",
    "eff_end_date",
    "is_current",
    "is_deleted",
    "change_type",
    "changed_columns",
]


def quoted(identifier: str) -> str:
    safe_identifier = identifier.replace('"', '""')
    return f'"{safe_identifier}"'


def staging_table_name(config: dict[str, Any]) -> str:
    return f"stg_{config['base_table']}"


def curated_table_name(config: dict[str, Any]) -> str:
    return f"curated_{config['base_table']}"


def create_curated_table(
    connection: sqlite3.Connection,
    config: dict[str, Any],
) -> None:

    table_name = curated_table_name(config)

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

    scd_definitions = [
        f'{quoted("scd_key")} TEXT PRIMARY KEY',
        f'{quoted("scd_version")} INTEGER NOT NULL',
        f'{quoted("eff_start_date")} TEXT NOT NULL',
        f'{quoted("eff_end_date")} TEXT NOT NULL',
        f'{quoted("is_current")} INTEGER NOT NULL',
        f'{quoted("is_deleted")} INTEGER NOT NULL',
        f'{quoted("change_type")} TEXT NOT NULL',
        f'{quoted("changed_columns")} TEXT',
        (
            f'CHECK ({quoted("is_current")} '
            f'IN (0, 1))'
        ),
        (
            f'CHECK ({quoted("is_deleted")} '
            f'IN (0, 1))'
        ),
    ]

    all_definitions = (
        source_definitions
        + metadata_definitions
        + scd_definitions
    )

    create_sql = (
        f"CREATE TABLE IF NOT EXISTS "
        f"{quoted(table_name)} ("
        f"{', '.join(all_definitions)}"
        f")"
    )

    connection.execute(create_sql)

    key_columns = ", ".join(
        quoted(column_name)
        for column_name in config["primary_key"]
    )

    current_index_name = (
        f"idx_{table_name}_business_key_current"
    )

    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS
        {quoted(current_index_name)}
        ON {quoted(table_name)}
        ({key_columns}, {quoted("is_current")})
        """
    )

    batch_index_name = (
        f"idx_{table_name}_batch"
    )

    connection.execute(
        f"""
        CREATE INDEX IF NOT EXISTS
        {quoted(batch_index_name)}
        ON {quoted(table_name)}
        ({quoted("_batch_id")})
        """
    )


def get_business_key(
    record: dict[str, Any],
    config: dict[str, Any],
) -> tuple[str, ...]:

    key_values = []

    for column_name in config["primary_key"]:
        value = record.get(column_name)

        if value is None or str(value).strip() == "":
            raise ValueError(
                f"Null primary key found for "
                f"{config['base_table']}: "
                f"{column_name}"
            )

        key_values.append(str(value))

    return tuple(key_values)


def build_key_where_clause(
    config: dict[str, Any],
) -> str:

    conditions = [
        f"{quoted(column_name)} = ?"
        for column_name in config["primary_key"]
    ]

    return " AND ".join(conditions)


def find_current_version(
    connection: sqlite3.Connection,
    config: dict[str, Any],
    key_values: tuple[str, ...],
) -> sqlite3.Row | None:

    table_name = curated_table_name(config)
    key_where = build_key_where_clause(config)

    return connection.execute(
        f"""
        SELECT *
        FROM {quoted(table_name)}
        WHERE {key_where}
          AND {quoted("is_current")} = 1
        ORDER BY {quoted("scd_version")} DESC
        LIMIT 1
        """,
        key_values,
    ).fetchone()


def create_scd_key(
    config: dict[str, Any],
    key_values: tuple[str, ...],
    version: int,
    batch_id: str,
) -> str:

    business_key = "|".join(key_values)

    return (
        f"{config['base_table']}|"
        f"{business_key}|"
        f"v{version}|"
        f"{batch_id}"
    )


def compare_changed_columns(
    old_record: dict[str, Any] | None,
    new_record: dict[str, Any],
    config: dict[str, Any],
) -> str | None:

    if old_record is None:
        return None

    changed_columns = []

    for column_name in config["columns"]:
        old_value = old_record.get(column_name)
        new_value = new_record.get(column_name)

        if old_value != new_value:
            changed_columns.append(column_name)

    if not changed_columns:
        return None

    return ",".join(changed_columns)


def insert_version(
    connection: sqlite3.Connection,
    config: dict[str, Any],
    record: dict[str, Any],
    version: int,
    change_type: str,
    changed_columns: str | None,
) -> None:

    table_name = curated_table_name(config)

    key_values = get_business_key(
        record,
        config,
    )

    scd_key = create_scd_key(
        config=config,
        key_values=key_values,
        version=version,
        batch_id=record["_batch_id"],
    )

    source_ts = record["_source_ts"]

    row_data = {
        **record,
        "scd_key": scd_key,
        "scd_version": version,
        "eff_start_date": source_ts,
        "eff_end_date": ACTIVE_END_DATE,
        "is_current": 1,
        "is_deleted": 0,
        "change_type": change_type,
        "changed_columns": changed_columns,
    }

    insert_columns = (
        list(config["columns"])
        + SOURCE_METADATA_COLUMNS
        + SCD_COLUMNS
    )

    placeholders = ", ".join(
        "?"
        for _ in insert_columns
    )

    insert_sql = (
        f"INSERT INTO {quoted(table_name)} "
        f"("
        f"{', '.join(quoted(column) for column in insert_columns)}"
        f") "
        f"VALUES ({placeholders})"
    )

    values = [
        row_data.get(column)
        for column in insert_columns
    ]

    connection.execute(
        insert_sql,
        values,
    )


def close_current_for_update(
    connection: sqlite3.Connection,
    config: dict[str, Any],
    current_row: sqlite3.Row,
    end_date: str,
) -> None:

    table_name = curated_table_name(config)

    connection.execute(
        f"""
        UPDATE {quoted(table_name)}
        SET
            {quoted("is_current")} = 0,
            {quoted("eff_end_date")} = ?
        WHERE {quoted("scd_key")} = ?
        """,
        (
            end_date,
            current_row["scd_key"],
        ),
    )


def close_current_for_delete(
    connection: sqlite3.Connection,
    config: dict[str, Any],
    current_row: sqlite3.Row,
    end_date: str,
) -> None:

    table_name = curated_table_name(config)

    connection.execute(
        f"""
        UPDATE {quoted(table_name)}
        SET
            {quoted("is_current")} = 0,
            {quoted("is_deleted")} = 1,
            {quoted("eff_end_date")} = ?,
            {quoted("change_type")} = 'DELETE'
        WHERE {quoted("scd_key")} = ?
        """,
        (
            end_date,
            current_row["scd_key"],
        ),
    )


def process_transaction_table(
    connection: sqlite3.Connection,
    config: dict[str, Any],
    batch_id: str,
) -> dict[str, int]:

    staging_table = staging_table_name(config)
    curated_table = curated_table_name(config)

    create_curated_table(
        connection,
        config,
    )

    staging_rows = connection.execute(
        f"""
        SELECT *
        FROM {quoted(staging_table)}
        WHERE {quoted("_batch_id")} = ?
          AND {quoted("_is_processed")} = 0
        ORDER BY {quoted("_stage_seq")}
        """,
        (batch_id,),
    ).fetchall()

    if not staging_rows:
        raise ValueError(
            f"No unprocessed records found in "
            f"{staging_table} for batch {batch_id}."
        )

    before_images: dict[
        tuple[str, ...],
        dict[str, Any],
    ] = {}

    insert_count = 0
    update_count = 0
    delete_count = 0
    version_rows_inserted = 0

    for staging_row in staging_rows:
        record = dict(staging_row)

        operation = record["_cdc_op"]
        key_values = get_business_key(
            record,
            config,
        )

        if operation == "append":
            current_row = find_current_version(
                connection,
                config,
                key_values,
            )

            if current_row is not None:
                raise ValueError(
                    f"Duplicate append for "
                    f"{config['base_table']} "
                    f"key {key_values}."
                )

            insert_version(
                connection=connection,
                config=config,
                record=record,
                version=1,
                change_type="INSERT",
                changed_columns=None,
            )

            insert_count += 1
            version_rows_inserted += 1

        elif operation == "repold":
            before_images[key_values] = record

        elif operation == "repnew":
            old_record = before_images.pop(
                key_values,
                None,
            )

            changed_columns = compare_changed_columns(
                old_record,
                record,
                config,
            )

            current_row = find_current_version(
                connection,
                config,
                key_values,
            )

            if current_row is None:
                next_version = 1
            else:
                close_current_for_update(
                    connection=connection,
                    config=config,
                    current_row=current_row,
                    end_date=record["_source_ts"],
                )

                next_version = (
                    int(current_row["scd_version"])
                    + 1
                )

            insert_version(
                connection=connection,
                config=config,
                record=record,
                version=next_version,
                change_type="UPDATE",
                changed_columns=changed_columns,
            )

            update_count += 1
            version_rows_inserted += 1

        elif operation == "delete":
            current_row = find_current_version(
                connection,
                config,
                key_values,
            )

            if current_row is None:
                raise ValueError(
                    f"Delete has no current version for "
                    f"{config['base_table']} "
                    f"key {key_values}."
                )

            close_current_for_delete(
                connection=connection,
                config=config,
                current_row=current_row,
                end_date=record["_source_ts"],
            )

            delete_count += 1

        else:
            raise ValueError(
                f"Unsupported CDC operation: {operation}"
            )

    if before_images:
        unmatched_keys = list(
            before_images.keys()
        )

        raise ValueError(
            f"Unmatched repold records remain in "
            f"{config['base_table']}: "
            f"{unmatched_keys}"
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

    total_curated_rows = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM {quoted(curated_table)}
        """
    ).fetchone()[0]

    return {
        "raw_events": len(staging_rows),
        "insert_count": insert_count,
        "update_count": update_count,
        "delete_count": delete_count,
        "version_rows_inserted": version_rows_inserted,
        "total_curated_rows": total_curated_rows,
    }


def resolve_batch_id(
    connection: sqlite3.Connection,
    requested_batch_id: str | None,
) -> str:

    if requested_batch_id:
        return requested_batch_id

    batch_ids = set()

    for config in TABLE_SCHEMAS.values():
        if config["table_type"] != "transaction":
            continue

        staging_table = staging_table_name(config)

        rows = connection.execute(
            f"""
            SELECT DISTINCT {quoted("_batch_id")}
            FROM {quoted(staging_table)}
            WHERE {quoted("_is_processed")} = 0
            """
        ).fetchall()

        for row in rows:
            batch_ids.add(row[0])

    if not batch_ids:
        raise ValueError(
            "No unprocessed transaction batch was found."
        )

    if len(batch_ids) > 1:
        raise ValueError(
            "Multiple unprocessed transaction batches "
            "were found. Use --batch-id."
        )

    return next(iter(batch_ids))


def run_transaction_loader(
    database_path: Path,
    requested_batch_id: str | None,
) -> None:

    connection = sqlite3.connect(
        database_path
    )

    connection.row_factory = sqlite3.Row

    try:
        batch_id = resolve_batch_id(
            connection,
            requested_batch_id,
        )

        print("HDMF SCD Type 2 Transaction Load")
        print("=" * 105)
        print(f"Database: {database_path.resolve()}")
        print(f"Batch ID: {batch_id}")
        print("=" * 105)

        total_events = 0
        total_versions = 0

        for config in TABLE_SCHEMAS.values():
            if config["table_type"] != "transaction":
                continue

            result = process_transaction_table(
                connection=connection,
                config=config,
                batch_id=batch_id,
            )

            total_events += result["raw_events"]
            total_versions += result[
                "version_rows_inserted"
            ]

            print(
                f"[LOADED] "
                f"{curated_table_name(config):<42} "
                f"Events: {result['raw_events']:<3} | "
                f"INSERT: {result['insert_count']:<3} | "
                f"UPDATE: {result['update_count']:<3} | "
                f"DELETE: {result['delete_count']:<3} | "
                f"Versions: {result['version_rows_inserted']}"
            )

        connection.commit()

        print("=" * 105)
        print("Transaction Load Summary")
        print(f"Raw transaction events: {total_events}")
        print(f"Version rows inserted:  {total_versions}")
        print("Transaction load completed successfully.")

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Apply SCD Type 2 processing to HDMF "
            "transaction staging tables."
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

    run_transaction_loader(
        database_path=database_path,
        requested_batch_id=arguments.batch_id,
    )


if __name__ == "__main__":
    main()
