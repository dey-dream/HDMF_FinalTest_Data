from __future__ import annotations

import argparse
import sqlite3

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from schemas import RISK_01_FILES, TABLE_SCHEMAS


def quoted(identifier: str) -> str:
    safe_identifier = identifier.replace('"', '""')
    return f'"{safe_identifier}"'


def staging_table_name(config: dict[str, Any]) -> str:
    return f"stg_{config['base_table']}"


def target_table_name(config: dict[str, Any]) -> str:
    if config["table_type"] == "audit":
        return f"audit_{config['base_table']}"

    return f"curated_{config['base_table']}"


def table_exists(
    connection: sqlite3.Connection,
    table_name: str,
) -> bool:

    result = connection.execute(
        """
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?
        """,
        (table_name,),
    ).fetchone()[0]

    return result == 1


def create_migration_log(
    connection: sqlite3.Connection,
) -> None:

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS migration_log (
            batch_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            source_file TEXT NOT NULL,
            records_parsed INTEGER NOT NULL DEFAULT 0,
            records_staged INTEGER NOT NULL DEFAULT 0,
            records_loaded INTEGER NOT NULL DEFAULT 0,
            insert_count INTEGER NOT NULL DEFAULT 0,
            update_count INTEGER NOT NULL DEFAULT 0,
            delete_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            risk_flag TEXT,
            error_message TEXT,
            started_at TEXT,
            completed_at TEXT,
            UNIQUE (batch_id, source_file)
        )
        """
    )


def resolve_batch_id(
    connection: sqlite3.Connection,
    requested_batch_id: str | None,
) -> str:

    if requested_batch_id:
        return requested_batch_id

    batch_ids = set()

    for config in TABLE_SCHEMAS.values():
        staging_table = staging_table_name(config)

        if not table_exists(
            connection,
            staging_table,
        ):
            continue

        rows = connection.execute(
            f"""
            SELECT DISTINCT {quoted("_batch_id")}
            FROM {quoted(staging_table)}
            """
        ).fetchall()

        for row in rows:
            batch_ids.add(row[0])

    if not batch_ids:
        raise ValueError(
            "No batch ID was found in the staging tables."
        )

    if len(batch_ids) > 1:
        raise ValueError(
            "Multiple batch IDs were found. "
            "Use --batch-id to select one."
        )

    return next(iter(batch_ids))


def get_operation_count(
    connection: sqlite3.Connection,
    staging_table: str,
    batch_id: str,
    operation: str,
) -> int:

    return connection.execute(
        f"""
        SELECT COUNT(*)
        FROM {quoted(staging_table)}
        WHERE {quoted("_batch_id")} = ?
          AND {quoted("_cdc_op")} = ?
        """,
        (
            batch_id,
            operation,
        ),
    ).fetchone()[0]


def write_completed_file_log(
    connection: sqlite3.Connection,
    filename: str,
    config: dict[str, Any],
    batch_id: str,
) -> dict[str, Any]:

    staging_table = staging_table_name(config)
    target_table = target_table_name(config)

    records_staged = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM {quoted(staging_table)}
        WHERE {quoted("_batch_id")} = ?
        """,
        (batch_id,),
    ).fetchone()[0]

    unprocessed_records = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM {quoted(staging_table)}
        WHERE {quoted("_batch_id")} = ?
          AND {quoted("_is_processed")} = 0
        """,
        (batch_id,),
    ).fetchone()[0]

    append_count = get_operation_count(
        connection,
        staging_table,
        batch_id,
        "append",
    )

    repnew_count = get_operation_count(
        connection,
        staging_table,
        batch_id,
        "repnew",
    )

    delete_count = get_operation_count(
        connection,
        staging_table,
        batch_id,
        "delete",
    )

    if table_exists(
        connection,
        target_table,
    ):
        records_loaded = connection.execute(
            f"""
            SELECT COUNT(*)
            FROM {quoted(target_table)}
            WHERE {quoted("_batch_id")} = ?
            """,
            (batch_id,),
        ).fetchone()[0]

    else:
        records_loaded = 0

    started_at = connection.execute(
        f"""
        SELECT MIN({quoted("_ingested_at")})
        FROM {quoted(staging_table)}
        WHERE {quoted("_batch_id")} = ?
        """,
        (batch_id,),
    ).fetchone()[0]

    status = (
        "COMPLETE"
        if records_staged > 0
        and unprocessed_records == 0
        and records_loaded > 0
        else "FAILED"
    )

    error_message = None

    if unprocessed_records > 0:
        error_message = (
            f"{unprocessed_records} staging records "
            f"remain unprocessed."
        )

    elif records_loaded == 0:
        error_message = (
            f"No records were found in {target_table}."
        )

    completed_at = datetime.now(
        timezone.utc
    ).isoformat()

    connection.execute(
        """
        INSERT INTO migration_log (
            batch_id,
            table_name,
            source_file,
            records_parsed,
            records_staged,
            records_loaded,
            insert_count,
            update_count,
            delete_count,
            status,
            risk_flag,
            error_message,
            started_at,
            completed_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            batch_id,
            config["base_table"],
            filename,
            records_staged,
            records_staged,
            records_loaded,
            append_count,
            repnew_count,
            delete_count,
            status,
            None,
            error_message,
            started_at,
            completed_at,
        ),
    )

    return {
        "filename": filename,
        "status": status,
        "parsed": records_staged,
        "loaded": records_loaded,
        "insert_count": append_count,
        "update_count": repnew_count,
        "delete_count": delete_count,
    }


def write_risk_log(
    connection: sqlite3.Connection,
    data_directory: Path,
    filename: str,
    batch_id: str,
) -> dict[str, Any]:

    file_path = data_directory / filename
    completed_at = datetime.now(
        timezone.utc
    ).isoformat()

    if not file_path.exists():
        status = "FAILED"
        risk_flag = "RISK-01"
        error_message = "Expected RISK-01 file was not found."

    elif file_path.stat().st_size == 0:
        status = "SKIPPED"
        risk_flag = "RISK-01"
        error_message = (
            "Source trail file contains zero bytes. "
            "No data was received for this table."
        )

    else:
        status = "FAILED"
        risk_flag = "RISK-01"
        error_message = (
            "File was expected to be empty under "
            "RISK-01 but contains data."
        )

    table_name = Path(filename).stem

    connection.execute(
        """
        INSERT INTO migration_log (
            batch_id,
            table_name,
            source_file,
            records_parsed,
            records_staged,
            records_loaded,
            insert_count,
            update_count,
            delete_count,
            status,
            risk_flag,
            error_message,
            started_at,
            completed_at
        )
        VALUES (?, ?, ?, 0, 0, 0, 0, 0, 0, ?, ?, ?, ?, ?)
        """,
        (
            batch_id,
            table_name,
            filename,
            status,
            risk_flag,
            error_message,
            completed_at,
            completed_at,
        ),
    )

    return {
        "filename": filename,
        "status": status,
        "risk_flag": risk_flag,
    }


def generate_migration_log(
    database_path: Path,
    data_directory: Path,
    requested_batch_id: str | None,
) -> None:

    connection = sqlite3.connect(
        database_path
    )

    connection.row_factory = sqlite3.Row

    try:
        create_migration_log(
            connection
        )

        batch_id = resolve_batch_id(
            connection,
            requested_batch_id,
        )

        connection.execute(
            """
            DELETE FROM migration_log
            WHERE batch_id = ?
            """,
            (batch_id,),
        )

        print("HDMF Migration Log")
        print("=" * 110)
        print(f"Database:       {database_path.resolve()}")
        print(f"Data directory: {data_directory.resolve()}")
        print(f"Batch ID:       {batch_id}")
        print("=" * 110)

        complete_count = 0
        skipped_count = 0
        failed_count = 0

        for filename, config in TABLE_SCHEMAS.items():
            result = write_completed_file_log(
                connection=connection,
                filename=filename,
                config=config,
                batch_id=batch_id,
            )

            print(
                f"[{result['status']:<8}] "
                f"{result['filename']:<40} "
                f"Parsed: {result['parsed']:<3} | "
                f"Loaded: {result['loaded']:<3} | "
                f"INSERT: {result['insert_count']:<3} | "
                f"UPDATE: {result['update_count']:<3} | "
                f"DELETE: {result['delete_count']:<3}"
            )

            if result["status"] == "COMPLETE":
                complete_count += 1
            else:
                failed_count += 1

        for filename in sorted(RISK_01_FILES):
            result = write_risk_log(
                connection=connection,
                data_directory=data_directory,
                filename=filename,
                batch_id=batch_id,
            )

            print(
                f"[{result['status']:<8}] "
                f"{result['filename']:<40} "
                f"Risk: {result['risk_flag']}"
            )

            if result["status"] == "SKIPPED":
                skipped_count += 1
            else:
                failed_count += 1

        connection.commit()

        total_rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM migration_log
            WHERE batch_id = ?
            """,
            (batch_id,),
        ).fetchone()[0]

        print("=" * 110)
        print("Migration Log Summary")
        print(f"Complete files: {complete_count}")
        print(f"Skipped files:  {skipped_count}")
        print(f"Failed files:   {failed_count}")
        print(f"Log rows:       {total_rows}")

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate the HDMF SQLite migration log."
        )
    )

    parser.add_argument(
        "data_directory",
        nargs="?",
        default=".",
        help="Directory containing the TRL files.",
    )

    parser.add_argument(
        "--db",
        default="hdmf_migration_output.db",
        help="SQLite database path.",
    )

    parser.add_argument(
        "--batch-id",
        default=None,
        help="Specific batch ID to log.",
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    database_path = Path(
        arguments.db
    )

    data_directory = Path(
        arguments.data_directory
    )

    if not database_path.exists():
        raise FileNotFoundError(
            f"Database does not exist: {database_path}"
        )

    if not data_directory.exists():
        raise FileNotFoundError(
            f"Data directory does not exist: {data_directory}"
        )

    generate_migration_log(
        database_path=database_path,
        data_directory=data_directory,
        requested_batch_id=arguments.batch_id,
    )


if __name__ == "__main__":
    main()
