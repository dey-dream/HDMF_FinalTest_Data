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


def scalar(
    connection: sqlite3.Connection,
    sql: str,
    parameters: tuple[Any, ...] = (),
) -> int:

    result = connection.execute(
        sql,
        parameters,
    ).fetchone()

    return int(result[0])


def table_exists(
    connection: sqlite3.Connection,
    table_name: str,
) -> bool:

    return scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM sqlite_master
        WHERE type = 'table'
          AND name = ?
        """,
        (table_name,),
    ) == 1


def create_validation_results_table(
    connection: sqlite3.Connection,
) -> None:

    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS validation_results (
            validation_id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            table_name TEXT NOT NULL,
            validation_name TEXT NOT NULL,
            validation_status TEXT NOT NULL,
            details TEXT,
            validated_at TEXT NOT NULL
        )
        """
    )


def resolve_batch_id(
    connection: sqlite3.Connection,
    requested_batch_id: str | None,
) -> str:

    if requested_batch_id:
        return requested_batch_id

    rows = connection.execute(
        """
        SELECT DISTINCT batch_id
        FROM migration_log
        ORDER BY batch_id
        """
    ).fetchall()

    batch_ids = [
        row[0]
        for row in rows
    ]

    if not batch_ids:
        raise ValueError(
            "No batch ID was found in migration_log."
        )

    if len(batch_ids) > 1:
        raise ValueError(
            "Multiple batch IDs were found. "
            "Use --batch-id to select one."
        )

    return batch_ids[0]


def add_result(
    results: list[dict[str, str]],
    table_name: str,
    validation_name: str,
    passed: bool,
    details: str,
) -> None:

    status = "PASS" if passed else "FAIL"

    results.append(
        {
            "table_name": table_name,
            "validation_name": validation_name,
            "validation_status": status,
            "details": details,
        }
    )

    print(
        f"[{status}] "
        f"{table_name}: "
        f"{validation_name} - "
        f"{details}"
    )


def get_migration_log_row(
    connection: sqlite3.Connection,
    batch_id: str,
    source_file: str,
) -> sqlite3.Row | None:

    return connection.execute(
        """
        SELECT *
        FROM migration_log
        WHERE batch_id = ?
          AND source_file = ?
        """,
        (
            batch_id,
            source_file,
        ),
    ).fetchone()


def validate_audit_table(
    connection: sqlite3.Connection,
    filename: str,
    config: dict[str, Any],
    batch_id: str,
    results: list[dict[str, str]],
) -> None:

    staging_table = staging_table_name(config)
    audit_table = target_table_name(config)

    if not table_exists(connection, staging_table):
        add_result(
            results,
            config["base_table"],
            "required staging table exists",
            False,
            f"Missing table: {staging_table}",
        )

        return

    if not table_exists(connection, audit_table):
        add_result(
            results,
            config["base_table"],
            "required audit table exists",
            False,
            f"Missing table: {audit_table}",
        )

        return

    staged_count = scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM {quoted(staging_table)}
        WHERE {quoted("_batch_id")} = ?
        """,
        (batch_id,),
    )

    loaded_count = scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM {quoted(audit_table)}
        WHERE {quoted("_batch_id")} = ?
        """,
        (batch_id,),
    )

    log_row = get_migration_log_row(
        connection,
        batch_id,
        filename,
    )

    log_matches = (
        log_row is not None
        and log_row["records_parsed"] == staged_count
        and log_row["records_staged"] == staged_count
        and log_row["records_loaded"] == loaded_count
        and log_row["status"] == "COMPLETE"
    )

    reconciliation_passed = (
        staged_count == loaded_count
        and log_matches
    )

    add_result(
        results,
        config["base_table"],
        "record count reconciliation",
        reconciliation_passed,
        (
            f"staged={staged_count}, "
            f"audit={loaded_count}, "
            f"log_matches={log_matches}"
        ),
    )

    unprocessed_count = scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM {quoted(staging_table)}
        WHERE {quoted("_batch_id")} = ?
          AND {quoted("_is_processed")} = 0
        """,
        (batch_id,),
    )

    add_result(
        results,
        config["base_table"],
        "staging completeness",
        unprocessed_count == 0,
        f"unprocessed={unprocessed_count}",
    )

    null_conditions = " OR ".join(
        (
            f"{quoted(column_name)} IS NULL "
            f"OR TRIM({quoted(column_name)}) = ''"
        )
        for column_name in config["primary_key"]
    )

    null_key_count = scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM {quoted(audit_table)}
        WHERE {null_conditions}
        """
    )

    add_result(
        results,
        config["base_table"],
        "valid primary keys",
        null_key_count == 0,
        f"invalid_keys={null_key_count}",
    )

    key_columns = ", ".join(
        quoted(column_name)
        for column_name in config["primary_key"]
    )

    duplicate_key_count = scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT
                {key_columns},
                COUNT(*) AS row_count
            FROM {quoted(audit_table)}
            GROUP BY {key_columns}
            HAVING COUNT(*) > 1
        )
        """
    )

    add_result(
        results,
        config["base_table"],
        "no duplicate business keys",
        duplicate_key_count == 0,
        f"duplicate_keys={duplicate_key_count}",
    )


def validate_transaction_table(
    connection: sqlite3.Connection,
    filename: str,
    config: dict[str, Any],
    batch_id: str,
    results: list[dict[str, str]],
) -> None:

    staging_table = staging_table_name(config)
    curated_table = target_table_name(config)

    if not table_exists(connection, staging_table):
        add_result(
            results,
            config["base_table"],
            "required staging table exists",
            False,
            f"Missing table: {staging_table}",
        )

        return

    if not table_exists(connection, curated_table):
        add_result(
            results,
            config["base_table"],
            "required curated table exists",
            False,
            f"Missing table: {curated_table}",
        )

        return

    raw_event_count = scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM {quoted(staging_table)}
        WHERE {quoted("_batch_id")} = ?
        """,
        (batch_id,),
    )

    append_count = scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM {quoted(staging_table)}
        WHERE {quoted("_batch_id")} = ?
          AND {quoted("_cdc_op")} = 'append'
        """,
        (batch_id,),
    )

    repnew_count = scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM {quoted(staging_table)}
        WHERE {quoted("_batch_id")} = ?
          AND {quoted("_cdc_op")} = 'repnew'
        """,
        (batch_id,),
    )

    expected_version_count = (
        append_count
        + repnew_count
    )

    actual_version_count = scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM {quoted(curated_table)}
        WHERE {quoted("_batch_id")} = ?
        """,
        (batch_id,),
    )

    log_row = get_migration_log_row(
        connection,
        batch_id,
        filename,
    )

    log_matches = (
        log_row is not None
        and log_row["records_parsed"] == raw_event_count
        and log_row["records_staged"] == raw_event_count
        and log_row["records_loaded"] == actual_version_count
        and log_row["status"] == "COMPLETE"
    )

    reconciliation_passed = (
        expected_version_count
        == actual_version_count
        and log_matches
    )

    add_result(
        results,
        config["base_table"],
        "record count reconciliation",
        reconciliation_passed,
        (
            f"events={raw_event_count}, "
            f"expected_versions={expected_version_count}, "
            f"curated_versions={actual_version_count}, "
            f"log_matches={log_matches}"
        ),
    )

    unprocessed_count = scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM {quoted(staging_table)}
        WHERE {quoted("_batch_id")} = ?
          AND {quoted("_is_processed")} = 0
        """,
        (batch_id,),
    )

    add_result(
        results,
        config["base_table"],
        "staging completeness",
        unprocessed_count == 0,
        f"unprocessed={unprocessed_count}",
    )

    null_conditions = " OR ".join(
        (
            f"{quoted(column_name)} IS NULL "
            f"OR TRIM({quoted(column_name)}) = ''"
        )
        for column_name in config["primary_key"]
    )

    null_key_count = scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM {quoted(curated_table)}
        WHERE {null_conditions}
        """
    )

    add_result(
        results,
        config["base_table"],
        "valid primary keys",
        null_key_count == 0,
        f"invalid_keys={null_key_count}",
    )

    key_columns = ", ".join(
        quoted(column_name)
        for column_name in config["primary_key"]
    )

    duplicate_current_count = scalar(
        connection,
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT
                {key_columns},
                SUM(
                    CASE
                        WHEN {quoted("is_current")} = 1
                        THEN 1
                        ELSE 0
                    END
                ) AS current_count
            FROM {quoted(curated_table)}
            GROUP BY {key_columns}
            HAVING SUM(
                CASE
                    WHEN {quoted("is_current")} = 1
                    THEN 1
                    ELSE 0
                END
            ) > 1
        )
        """
    )

    add_result(
        results,
        config["base_table"],
        "no duplicate current versions",
        duplicate_current_count == 0,
        (
            f"keys_with_multiple_current_versions="
            f"{duplicate_current_count}"
        ),
    )

    version_sequence_errors = scalar(
        connection,
        f"""
        WITH numbered_versions AS (
            SELECT
                {key_columns},
                {quoted("scd_version")},
                ROW_NUMBER() OVER (
                    PARTITION BY {key_columns}
                    ORDER BY {quoted("scd_version")}
                ) AS expected_version
            FROM {quoted(curated_table)}
        )
        SELECT COUNT(*)
        FROM numbered_versions
        WHERE {quoted("scd_version")} <> expected_version
        """
    )

    add_result(
        results,
        config["base_table"],
        "valid version sequence",
        version_sequence_errors == 0,
        f"sequence_errors={version_sequence_errors}",
    )

    date_order_errors = scalar(
        connection,
        f"""
        WITH ordered_versions AS (
            SELECT
                {key_columns},
                {quoted("scd_version")},
                {quoted("eff_start_date")},
                LAG({quoted("eff_end_date")}) OVER (
                    PARTITION BY {key_columns}
                    ORDER BY {quoted("scd_version")}
                ) AS previous_end_date
            FROM {quoted(curated_table)}
        )
        SELECT COUNT(*)
        FROM ordered_versions
        WHERE previous_end_date IS NOT NULL
          AND {quoted("eff_start_date")} < previous_end_date
        """
    )

    add_result(
        results,
        config["base_table"],
        "effective date ordering",
        date_order_errors == 0,
        f"date_order_errors={date_order_errors}",
    )


def validate_control_tables(
    connection: sqlite3.Connection,
    batch_id: str,
    results: list[dict[str, str]],
) -> None:

    expected_files = (
        set(TABLE_SCHEMAS.keys())
        | set(RISK_01_FILES)
    )

    log_rows = connection.execute(
        """
        SELECT source_file
        FROM migration_log
        WHERE batch_id = ?
        """,
        (batch_id,),
    ).fetchall()

    actual_files = {
        row[0]
        for row in log_rows
    }

    missing_files = sorted(
        expected_files - actual_files
    )

    extra_files = sorted(
        actual_files - expected_files
    )

    file_coverage_passed = (
        not missing_files
        and not extra_files
        and len(actual_files) == 13
    )

    add_result(
        results,
        "migration_log",
        "all source files logged",
        file_coverage_passed,
        (
            f"log_rows={len(actual_files)}, "
            f"missing={missing_files}, "
            f"extra={extra_files}"
        ),
    )

    failed_file_count = scalar(
        connection,
        """
        SELECT COUNT(*)
        FROM migration_log
        WHERE batch_id = ?
          AND status = 'FAILED'
        """,
        (batch_id,),
    )

    add_result(
        results,
        "migration_log",
        "no failed files",
        failed_file_count == 0,
        f"failed_files={failed_file_count}",
    )

    placeholders = ", ".join(
        "?"
        for _ in RISK_01_FILES
    )

    risk_rows = connection.execute(
        f"""
        SELECT
            source_file,
            status,
            risk_flag
        FROM migration_log
        WHERE batch_id = ?
          AND source_file IN ({placeholders})
        """,
        (
            batch_id,
            *sorted(RISK_01_FILES),
        ),
    ).fetchall()

    risk_records = {
        row["source_file"]: {
            "status": row["status"],
            "risk_flag": row["risk_flag"],
        }
        for row in risk_rows
    }

    risk_problems = []

    for filename in sorted(RISK_01_FILES):
        record = risk_records.get(filename)

        if record is None:
            risk_problems.append(
                f"{filename}: missing"
            )

            continue

        if record["status"] != "SKIPPED":
            risk_problems.append(
                f"{filename}: status={record['status']}"
            )

        if record["risk_flag"] != "RISK-01":
            risk_problems.append(
                f"{filename}: risk={record['risk_flag']}"
            )

    risk_coverage_passed = (
        len(risk_records) == 7
        and not risk_problems
    )

    add_result(
        results,
        "migration_log",
        "RISK-01 coverage",
        risk_coverage_passed,
        (
            f"covered={len(risk_records)}/7, "
            f"problems={risk_problems}"
        ),
    )


def save_validation_results(
    connection: sqlite3.Connection,
    batch_id: str,
    results: list[dict[str, str]],
) -> None:

    create_validation_results_table(
        connection
    )

    connection.execute(
        """
        DELETE FROM validation_results
        WHERE batch_id = ?
        """,
        (batch_id,),
    )

    validated_at = datetime.now(
        timezone.utc
    ).isoformat()

    rows_to_insert = [
        (
            batch_id,
            result["table_name"],
            result["validation_name"],
            result["validation_status"],
            result["details"],
            validated_at,
        )
        for result in results
    ]

    connection.executemany(
        """
        INSERT INTO validation_results (
            batch_id,
            table_name,
            validation_name,
            validation_status,
            details,
            validated_at
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        rows_to_insert,
    )


def write_report(
    report_path: Path,
    database_path: Path,
    batch_id: str,
    results: list[dict[str, str]],
) -> None:

    passed_count = sum(
        result["validation_status"] == "PASS"
        for result in results
    )

    failed_count = sum(
        result["validation_status"] == "FAIL"
        for result in results
    )

    overall_status = (
        "PASS"
        if failed_count == 0
        else "FAIL"
    )

    report_lines = [
        "HDMF MIGRATION VALIDATION REPORT",
        "=" * 90,
        f"Database: {database_path.resolve()}",
        f"Batch ID: {batch_id}",
        (
            "Validated at: "
            f"{datetime.now(timezone.utc).isoformat()}"
        ),
        "=" * 90,
    ]

    for result in results:
        report_lines.append(
            f"[{result['validation_status']}] "
            f"{result['table_name']}: "
            f"{result['validation_name']} - "
            f"{result['details']}"
        )

    report_lines.extend(
        [
            "=" * 90,
            "VALIDATION SUMMARY",
            f"Total checks: {len(results)}",
            f"Passed:       {passed_count}",
            f"Failed:       {failed_count}",
            f"Overall:      {overall_status}",
        ]
    )

    report_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    report_path.write_text(
        "\n".join(report_lines) + "\n",
        encoding="utf-8",
    )


def run_validation(
    database_path: Path,
    report_path: Path,
    requested_batch_id: str | None,
) -> None:

    connection = sqlite3.connect(
        database_path
    )

    connection.row_factory = sqlite3.Row

    results: list[dict[str, str]] = []

    try:
        if not table_exists(
            connection,
            "migration_log",
        ):
            raise ValueError(
                "migration_log table does not exist."
            )

        batch_id = resolve_batch_id(
            connection,
            requested_batch_id,
        )

        print("HDMF Migration Validation")
        print("=" * 110)
        print(f"Database: {database_path.resolve()}")
        print(f"Batch ID: {batch_id}")
        print("=" * 110)

        for filename, config in TABLE_SCHEMAS.items():
            if config["table_type"] == "audit":
                validate_audit_table(
                    connection=connection,
                    filename=filename,
                    config=config,
                    batch_id=batch_id,
                    results=results,
                )

            else:
                validate_transaction_table(
                    connection=connection,
                    filename=filename,
                    config=config,
                    batch_id=batch_id,
                    results=results,
                )

        validate_control_tables(
            connection=connection,
            batch_id=batch_id,
            results=results,
        )

        save_validation_results(
            connection=connection,
            batch_id=batch_id,
            results=results,
        )

        connection.commit()

        write_report(
            report_path=report_path,
            database_path=database_path,
            batch_id=batch_id,
            results=results,
        )

        passed_count = sum(
            result["validation_status"] == "PASS"
            for result in results
        )

        failed_count = sum(
            result["validation_status"] == "FAIL"
            for result in results
        )

        overall_status = (
            "PASS"
            if failed_count == 0
            else "FAIL"
        )

        print("=" * 110)
        print("Validation Summary")
        print(f"Total checks: {len(results)}")
        print(f"Passed:       {passed_count}")
        print(f"Failed:       {failed_count}")
        print(f"Overall:      {overall_status}")
        print(f"Report file:  {report_path}")

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the HDMF SQLite migration output."
        )
    )

    parser.add_argument(
        "--db",
        default="hdmf_migration_output.db",
        help="SQLite database path.",
    )

    parser.add_argument(
        "--report",
        default="validation_report.txt",
        help="Validation report output path.",
    )

    parser.add_argument(
        "--batch-id",
        default=None,
        help=(
            "Batch ID to validate. When omitted, "
            "the single batch in migration_log is used."
        ),
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    database_path = Path(
        arguments.db
    )

    report_path = Path(
        arguments.report
    )

    if not database_path.exists():
        raise FileNotFoundError(
            f"Database does not exist: {database_path}"
        )

    run_validation(
        database_path=database_path,
        report_path=report_path,
        requested_batch_id=arguments.batch_id,
    )


if __name__ == "__main__":
    main()
