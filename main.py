from __future__ import annotations

import argparse
import uuid

from pathlib import Path

from audit_loader import run_audit_loader
from migration_log import generate_migration_log
from staging import load_all_staging_tables
from transaction_loader import run_transaction_loader
from validation import run_validation


DEFAULT_DATABASE = "hdmf_migration_output.db"
DEFAULT_REPORT = "validation_report.txt"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the complete HDMF Actian TRL "
            "to SQLite migration pipeline."
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
        default=DEFAULT_DATABASE,
        help="SQLite output database path.",
    )

    parser.add_argument(
        "--report",
        default=DEFAULT_REPORT,
        help="Validation report output path.",
    )

    parser.add_argument(
        "--batch-id",
        default=None,
        help=(
            "Optional batch identifier. "
            "A UUID is generated when omitted."
        ),
    )

    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete the existing output database before running.",
    )

    return parser.parse_args()


def main() -> None:
    arguments = parse_arguments()

    data_directory = Path(arguments.data_directory)
    database_path = Path(arguments.db)
    report_path = Path(arguments.report)

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

    print("\nHDMF COMPLETE MIGRATION PIPELINE")
    print("=" * 90)
    print(f"Data directory: {data_directory.resolve()}")
    print(f"Database:       {database_path.resolve()}")
    print(f"Report:         {report_path.resolve()}")
    print(f"Batch ID:       {batch_id}")
    print("=" * 90)

    print("\nSTEP 1 — Load staging tables")
    load_all_staging_tables(
        data_directory=data_directory,
        database_path=database_path,
        batch_id=batch_id,
    )

    print("\nSTEP 2 — Load audit table")
    run_audit_loader(
        database_path=database_path,
        requested_batch_id=batch_id,
    )

    print("\nSTEP 3 — Apply SCD Type 2")
    run_transaction_loader(
        database_path=database_path,
        requested_batch_id=batch_id,
    )

    print("\nSTEP 4 — Generate migration log")
    generate_migration_log(
        database_path=database_path,
        data_directory=data_directory,
        requested_batch_id=batch_id,
    )

    print("\nSTEP 5 — Run validation")
    run_validation(
        database_path=database_path,
        report_path=report_path,
        requested_batch_id=batch_id,
    )

    print("\n" + "=" * 90)
    print("HDMF MIGRATION COMPLETED")
    print(f"Database: {database_path}")
    print(f"Report:   {report_path}")
    print(f"Batch ID: {batch_id}")
    print("=" * 90)


if __name__ == "__main__":
    main()
