from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Iterator


HEADER_SIZE = 56
ALIGNMENT = 8
RECORD_MARKER = b"\x3d\x00"

VALID_OPERATIONS = {
    "append",
    "repold",
    "repnew",
    "delete",
}


class TrlParseError(ValueError):
    """Raised when a TRL record fails structural validation."""


def decode_text(raw_bytes: bytes) -> str:
    return (
        raw_bytes
        .decode("latin-1")
        .rstrip("\x00 ")
        .strip()
    )


def align_offset(offset: int) -> int:
    return (
        (offset + ALIGNMENT - 1)
        // ALIGNMENT
    ) * ALIGNMENT


def parse_header(record: bytes) -> dict[str, Any]:
    if len(record) < HEADER_SIZE:
        raise TrlParseError(
            f"Record is shorter than the "
            f"{HEADER_SIZE}-byte header."
        )

    if record[0:2] != RECORD_MARKER:
        raise TrlParseError(
            f"Invalid record marker: "
            f"{record[0:2].hex()}"
        )

    year = int.from_bytes(
        record[2:4],
        byteorder="little"
    )

    month = record[4]
    day = record[6]

    try:
        source_date = date(
            year,
            month,
            day
        ).isoformat()

    except ValueError as error:
        raise TrlParseError(
            f"Invalid source date: "
            f"{year}-{month}-{day}"
        ) from error

    transaction_sequence = int.from_bytes(
        record[8:12],
        byteorder="little"
    )

    source_user = decode_text(
        record[12:44]
    )

    operation = decode_text(
        record[44:50]
    ).lower()

    if operation not in VALID_OPERATIONS:
        raise TrlParseError(
            f"Invalid CDC operation: "
            f"{operation!r}"
        )

    return {
        "_transaction_seq": transaction_sequence,
        "_source_user": source_user,
        "_source_ts": source_date,
        "_cdc_op": operation,
    }


def parse_fields(
    record: bytes,
    columns: list[str]
) -> dict[str, str | None]:

    current_offset = HEADER_SIZE
    parsed_fields = {}

    for column_name in columns:
        if current_offset + 2 > len(record):
            raise TrlParseError(
                f"Missing length prefix for "
                f"{column_name} at byte "
                f"{current_offset}."
            )

        declared_length = int.from_bytes(
            record[
                current_offset:
                current_offset + 2
            ],
            byteorder="little"
        )

        value_start = current_offset + 2
        value_end = value_start + declared_length

        if value_end > len(record):
            raise TrlParseError(
                f"{column_name} declares "
                f"{declared_length} bytes, "
                f"which exceeds the record."
            )

        value = decode_text(
            record[value_start:value_end]
        )

        parsed_fields[column_name] = (
            value if value else None
        )

        current_offset = align_offset(
            value_end
        )

    return parsed_fields


def parse_record(
    record: bytes,
    config: dict[str, Any],
    stage_seq: int
) -> dict[str, Any]:

    expected_stride = config["stride"]

    if len(record) != expected_stride:
        raise TrlParseError(
            f"Record {stage_seq} contains "
            f"{len(record)} bytes; expected "
            f"{expected_stride}."
        )

    header = parse_header(record)

    fields = parse_fields(
        record,
        config["columns"]
    )

    return {
        **fields,
        "_stage_seq": stage_seq,
        **header,
    }


def iter_records(
    file_path: Path,
    config: dict[str, Any]
) -> Iterator[dict[str, Any]]:

    file_size = file_path.stat().st_size
    stride = config["stride"]

    if file_size == 0:
        return

    remaining_bytes = file_size % stride

    if remaining_bytes != 0:
        raise TrlParseError(
            f"{file_path.name}: file size "
            f"{file_size} is not divisible "
            f"by stride {stride}. "
            f"Remainder: {remaining_bytes}"
        )

    with file_path.open("rb") as file:
        stage_seq = 0

        while True:
            record = file.read(stride)

            if not record:
                break

            yield parse_record(
                record,
                config,
                stage_seq
            )

            stage_seq += 1
