import argparse
import random
import shutil
from concurrent.futures.process import ProcessPoolExecutor
from itertools import repeat
from pathlib import Path

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from lakeview.utils import timeit


def generate_sample_data(num_rows: int, batch_id: int = 0) -> pa.Table:
    address_type = pa.struct(
        [
            pa.field("country", pa.string()),
            pa.field("zipcode", pa.string()),
        ]
    )

    nested_data = [
        [
            {"country": "USA", "zipcode": "12345"},
            {"country": "Canada", "zipcode": "A1B2C3"},
        ]
        for _ in range(num_rows)
    ]

    table = pa.table(
        {
            "id": pa.array(
                range(batch_id * num_rows, (batch_id + 1) * num_rows)
            ),
            "batch_id": pa.array([batch_id] * num_rows),
            "value": pa.array([f"value_{i}" for i in range(num_rows)]),
            "amount": pa.array(
                [float(i * 1.5) for i in range(num_rows)], type=pa.float64()
            ),
            "addresses": pa.array(nested_data, type=pa.list_(address_type)),
            "phone_number": pa.array(
                [
                    {"country_code": "+1", "number": f"555-{i:04d}"}
                    for i in range(num_rows)
                ],
                type=pa.struct(
                    [
                        pa.field("country_code", pa.string()),
                        pa.field("number", pa.string()),
                    ]
                ),
            ),
        }
    )

    return table


@timeit
def create_fragmented_table(
    base_path: Path,
    num_batches: int = 1000,
    max_rows_per_batch: int = 100,
    table_suffix: str = "",
    overwrite: bool = False,
) -> Path:
    table_path = base_path / f"fragmented{table_suffix}"
    if table_path.exists():
        if overwrite:
            shutil.rmtree(table_path)
        else:
            return table_path
    table_path.mkdir(parents=True)

    table_uri = str(table_path)

    rows_per_batch = int(max_rows_per_batch * random.random())
    for batch_id in range(num_batches):
        data = generate_sample_data(rows_per_batch, batch_id)
        write_deltalake(table_uri, data.to_reader(), mode="append")

    # Perform some delete operations to add tombstones
    dt = DeltaTable(table_uri)

    # Delete rows where batch_id is divisible by 4
    for batch_id in range(0, num_batches, 4):
        dt.delete(f"batch_id = {batch_id}")

    print(f"Created fragmented table at {table_path}")
    print(f"  - Files: {len(dt.file_uris())}")
    print(f"  - Version: {dt.version()}")

    return table_path


@timeit
def create_compacted_table(
    base_path: Path,
    num_batches: int = 100,
    max_rows_per_batch: int = 100,
    table_suffix: str = "",
    overwrite: bool = False,
) -> Path:
    table_path = base_path / f"compacted{table_suffix}"
    if table_path.exists():
        if overwrite:
            shutil.rmtree(table_path)
        else:
            return table_path
    table_path.mkdir(parents=True)

    table_uri = str(table_path)

    rows_per_batch = int(max_rows_per_batch * random.random())
    for batch_id in range(num_batches):
        data = generate_sample_data(rows_per_batch, batch_id)
        write_deltalake(table_uri, data, mode="append")

    dt = DeltaTable(table_uri)

    for batch_id in range(0, num_batches, 4):
        dt.delete(f"batch_id = {batch_id}")

    dt.optimize.compact()

    dt.vacuum(
        retention_hours=0, enforce_retention_duration=False, dry_run=False
    )

    print(f"Created compacted table at {table_path}")
    print(f"  - Files: {len(dt.file_uris())}")
    print(f"  - Version: {dt.version()}")

    return table_path


@timeit
def create_uniform_table(
    base_path: Path,
    num_files: int = 100,
    max_rows_per_file: int = 100,
    table_suffix: str = "",
    overwrite: bool = False,
) -> Path:
    table_path = base_path / f"uniform{table_suffix}"
    if table_path.exists():
        if overwrite:
            shutil.rmtree(table_path)
        else:
            return table_path

    table_path.mkdir(parents=True)

    table_uri = str(table_path)

    rows_per_file = int(max_rows_per_file * random.random())
    for batch_id in range(num_files):
        data = generate_sample_data(rows_per_file, batch_id)
        write_deltalake(table_uri, data, mode="append")

    dt = DeltaTable(table_uri)

    print(f"Created uniform table at {table_path}")
    print(f"  - Files: {len(dt.file_uris())}")
    print(f"  - Version: {dt.version()}")

    return table_path


@timeit
def create_partitioned_table(
    base_path: Path,
    num_partitions: int = 5,
    max_rows_per_partition: int = 10_000,
    table_suffix: str = "",
    overwrite: bool = False,
) -> Path:
    table_path = base_path / f"partitioned{table_suffix}"
    if table_path.exists():
        if overwrite:
            shutil.rmtree(table_path)
        else:
            return table_path
    table_path.mkdir(parents=True)

    table_uri = str(table_path)
    for partition_id in range(num_partitions):
        rows_per_partition = int(max_rows_per_partition * random.random())
        address_type = pa.struct(
            [
                pa.field("country", pa.string()),
                pa.field("zipcode", pa.string()),
            ]
        )

        nested_data = [
            [
                {"country": "USA", "zipcode": "12345"},
                {"country": "Canada", "zipcode": "A1B2C3"},
            ]
            for _ in range(rows_per_partition)
        ]
        data = pa.table(
            {
                "id": pa.array(
                    range(
                        partition_id * rows_per_partition,
                        (partition_id + 1) * rows_per_partition,
                    )
                ),
                "category": pa.array(
                    [f"category_{partition_id}"] * rows_per_partition
                ),
                "value": pa.array(
                    [f"value_{i}" for i in range(rows_per_partition)]
                ),
                "amount": pa.array(
                    [float(i * 1.5) for i in range(rows_per_partition)],
                    type=pa.float64(),
                ),
                "addresses": pa.array(nested_data, type=pa.list_(address_type)),
                "phone_number": pa.array(
                    [
                        {"country_code": "+1", "number": f"555-{i:04d}"}
                        for i in range(rows_per_partition)
                    ],
                    type=pa.struct(
                        [
                            pa.field("country_code", pa.string()),
                            pa.field("number", pa.string()),
                        ]
                    ),
                ),
            }
        )
        write_deltalake(
            table_uri, data, mode="append", partition_by=["category"]
        )

    dt = DeltaTable(table_uri)

    print(f"Created partitioned table at {table_path}")
    print(f"  - Files: {len(dt.file_uris())}")
    print(f"  - Version: {dt.version()}")

    return table_path


def create_all_tables(
    suffix: str,
    output_dir: Path,
    operation_limit: int,
    overwrite: bool = False,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    create_fragmented_table(
        output_dir,
        num_batches=operation_limit,
        table_suffix=suffix,
        overwrite=overwrite,
    )
    create_compacted_table(
        output_dir,
        num_batches=operation_limit,
        table_suffix=suffix,
        overwrite=overwrite,
    )
    create_uniform_table(
        output_dir,
        num_files=operation_limit,
        table_suffix=suffix,
        overwrite=overwrite,
    )
    create_partitioned_table(
        output_dir, table_suffix=suffix, overwrite=overwrite
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate delta table variants for testing and development"
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("tests/data/delta"),
        help="Output directory for generated tables (default: tests/data)",
    )
    parser.add_argument(
        "-t",
        "--table-type",
        choices=["all", "fragmented", "compacted", "uniform", "partitioned"],
        default="all",
        help="Type of table to generate (default: all)",
    )

    parser.add_argument(
        "-n",
        "--n-tables",
        default=1,
        help="Number of tables to generate",
    )

    parser.add_argument(
        "-l",
        "--operation-limit",
        default=100,
        help="Number of operations",
    )

    parser.add_argument(
        "-f",
        "--overwrite",
        action="store_true",
        help="Overwrite existing table",
    )
    args = parser.parse_args()

    print(f"Generating delta tables in: {args.output_dir.absolute()}")
    print("-" * 50)

    if args.table_type == "all":
        with ProcessPoolExecutor(max_workers=10) as executor:
            table_list = [f"{i:02d}" for i in range(int(args.n_tables))]
            executor.map(
                create_all_tables,
                table_list,
                repeat(args.output_dir),
                repeat(int(args.operation_limit)),
                repeat(args.overwrite),
            )

    elif args.table_type == "fragmented":
        create_fragmented_table(args.output_dir)
    elif args.table_type == "compacted":
        create_compacted_table(args.output_dir)
    elif args.table_type == "uniform":
        create_uniform_table(args.output_dir)
    elif args.table_type == "partitioned":
        create_partitioned_table(args.output_dir)

    print("-" * 50)
    print("Done!")


if __name__ == "__main__":
    main()
