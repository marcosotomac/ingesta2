import csv
import logging
import os
from contextlib import closing
from datetime import datetime, timedelta

import boto3
import mysql.connector
from mysql.connector import Error


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def get_db_config() -> dict:
    """Read database connection parameters from environment variables."""
    try:
        user = os.environ["MYSQL_USER"]
        password = os.environ["MYSQL_PASSWORD"]
        database = os.environ["MYSQL_DATABASE"]
    except KeyError as missing:
        raise RuntimeError(f"Missing required environment variable: {missing}") from missing

    return {
        "host": os.environ.get("MYSQL_HOST", "localhost"),
        "port": int(os.environ.get("MYSQL_PORT", "3306")),
        "user": user,
        "password": password,
        "database": database,
    }


def get_table_name() -> str:
    table_name = os.environ.get("MYSQL_TABLE")
    if not table_name:
        raise RuntimeError("Environment variable MYSQL_TABLE must be set with the table to export")
    return table_name


def should_seed_mock_data() -> bool:
    flag = os.environ.get("SEED_MOCK_DATA", "true").lower()
    return flag in {"1", "true", "yes"}


def generate_mock_value(column_type: str, column_name: str, row_index: int):
    """Produce a deterministic mock value based on MySQL column type."""
    column_type = column_type.lower()
    ordinal = row_index + 1

    if "int" in column_type and "unsigned" in column_type:
        return ordinal
    if column_type.startswith("int") or "bigint" in column_type or "smallint" in column_type:
        return ordinal
    if "decimal" in column_type or "numeric" in column_type or "float" in column_type or "double" in column_type:
        return float(ordinal)
    if "bool" in column_type or column_type == "tinyint(1)":
        return row_index % 2
    if "date" in column_type and "time" not in column_type:
        base_date = datetime(2024, 1, 1) + timedelta(days=row_index)
        return base_date.date().isoformat()
    if "datetime" in column_type or "timestamp" in column_type or "time" in column_type:
        base_datetime = datetime(2024, 1, 1, 12, 0, 0) + timedelta(hours=row_index)
        return base_datetime.strftime("%Y-%m-%d %H:%M:%S")
    if "char" in column_type or "text" in column_type:
        return f"mock_{column_name}_{ordinal}"
    if "json" in column_type:
        return f'{{"mock": "{column_name}_{ordinal}"}}'
    return f"mock_{column_name}_{ordinal}"


def seed_mock_data(connection: mysql.connector.MySQLConnection, table_name: str, rows_to_insert: int = 3) -> None:
    """Populate the target table with deterministic mock data when it is empty."""
    sanitized_table = table_name.replace("`", "``")

    with closing(connection.cursor(dictionary=True)) as cursor:
        cursor.execute(f"SELECT COUNT(*) AS count FROM `{sanitized_table}`")
        existing_rows = cursor.fetchone()["count"]
        if existing_rows:
            logging.info("Skipping mock data insert for %s; table already has %d rows", table_name, existing_rows)
            return

        cursor.execute(f"DESCRIBE `{sanitized_table}`")
        column_metadata = cursor.fetchall()

        insertable_columns = [
            column for column in column_metadata if "auto_increment" not in column.get("Extra", "")
        ]

        if not insertable_columns:
            logging.warning("No columns available for inserting mock data into %s", table_name)
            return

        column_names = [col["Field"] for col in insertable_columns]
        placeholders = ", ".join(["%s"] * len(column_names))
        column_list = ", ".join(f"`{name.replace('`', '``')}`" for name in column_names)
        insert_query = f"INSERT INTO `{sanitized_table}` ({column_list}) VALUES ({placeholders})"

        mock_rows = []
        for row_index in range(rows_to_insert):
            row_values = [
                generate_mock_value(column["Type"], column["Field"], row_index)
                for column in insertable_columns
            ]
            mock_rows.append(tuple(row_values))

        cursor.executemany(insert_query, mock_rows)
        connection.commit()
        logging.info("Inserted %d mock rows into %s", len(mock_rows), table_name)


def fetch_table_data(connection: mysql.connector.MySQLConnection, table_name: str):
    """Fetch all rows from a table, returning column names and data."""
    sanitized_table = table_name.replace("`", "``")
    query = f"SELECT * FROM `{sanitized_table}`"

    with closing(connection.cursor()) as cursor:
        cursor.execute(query)
        column_names = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        return column_names, rows


def write_csv(file_path: str, columns, rows) -> None:
    """Write rows to CSV including headers."""
    with open(file_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(columns)
        writer.writerows(rows)


def upload_to_s3(file_path: str, bucket: str, object_key: str) -> None:
    s3_client = boto3.client("s3")
    s3_client.upload_file(file_path, bucket, object_key)


def main() -> None:
    output_file = os.environ.get("OUTPUT_CSV", "ingesta_mysql.csv")
    s3_bucket = os.environ.get("S3_BUCKET", "gcr-output-01")
    s3_key = os.environ.get("S3_OBJECT_KEY", os.path.basename(output_file))

    logging.info("Starting data extraction from MySQL")
    try:
        connection = mysql.connector.connect(**get_db_config())
    except Error as exc:
        raise RuntimeError(f"Could not connect to MySQL: {exc}") from exc

    try:
        table_name = get_table_name()
        if should_seed_mock_data():
            try:
                rows_to_insert = int(os.environ.get("MOCK_ROWS", "3"))
            except ValueError:
                rows_to_insert = 3
            seed_mock_data(connection, table_name, rows_to_insert)
        columns, rows = fetch_table_data(connection, table_name)
        logging.info("Fetched %d rows from table %s", len(rows), table_name)
        write_csv(output_file, columns, rows)
        logging.info("Wrote data to %s", output_file)
        upload_to_s3(output_file, s3_bucket, s3_key)
        logging.info("Uploaded %s to bucket %s as %s", output_file, s3_bucket, s3_key)
    finally:
        connection.close()
        logging.info("Closed MySQL connection")


if __name__ == "__main__":
    main()
