"""Spark 3.5.7 catalog adapter, importable by its Python 3.8 driver.

No supplied SQL is accepted. Descriptor validation and the operation coordinator
must run before these methods. Only actual catalog and Parquet reads are used.
"""
import re
from datetime import date, datetime

from .real_hive_contract import GROUP_COLUMNS, properties, validate_descriptor


def quoted(value):
    return "'" + value.replace("'", "''") + "'"


def field_type(name):
    if name in {"source", "app", "channel", "observation_d1", "observation_d7"}:
        return "string"
    if name in {"date", "business_date", "cohort_date"}:
        return "date"
    return "double" if name == "duration_seconds" else "bigint"


def describe_columns(rows):
    """Use DESCRIBE's stored schema; Catalog.listColumns resolves table files."""
    columns, partitions, section = [], [], "columns"
    for row in rows:
        name, kind = row.col_name.strip(), row.data_type.strip()
        if name == "# Partition Information":
            section = "partitions"
            continue
        if not name or name.startswith("# Detailed"):
            break
        if name.startswith("#"):
            continue
        if kind != field_type(name):
            raise ValueError("Hive aggregate column types changed")
        (columns if section == "columns" else partitions).append(name)
    return columns, partitions


def partition_location(rows, table_location):
    # Spark 3.5.7 emits both the partition Location and the table storage
    # Location. Never select the last occurrence and accidentally validate the
    # table root while ignoring an escaped partition location.
    section, partition, table = None, [], []
    for row in rows:
        name = row.col_name.strip()
        if name == "# Detailed Partition Information":
            section = "partition"
        elif name == "# Storage Information":
            section = "table"
        elif name == "Location":
            if section == "partition":
                partition.append(row.data_type.strip())
            elif section == "table":
                table.append(row.data_type.strip())
    if len(partition) != 1 or table != [table_location]:
        raise ValueError("Missing exact Hive partition/table storage locations")
    return partition[0]


class SparkHiveCatalog:
    def __init__(self, spark):
        self.spark = spark

    def names(self, prefix):
        if not self.spark.catalog.databaseExists("snow_real"):
            return []
        rows = self.spark.sql("SHOW TABLES IN snow_real").limit(10001).collect()
        if len(rows) > 10000:
            raise ValueError("Hive table inventory exceeds the bounded catalog scope")
        return ["snow_real." + row.tableName for row in rows if ("snow_real." + row.tableName).startswith(prefix)]

    def inspect(self, table):
        if not self.spark.catalog.tableExists(table):
            return None
        information = {}
        described = self.spark.sql("DESCRIBE EXTENDED " + table).collect()
        for row in described:
            if row.col_name.strip() in {"Type", "Provider", "Location"}:
                information[row.col_name.strip()] = row.data_type.strip()
        selected = {}
        for row in self.spark.sql("SHOW TBLPROPERTIES " + table).collect():
            if row.key.startswith("snow.") or row.key == "external.table.purge":
                selected[row.key] = row.value
            if row.key.lower() in {"auto.purge", "external.table.purge"} and row.value.lower() not in {"false", "0"}:
                raise ValueError("Catalog table may not enable payload purge")
        columns, partitions = describe_columns(described)
        locations = {}
        if partitions:
            if partitions != ["source", "business_date"]:
                raise ValueError("Unknown partition schema")
            rows = self.spark.sql("SHOW PARTITIONS " + table).limit(10001).collect()
            if len(rows) > 10000:
                raise ValueError("Unbounded partition catalog")
            for row in rows:
                partition = row[0]
                if not re.fullmatch(r"source=real/business_date=\d{4}-\d{2}-\d{2}", partition):
                    raise ValueError("Catalog partition is outside the real source allowlist")
                day = partition.split("=")[-1]
                date.fromisoformat(day)
                details = self.spark.sql("DESCRIBE EXTENDED " + table + " PARTITION (source='real', business_date=" + quoted(day) + ")").collect()
                locations[partition] = partition_location(details, information.get("Location"))
        return dict(type=information.get("Type"), provider=information.get("Provider", ""),
                    location=information.get("Location"), properties=selected,
                    columns=columns, partition_columns=partitions, partitions=locations)

    def create(self, table, descriptor):
        validate_descriptor(table, descriptor)
        # The DB is shared metadata, never dropped by this adapter. Its first
        # location is an empty owned directory, never used for managed tables.
        location = descriptor["location"]
        self.spark.sql("CREATE DATABASE IF NOT EXISTS snow_real LOCATION " + quoted(location.rsplit("/", 1)[0] + "/_catalog"))
        frame = self.spark.read.parquet(location)
        expected = GROUP_COLUMNS[descriptor["group"]]
        if descriptor["group"] == "daily":
            expected = expected - {"date"} | {"business_date"}
        if set(frame.columns) != expected or len(frame.columns) != len(expected):
            raise ValueError("Original Parquet schema has unexpected row-level fields")
        if any(field.dataType.simpleString() != field_type(field.name) for field in frame.schema.fields):
            raise ValueError("Only fixed primitive aggregate types may enter Hive")
        fields = ", ".join("`" + field.name + "` " + field_type(field.name) for field in frame.schema.fields)
        props = ", ".join(quoted(key) + "=" + quoted(value) for key, value in properties(descriptor).items())
        partition = " PARTITIONED BY (source, business_date)" if descriptor["group"] == "daily" else ""
        self.spark.sql("CREATE TABLE " + table + " (" + fields + ") USING PARQUET" + partition + " LOCATION " + quoted(location) + " TBLPROPERTIES (" + props + ")")
        if descriptor["group"] == "daily":
            self.spark.sql("MSCK REPAIR TABLE " + table)

    def drop(self, table):
        # Coordinator already checked external type, no-purge flags and exact
        # owner/partition locations. Never use PURGE or DROP DATABASE/CASCADE.
        self.spark.sql("DROP TABLE " + table)

    def rows(self, table, descriptor):
        frame = self.spark.table(table)
        rows = []
        for item in frame.limit(descriptor["expected_rows"] + 1).collect():
            row = item.asDict()
            if descriptor["group"] == "daily":
                row["date"] = row.pop("business_date")
            for key, value in row.items():
                if isinstance(value, (date, datetime)):
                    row[key] = value.isoformat()
            rows.append(row)
        return rows
