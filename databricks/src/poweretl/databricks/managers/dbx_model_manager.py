# pylint: disable=R0914, W0718, R0912, R0915, R1724
import json

from poweretl.common import BaseModelManager
from poweretl.defs import IMetaProvider
from poweretl.defs.model import Column, Columns, NameValue, NameValues, Table
from pyspark.sql import SparkSession  # pylint: disable=C0411


class DbxModelManager(BaseModelManager):

    def __init__(self, spark: SparkSession, meta_provider: IMetaProvider):
        super().__init__(meta_provider=meta_provider)
        self._spark = spark

    def _execute_command(self, command: str):
        self._spark.sql(command)

    def get_table_model_from_source(self, table_name: str) -> Table:
        """Get table model from Databricks source by
        querying the actual table metadata."""
        try:

            # Single query to get all information
            describe_df = self._spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}")
            describe_rows = describe_df.collect()

            # Parse all information from DESCRIBE TABLE EXTENDED
            columns_dict = {}
            external_location = None
            comment = None
            properties = {}
            tags = {}
            clustering_columns = None
            settings = {}

            in_clustering_info = False
            in_detailed_info = False

            for row in describe_rows:
                col_name = row["col_name"].strip() if row["col_name"] else ""
                data_type = row["data_type"].strip() if row["data_type"] else ""
                col_comment = (
                    row["comment"]
                    if "comment" in row.asDict() and row["comment"]
                    else None
                )

                # Check for section markers
                if col_name == "# Clustering Information":
                    in_clustering_info = True
                    continue
                elif col_name == "# Detailed Table Information":
                    in_detailed_info = True
                    in_clustering_info = False
                    continue
                elif col_name in ["", "# col_name"]:
                    continue

                # Parse columns (before detailed info section)
                if (
                    not in_detailed_info
                    and not in_clustering_info
                    and col_name
                    and data_type
                ):
                    column = Column(
                        name=col_name,
                        type=data_type,
                        comment=col_comment,
                        tags=NameValues(items={}),
                    )
                    columns_dict[col_name] = column

                # Parse detailed table information
                elif in_detailed_info:
                    if col_name == "Location":
                        external_location = data_type if data_type else None
                    elif col_name == "Comment":
                        comment = data_type if data_type else None
                    elif col_name == "Table Properties":
                        # Table Properties section exists but we'll read properties from
                        # SHOW TBLPROPERTIES instead
                        pass

            # Get all table properties from SHOW TBLPROPERTIES
            tblproperties_df = self._spark.sql(f"SHOW TBLPROPERTIES {table_name}")
            for row in tblproperties_df.collect():
                key = row["key"] if "key" in row.asDict() else None
                value = row["value"] if "value" in row.asDict() else None

                if key:
                    # Handle clusteringColumns specially
                    if key == "clusteringColumns":
                        # Parse JSON array format [["col1"],["col2"]]
                        # and convert to (col1, col2)
                        # Remove escaped quotes if present
                        value_clean = value.replace('""', '"')
                        cols_array = json.loads(value_clean)
                        col_names = [
                            col[0] if isinstance(col, list) else col
                            for col in cols_array
                        ]
                        clustering_columns = f"({', '.join(col_names)})"

                    # Add all properties to the properties dict
                    properties[key] = NameValue(name=key, value=value)

            # Get column tags from INFORMATION_SCHEMA.COLUMN_TAGS
            # Extract catalog, schema, and table name from full table name
            table_parts = table_name.split(".")
            if len(table_parts) == 3:
                catalog_name, schema_name, table_only = table_parts
            else:
                raise ValueError(
                    f"Table name must be in format 'catalog.schema.table', got: {table_name}"  # noqa:E501, pylint: disable=C0301
                )

            column_tags_df = self._spark.sql(
                f"""
                SELECT column_name, tag_name, tag_value
                FROM {catalog_name}.INFORMATION_SCHEMA.COLUMN_TAGS
                WHERE catalog_name = '{catalog_name}'
                  AND schema_name = '{schema_name}'
                  AND table_name = '{table_only}'
            """
            )

            for row in column_tags_df.collect():
                column_name = (
                    row["column_name"] if "column_name" in row.asDict() else None
                )
                tag_name = row["tag_name"] if "tag_name" in row.asDict() else None
                tag_value = row["tag_value"] if "tag_value" in row.asDict() else None

                if column_name in columns_dict and tag_name:
                    columns_dict[column_name].tags.items[tag_name] = NameValue(
                        name=tag_name, value=tag_value
                    )

            # Get table tags from INFORMATION_SCHEMA.TABLE_TAGS
            tags_df = self._spark.sql(
                f"""
                SELECT tag_name, tag_value
                FROM {catalog_name}.INFORMATION_SCHEMA.TABLE_TAGS
                WHERE catalog_name = '{catalog_name}'
                  AND schema_name = '{schema_name}'
                  AND table_name = '{table_only}'
            """
            )

            for row in tags_df.collect():
                tag_name = row["tag_name"] if "tag_name" in row.asDict() else None
                tag_value = row["tag_value"] if "tag_value" in row.asDict() else None
                if tag_name:
                    tags[tag_name] = NameValue(name=tag_name, value=tag_value)

            # Add clustering columns as a setting if present
            if clustering_columns:
                settings["CLUSTER BY"] = NameValue(
                    name="CLUSTER BY", value=clustering_columns
                )

            # Create and return Table model
            table = Table(
                name=table_name,
                external_location=external_location,
                comment=comment,
                columns=Columns(items=columns_dict),
                tags=NameValues(items=tags),
                properties=NameValues(items=properties),
                settings=NameValues(items=settings),
            )

            return table

        except Exception:
            # If any error occurs (table doesn't exist, permission issues, etc.),
            # return None
            return None
