# flake8: noqa: E501
# pylint: disable=C0301

from poweretl.databricks.helpers import DbxSchemaReader, get_or_connect


def test_dbx_schema_reader_int():
    session = get_or_connect()
    reader = DbxSchemaReader(spark=session.spark)
    catalogs = reader.get_catalogs(catalog_name_like="%sys%").collect()
    for row in catalogs:
        print(f"Catalog: {row['catalog_name']}")

    print("----------------------")
    schemas = reader.get_schemas(catalog_name="system").collect()
    for row in schemas:
        print(f"Schema: {row['schema_name']}")

    print("----------------------")
    tables = reader.get_tables(
        catalog_name="system", table_type=None, table_name_like="%job%"
    ).collect()
    for row in tables:
        print(
            f"Table: {row['table_name']}, Schema: {row['table_schema']}, Type: {row['table_type']}, Tags: {row['tags']}"
        )

    print("----------------------")
    columns = reader.get_columns(
        catalog_name="system", schema_name="lakeflow", table_name="jobs"
    ).collect()
    for row in columns:
        print(
            f"Column: {row['column_name']}, Table: {row['table_name']}, Schema: {row['table_schema']}, Type: {row['data_type']}, Tags: {row['tags']}"
        )


if __name__ == "__main__":
    test_dbx_schema_reader_int()
