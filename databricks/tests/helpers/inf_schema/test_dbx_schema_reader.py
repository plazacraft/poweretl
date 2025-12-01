import os
from poweretl.databricks.helpers import get_or_connect, DbxSchemaReader



def test_dbx_schema_reader_int():
    session = get_or_connect()
    reader = DbxSchemaReader(spark=session.spark)
    catalogs = reader.get_catalogs(catalog_name_like="%sys%").collect()
    for row in catalogs:
        print(f"Catalog: {row['catalog_name']}")

    print ("----------------------")
    schemas = reader.get_schemas(catalog_name="system").collect()
    for row in schemas:
        print(f"Schema: {row['schema_name']}")

    print ("----------------------")
    tables = reader.get_tables(catalog_name="system", table_type=None, table_name_like="%job%").collect()
    for row in tables:
        print(f"Table: {row['table_name']}, Schema: {row['table_schema']}, Type: {row['table_type']}, Tags: {row['tags']}")


if (__name__ == "__main__"):
    test_dbx_schema_reader_int()