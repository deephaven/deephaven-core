#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
""" This module adds Iceberg table support into Deephaven. """
from typing import List, Optional, Union, Dict, Sequence

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.experimental import s3
from deephaven.table import Table, TableDefinition, TableDefinitionLike

from deephaven.jcompat import j_hashmap

_JIcebergReadInstructions = jpy.get_type("io.deephaven.iceberg.util.IcebergReadInstructions")
_JIcebergParquetWriteInstructions = jpy.get_type("io.deephaven.iceberg.util.IcebergParquetWriteInstructions")
_JIcebergCatalogAdapter = jpy.get_type("io.deephaven.iceberg.util.IcebergCatalogAdapter")
_JIcebergTools = jpy.get_type("io.deephaven.iceberg.util.IcebergTools")
_JIcebergAppend = jpy.get_type("io.deephaven.iceberg.util.IcebergAppend")
_JIcebergOverwrite = jpy.get_type("io.deephaven.iceberg.util.IcebergOverwrite")
_JIcebergWriteDataFile = jpy.get_type("io.deephaven.iceberg.util.IcebergWriteDataFiles")

# IcebergToolsS3 is an optional library
try:
    _JIcebergToolsS3 = jpy.get_type("io.deephaven.iceberg.util.IcebergToolsS3")
except Exception:
    _JIcebergToolsS3 = None

_JNamespace = jpy.get_type("org.apache.iceberg.catalog.Namespace")
_JTableIdentifier = jpy.get_type("org.apache.iceberg.catalog.TableIdentifier")
_JSnapshot = jpy.get_type("org.apache.iceberg.Snapshot")


class IcebergReadInstructions(JObjectWrapper):
    """
    This class specifies the instructions for reading an Iceberg table into Deephaven. These include column rename
    instructions and table definitions, as well as special data instructions for loading data files from the cloud.
    """

    j_object_type = _JIcebergReadInstructions

    def __init__(self,
                 table_definition: Optional[TableDefinitionLike] = None,
                 data_instructions: Optional[s3.S3Instructions] = None,
                 column_renames: Optional[Dict[str, str]] = None):
        """
        Initializes the instructions using the provided parameters.

        Args:
            table_definition (Optional[TableDefinitionLike]): the table definition; if omitted,
                the definition is inferred from the Iceberg schema. Setting a definition guarantees the returned table
                will have that definition. This is useful for specifying a subset of the Iceberg schema columns.
            data_instructions (Optional[s3.S3Instructions]): Special instructions for reading data files, useful when
                reading files from a non-local file system, like S3.
            column_renames (Optional[Dict[str, str]]): A dictionary of old to new column names that will be renamed in
                the output table.

        Raises:
            DHError: If unable to build the instructions object.
        """

        try:
            builder = self.j_object_type.builder()

            if table_definition is not None:
                builder.tableDefinition(TableDefinition(table_definition).j_table_definition)

            if data_instructions is not None:
                builder.dataInstructions(data_instructions.j_object)

            if column_renames is not None:
                for old_name, new_name in column_renames.items():
                    builder.putColumnRenames(old_name, new_name)

            self._j_object = builder.build()
        except Exception as e:
            raise DHError(e, "Failed to build Iceberg instructions") from e

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object


class IcebergParquetWriteInstructions(JObjectWrapper):
    """
    This class specifies the instructions for writing Iceberg tables as Parquet data files. These include column rename
    instructions, table definitions, special data instructions for loading data files from the cloud, etc.
    """

    j_object_type = _JIcebergParquetWriteInstructions

    def __init__(self,
                 compression_codec_name: Optional[str] = None,
                 maximum_dictionary_keys: Optional[int] = None,
                 maximum_dictionary_size: Optional[int] = None,
                 target_page_size: Optional[int] = None,
                 create_table_if_not_exist: Optional[bool] = None,
                 verify_schema: Optional[bool] = None,
                 dh_to_iceberg_column_renames: Optional[Dict[str, str]] = None,
                 table_definition: Optional[TableDefinitionLike] = None,
                 data_instructions: Optional[s3.S3Instructions] = None):
        """
        Initializes the instructions using the provided parameters.

        Args:
            compression_codec_name (Optional[str]): the compression codec to use. Allowed values include "UNCOMPRESSED",
                "SNAPPY", "GZIP", "LZO", "LZ4", "LZ4_RAW", "ZSTD", etc. If not specified, defaults to "SNAPPY".
            maximum_dictionary_keys (Optional[int]): the maximum number of unique keys the writer should add to a
                dictionary page before switching to non-dictionary encoding, never evaluated for non-String columns,
                defaults to 2^20 (1,048,576)
            maximum_dictionary_size (Optional[int]): the maximum number of bytes the writer should add to the dictionary
                before switching to non-dictionary encoding, never evaluated for non-String columns, defaults to
                2^20 (1,048,576)
            target_page_size (Optional[int]): the target page size in bytes, if not specified, defaults to
                2^20 bytes (1 MiB)
            create_table_if_not_exist (Optional[bool]): if true, the table will be created if it does not exist,
                defaults to false
            verify_schema (Optional[bool]): Specifies whether to verify that the partition spec and schema of the table
                being written are consistent with the Iceberg table. Verification behavior differs based on the
                operation type:
                - Appending Data or Writing Data Files: Verification is enabled by default. It ensures that:
                    - All columns from the Deephaven table are present in the Iceberg table and have compatible types.
                    - All required columns in the Iceberg table are present in the Deephaven table.
                    - The set of partitioning columns in both the Iceberg and Deephaven tables are identical.
                - Overwriting Data: Verification is disabled by default. When enabled, it ensures that the
                    schema and partition spec of the table being written are identical to those of the Iceberg table.
            dh_to_iceberg_column_renames (Optional[Dict[str, str]]): A dictionary from Deephaven to Iceberg column names
                to use when writing deephaven tables to Iceberg tables.
            table_definition (Optional[TableDefinitionLike]): the table definition; if omitted,
                the definition is inferred from the Iceberg schema. Setting a definition guarantees the returned table
                will have that definition. This is useful for specifying a subset of the Iceberg schema columns.
            data_instructions (Optional[s3.S3Instructions]): Special instructions for reading data files, useful when
                reading files from a non-local file system, like S3.

        Raises:
            DHError: If unable to build the instructions object.
        """

        try:
            builder = self.j_object_type.builder()

            if compression_codec_name is not None:
                builder.compressionCodecName(compression_codec_name)

            if maximum_dictionary_keys is not None:
                builder.maximumDictionaryKeys(maximum_dictionary_keys)

            if maximum_dictionary_size is not None:
                builder.maximumDictionarySize(maximum_dictionary_size)

            if target_page_size is not None:
                builder.targetPageSize(target_page_size)

            if create_table_if_not_exist is not None:
                builder.createTableIfNotExist(create_table_if_not_exist)

            if verify_schema is not None:
                builder.verifySchema(verify_schema)

            if dh_to_iceberg_column_renames is not None:
                for dh_name, iceberg_name in dh_to_iceberg_column_renames.items():
                    builder.putDhToIcebergColumnRenames(dh_name, iceberg_name)

            if table_definition is not None:
                builder.tableDefinition(TableDefinition(table_definition).j_table_definition)

            if data_instructions is not None:
                builder.dataInstructions(data_instructions.j_object)

            self._j_object = builder.build()

        except Exception as e:
            raise DHError(e, "Failed to build Iceberg write instructions") from e

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object


class IcebergCatalogAdapter(JObjectWrapper):
    """
    This class provides an interface for interacting with Iceberg catalogs. It allows listing namespaces, tables and
    snapshots, as well as reading Iceberg tables into Deephaven tables.
    """
    j_object_type = _JIcebergCatalogAdapter or type(None)

    def __init__(self, j_object: _JIcebergCatalogAdapter):
        self.j_catalog_adapter = j_object

    def namespaces(self, namespace: Optional[str] = None) -> Table:
        """
        Returns information on the namespaces in the catalog as a Deephaven table. If a namespace is specified, the
        tables in that namespace are listed; otherwise the top-level namespaces are listed.

        Args:
            namespace (Optional[str]): the higher-level namespace from which to list namespaces; if omitted, the
                top-level namespaces are listed.

        Returns:
            a table containing the namespaces.
        """

        if namespace is not None:
            return Table(self.j_object.listNamespaces(namespace))
        return Table(self.j_object.listNamespacesAsTable())

    def tables(self, namespace: str) -> Table:
        """
        Returns information on the tables in the specified namespace as a Deephaven table.

        Args:
            namespace (str): the namespace from which to list tables.

        Returns:
            a table containing the tables in the provided namespace.
        """

        if namespace is not None:
            return Table(self.j_object.listTablesAsTable(namespace))
        return Table(self.j_object.listTablesAsTable())

    def snapshots(self, table_identifier: str) -> Table:
        """
        Returns information on the snapshots of the specified table as a Deephaven table.

        Args:
            table_identifier (str): the table from which to list snapshots.

        Returns:
            a table containing the snapshot information.
        """

        return self.j_object.listSnapshotsAsTable(table_identifier)

    def read_table(
            self,
            table_identifier: str,
            instructions: Optional[IcebergReadInstructions] = None,
            snapshot_id: Optional[int] = None) -> Table:
        """
        Reads the table from the catalog using the provided instructions. Optionally, a snapshot id can be provided to
        read a specific snapshot of the table.

        Args:
            table_identifier (str): the table to read.
            instructions (Optional[IcebergReadInstructions]): the instructions for reading the table. These instructions
                can include column renames, table definition, and specific data instructions for reading the data files
                from the provider. If omitted, the table will be read with default instructions.
            snapshot_id (Optional[int]): the snapshot id to read; if omitted the most recent snapshot will be selected.

        Returns:
            Table: the table read from the catalog.
        """

        if instructions is not None:
            instructions_object = instructions.j_object
        else:
            instructions_object = _JIcebergReadInstructions.DEFAULT

        if snapshot_id is not None:
            return Table(self.j_object.readTable(table_identifier, snapshot_id, instructions_object))
        return Table(self.j_object.readTable(table_identifier, instructions_object))

    def append(self,
               table_identifier: str,
               tables: List[Table],
               partition_paths: Optional[List[str]] = None,
               instructions: Optional[IcebergParquetWriteInstructions] = None):
        # TODO Review javadoc in this file once again
        """
        Append the provided Deephaven table as a new partition to the existing Iceberg table in a single snapshot. This
        will not change the schema of the existing table.

        Args:
            table_identifier (str): the identifier string for iceberg table to append to.
            tables (List[Table]): the tables to append.
            partition_paths (Optional[List[str]]): the partitioning path at which data would be appended, for example,
                "year=2021/month=01". If omitted, we will try to append data to the table without partitioning.
            instructions (Optional[IcebergParquetWriteInstructions]): the instructions for customizations while writing.
        """
        builder = _JIcebergAppend.builder().tableIdentifier(table_identifier)

        for table in tables:
            builder.addDhTables(table.j_table)

        for partition_path in partition_paths:
            builder.addPartitionPaths(partition_path)

        if instructions is not None:
            builder.instructions(instructions.j_object)

        return self.j_object.append(builder.build())

    def overwrite(self,
                  table_identifier: str,
                  tables: List[Table],
                  partition_paths: Optional[List[str]] = None,
                  instructions: Optional[IcebergParquetWriteInstructions] = None):
        """
        Overwrite the existing Iceberg table with the provided Deephaven tables in a single snapshot. This will
        overwrite the schema of the existing table to match the provided Deephaven table if they do not match.
        Overwriting a table while racing with other writers can lead to failure/undefined results.

        Args:
            table_identifier (str): the identifier string for iceberg table to overwrite.
            tables (List[Table]): the tables to overwrite.
            partition_paths (Optional[List[str]]): the partitioning path at which data would be overwritten, for example,
                "year=2021/month=01". If omitted, we will try to overwrite data to the table without partitioning.
            instructions (Optional[IcebergParquetWriteInstructions]): the instructions for customizations while writing.
        """
        builder = _JIcebergOverwrite.builder().tableIdentifier(table_identifier)

        for table in tables:
            builder.addDhTables(table.j_table)

        for partition_path in partition_paths:
            builder.addPartitionPaths(partition_path)

        if instructions is not None:
            builder.instructions(instructions.j_object)

        return self.j_object.overwrite(builder.build())

    def write_data_file(self,
                        table_identifier: str,
                        tables: List[Table],
                        partition_paths: Optional[List[str]] = None,
                        instructions: Optional[IcebergParquetWriteInstructions] = None):
        """
        Writes data from Deephaven tables to an Iceberg table without creating a new snapshot. This method returns a list
        of data files that were written. Users can use this list to create a transaction/snapshot if needed.

        Args:
            table_identifier (str): the identifier string for iceberg table to write to.
            tables (List[Table]): the tables to write.
            partition_paths (Optional[List[str]]): the partitioning path at which data would be written, for example,
                "year=2021/month=01". If omitted, we will try to write data to the table without partitioning.
            instructions (Optional[IcebergParquetWriteInstructions]): the instructions for customizations while writing.
        """
        builder = _JIcebergWriteDataFile.builder().tableIdentifier(table_identifier)

        for table in tables:
            builder.addDhTables(table.j_table)

        for partition_path in partition_paths:
            builder.addPartitionPaths(partition_path)

        if instructions is not None:
            builder.instructions(instructions.j_object)

        return self.j_object.writeDataFiles(builder.build())

    @property
    def j_object(self) -> jpy.JType:
        return self.j_catalog_adapter


def adapter_s3_rest(
        catalog_uri: str,
        warehouse_location: str,
        name: Optional[str] = None,
        region_name: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        end_point_override: Optional[str] = None
) -> IcebergCatalogAdapter:
    """
    Create a catalog adapter using an S3-compatible provider and a REST catalog.

    Args:
        catalog_uri (str): the URI of the REST catalog.
        warehouse_location (str): the location of the warehouse.
        name (Optional[str]): a descriptive name of the catalog; if omitted the catalog name is inferred from the
            catalog URI.
        region_name (Optional[str]): the S3 region name to use; If not provided, the default region will be
            picked by the AWS SDK from 'aws.region' system property, "AWS_REGION" environment variable, the
            {user.home}/.aws/credentials or {user.home}/.aws/config files, or from EC2 metadata service, if running in
            EC2.
        access_key_id (Optional[str]): the access key for reading files. Both access key and secret access key must be
            provided to use static credentials, else default credentials will be used.
        secret_access_key (Optional[str]): the secret access key for reading files. Both access key and secret key
            must be provided to use static credentials, else default credentials will be used.
        end_point_override (Optional[str]): the S3 endpoint to connect to. Callers connecting to AWS do not typically
            need to set this; it is most useful when connecting to non-AWS, S3-compatible APIs.

    Returns:
        IcebergCatalogAdapter: the catalog adapter for the provided S3 REST catalog.

    Raises:
        DHError: If unable to build the catalog adapter.
    """
    if not _JIcebergToolsS3:
        raise DHError(message="`adapter_s3_rest` requires the Iceberg specific deephaven S3 extensions to be "
                              "included in the package")

    try:
        return IcebergCatalogAdapter(
            _JIcebergToolsS3.createS3Rest(
                name,
                catalog_uri,
                warehouse_location,
                region_name,
                access_key_id,
                secret_access_key,
                end_point_override))
    except Exception as e:
        raise DHError(e, "Failed to build Iceberg Catalog Adapter") from e


def adapter_aws_glue(
        catalog_uri: str,
        warehouse_location: str,
        name: Optional[str] = None
) -> IcebergCatalogAdapter:
    """
    Create a catalog adapter using an AWS Glue catalog.

    Args:
        catalog_uri (str): the URI of the REST catalog.
        warehouse_location (str): the location of the warehouse.
        name (Optional[str]): a descriptive name of the catalog; if omitted the catalog name is inferred from the
            catalog URI.

    Returns:
        IcebergCatalogAdapter: the catalog adapter for the provided AWS Glue catalog.

    Raises:
        DHError: If unable to build the catalog adapter.
    """
    if not _JIcebergToolsS3:
        raise DHError(message="`adapter_aws_glue` requires the Iceberg specific deephaven S3 extensions to "
                              "be included in the package")

    try:
        return IcebergCatalogAdapter(
            _JIcebergToolsS3.createGlue(
                name,
                catalog_uri,
                warehouse_location))
    except Exception as e:
        raise DHError(e, "Failed to build Iceberg Catalog Adapter") from e


def adapter(
        name: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        hadoop_config: Optional[Dict[str, str]] = None
) -> IcebergCatalogAdapter:
    """
    Create an Iceberg catalog adapter from configuration properties. These properties map to the Iceberg catalog Java
    API properties and are used to select the catalog and file IO implementations.

    The minimal set of properties required to create an Iceberg catalog are the following:
    - `catalog-impl` or `type` - the Java catalog implementation to use. When providing `catalog-impl`, the
            implementing Java class should be provided (e.g. `org.apache.iceberg.rest.RESTCatalog` or
            `org.apache.iceberg.aws.glue.GlueCatalog`). Choices for `type` include `hive`, `hadoop`, `rest`, `glue`,
            `nessie`, `jdbc`.

    Other common properties include:
    - `uri` - the URI of the catalog
    - `warehouse` - the root path of the data warehouse.
    - `client.region` - the region of the AWS client.
    - `s3.access-key-id` - the S3 access key for reading files.
    - `s3.secret-access-key` - the S3 secret access key for reading files.
    - `s3.endpoint` - the S3 endpoint to connect to.

    Example usage #1 - REST catalog with an S3 backend (using MinIO):
    ```
    from deephaven.experimental import iceberg

    adapter = iceberg.adapter(name="generic-adapter", properties={
        "type" : "rest",
        "uri" : "http://rest:8181",
        "client.region" : "us-east-1",
        "s3.access-key-id" : "admin",
        "s3.secret-access-key" : "password",
        "s3.endpoint" : "http://minio:9000"
    })
    ```

    Example usage #2 - AWS Glue catalog:
    ```
    from deephaven.experimental import iceberg

    ## Note: region and credential information are loaded by the catalog from the environment
    adapter = iceberg.adapter(name="generic-adapter", properties={
        "type" : "glue",
        "uri" : "s3://lab-warehouse/sales",
    });
    ```

    Args:
        name (Optional[str]): a descriptive name of the catalog; if omitted the catalog name is inferred from the
            catalog URI property.
        properties (Optional[Dict[str, str]]): the properties of the catalog to load
        hadoop_config (Optional[Dict[str, str]]): hadoop configuration properties for the catalog to load

    Returns:
        IcebergCatalogAdapter: the catalog adapter created from the provided properties

    Raises:
        DHError: If unable to build the catalog adapter
    """

    try:
        return IcebergCatalogAdapter(
            _JIcebergTools.createAdapter(
                name,
                j_hashmap(properties if properties is not None else {}),
                j_hashmap(hadoop_config if hadoopConfig is not None else {})))
    except Exception as e:
        raise DHError(e, "Failed to build Iceberg Catalog Adapter") from e

