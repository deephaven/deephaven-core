#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
""" This module supports reading external Iceberg tables into Deephaven. """
from typing import List, Optional, Union, Dict, Sequence

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.column import Column
from deephaven.dtypes import DType
from deephaven.experimental import s3

from deephaven.jcompat import j_table_definition

from deephaven.table import Table

_JIcebergInstructions = jpy.get_type("io.deephaven.iceberg.util.IcebergInstructions")
_JIcebergCatalogAdapter = jpy.get_type("io.deephaven.iceberg.util.IcebergCatalogAdapter")

# IcebergToolsS3 is an optional library
try:
    _JIcebergToolsS3 = jpy.get_type("io.deephaven.iceberg.util.IcebergToolsS3")
except Exception:
    _JIcebergToolsS3 = None

_JNamespace = jpy.get_type("org.apache.iceberg.catalog.Namespace")
_JTableIdentifier = jpy.get_type("org.apache.iceberg.catalog.TableIdentifier")
_JSnapshot = jpy.get_type("org.apache.iceberg.Snapshot")


class IcebergInstructions(JObjectWrapper):
    """
    This class specifies the instructions for reading an Iceberg table into Deephaven. These include column rename
    instructions and table definitions, as well as special data instructions for loading data files from the cloud.
    """

    j_object_type = _JIcebergInstructions or type(None)

    def __init__(self,
                 table_definition: Optional[Union[Dict[str, DType], List[Column]]] = None,
                 data_instructions: Optional[s3.S3Instructions] = None,
                 column_renames: Optional[Dict[str, str]] = None):
        """
        Initializes the instructions using the provided parameters.

        Args:
            table_definition (Optional[Union[Dict[str, DType], List[Column], None]]): the table definition; if omitted,
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
                builder.tableDefinition(j_table_definition(table_definition))

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
        Returns the namespaces in the catalog as a Deephaven table.

        Args:
            namespace (Optional[str]): the higher-level namespace from which to list namespaces; if omitted, the
                top-level namespaces are listed.

        Returns:
            a table containing the namespaces.
        """

        if namespace is not None:
            return Table(self.j_object.listNamespaces(namespace))
        return Table(self.j_object.listNamespacesAsTable())

    def tables(self, namespace: Optional[str] = None) -> Table:
        """
        Returns the list of tables in the provided namespace as a Deephaven table.

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
        Returns the list of snapshots of the provided table as a Deephaven table.

        Args:
            table_identifier (str): the table from which to list snapshots.

        Returns:
            a table containing the snapshot information.
        """

        return self.j_object.listSnapshotsAsTable(table_identifier)

    def read_table(self, table_identifier: str, instructions: IcebergInstructions, snapshot_id: Optional[int] = None) -> Table:
        """
        Reads the table from the catalog using the provided instructions. Optionally, a snapshot id can be provided to
        read a specific snapshot of the table.

        Args:
            table_identifier (str): the table to read.
            instructions (IcebergInstructions): the instructions for reading the table. These instructions can include
                column renames, table definition, and specific data instructions for reading the data files from the
                provider.
            snapshot_id (Optional[int]): the snapshot id to read; if omitted the most recent snapshot will be selected.

        Returns:
            Table: the table read from the catalog.
        """

        if snapshot_id is not None:
            return Table(self.j_object.readTable(table_identifier, snapshot_id, instructions.j_object))
        return Table(self.j_object.readTable(table_identifier, instructions.j_object))

    @property
    def j_object(self) -> jpy.JType:
        return self.j_catalog_adapter


def s3_rest_adapter(
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
        region_name (Optional[str]): the S3 region name to use.
        access_key_id (Optional[str]): the access key for reading files. Both access key and secret access key must be
            provided to use static credentials, else default credentials will be used.
        secret_access_key (Optional[str]): the secret access key for reading files. Both access key and secret key
            must be provided to use static credentials, else default credentials will be used.
        end_point_override (Optional[str]): the S3 endpoint to connect to. Callers connecting to AWS do not typically
            need to set this; it is most useful when connecting to non-AWS, S3-compatible APIs.

    Returns:
        IcebergCatalogAdapter: the catalog adapter for the provided S3 REST catalog.
    """
    if not _JIcebergToolsS3:
        raise DHError(message="`create_s3_rest_adapter` requires the Iceberg specific deephaven S3 extensions to be "
                              "included in the package")

    return IcebergCatalogAdapter(
        _JIcebergToolsS3.createS3Rest(
            name,
            catalog_uri,
            warehouse_location,
            region_name,
            access_key_id,
            secret_access_key,
            end_point_override))


def aws_glue_adapter(
        catalog_uri: str,
        warehouse_location: str,
        name: Optional[str] = None
) -> IcebergCatalogAdapter:
    """
    Create a catalog adapter using an AWS Glue catalog .

    Args:
        catalog_uri (Optional[str]): the URI of the REST catalog.
        warehouse_location (Optional[str]): the location of the warehouse.
        name (Optional[str]): a descriptive name of the catalog; if omitted the catalog name is inferred from the
            catalog URI.

    Returns:
        IcebergCatalogAdapter: the catalog adapter for the provided S3 REST catalog.
    """
    if not _JIcebergToolsS3:
        raise DHError(message="`create_s3_aws_glue_adapter` requires the Iceberg specific deephaven S3 extensions to "
                              "be included in the package")

    return IcebergCatalogAdapter(
        _JIcebergToolsS3.createGlue(
            name,
            catalog_uri,
            warehouse_location))