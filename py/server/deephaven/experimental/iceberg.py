#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
""" This module adds Iceberg table support into Deephaven. """
from __future__ import annotations
from typing import List, Optional, Union, Dict, Sequence

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.experimental import s3
from deephaven.table import Table, TableDefinition, TableDefinitionLike

_JIcebergInstructions = jpy.get_type("io.deephaven.iceberg.util.IcebergInstructions")
_JIcebergUpdateMode = jpy.get_type("io.deephaven.iceberg.util.IcebergUpdateMode")
_JIcebergCatalogAdapter = jpy.get_type("io.deephaven.iceberg.util.IcebergCatalogAdapter")
_JIcebergTable = jpy.get_type("io.deephaven.iceberg.util.IcebergTable")

# IcebergToolsS3 is an optional library
try:
    _JIcebergToolsS3 = jpy.get_type("io.deephaven.iceberg.util.IcebergToolsS3")
except Exception:
    _JIcebergToolsS3 = None

_JNamespace = jpy.get_type("org.apache.iceberg.catalog.Namespace")
_JTableIdentifier = jpy.get_type("org.apache.iceberg.catalog.TableIdentifier")
_JSnapshot = jpy.get_type("org.apache.iceberg.Snapshot")


class IcebergUpdateMode(JObjectWrapper):
    """
    This class specifies the update mode for the Iceberg table to be loaded into Deephaven. The modes are:

    - `STATIC`: The table is loaded once and does not change
    - `MANUAL_REFRESHING`: The table can be manually refreshed by the user.
    - `AUTO_REFRESHING`: The table will be automatically refreshed at a specified interval (use
            `auto_refreshing(auto_refresh_ms: int)` to specify an interval rather than use the system default
            of 60 seconds).
    """
    j_object_type = _JIcebergUpdateMode

    def __init__(self, mode: _JIcebergUpdateMode):
        self._j_object = mode

    @classmethod
    def auto_refreshing(cls, auto_refresh_ms: int) -> IcebergUpdateMode:
        """
        Creates an IcebergUpdateMode with auto-refreshing mode enabled using the provided refresh interval.

        :param auto_refresh_ms (int): the refresh interval in milliseconds.
        """
        return IcebergUpdateMode(_JIcebergUpdateMode.autoRefreshing(auto_refresh_ms))

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object

IcebergUpdateMode.STATIC=IcebergUpdateMode(_JIcebergUpdateMode.STATIC)
IcebergUpdateMode.MANUAL_REFRESHING=IcebergUpdateMode(_JIcebergUpdateMode.MANUAL_REFRESHING)
IcebergUpdateMode.AUTO_REFRESHING=IcebergUpdateMode(_JIcebergUpdateMode.AUTO_REFRESHING)


class IcebergInstructions(JObjectWrapper):
    """
    This class specifies the instructions for reading an Iceberg table into Deephaven. These include column rename
    instructions and table definitions, as well as special data instructions for loading data files from the cloud.
    """

    j_object_type = _JIcebergInstructions

    def __init__(self,
                 table_definition: Optional[TableDefinitionLike] = None,
                 data_instructions: Optional[s3.S3Instructions] = None,
                 column_renames: Optional[Dict[str, str]] = None,
                 update_mode: Optional[IcebergUpdateMode] = None):
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
            update_mode (Optional[IcebergUpdateMode]): The update mode for the table. If omitted, the default update
                mode of `IcebergUpdateMode.STATIC` is used.

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

            if update_mode is not None:
                builder.updateMode(update_mode.j_object)

            self._j_object = builder.build()
        except Exception as e:
            raise DHError(e, "Failed to build Iceberg instructions") from e

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object


class IcebergTable(Table):
    """
    IcebergTable is a subclass of Table that allows the users to dynamically update the table with new snapshots from
    the Iceberg catalog.
    """
    j_object_type = _JIcebergTable

    def __init__(self, j_table: jpy.JType):
        super().__init__(j_table)

    def update(self, snapshot_id:Optional[int] = None):
        """
        Updates the table with a specific snapshot. If no snapshot is provided, the most recent snapshot is used.

        NOTE: this method is only valid when the table is in `MANUAL_REFRESHING` mode. `STATIC` and `AUTO_REFRESHING`
        Iceberg tables cannot be updated manually and will throw an exception if this method is called.

        Args:
            snapshot_id (Optional[int]): the snapshot id to update to; if omitted the most recent snapshot will be used.

        Raises:
            DHError: If unable to update the Iceberg table.

        """
        try:
            if snapshot_id is not None:
                self.j_object.update(snapshot_id)
                return
            self.j_object.update()
        except Exception as e:
            raise DHError(e, "Failed to update Iceberg table") from e

    @property
    def j_object(self) -> jpy.JType:
        return self.j_table


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
        Returns information on the snapshots of the specified table as a Deephaven table. The table contains the
        following columns:
        - `Id`: the snapshot identifier (can be used for updating the table or loading a specific snapshot).
        - `TimestampMs`: the timestamp of the snapshot.
        - `Operation`: the data operation that created this snapshot.
        - `Summary`: additional information about this snapshot from the Iceberg metadata.
        - `SnapshotObject`: a Java object containing the Iceberg API snapshot.

        Args:
            table_identifier (str): the table from which to list snapshots.

        Returns:
            a table containing the snapshot information.
        """

        return self.j_object.listSnapshotsAsTable(table_identifier)

    def read_table(self, table_identifier: str, instructions: Optional[IcebergInstructions] = None, snapshot_id: Optional[int] = None) -> IcebergTable:
        """
        Reads the table from the catalog using the provided instructions. Optionally, a snapshot id can be provided to
        read a specific snapshot of the table.

        Args:
            table_identifier (str): the table to read.
            instructions (Optional[IcebergInstructions]): the instructions for reading the table. These instructions
                can include column renames, table definition, and specific data instructions for reading the data files
                from the provider. If omitted, the table will be read with default instructions.
            snapshot_id (Optional[int]): the snapshot id to read; if omitted the most recent snapshot will be selected.

        Returns:
            Table: the table read from the catalog.
        """

        if instructions is not None:
            instructions_object = instructions.j_object
        else:
            instructions_object = _JIcebergInstructions.DEFAULT

        if snapshot_id is not None:
            return IcebergTable(self.j_object.readTable(table_identifier, snapshot_id, instructions_object))
        return IcebergTable(self.j_object.readTable(table_identifier, instructions_object))

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

