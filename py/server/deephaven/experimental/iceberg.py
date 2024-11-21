#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
""" This module adds Iceberg table support into Deephaven. """
from __future__ import annotations
from typing import Optional, Dict

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.experimental import s3
from deephaven.table import Table, TableDefinition, TableDefinitionLike

from deephaven.jcompat import j_hashmap

_JIcebergReadInstructions = jpy.get_type("io.deephaven.iceberg.util.IcebergReadInstructions")
_JIcebergUpdateMode = jpy.get_type("io.deephaven.iceberg.util.IcebergUpdateMode")
_JIcebergCatalogAdapter = jpy.get_type("io.deephaven.iceberg.util.IcebergCatalogAdapter")
_JIcebergTableAdapter = jpy.get_type("io.deephaven.iceberg.util.IcebergTableAdapter")
_JIcebergTable = jpy.get_type("io.deephaven.iceberg.util.IcebergTable")
_JIcebergTools = jpy.get_type("io.deephaven.iceberg.util.IcebergTools")

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
    This class specifies the update mode for an Iceberg table to be loaded into Deephaven. The modes are:

    - :py:func:`static() <IcebergUpdateMode.static>`: The table is loaded once and does not change
    - :py:func:`manual_refresh() <IcebergUpdateMode.manual_refresh>`: The table can be manually refreshed by the user.
    - :py:func:`auto_refresh() <IcebergUpdateMode.auto_refresh>`: The table will be automatically refreshed at a
            system-defined interval (also can call :py:func:`auto_refresh(auto_refresh_ms: int) <IcebergUpdateMode.auto_refresh>`
            to specify an interval rather than use the system default of 60 seconds).
    """
    j_object_type = _JIcebergUpdateMode

    def __init__(self, mode: _JIcebergUpdateMode):
        self._j_object = mode

    @classmethod
    def static(cls) -> IcebergUpdateMode:
        """
        Creates an IcebergUpdateMode with no refreshing supported.
        """
        return IcebergUpdateMode(_JIcebergUpdateMode.staticMode())

    @classmethod
    def manual_refresh(cls) -> IcebergUpdateMode:
        """
        Creates an IcebergUpdateMode with manual refreshing enabled.
        """
        return IcebergUpdateMode(_JIcebergUpdateMode.manualRefreshingMode())

    @classmethod
    def auto_refresh(cls, auto_refresh_ms:Optional[int] = None) -> IcebergUpdateMode:
        """
        Creates an IcebergUpdateMode with auto-refreshing enabled.

        Args:
            auto_refresh_ms (int): the refresh interval in milliseconds; if omitted, the default of 60 seconds
                is used.
        """
        if auto_refresh_ms is None:
            return IcebergUpdateMode(_JIcebergUpdateMode.autoRefreshingMode())
        return IcebergUpdateMode(_JIcebergUpdateMode.autoRefreshingMode(auto_refresh_ms))

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object


class IcebergReadInstructions(JObjectWrapper):
    """
    This class specifies the instructions for reading an Iceberg table into Deephaven. These include column rename
    instructions and table definitions, as well as special data instructions for loading data files from the cloud.
    """

    j_object_type = _JIcebergReadInstructions

    def __init__(self,
                 table_definition: Optional[TableDefinitionLike] = None,
                 data_instructions: Optional[s3.S3Instructions] = None,
                 column_renames: Optional[Dict[str, str]] = None,
                 update_mode: Optional[IcebergUpdateMode] = None,
                 snapshot_id: Optional[int] = None):
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
                mode of :py:func:`IcebergUpdateMode.static() <IcebergUpdateMode.static>` is used.
            snapshot_id (Optional[int]): the snapshot id to read; if omitted the most recent snapshot will be selected.

        Raises:
            DHError: If unable to build the instructions object.
        """

        try:
            builder = self.j_object_type.builder()

            if table_definition:
                builder.tableDefinition(TableDefinition(table_definition).j_table_definition)

            if data_instructions:
                builder.dataInstructions(data_instructions.j_object)

            if column_renames:
                for old_name, new_name in column_renames.items():
                    builder.putColumnRenames(old_name, new_name)

            if update_mode:
                builder.updateMode(update_mode.j_object)

            if snapshot_id:
                builder.snapshotId(snapshot_id)

            self._j_object = builder.build()
        except Exception as e:
            raise DHError(e, "Failed to build Iceberg instructions") from e

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object


class IcebergTable(Table):
    """
    IcebergTable is a subclass of Table that allows users to dynamically update the table with new snapshots from
    the Iceberg catalog.
    """
    j_object_type = _JIcebergTable

    def __init__(self, j_table: jpy.JType):
        super().__init__(j_table)

    def update(self, snapshot_id:Optional[int] = None):
        """
        Updates the table to match the contents of the specified snapshot. This may result in row removes and additions
        that will be propagated asynchronously via this IcebergTable's UpdateGraph. If no snapshot is provided, the
        most recent snapshot is used.

        NOTE: this method is only valid when the table is in `manual_refresh()` mode. Iceberg tables in `static()` or
        `auto_refresh()` mode cannot be updated manually and will throw an exception if this method is called.

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


class IcebergTableAdapter(JObjectWrapper):
    """
    This class provides an interface for interacting with Iceberg tables. It allows the user to list snapshots,
    retrieve table definitions and reading Iceberg tables into Deephaven tables.
    """
    j_object_type = _JIcebergTableAdapter or type(None)

    def __init__(self, j_object: _JIcebergTableAdapter):
        self.j_table_adapter = j_object

    def snapshots(self) -> Table:
        """
        Returns information on the snapshots of this table as a Deephaven table. The table contains the
        following columns:
        - `Id`: the snapshot identifier (can be used for updating the table or loading a specific snapshot).
        - `TimestampMs`: the timestamp of the snapshot.
        - `Operation`: the data operation that created this snapshot.
        - `Summary`: additional information about this snapshot from the Iceberg metadata.
        - `SnapshotObject`: a Java object containing the Iceberg API snapshot.

        Returns:
            a table containing the snapshot information.
        """
        return Table(self.j_object.snapshots())

    def definition(self, instructions: Optional[IcebergReadInstructions] = None) -> Table:
        """
        Returns the Deephaven table definition as a Deephaven table.

        Args:
            instructions (Optional[IcebergReadInstructions]): the instructions for reading the table. These instructions
                can include column renames, table definition, and specific data instructions for reading the data files
                from the provider. If omitted, the table will be read with default instructions.

        Returns:
            a table containing the table definition.
        """

        if instructions is not None:
            return Table(self.j_object.definitionTable(instructions.j_object))
        return Table(self.j_object.definitionTable())

    def table(self, instructions: Optional[IcebergReadInstructions] = None) -> IcebergTable:
        """
        Reads the table using the provided instructions. Optionally, a snapshot id can be provided to read a specific
        snapshot of the table.

        Args:
            instructions (Optional[IcebergReadInstructions]): the instructions for reading the table. These instructions
                can include column renames, table definition, and specific data instructions for reading the data files
                from the provider. If omitted, the table will be read in `static()` mode without column renames or data
                instructions.

        Returns:
            Table: the table read from the catalog.
        """

        if instructions is not None:
            return IcebergTable(self.j_object.table(instructions.j_object))
        return IcebergTable(self.j_object.table())


    @property
    def j_object(self) -> jpy.JType:
        return self.j_table_adapter


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
            return Table(self.j_object.namespaces(namespace))
        return Table(self.j_object.namespaces())

    def tables(self, namespace: str) -> Table:
        """
        Returns information on the tables in the specified namespace as a Deephaven table.

        Args:
            namespace (str): the namespace from which to list tables.

        Returns:
            a table containing the tables in the provided namespace.
        """

        return Table(self.j_object.tables(namespace))

    def load_table(self, table_identifier: str) -> IcebergTableAdapter:
        """
        Load the table from the catalog.

        Args:
            table_identifier (str): the table to read.

        Returns:
            Table: the table read from the catalog.
        """

        return IcebergTableAdapter(self.j_object.loadTable(table_identifier))

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
        hadoop_config: Optional[Dict[str, str]] = None,
        s3_instructions: Optional[s3.S3Instructions] = None
) -> IcebergCatalogAdapter:
    """
    Create an Iceberg catalog adapter from configuration properties. These properties map to the Iceberg catalog Java
    API properties and are used to select the catalog and file IO implementations.

    The minimal set of properties required to create an Iceberg catalog are the following:
    - `catalog-impl` or `type` - the Java catalog implementation to use. When providing `catalog-impl`, the
            implementing Java class should be provided (e.g. `org.apache.iceberg.rest.RESTCatalog` or
            `org.apache.iceberg.aws.glue.GlueCatalog`). Choices for `type` include `hive`, `hadoop`, `rest`, `glue`,
            `nessie`, `jdbc`.

    To ensure consistent behavior across Iceberg-managed and Deephaven-managed AWS clients, it's recommended to use the
    `s3_instructions` parameter to specify AWS/S3 connectivity details. This approach offers a high degree of parity in
    construction logic.

    For complex use cases, consider using S3 Instruction profiles, which provide extensive configuration options. When
    set, these instructions are automatically included in the :meth:`.IcebergInstructions.__init__` `data_instructions`.

    If you prefer to use Iceberg's native AWS properties, Deephaven will attempt to infer the necessary construction
    logic. However, in advanced scenarios, there might be discrepancies between the two clients, potentially leading to
    limitations like being able to browse catalog metadata but not retrieve table data.

    Other common properties include:
    - `uri` - the URI of the catalog
    - `warehouse` - the root path of the data warehouse.

    Example usage #1 - REST catalog with an S3 backend:
    ```
    from deephaven.experimental import iceberg
    from deephaven.experimental.s3 import S3Instructions, Credentials

    adapter = iceberg.adapter(
        name="MyCatalog",
        properties={
            "type": "rest",
            "uri": "http://my-rest-catalog:8181/api",
            # Note: Other properties may be needed depending on the REST Catalog implementation
            # "warehouse": "catalog-id",
            # "credential": "username:password"
        },
        s3_instructions=S3Instructions(
            region_name="us-east-1",
            credentials=Credentials.basic("my_access_key_id", "my_secret_access_key"),
        ),
    )
    ```

    Example usage #2 - AWS Glue catalog:
    ```
    from deephaven.experimental import iceberg
    from deephaven.experimental.s3 import S3Instructions

    # Note: region and credential information will be loaded from the specified profile
    adapter = iceberg.adapter(
        name="MyCatalog",
        properties={
            "type": "glue",
            "uri": "s3://lab-warehouse/sales",
        },
        s3_instructions=S3Instructions(
            profile_name="MyGlueProfile",
        ),
    )
    ```

    Args:
        name (Optional[str]): a descriptive name of the catalog; if omitted the catalog name is inferred from the
            catalog URI property.
        properties (Optional[Dict[str, str]]): the properties of the catalog to load
        hadoop_config (Optional[Dict[str, str]]): hadoop configuration properties for the catalog to load
        s3_instructions (Optional[s3.S3Instructions]): the S3 instructions if applicable
    Returns:
        IcebergCatalogAdapter: the catalog adapter created from the provided properties

    Raises:
        DHError: If unable to build the catalog adapter
    """
    if s3_instructions:
        if not _JIcebergToolsS3:
            raise DHError(
                message="`adapter` with s3_instructions requires the Iceberg-specific Deephaven S3 extensions to be installed"
            )
        try:
            return IcebergCatalogAdapter(
                _JIcebergToolsS3.createAdapter(
                    name,
                    j_hashmap(properties if properties is not None else {}),
                    j_hashmap(hadoop_config if hadoop_config is not None else {}),
                    s3_instructions.j_object,
                )
            )
        except Exception as e:
            raise DHError(e, "Failed to build Iceberg Catalog Adapter") from e

    try:
        return IcebergCatalogAdapter(
            _JIcebergTools.createAdapter(
                name,
                j_hashmap(properties if properties is not None else {}),
                j_hashmap(hadoop_config if hadoop_config is not None else {}),
            )
        )
    except Exception as e:
        raise DHError(e, "Failed to build Iceberg Catalog Adapter") from e
