#
# Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
#
""" This module adds Iceberg table support into Deephaven. """
from __future__ import annotations
from typing import Optional, Dict, Union, Sequence, Mapping

import jpy
from warnings import warn

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.experimental import s3
from deephaven.table import Table, TableDefinition, TableDefinitionLike

from deephaven.jcompat import j_hashmap

_JBuildCatalogOptions = jpy.get_type("io.deephaven.iceberg.util.BuildCatalogOptions")
_JIcebergUpdateMode = jpy.get_type("io.deephaven.iceberg.util.IcebergUpdateMode")
_JIcebergReadInstructions = jpy.get_type("io.deephaven.iceberg.util.IcebergReadInstructions")
_JIcebergWriteInstructions = jpy.get_type("io.deephaven.iceberg.util.IcebergWriteInstructions")
_JSchemaProvider = jpy.get_type("io.deephaven.iceberg.util.SchemaProvider")
_JSortOrderProvider = jpy.get_type("io.deephaven.iceberg.util.SortOrderProvider")
_JTableParquetWriterOptions = jpy.get_type("io.deephaven.iceberg.util.TableParquetWriterOptions")
_JIcebergCatalogAdapter = jpy.get_type("io.deephaven.iceberg.util.IcebergCatalogAdapter")
_JIcebergTableAdapter = jpy.get_type("io.deephaven.iceberg.util.IcebergTableAdapter")
_JIcebergTableWriter = jpy.get_type("io.deephaven.iceberg.util.IcebergTableWriter")
_JIcebergTable = jpy.get_type("io.deephaven.iceberg.util.IcebergTable")
_JIcebergTools = jpy.get_type("io.deephaven.iceberg.util.IcebergTools")
_JLoadTableOptions = jpy.get_type("io.deephaven.iceberg.util.LoadTableOptions")
_JResolverProvider = jpy.get_type("io.deephaven.iceberg.util.ResolverProvider")
_JInferenceResolver = jpy.get_type("io.deephaven.iceberg.util.InferenceResolver")
_JUnboundResolver = jpy.get_type("io.deephaven.iceberg.util.UnboundResolver")
_JColumnInstructions = jpy.get_type("io.deephaven.iceberg.util.ColumnInstructions")

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
    `IcebergUpdateMode` specifies the update mode for an Iceberg table to be loaded into Deephaven. The modes
    are:

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
    def auto_refresh(cls, auto_refresh_ms: Optional[int] = None) -> IcebergUpdateMode:
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
    `IcebergReadInstructions` specifies the instructions for reading an Iceberg table into Deephaven. These include
    special data instructions for loading data files from the cloud.
    """

    j_object_type = _JIcebergReadInstructions

    def __init__(self,
                 table_definition: Optional[TableDefinitionLike] = None,
                 data_instructions: Optional[s3.S3Instructions] = None,
                 column_renames: Optional[Dict[str, str]] = None,
                 update_mode: Optional[IcebergUpdateMode] = None,
                 snapshot_id: Optional[int] = None,
                 ignore_resolving_errors: bool = False):
        """
        Initializes the instructions using the provided parameters.

        Args:
            table_definition (Optional[TableDefinitionLike]): this parameter is deprecated and has no effect
            data_instructions (Optional[s3.S3Instructions]): Special instructions for reading data files, useful when
                reading files from a non-local file system, like S3. If omitted, the data instructions will be derived
                from the catalog.
            column_renames (Optional[Dict[str, str]]): this parameter is deprecated and has no effect
            update_mode (Optional[IcebergUpdateMode]): The update mode for the table. If omitted, the default update
                mode of :py:func:`IcebergUpdateMode.static() <IcebergUpdateMode.static>` is used.
            snapshot_id (Optional[int]): the snapshot id to read; if omitted the most recent snapshot will be selected.
            ignore_resolving_errors (bool): Controls whether to ignore unexpected resolving errors by silently returning
                null data for columns that can't be resolved in DataFiles where they should be present. These errors may
                be a sign of an incorrect resolver or name mapping; or an Iceberg metadata / data issue. By default, is
                `False`.
        Raises:
            DHError: If unable to build the instructions object.
        """

        try:
            builder = self.j_object_type.builder()

            if table_definition:
                warn(
                    "The table_definition parameter is deprecated, has no effect",
                    DeprecationWarning,
                    stacklevel=2,
                )

            if data_instructions:
                builder.dataInstructions(data_instructions.j_object)

            if column_renames:
                warn(
                    "This column_renames parameter is deprecated, has no effect",
                    DeprecationWarning,
                    stacklevel=2,
                )

            if update_mode:
                builder.updateMode(update_mode.j_object)

            if snapshot_id:
                builder.snapshotId(snapshot_id)

            builder.ignoreResolvingErrors(ignore_resolving_errors)

            self._j_object = builder.build()
        except Exception as e:
            raise DHError(e, "Failed to build Iceberg instructions") from e

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object


class IcebergWriteInstructions(JObjectWrapper):
    """
    `IcebergWriteInstructions` provides instructions intended for writing deephaven tables as partitions to Iceberg
    tables.
    """

    j_object_type = _JIcebergWriteInstructions

    def __init__(self,
                 tables: Union[Table, Sequence[Table]],
                 partition_paths: Optional[Union[str, Sequence[str]]] = None):
        """
        Initializes the instructions using the provided parameters.

        Args:
            tables (Union[Table, Sequence[Table]]): The deephaven tables to write.
            partition_paths (Optional[Union[str, Sequence[str]]]): The partition paths where each table will be written.
                For example, if the Iceberg table is partitioned by "year" and "month", a partition path could be
                "year=2021/month=01".
                If writing to a partitioned Iceberg table, users must provide partition path for each table in tables
                argument in the same order.
                Else when writing to a non-partitioned table, users should not provide any partition paths.
                Defaults to `None`, which means the deephaven tables will be written to the root data directory of the
                Iceberg table.

        Raises:
            DHError: If unable to build the instructions object.
        """

        try:
            builder = self.j_object_type.builder()

            if isinstance(tables, Table):
                builder.addTables(tables.j_table)
            elif isinstance(tables, Sequence):
                for table in tables:
                    builder.addTables(table.j_table)

            if partition_paths:
                if isinstance(partition_paths, str):
                    builder.addPartitionPaths(partition_paths)
                elif isinstance(partition_paths, Sequence):
                    for partition_path in partition_paths:
                        builder.addPartitionPaths(partition_path)

            self._j_object = builder.build()

        except Exception as e:
            raise DHError(e, "Failed to build Iceberg write instructions") from e

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object


class SchemaProvider(JObjectWrapper):
    """
    `SchemaProvider` is used to extract the schema from an Iceberg table. Users can specify multiple ways to do
    so, for example, by schema ID, snapshot ID, current schema, etc. This can be useful for passing a schema when
    writing to an Iceberg table.
    """

    j_object_type = _JSchemaProvider

    def __init__(self, _j_object: jpy.JType):
        """
        Initializes the `SchemaProvider` object.

        Args:
            _j_object (SchemaProvider): the Java `SchemaProvider` object.
        """
        self._j_object = _j_object

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object

    @classmethod
    def from_current(cls) -> 'SchemaProvider':
        """
        Used for extracting the current schema from the table.

        Returns:
            the `SchemaProvider` object.
        """
        return cls(_JSchemaProvider.fromCurrent())

    @classmethod
    def from_schema_id(cls, schema_id: int) -> 'SchemaProvider':
        """
        Used for extracting the schema from the table using the specified schema id.

        Args:
            schema_id (int): the schema id to use.

        Returns:
            the `SchemaProvider` object.
        """
        return cls(_JSchemaProvider.fromSchemaId(schema_id))

    @classmethod
    def from_snapshot_id(cls, snapshot_id: int) -> 'SchemaProvider':
        """
        Used for extracting the schema from the table using the specified snapshot id.

        Args:
            snapshot_id (int): the snapshot id to use.

        Returns:
            the `SchemaProvider` object.
        """
        return cls(_JSchemaProvider.fromSnapshotId(snapshot_id))

    @classmethod
    def from_current_snapshot(cls) -> 'SchemaProvider':
        """
        Used for extracting the schema from the table using the current snapshot.

        Returns:
            the SchemaProvider object.
        """
        return cls(_JSchemaProvider.fromCurrentSnapshot())


class SortOrderProvider(JObjectWrapper):
    """
    `SortOrderProvider` is used to specify the sort order for new data when writing to an Iceberg table. More details
    about sort order can be found in the `Iceberg spec <https://iceberg.apache.org/spec/#sorting>`_
    Users can specify the sort order in multiple ways, such as by providing a sort ID or using the table's default sort
    order. This class consists of factory methods to create different sort order providers.
    """

    j_object_type = _JSortOrderProvider

    def __init__(self, _j_object: jpy.JType):
        """
        Initializes the `SortOrderProvider` object.

        Args:
            _j_object (SortOrderProvider): the Java `SortOrderProvider` object.
        """
        self._j_object = _j_object

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object

    @classmethod
    def unsorted(cls) -> 'SortOrderProvider':
        """
        Used to disable sorting while writing new data to the Iceberg table.

        Returns:
            the `SortOrderProvider` object.
        """
        return cls(_JSortOrderProvider.unsorted())

    @classmethod
    def use_table_default(cls) -> 'SortOrderProvider':
        """
        Use the default sort order of the table while writing new data. If no sort order is set on the table, no sorting
        will be done.

        Returns:
            the `SortOrderProvider` object.
        """
        return cls(_JSortOrderProvider.useTableDefault())

    @classmethod
    def from_sort_id(cls, sort_order_id: int) -> 'SortOrderProvider':
        """
        Use the sort order with the given ID to sort new data while writing to the Iceberg table.

        Args:
            sort_order_id (int): the id of the sort order to use.

        Returns:
            the `.SortOrderProvider` object.
        """
        return cls(_JSortOrderProvider.fromSortId(sort_order_id))

    def with_id(self, sort_order_id: int) -> 'SortOrderProvider':
        """
        Returns a sort order provider that uses the current provider to determine the columns to sort on, but writes a
        different sort order ID to the Iceberg table.
        For example, this provider might sort by columns {A, B, C}, but the ID written to Iceberg corresponds to a sort
        order with columns {A, B}.

        Args:
            sort_order_id (int): the sort order ID to write to the Iceberg table.

        Returns:
            the `SortOrderProvider` object.
        """
        return SortOrderProvider(self._j_object.withId(sort_order_id))

    def with_fail_on_unmapped(self, fail_on_unmapped: bool) -> 'SortOrderProvider':
        """
        Returns a sort order provider configured to fail (or not) if the sort order cannot be applied to the tables
        being written. By default, all providers fail if the sort order cannot be applied.

        Args:
            fail_on_unmapped: whether to fail if the sort order cannot be applied to the tables being written. If
                `False` and the sort order cannot be applied, the tables will be written without sorting.

        Returns:
            the `SortOrderProvider` object.
        """
        return SortOrderProvider(self._j_object.withFailOnUnmapped(fail_on_unmapped))


class InferenceResolver(JObjectWrapper):
    """
    This provides a set of inference options for use in :meth:`~IcebergTableAdapter.load_table`. This is useful when the
    caller does not know the structure of the table to be loaded, and wants the resulting Deephaven Table definition
    (and mapping to the Iceberg fields) to be inferred.
    """

    j_object_type = _JInferenceResolver

    def __init__(
        self,
        infer_partitioning_columns: bool = False,
        fail_on_unsupported_types: bool = False,
        schema_provider: Optional[SchemaProvider] = None,
    ):
        """
        Initializes the `InferenceResolver` object.

        Args:
            infer_partitioning_columns (bool): if Partitioning column should be inferred based on the latest Iceberg
                PartitionSpec. By default, is `False`. Warning: inferring partition columns for general-purpose use is
                dangerous. This is only meant to be applied in situations where caller knows that the latest Iceberg
                PartitionSpec contains identity transforms that are not expected to ever change.
            fail_on_unsupported_types (bool): If inference should fail if any of the Iceberg fields fail to map to
                Deephaven columns. By default, is `False`.
            schema_provider (Optional[SchemaProvider]): The Iceberg Schema to be used for inference. Defaults to
                `None`, which means use the current schema from the Iceberg Table.
        """
        builder = _JInferenceResolver.builder()
        builder.inferPartitioningColumns(infer_partitioning_columns)
        builder.failOnUnsupportedTypes(fail_on_unsupported_types)
        if schema_provider:
            builder.schema(schema_provider.j_object)
        self._j_object = builder.build()

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object


class UnboundResolver(JObjectWrapper):
    """
    This provides a set of resolver options for use in :meth:`~IcebergTableAdapter.load_table`. This is useful when the
    caller knows the definition of the table they want to load, and can provide an explicit mapping between the
    Deephaven columns and Iceberg fields.
    """

    j_object_type = _JUnboundResolver

    def __init__(
        self,
        table_definition: TableDefinitionLike,
        column_instructions: Optional[Mapping[str, Union[int, str]]] = None,
        schema_provider: Optional[SchemaProvider] = None,
    ):
        """
        Initializes the `UnboundResolver` object.

        Args:
            table_definition (TableDefinitionLike): the table definition
            column_instructions (Optional[Mapping[str, Union[int, str]]]): The map from Deephaven column names to
                instructions for mapping to Iceberg columns. An int value will be treated as a schema field id, and a
                str value will be treated as a schema field name. Any columns from table_definition not in this map will
                be assumed as exact name matches for the fields in the Schema. Callers are encouraged to use schema
                field ids as they will remain valid across Iceberg Schema evolution.
            schema_provider (Optional[SchemaProvider]): The Iceberg Schema to be used for inference. Defaults to
                `None`, which means use the current schema from the Iceberg Table.
        """
        builder = _JUnboundResolver.builder()
        builder.definition(TableDefinition(table_definition).j_table_definition)
        if column_instructions:
            for column_name, value in column_instructions.items():
                if isinstance(value, int):
                    ci = _JColumnInstructions.schemaField(value)
                elif isinstance(value, str):
                    ci = _JColumnInstructions.schemaFieldName(value)
                else:
                    raise DHError(message="Unexpected value in Mapping")
                builder.putColumnInstructions(column_name, ci)
        if schema_provider:
            builder.schema(schema_provider.j_object)
        self._j_object = builder.build()

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object


class TableParquetWriterOptions(JObjectWrapper):
    """
    `TableParquetWriterOptions` provides specialized instructions for configuring `IcebergTableWriter`
    instances.
    """

    j_object_type = _JTableParquetWriterOptions

    def __init__(self,
                 table_definition: TableDefinitionLike,
                 schema_provider: Optional[SchemaProvider] = None,
                 field_id_to_column_name: Optional[Dict[int, str]] = None,
                 compression_codec_name: Optional[str] = None,
                 maximum_dictionary_keys: Optional[int] = None,
                 maximum_dictionary_size: Optional[int] = None,
                 target_page_size: Optional[int] = None,
                 sort_order_provider: Optional[SortOrderProvider] = None,
                 data_instructions: Optional[s3.S3Instructions] = None):
        """
        Initializes the instructions using the provided parameters.

        Args:
            table_definition: TableDefinitionLike: The table definition to use when writing Iceberg data files using
                this writer instance. This definition can be used to skip some columns or add additional columns with
                null values. The provided definition should have at least one column.
            schema_provider: Optional[SchemaProvider]: Used to extract a Schema from an Iceberg table. This schema will
                be used in conjunction with the field_id_to_column_name to map Deephaven columns from table_definition
                to Iceberg columns.
                Defaults to `None`, which means use the current schema from the table.
            field_id_to_column_name: Optional[Dict[int, str]]: A one-to-one map from Iceberg field IDs from the
                schema_spec to Deephaven column names from the table_definition.
                Defaults to `None`, which means map Iceberg columns to Deephaven columns using column names.
            compression_codec_name (Optional[str]): The compression codec to use for writing the parquet file. Allowed
                values include "UNCOMPRESSED", "SNAPPY", "GZIP", "LZO", "LZ4", "LZ4_RAW", "ZSTD", etc. Defaults to
                `None`, which means use "SNAPPY".
            maximum_dictionary_keys (Optional[int]): the maximum number of unique keys the Parquet writer should add to
                a dictionary page before switching to non-dictionary encoding, never used for non-String columns.
                Defaults to `None`, which means use 2^20 (1,048,576)
            maximum_dictionary_size (Optional[int]): the maximum number of bytes the Parquet writer should add to the
                dictionary before switching to non-dictionary encoding, never used for non-String columns. Defaults to
                `None`, which means use 2^20 (1,048,576)
            target_page_size (Optional[int]): the target Parquet file page size in bytes, if not specified. Defaults to
                `None`, which means use 2^20 bytes (1 MiB)
            sort_order_provider (Optional[SortOrderProvider]): Specifies the sort order to use for sorting new data
                when writing to an Iceberg table with this writer. The sort order is determined at the time the writer
                is created and does not change if the table's sort order changes later. Defaults to `None`, which means
                the table's default sort order is used. More details about sort order can be found in the
                `Iceberg spec <https://iceberg.apache.org/spec/#sorting>`_
            data_instructions (Optional[s3.S3Instructions]): Special instructions for writing data files, useful when
                writing files to a non-local file system, like S3. If omitted, the data instructions will be derived
                from the catalog.

        Raises:
            DHError: If unable to build the object.
        """

        try:
            builder = self.j_object_type.builder()

            builder.tableDefinition(TableDefinition(table_definition).j_table_definition)

            if schema_provider:
                builder.schemaProvider(schema_provider.j_object)

            if field_id_to_column_name:
                for field_id, column_name in field_id_to_column_name.items():
                    builder.putFieldIdToColumnName(field_id, column_name)

            if compression_codec_name:
                builder.compressionCodecName(compression_codec_name)

            if maximum_dictionary_keys:
                builder.maximumDictionaryKeys(maximum_dictionary_keys)

            if maximum_dictionary_size:
                builder.maximumDictionarySize(maximum_dictionary_size)

            if target_page_size:
                builder.targetPageSize(target_page_size)

            if sort_order_provider:
                builder.sortOrderProvider(sort_order_provider.j_object)

            if data_instructions:
                builder.dataInstructions(data_instructions.j_object)

            self._j_object = builder.build()

        except Exception as e:
            raise DHError(e, "Failed to build Iceberg write instructions") from e

    @property
    def j_object(self) -> jpy.JType:
        return self._j_object


class IcebergTable(Table):
    """
    `IcebergTable` is a subclass of Table that allows users to dynamically update the table with new snapshots
    from the Iceberg catalog.
    """
    j_object_type = _JIcebergTable

    def __init__(self, j_table: jpy.JType):
        super().__init__(j_table)

    def update(self, snapshot_id: Optional[int] = None):
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
            if snapshot_id:
                self.j_object.update(snapshot_id)
                return
            self.j_object.update()
        except Exception as e:
            raise DHError(e, "Failed to update Iceberg table") from e

    @property
    def j_object(self) -> jpy.JType:
        return self.j_table


class IcebergTableWriter(JObjectWrapper):
    """
    `IcebergTableWriter` is responsible for writing Deephaven tables to an Iceberg table. Each
    `IcebergTableWriter` instance associated with a single `IcebergTableAdapter` and can be used to
    write multiple Deephaven tables to this Iceberg table.
    """
    j_object_type = _JIcebergTableWriter or type(None)

    def __init__(self, j_object: _JIcebergTableWriter):
        self.j_table_writer = j_object

    def append(self, instructions: IcebergWriteInstructions):
        """
        Append the provided Deephaven tables as new partitions to the existing Iceberg table in a single snapshot.
        Users can provide the tables using the :attr:`.IcebergWriteInstructions.tables` parameter and optionally provide the
        partition paths where each table will be written using the :attr:`.IcebergWriteInstructions.partition_paths`
        parameter.
        This method will not perform any compatibility checks between the existing schema and the provided Deephaven
        tables. All such checks happen at the time of creation of the `IcebergTableWriter` instance.

        Args:
            instructions (IcebergWriteInstructions): the customization instructions for write.
        """
        self.j_object.append(instructions.j_object)

    @property
    def j_object(self) -> jpy.JType:
        return self.j_table_writer


class IcebergTableAdapter(JObjectWrapper):
    """
    `IcebergTableAdapter` provides an interface for interacting with Iceberg tables. It allows the user to list
    snapshots, retrieve table definitions and reading Iceberg tables into Deephaven tables.
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
            instructions (Optional[IcebergReadInstructions]): has no effect, deprecated

        Returns:
            a table containing the table definition.
        """

        if instructions:
            warn(
                "The instructions parameter is deprecated, has no effect",
                DeprecationWarning,
                stacklevel=2,
            )

        return Table(self.j_object.definitionTable())

    def table(
        self,
        instructions: Optional[IcebergReadInstructions] = None,
        snapshot_id: Optional[int] = None,
        update_mode: Optional[IcebergUpdateMode] = None,
        data_instructions: Optional[s3.S3Instructions] = None,
        ignore_resolving_errors: bool = False,
    ) -> IcebergTable:
        """
        Reads the table using the provided instructions.

        Args:
            instructions (Optional[IcebergReadInstructions]): deprecated, use other parameters directly
            snapshot_id (Optional[int]): the snapshot id to read; if omitted the most recent snapshot will be selected.
            update_mode (Optional[IcebergUpdateMode]): The update mode for the table. If omitted, the default update
                mode of :py:func:`IcebergUpdateMode.static() <IcebergUpdateMode.static>` is used.
            data_instructions (Optional[s3.S3Instructions]): Special instructions for reading data files, useful when
                reading files from a non-local file system, like S3. If omitted, the data instructions will be derived
                from the catalog.
            ignore_resolving_errors (bool): Controls whether to ignore unexpected resolving errors by silently returning
                null data for columns that can't be resolved in DataFiles where they should be present. These errors may
                be a sign of an incorrect resolver or name mapping; or an Iceberg metadata / data issue. By default, is
                `False`.
        Returns:
            the table read from the catalog.
        """
        if snapshot_id or update_mode or data_instructions or ignore_resolving_errors:
            instructions = IcebergReadInstructions(
                snapshot_id=snapshot_id,
                update_mode=update_mode,
                data_instructions=data_instructions,
                ignore_resolving_errors=ignore_resolving_errors,
            )
        elif instructions:
            warn(
                "instructions on table are deprecated, prefer settings the other parameters",
                DeprecationWarning,
                stacklevel=2,
            )
        else:
            instructions = IcebergReadInstructions()
        return IcebergTable(self.j_object.table(instructions.j_object))

    def table_writer(self, writer_options: TableParquetWriterOptions) -> IcebergTableWriter:
        """
        Create a new `IcebergTableWriter` for this Iceberg table using the provided writer options.
        This method will perform schema validation to ensure that the provided table definition from the writer options
        is compatible with the Iceberg table schema. All further writes performed by the returned writer will not be
        validated against the table's schema, and thus will be faster.

        Args:
            writer_options: The options to configure the table writer.

        Returns:
            the table writer object
        """
        return IcebergTableWriter(self.j_object.tableWriter(writer_options.j_object))

    @property
    def j_object(self) -> jpy.JType:
        return self.j_table_adapter


class IcebergCatalogAdapter(JObjectWrapper):
    """
    `IcebergCatalogAdapter` provides an interface for interacting with Iceberg catalogs. It allows listing
    namespaces, tables and snapshots, as well as reading Iceberg tables into Deephaven tables.
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

        if namespace:
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

    def load_table(
        self,
        table_identifier: str,
        resolver: Union[InferenceResolver, UnboundResolver] = None,
    ) -> IcebergTableAdapter:
        """
        Load the table from the catalog.

        Args:
            table_identifier (str): the table to read.
            resolver (Union[InferenceResolver, UnboundResolver]): the resolver, defaults to None, meaning to use a
                InferenceResolver with all the default options.

        Returns:
            Table: the table read from the catalog.
        """
        builder = _JLoadTableOptions.builder()
        builder.id(table_identifier)
        builder.resolver((resolver if resolver else InferenceResolver()).j_object)
        return IcebergTableAdapter(self.j_object.loadTable(builder.build()))

    def create_table(self, table_identifier: str, table_definition: TableDefinitionLike) -> IcebergTableAdapter:
        """
        Create a new Iceberg table in the catalog with the given table identifier and definition.
        All columns of partitioning type will be used to create the partition spec for the table.

        Args:
            table_identifier (str): the identifier of the new table.
            table_definition (TableDefinitionLike): the table definition of the new table.

        Returns:
            `IcebergTableAdapter`: the table adapter for the new Iceberg table.
        """

        return IcebergTableAdapter(self.j_object.createTable(table_identifier,
                                                             TableDefinition(table_definition).j_table_definition))

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
    DEPRECATED: Use `adapter()` instead.

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
        `IcebergCatalogAdapter`: the catalog adapter for the provided S3 REST catalog.

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
    DEPRECATED: Use `adapter()` instead.

    Create a catalog adapter using an AWS Glue catalog.

    Args:
        catalog_uri (str): the URI of the REST catalog.
        warehouse_location (str): the location of the warehouse.
        name (Optional[str]): a descriptive name of the catalog; if omitted the catalog name is inferred from the
            catalog URI.

    Returns:
        `IcebergCatalogAdapter`: the catalog adapter for the provided AWS Glue catalog.

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
        s3_instructions: Optional[s3.S3Instructions] = None,
        enable_property_injection: Optional[bool] = True,
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
        properties (Optional[Dict[str, str]]): the properties of the catalog to load. By default, no properties are set.
        hadoop_config (Optional[Dict[str, str]]): hadoop configuration properties for the catalog to load. By default,
            no properties are set.
        s3_instructions (Optional[s3.S3Instructions]): the S3 instructions to use for configuring the Deephaven managed
            AWS clients. If not provided, the catalog will internally use the Iceberg-managed AWS clients configured
            using the provided `properties`.
        enable_property_injection (bool): whether to enable Deephaven’s automatic injection of additional properties
            that work around upstream issues and supply defaults needed for Deephaven’s Iceberg usage. The injection is
            strictly additive—any keys already present in `properties` are left unchanged. When set to `False` (not
            recommended), the property map is forwarded exactly as supplied, with no automatic additions. Defaults to
            `True`.

    Returns:
    `IcebergCatalogAdapter`: the catalog adapter created from the provided properties

    Raises:
        DHError: If unable to build the catalog adapter
    """

    catalog_options = _build_catalog_options(
        name=name,
        properties=properties,
        hadoop_config=hadoop_config,
        enable_property_injection=enable_property_injection,
    )

    if s3_instructions:
        if not _JIcebergToolsS3:
            raise DHError(
                message="`adapter` with s3_instructions requires the Iceberg-specific Deephaven S3 extensions to be installed"
            )
        try:
            return IcebergCatalogAdapter(
                _JIcebergToolsS3.createAdapter(
                    catalog_options,
                    s3_instructions.j_object,
                )
            )
        except Exception as e:
            raise DHError(e, "Failed to build Iceberg Catalog Adapter") from e

    try:
        return IcebergCatalogAdapter(
            _JIcebergTools.createAdapter(
                catalog_options
            )
        )
    except Exception as e:
        raise DHError(e, "Failed to build Iceberg Catalog Adapter") from e


def _build_catalog_options(
        name: Optional[str] = None,
        properties: Optional[Dict[str, str]] = None,
        hadoop_config: Optional[Dict[str, str]] = None,
        enable_property_injection: bool = True
) -> jpy.JType:
    try:
        builder = _JBuildCatalogOptions.builder()

        if name:
            builder.name(name)

        builder.putAllProperties(j_hashmap(properties if properties else {}))
        builder.putAllHadoopConfig(j_hashmap(hadoop_config  if hadoop_config else {}))
        builder.enablePropertyInjection(enable_property_injection)

        return builder.build()

    except Exception as e:
        raise DHError(e, "Failed to build catalog options") from e
