#
# Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
#
from typing import List, Optional, Union, Dict, Sequence

import jpy

from deephaven import DHError
from deephaven._wrapper import JObjectWrapper
from deephaven.column import Column
from deephaven.dtypes import DType
from deephaven.experimental import s3

from deephaven.jcompat import j_list_to_list

from deephaven.table import Table

# If we move Iceberg to a permanent module, we should remove this try/except block and just import the types directly.
try:
    _JIcebergInstructions = jpy.get_type("io.deephaven.iceberg.util.IcebergInstructions")
except Exception:
    _JIcebergInstructions = None
try:
    _JIcebergCatalog = jpy.get_type("io.deephaven.iceberg.util.IcebergCatalog")
except Exception:
    _JIcebergCatalog = None
try:
    _JIcebergCatalogAdapter = jpy.get_type("io.deephaven.iceberg.util.IcebergCatalogAdapter")
except Exception:
    _JIcebergCatalogAdapter = None
try:
    _JIcebergToolsS3 = jpy.get_type("io.deephaven.iceberg.util.IcebergToolsS3")
except Exception:
    _JIcebergToolsS3 = None

_JNamespace = jpy.get_type("org.apache.iceberg.catalog.Namespace")
_JTableIdentifier = jpy.get_type("org.apache.iceberg.catalog.TableIdentifier")
_JSnapshot = jpy.get_type("org.apache.iceberg.Snapshot")

_JTableDefinition = jpy.get_type("io.deephaven.engine.table.TableDefinition")


"""
    XXXXXXXX
"""
class IcebergInstructions(JObjectWrapper):
    """
    XXXXXXXXXX provides specialized instructions for reading from S3-compatible APIs.
    """

    j_object_type = _JIcebergInstructions or type(None)

    def __init__(self,
                 table_definition: Optional[Union[Dict[str, DType], List[Column]]] = None,
                 data_instructions: Optional[s3.S3Instructions] = None,
                 column_renames: Optional[Dict[str, str]] = None):

        """
        Initializes the instructions.

        Args:
        table_definition (Union[Dict[str, DType], List[Column], None]): the table definition, by default None. When None,
            the definition is inferred from the parquet file(s). Setting a definition guarantees the returned table will
            have that definition. This is useful for bootstrapping purposes when the initially partitioned directory is
            empty and is_refreshing=True. It is also useful for specifying a subset of the parquet definition.
        data_instructions (Optional[s3.S3Instructions]): Special instructions for reading parquet files, useful when
            reading files from a non-local file system, like S3. By default, None.
        column_renames (Optional[Dict[str, str]]): A dictionary of column renames, by default None. When None, no columns will be renamed.

        Raises:
            DHError: If unable to build the instructions object.
        """

        if not _JIcebergInstructions:
            raise DHError(message="IcebergInstructions requires the Iceberg specific deephaven extensions to be "
                                  "included in the package")

        try:
            builder = self.j_object_type.builder()

            if table_definition is not None:
                builder.tableDefinition(_j_table_definition(table_definition))

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

def _j_table_definition(table_definition: Union[Dict[str, DType], List[Column], None]) -> Optional[jpy.JType]:
    if table_definition is None:
        return None
    elif isinstance(table_definition, Dict):
        return _JTableDefinition.of(
            [
                Column(name=name, data_type=dtype).j_column_definition
                for name, dtype in table_definition.items()
            ]
        )
    elif isinstance(table_definition, List):
        return _JTableDefinition.of(
            [col.j_column_definition for col in table_definition]
        )
    else:
        raise DHError(f"Unexpected table_definition type: {type(table_definition)}")

def _j_object_list_to_str_list(j_object_list: jpy.JType) -> List[str]:
    return [x.toString() for x in j_list_to_list(j_object_list)]

class IcebergCatalogAdapter(JObjectWrapper):
    """
    """
    j_object_type = _JIcebergCatalogAdapter or type(None)

    def __init__(self, j_object: _JIcebergCatalogAdapter):
        self.j_catalog_adapter = j_object

    def namespaces(self, namespace: Optional[str] = None) -> Sequence[str]:
        """
        Returns the list of namespaces in the catalog as strings.

        :param namespace:
        :return:
        """
        if namespace is not None:
            return _j_object_list_to_str_list(self.j_object.listNamespaces(namespace))
        return _j_object_list_to_str_list(self.j_object.listNamespaces())

    def namespaces_as_table(self, namespace: Optional[str] = None) -> Table:
        if namespace is not None:
            return Table(self.j_object.listNamespaces(namespace))
        return Table(self.j_object.listNamespacesAsTable())

    def tables(self, namespace: Optional[str] = None) -> Sequence[str]:
        if namespace is not None:
            return _j_object_list_to_str_list(self.j_object.listTables(namespace))
        return _j_object_list_to_str_list(self.j_object.listTables())

    def tables_as_table(self, namespace: Optional[str] = None) -> Table:
        if namespace is not None:
            return Table(self.j_object.listTablesAsTable(namespace))
        return Table(self.j_object.listTablesAsTable())

    def snapshots(self, table_identifier: str) -> Sequence[str]:
        """
        Returns a list of snapshots for the specified table.

        :param table_identifier:
        :return:
        """

        snaphot_list = []
        for snapshot in j_list_to_list(self.j_object.listSnapshots(table_identifier)):
            snaphot_list.append({
                "id": snapshot.snapshotId(),
                "timestamp_ms": snapshot.timestampMillis(),
                "operation": snapshot.operation(),
                "summary": snapshot.summary().toString()

            })
        return snaphot_list

    def snapshots_as_table(self, table_identifier: str) -> Table:
        """
        Returns a list of snapshots for the specified table.

        :param table_identifier:
        :return:
        """

        return self.j_object.listSnapshotsAsTable(table_identifier)

    def read_table(self, table_identifier: str, instructions: IcebergInstructions, snapshot_id: Optional[int] = None) -> Table:
        """
        Reads the specified table.

        :param table_identifier:
        :param snapshot_id:
        :return:
        """

        if snapshot_id is not None:
            return Table(self.j_object.readTable(table_identifier, snapshot_id, instructions.j_object))
        return Table(self.j_object.readTable(table_identifier, instructions.j_object))

    @property
    def j_object(self) -> jpy.JType:
        return self.j_catalog_adapter

def create_s3_rest_adapter(
        name: Optional[str] = None,
        catalog_uri: Optional[str] = None,
        warehouse_location: Optional[str] = None,
        region_name: Optional[str] = None,
        access_key_id: Optional[str] = None,
        secret_access_key: Optional[str] = None,
        end_point_override: Optional[str] = None
) -> IcebergCatalogAdapter:
    return IcebergCatalogAdapter(
        _JIcebergToolsS3.createS3Rest(
            name,
            catalog_uri,
            warehouse_location,
            region_name,
            access_key_id,
            secret_access_key,
            end_point_override))

def create_s3_aws_glue_adapter() -> IcebergCatalogAdapter:
    return IcebergCatalogAdapter(_JIcebergCatalogAdapter.builder().build())