//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.location;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.iceberg.util.IcebergTableAdapter;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocation;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortField;
import org.apache.iceberg.SortOrder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class IcebergTableParquetLocation extends ParquetTableLocation implements TableLocation {

    private volatile List<SortColumn> sortedColumns;

    public IcebergTableParquetLocation(
            @NotNull final IcebergTableAdapter tableAdapter,
            @NotNull final TableKey tableKey,
            @NotNull final IcebergTableParquetLocationKey tableLocationKey,
            @NotNull final ParquetInstructions readInstructions) {
        super(tableKey, tableLocationKey, readInstructions);
        sortedColumns = computeSortedColumns(tableAdapter, tableLocationKey.dataFile());
    }

    @Override
    @NotNull
    public List<SortColumn> getSortedColumns() {
        List<SortColumn> local;
        if ((local = sortedColumns) != null) {
            return local;
        }
        synchronized (this) {
            if ((local = sortedColumns) != null) {
                return local;
            }
            local = Collections.unmodifiableList(super.getSortedColumns());
            sortedColumns = local;
            return local;
        }
    }

    @Nullable
    private static List<SortColumn> computeSortedColumns(
            @NotNull final IcebergTableAdapter tableAdapter,
            @NotNull final DataFile dataFile) {
        final Integer sortOrderId = dataFile.sortOrderId();
        // If sort order is missing or unknown, we cannot determine the sorted columns from the metadata and will
        // check the underlying parquet file for the sorted columns, when the user asks for them.
        if (sortOrderId == null) {
            return null;
        }
        final SortOrder sortOrder = tableAdapter.icebergTable().sortOrders().get(sortOrderId);
        if (sortOrder == null) {
            return null;
        }
        if (sortOrder.isUnsorted()) {
            return Collections.emptyList();
        }
        final Schema schema = sortOrder.schema();
        final List<SortColumn> sortColumns = new ArrayList<>(sortOrder.fields().size());
        for (final SortField field : sortOrder.fields()) {
            if (!field.transform().isIdentity()) {
                // TODO (DH-18160): Improve support for handling non-identity transforms
                break;
            }
            final ColumnName columnName = ColumnName.of(schema.findColumnName(field.sourceId()));
            final SortColumn sortColumn;
            if (field.nullOrder() == NullOrder.NULLS_FIRST && field.direction() == SortDirection.ASC) {
                sortColumn = SortColumn.asc(columnName);
            } else if (field.nullOrder() == NullOrder.NULLS_LAST && field.direction() == SortDirection.DESC) {
                sortColumn = SortColumn.desc(columnName);
            } else {
                // TODO Check with Devin if this is okay, The assumption here is that deephaven sorts nulls first for
                // ascending order and nulls last for descending, so if we don't have the correct nulls order, we
                // cannot use the column as a sort column
                break;
            }
            sortColumns.add(sortColumn);
        }
        return Collections.unmodifiableList(sortColumns);
    }
}
