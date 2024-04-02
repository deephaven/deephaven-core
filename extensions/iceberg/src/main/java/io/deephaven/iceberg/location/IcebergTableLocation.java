//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.location;

import io.deephaven.api.SortColumn;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocation;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.apache.iceberg.FileFormat;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

public class IcebergTableLocation implements TableLocation {

    private static final String IMPLEMENTATION_NAME = IcebergTableLocation.class.getSimpleName();

    private final ImmutableTableKey tableKey;
    private final ImmutableTableLocationKey tableLocationKey;

    AbstractTableLocation internalTableLocation;

    public IcebergTableLocation(@NotNull final TableKey tableKey,
            @NotNull final IcebergTableLocationKey tableLocationKey,
            @NotNull final Object readInstructions) {
        this.tableKey = Require.neqNull(tableKey, "tableKey").makeImmutable();
        this.tableLocationKey = Require.neqNull(tableLocationKey, "tableLocationKey").makeImmutable();

        if (tableLocationKey.format == FileFormat.PARQUET) {
            this.internalTableLocation = new ParquetTableLocation(tableKey,
                    (ParquetTableLocationKey) tableLocationKey.internalTableLocationKey,
                    (ParquetInstructions) readInstructions);
        } else {
            throw new IllegalArgumentException("Unsupported file format: " + tableLocationKey.format);
        }
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    @NotNull
    public ImmutableTableKey getTableKey() {
        return tableKey;
    }

    @Override
    public @NotNull ImmutableTableLocationKey getKey() {
        return tableLocationKey;
    }

    @Override
    public boolean supportsSubscriptions() {
        return internalTableLocation.supportsSubscriptions();
    }

    @Override
    public void subscribe(@NotNull Listener listener) {
        internalTableLocation.subscribe(listener);
    }

    @Override
    public void unsubscribe(@NotNull Listener listener) {
        internalTableLocation.unsubscribe(listener);
    }

    @Override
    public void refresh() {
        internalTableLocation.refresh();
    }

    @Override
    public @NotNull List<SortColumn> getSortedColumns() {
        return internalTableLocation.getSortedColumns();
    }

    @Override
    public @NotNull List<String[]> getDataIndexColumns() {
        return internalTableLocation.getDataIndexColumns();
    }

    @Override
    public boolean hasDataIndex(@NotNull String... columns) {
        return internalTableLocation.hasDataIndex(columns);
    }

    @Override
    public @Nullable BasicDataIndex getDataIndex(@NotNull String... columns) {
        return internalTableLocation.getDataIndex(columns);
    }

    @Override
    public @NotNull ColumnLocation getColumnLocation(@NotNull CharSequence name) {
        return internalTableLocation.getColumnLocation(name);
    }

    @Override
    public @NotNull Object getStateLock() {
        return internalTableLocation.getStateLock();
    }

    @Override
    public RowSet getRowSet() {
        return internalTableLocation.getRowSet();
    }

    @Override
    public long getSize() {
        return internalTableLocation.getSize();
    }

    @Override
    public long getLastModifiedTimeMillis() {
        return internalTableLocation.getLastModifiedTimeMillis();
    }
}
