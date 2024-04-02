/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.iceberg.location;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocation;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import org.apache.iceberg.FileFormat;
import org.jetbrains.annotations.NotNull;

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
