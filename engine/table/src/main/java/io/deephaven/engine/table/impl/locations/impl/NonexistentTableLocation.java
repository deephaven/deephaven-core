//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.api.SortColumn;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.BasicDataIndex;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * {@link TableLocation} implementation for locations that are found to not actually exist when accessed.
 */
public final class NonexistentTableLocation extends AbstractTableLocation {

    private static final String IMPLEMENTATION_NAME = NonexistentTableLocation.class.getSimpleName();

    public NonexistentTableLocation(@NotNull final TableKey tableKey,
            @NotNull final TableLocationKey tableLocationKey) {
        super(tableKey, tableLocationKey, false);
        handleUpdate(RowSetFactory.empty(), NULL_TIME);
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public void refresh() {}

    @Override
    @NotNull
    public List<SortColumn> getSortedColumns() {
        return List.of();
    }

    @Override
    @NotNull
    public List<String[]> getDataIndexColumns() {
        return List.of();
    }

    @Override
    public boolean hasDataIndex(@NotNull final String... columns) {
        return false;
    }

    @NotNull
    @Override
    protected ColumnLocation makeColumnLocation(@NotNull final String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    @Nullable
    public BasicDataIndex loadDataIndex(@NotNull final String... columns) {
        throw new UnsupportedOperationException();
    }
}
