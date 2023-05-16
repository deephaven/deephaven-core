/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.SortPair;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.rowset.RowSetFactory;
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

    @NotNull
    @Override
    protected ColumnLocation makeColumnLocation(@NotNull String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected @Nullable Table getDataIndexImpl(@NotNull String... columns) {
        return null;
    }

    @Override
    public @NotNull List<SortPair> getSortedColumns() {
        return List.of();
    }

    @Override
    public boolean hasDataIndexFor(@NotNull String... columns) {
        return false;
    }
}
