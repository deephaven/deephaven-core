package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.locations.TableKey;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import io.deephaven.engine.rowset.RowSetFactory;
import org.jetbrains.annotations.NotNull;

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
}
