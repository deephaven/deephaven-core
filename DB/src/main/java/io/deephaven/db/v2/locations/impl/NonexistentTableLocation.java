package io.deephaven.db.v2.locations.impl;

import io.deephaven.db.v2.locations.ColumnLocation;
import io.deephaven.db.v2.locations.TableKey;
import io.deephaven.db.v2.locations.TableLocation;
import io.deephaven.db.v2.locations.TableLocationKey;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;

/**
 * {@link TableLocation} implementation for locations that are found to not actually exist when accessed.
 */
public final class NonexistentTableLocation extends AbstractTableLocation {

    private static final String IMPLEMENTATION_NAME = NonexistentTableLocation.class.getSimpleName();

    public NonexistentTableLocation(@NotNull final TableKey tableKey, @NotNull final TableLocationKey tableLocationKey) {
        super(tableKey, tableLocationKey, false);
        handleUpdate(Index.CURRENT_FACTORY.getEmptyIndex(), NULL_TIME);
    }

    @Override
    public String getImplementationName() {
        return IMPLEMENTATION_NAME;
    }

    @Override
    public void refresh() {
    }

    @NotNull
    @Override
    protected ColumnLocation makeColumnLocation(@NotNull String name) {
        throw new UnsupportedOperationException();
    }
}
