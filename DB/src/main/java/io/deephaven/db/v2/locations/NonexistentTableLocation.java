package io.deephaven.db.v2.locations;

import org.jetbrains.annotations.NotNull;

/**
 * {@link TableLocation} implementation for locations that are found to not actually exist when accessed.
 */
public final class NonexistentTableLocation extends AbstractTableLocation {

    private static final String NAME = NonexistentTableLocation.class.getSimpleName();

    public NonexistentTableLocation(@NotNull final TableKey tableKey, @NotNull final TableLocationKey tableLocationKey) {
        super(tableKey, tableLocationKey, false);
    }

    @Override
    public String getImplementationName() {
        return NAME;
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
