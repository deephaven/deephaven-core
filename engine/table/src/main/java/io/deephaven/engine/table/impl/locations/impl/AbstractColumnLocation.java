package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.impl.locations.ColumnLocation;
import io.deephaven.engine.table.impl.locations.TableLocation;
import org.jetbrains.annotations.NotNull;

/**
 * Partial ColumnLocation implementation for use by TableDataService implementations.
 */
public abstract class AbstractColumnLocation implements ColumnLocation {

    private final TableLocation tableLocation;
    private final String name;

    protected AbstractColumnLocation(@NotNull final TableLocation tableLocation, @NotNull final String name) {
        this.tableLocation = Require.neqNull(tableLocation, "tableLocation");
        this.name = Require.neqNull(name, "name");
    }

    @Override
    public final String toString() {
        return toStringHelper();
    }

    // ------------------------------------------------------------------------------------------------------------------
    // Partial ColumnLocation implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public @NotNull final TableLocation getTableLocation() {
        return tableLocation;
    }

    @Override
    public @NotNull final String getName() {
        return name;
    }
}
