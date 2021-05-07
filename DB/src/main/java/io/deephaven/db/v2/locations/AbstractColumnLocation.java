package io.deephaven.db.v2.locations;

import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

/**
 * Partial ColumnLocation implementation for use by TableDataService implementations.
 */
public abstract class AbstractColumnLocation<TLT extends TableLocation> implements ColumnLocation<TLT> {

    private final TLT tableLocation;
    private final String name;

    protected AbstractColumnLocation(@NotNull final TLT tableLocation, @NotNull final String name) {
        this.tableLocation = Require.neqNull(tableLocation, "tableLocation");
        this.name = Require.neqNull(name, "name");
    }

    @Override
    public final String toString() {
        return toStringHelper();
    }

    //------------------------------------------------------------------------------------------------------------------
    // Partial ColumnLocation implementation
    //------------------------------------------------------------------------------------------------------------------

    @Override
    public @NotNull final TLT getTableLocation() {
        return tableLocation;
    }

    @Override
    public @NotNull final String getName() {
        return name;
    }
}
