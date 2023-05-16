package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.engine.table.Table;
import io.deephaven.util.annotations.InternalUseOnly;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An interface that exposes methods to find and retrieve Data Indexes created during the merge process.  A Data Index
 * is equivalent to a traditional Database index, which provides a table that maps unique keys to their index keys
 * in the source table.
 *
 * @implNote This is an experimental feature.  It's interface is likely to change, use at your own risk.
 */
@InternalUseOnly
public interface DataIndexProvider {
    /**
     * Get the data index for the specified set of key columns.
     *
     * @param columns the key columns
     * @return the data index table, or null if one was not available.
     */
    @Nullable
    Table getDataIndex(@NotNull String... columns);
}
