package io.deephaven.engine.table.hierarchical;

import io.deephaven.api.SortColumn;
import io.deephaven.engine.table.Table;

import java.util.Collection;

/**
 * Records sort operations to be applied to individual nodes of a hierarchical table for presentation. Supports a subset
 * of the {@link Table} API.
 */
public interface SortOperationsRecorder<IFACE_TYPE extends SortOperationsRecorder<IFACE_TYPE>> {

    /**
     * See {@link Table#sort(String...)}.
     *
     * @return A new recorder with this operation recorded to apply to node {@link Table tables}.
     */
    IFACE_TYPE sort(String... columnsToSortBy);

    /**
     * See {@link Table#sortDescending(String...)}.
     *
     * @return A new recorder with this operation recorded to apply to node {@link Table tables}.
     */
    IFACE_TYPE sortDescending(String... columnsToSortBy);

    /**
     * See {@link Table#sort(Collection)}.
     *
     * @return A new recorder with this operation recorded to apply to node {@link Table tables}.
     */
    IFACE_TYPE sort(Collection<SortColumn> columnsToSortBy);
}
