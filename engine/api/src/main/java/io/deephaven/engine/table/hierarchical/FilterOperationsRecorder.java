package io.deephaven.engine.table.hierarchical;

import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.Table;

import java.util.Collection;

/**
 * Records filter operations to be applied to individual nodes of a hierarchical table for presentation. Supports a
 * subset of the {@link Table} API.
 */
public interface FilterOperationsRecorder<IFACE_TYPE extends FilterOperationsRecorder<IFACE_TYPE>> {

    /**
     * See {@link Table#where(String...)}.
     *
     * @return A new recorder with this operation recorded to apply to node {@link Table tables}.
     */
    IFACE_TYPE where(String... filters);

    /**
     * See {@link Table#where(Collection)}.
     *
     * @return A new recorder with this operation recorded to apply to node {@link Table tables}.
     */
    IFACE_TYPE where(Collection<? extends Filter> filters);

    /**
     * See {@link Table#where(Filter...)}.
     *
     * @return A new recorder with this operation recorded to apply to node {@link Table tables}.
     */
    IFACE_TYPE where(Filter... filters);
}
