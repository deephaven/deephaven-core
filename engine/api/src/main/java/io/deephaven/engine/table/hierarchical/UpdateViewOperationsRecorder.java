//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.hierarchical;

import io.deephaven.api.Selectable;
import io.deephaven.engine.table.Table;

import java.util.Collection;

/**
 * Records updateView operations to be applied to individual nodes of a hierarchical table for presentation. Supports a
 * subset of the {@link Table} API.
 */
public interface UpdateViewOperationsRecorder<IFACE_TYPE extends UpdateViewOperationsRecorder<IFACE_TYPE>> {

    /**
     * See {@link Table#updateView(String...)} (String...)}.
     *
     * @return A new recorder with this operation recorded to apply to node {@link Table tables}.
     */
    IFACE_TYPE updateView(String... columns);

    /**
     * See {@link Table#updateView(Collection)}.
     *
     * @return A new recorder with this operation recorded to apply to node {@link Table tables}.
     */
    IFACE_TYPE updateView(Collection<Selectable> columns);
}
