package io.deephaven.engine.table.hierarchical;

import io.deephaven.engine.table.Table;

/**
 * Records format operations to be applied to individual nodes of a hierarchical table for presentation. Supports a
 * subset of the {@link Table} API.
 */
public interface FormatOperationsRecorder<IFACE_TYPE extends FormatOperationsRecorder<IFACE_TYPE>> {

    /**
     * See {@link Table#formatColumns(String...)}.
     *
     * @return A new recorder with this operation recorded to apply to node {@link Table tables}.
     */
    IFACE_TYPE formatColumns(String... columnFormats);

    /**
     * See {@link Table#formatRowWhere(String, String)}.
     *
     * @return A new recorder with this operation recorded to apply to node {@link Table tables}.
     */
    IFACE_TYPE formatRowWhere(String condition, String formula);

    /**
     * See {@link Table#formatColumnWhere(String, String, String)}.
     *
     * @return A new recorder with this operation recorded to apply to node {@link Table tables}.
     */
    IFACE_TYPE formatColumnWhere(String columnName, String condition, String formula);
}
