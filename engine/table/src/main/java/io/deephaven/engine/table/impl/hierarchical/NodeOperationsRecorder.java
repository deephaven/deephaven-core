package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.SortColumn;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.Table;

import java.util.Collection;

/**
 * Records operations to be applied to individual nodes of a {@link HierarchicalTable} for presentation. Supports
 * a subset of the {@link Table} API, including formatting, filtering, and sorting.
 */
public interface NodeOperationsRecorder {

    /**
     * See {@link Table#formatColumns(String...)}.
     *
     * @return A new NodeOperationsRecorder with this operation recorded to apply to node {@link Table tables}.
     */
    NodeOperationsRecorder formatColumns(String... columnFormats);

    /**
     * See {@link Table#formatRowWhere(String, String)}.
     *
     * @return A new NodeOperationsRecorder with this operation recorded to apply to node {@link Table tables}.
     */
    NodeOperationsRecorder formatRowWhere(String condition, String formula);

    /**
     * See {@link Table#formatColumnWhere(String, String, String)}.
     *
     * @return A new NodeOperationsRecorder with this operation recorded to apply to node {@link Table tables}.
     */
    NodeOperationsRecorder formatColumnWhere(String columnName, String condition, String formula);

    /**
     * See {@link Table#sort(String...)}.
     *
     * @return A new NodeOperationsRecorder with this operation recorded to apply to node {@link Table tables}.
     */
    NodeOperationsRecorder sort(String... columnsToSortBy);

    /**
     * See {@link Table#sortDescending(String...)}.
     *
     * @return A new NodeOperationsRecorder with this operation recorded to apply to node {@link Table tables}.
     */
    NodeOperationsRecorder sortDescending(String... columnsToSortBy);

    /**
     * See {@link Table#sort(Collection)}.
     *
     * @return A new NodeOperationsRecorder with this operation recorded to apply to node {@link Table tables}.
     */
    NodeOperationsRecorder sort(Collection<SortColumn> columnsToSortBy);

    /**
     * See {@link Table#where(String...)}.
     *
     * @return A new NodeOperationsRecorder with this operation recorded to apply to node {@link Table tables}.
     */
    NodeOperationsRecorder where(String... filters);

    /**
     * See {@link Table#where(Collection)}.
     *
     * @return A new NodeOperationsRecorder with this operation recorded to apply to node {@link Table tables}.
     */
    NodeOperationsRecorder where(Collection<? extends Filter> filters);

    /**
     * See {@link Table#where(Filter...)}.
     *
     * @return A new NodeOperationsRecorder with this operation recorded to apply to node {@link Table tables}.
     */
    NodeOperationsRecorder where(Filter... filters);
}
