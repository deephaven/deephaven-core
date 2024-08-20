//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.SortColumn;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.engine.table.hierarchical.TreeTable.NodeOperationsRecorder;
import io.deephaven.engine.table.impl.QueryCompilerRequestProcessor;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.WhereFilter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link TreeTable.NodeOperationsRecorder} implementation.
 */
class TreeNodeOperationsRecorder extends BaseNodeOperationsRecorder<TreeTable.NodeOperationsRecorder>
        implements TreeTable.NodeOperationsRecorder {

    private final Collection<? extends WhereFilter> recordedFilters;

    TreeNodeOperationsRecorder(@NotNull final TableDefinition definition) {
        this(definition, List.of(), List.of(), List.of(), List.of());
    }

    private TreeNodeOperationsRecorder(
            @NotNull final TableDefinition definition,
            @NotNull final Collection<? extends SelectColumn> recordedFormats,
            @NotNull final Collection<SortColumn> recordedSorts,
            @NotNull final Collection<? extends SelectColumn> recordedAbsoluteViews,
            @NotNull final Collection<? extends WhereFilter> recordedFilters) {
        super(definition, recordedFormats, recordedSorts, recordedAbsoluteViews);
        this.recordedFilters = recordedFilters;
    }

    Collection<? extends WhereFilter> getRecordedFilters() {
        return recordedFilters;
    }

    static Table applyFilters(@Nullable final TreeNodeOperationsRecorder nodeOperations, @NotNull final Table input) {
        if (nodeOperations != null && !nodeOperations.getRecordedFilters().isEmpty()) {
            return input.where(Filter.and(nodeOperations.getRecordedFilters()));
        }
        return input;
    }

    @Override
    public boolean isEmpty() {
        return super.isEmpty() && recordedFilters.isEmpty();
    }

    @Override
    TreeTable.NodeOperationsRecorder withFormats(@NotNull final Stream<? extends SelectColumn> formats) {
        return new TreeNodeOperationsRecorder(definition,
                mergeFormats(getRecordedFormats().stream(), formats),
                getRecordedSorts(), getRecordedAbsoluteViews(), recordedFilters);
    }

    @Override
    TreeTable.NodeOperationsRecorder withSorts(
            @NotNull final Stream<SortColumn> sorts,
            @NotNull final Stream<? extends SelectColumn> absoluteViews) {
        return new TreeNodeOperationsRecorder(definition, getRecordedFormats(),
                mergeSortColumns(getRecordedSorts().stream(), sorts),
                mergeAbsoluteViews(getRecordedAbsoluteViews().stream(), absoluteViews),
                getRecordedFilters());
    }

    TreeTable.NodeOperationsRecorder withFilters(@NotNull final Stream<? extends WhereFilter> filters) {
        return new TreeNodeOperationsRecorder(definition, getRecordedFormats(), getRecordedSorts(),
                getRecordedAbsoluteViews(),
                mergeFilters(getRecordedFilters().stream(), filters));
    }

    TreeNodeOperationsRecorder withOperations(@NotNull final TreeNodeOperationsRecorder other) {
        if (!getResultDefinition().equals(other.definition)) {
            throw new IllegalArgumentException(
                    "Incompatible operation recorders; compatible recorders must be created from the same table");
        }
        if (other.isEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return other;
        }
        return new TreeNodeOperationsRecorder(definition,
                mergeFormats(getRecordedFormats().stream(), other.getRecordedFormats().stream()),
                mergeSortColumns(getRecordedSorts().stream(), other.getRecordedSorts().stream()),
                mergeAbsoluteViews(getRecordedAbsoluteViews().stream(), other.getRecordedAbsoluteViews().stream()),
                mergeFilters(getRecordedFilters().stream(), other.getRecordedFilters().stream()));
    }

    private static Collection<? extends WhereFilter> mergeFilters(
            @NotNull final Stream<? extends WhereFilter> wfs1,
            @NotNull final Stream<? extends WhereFilter> wfs2) {
        return Stream.concat(wfs1, wfs2).collect(Collectors.toList());
    }

    @Override
    public TreeTable.NodeOperationsRecorder where(String... filters) {
        final FilterRecordingTableAdapter adapter = new FilterRecordingTableAdapter(definition);
        adapter.where(filters);
        final List<? extends WhereFilter> f = adapter.whereFilters().collect(Collectors.toList());
        return f.isEmpty() ? self() : withFilters(f.stream());
    }

    @Override
    public NodeOperationsRecorder where(Filter filter) {
        final FilterRecordingTableAdapter adapter = new FilterRecordingTableAdapter(definition);
        adapter.where(filter);
        final List<? extends WhereFilter> whereFilters = adapter.whereFilters().collect(Collectors.toList());
        // Note: it's possible that whereFilters _is_ empty; this happens if filter == Filter.ofTrue()
        return whereFilters.isEmpty() ? self() : withFilters(whereFilters.stream());
    }

    private static final class FilterRecordingTableAdapter extends RecordingTableAdapter {

        private Filter filter;

        private FilterRecordingTableAdapter(@NotNull final TableDefinition definition) {
            super(definition);
        }

        @Override
        public Table where(Filter filter) {
            this.filter = filter;
            return this;
        }

        private Stream<? extends WhereFilter> whereFilters() {
            final QueryCompilerRequestProcessor.BatchProcessor compilationProcessor =
                    QueryCompilerRequestProcessor.batch();
            final WhereFilter[] filters = WhereFilter.fromInternal(filter);
            for (final WhereFilter filter : filters) {
                filter.init(getDefinition(), compilationProcessor);
            }
            compilationProcessor.compile();
            return Stream.of(filters);
        }
    }
}
