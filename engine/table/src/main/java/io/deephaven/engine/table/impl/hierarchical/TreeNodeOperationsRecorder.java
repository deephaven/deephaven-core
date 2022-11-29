package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.SortColumn;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.TreeTable;
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
            return input.where(nodeOperations.getRecordedFilters());
        }
        return input;
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
        if (!definition.equals(other.definition)) {
            throw new IllegalArgumentException(
                    "Incompatible operation recorders; compatible recorders must be created from the same table");
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
        return adapter.hasWhereFilters() ? withFilters(adapter.whereFilters()) : self();
    }

    @Override
    public TreeTable.NodeOperationsRecorder where(Collection<? extends Filter> filters) {
        final FilterRecordingTableAdapter adapter = new FilterRecordingTableAdapter(definition);
        adapter.where(filters);
        return adapter.hasWhereFilters() ? withFilters(adapter.whereFilters()) : self();
    }

    @Override
    public TreeTable.NodeOperationsRecorder where(Filter... filters) {
        final FilterRecordingTableAdapter adapter = new FilterRecordingTableAdapter(definition);
        adapter.where(filters);
        return adapter.hasWhereFilters() ? withFilters(adapter.whereFilters()) : self();
    }

    private static final class FilterRecordingTableAdapter extends RecordingTableAdapter {

        private Collection<? extends Filter> filters;

        private FilterRecordingTableAdapter(@NotNull final TableDefinition definition) {
            super(definition);
        }

        @Override
        public Table where(@NotNull final Collection<? extends Filter> filters) {
            this.filters = filters;
            return this;
        }

        private boolean hasWhereFilters() {
            return !filters.isEmpty();
        }

        private Stream<? extends WhereFilter> whereFilters() {
            return Stream.of(WhereFilter.from(filters)).peek(wf -> wf.init(getDefinition()));
        }
    }
}
