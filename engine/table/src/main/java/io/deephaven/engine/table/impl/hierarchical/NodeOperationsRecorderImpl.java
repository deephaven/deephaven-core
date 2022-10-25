package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.*;
import io.deephaven.engine.table.impl.TableAdapter;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implementations of {@link RollupTable.NodeOperationsRecorder} and {@link TreeTable.NodeOperationsRecorder}.
 */
abstract class NodeOperationsRecorderImpl<TYPE> {

    protected final TableDefinition definition;

    protected final Collection<SelectColumn> recordedFormats;
    protected final Collection<SortColumn> recordedSorts;

    private NodeOperationsRecorderImpl(
            @NotNull final TableDefinition definition,
            @NotNull final Collection<SelectColumn> recordedFormats,
            @NotNull final Collection<SortColumn> recordedSorts) {
        this.definition = definition;
        this.recordedFormats = recordedFormats;
        this.recordedSorts = recordedSorts;
    }

    abstract TYPE withFormats(@NotNull Stream<? extends SelectColumn> formats);

    abstract TYPE withSorts(@NotNull Stream<SortColumn> sorts);

    public TYPE formatColumns(String... columnFormats) {
        final FormatRecordingTableAdapter adapter = new FormatRecordingTableAdapter(definition);
        adapter.formatColumns(columnFormats);
        return withFormats(adapter.selectColumns());
    }

    public TYPE formatRowWhere(String condition, String formula) {
        final FormatRecordingTableAdapter adapter = new FormatRecordingTableAdapter(definition);
        adapter.formatRowWhere(condition, formula);
        return withFormats(adapter.selectColumns());
    }

    public TYPE formatColumnWhere(String columnName, String condition, String formula) {
        final FormatRecordingTableAdapter adapter = new FormatRecordingTableAdapter(definition);
        adapter.formatColumnWhere(columnName, condition, formula);
        return withFormats(adapter.selectColumns());
    }

    public TYPE sort(String... columnsToSortBy) {
        final SortRecordingTableAdapter adapter = new SortRecordingTableAdapter(definition);
        adapter.sort(columnsToSortBy);
        return withSorts(adapter.sortColumns());
    }

    public TYPE sortDescending(String... columnsToSortBy) {
        final SortRecordingTableAdapter adapter = new SortRecordingTableAdapter(definition);
        adapter.sortDescending(columnsToSortBy);
        return withSorts(adapter.sortColumns());
    }

    public TYPE sort(Collection<SortColumn> columnsToSortBy) {
        final SortRecordingTableAdapter adapter = new SortRecordingTableAdapter(definition);
        adapter.sort(columnsToSortBy);
        return withSorts(adapter.sortColumns());
    }

    /**
     * {@link RollupTable.NodeOperationsRecorder} implementation.
     */
    static class ForRollup extends NodeOperationsRecorderImpl<RollupTable.NodeOperationsRecorder>
            implements RollupTable.NodeOperationsRecorder {

        ForRollup(@NotNull final TableDefinition definition) {
            this(definition, List.of(), List.of());
        }

        private ForRollup(
                @NotNull final TableDefinition definition,
                @NotNull final Collection<SelectColumn> recordedFormats,
                @NotNull final Collection<SortColumn> recordedSorts) {
            super(definition, recordedFormats, recordedSorts);
        }

        @Override
        RollupTable.NodeOperationsRecorder withFormats(@NotNull final Stream<? extends SelectColumn> formats) {
            return new ForRollup(definition,
                    Stream.concat(recordedFormats.stream(), formats).collect(Collectors.toList()),
                    recordedSorts);
        }

        @Override
        RollupTable.NodeOperationsRecorder withSorts(@NotNull final Stream<SortColumn> sorts) {
            return new ForRollup(definition, recordedFormats,
                    Stream.concat(sorts, recordedSorts.stream()).collect(Collectors.toList()));
        }
    }

    /**
     * {@link TreeTable.NodeOperationsRecorder} implementation.
     */
    static class ForTree extends NodeOperationsRecorderImpl<TreeTable.NodeOperationsRecorder>
            implements TreeTable.NodeOperationsRecorder {

        private final Collection<WhereFilter> recordedFilters;

        ForTree(@NotNull final TableDefinition definition) {
            this(definition, List.of(), List.of(), List.of());
        }

        private ForTree(@NotNull final TableDefinition definition,
                @NotNull final Collection<SelectColumn> recordedFormats,
                @NotNull final Collection<SortColumn> recordedSorts,
                @NotNull final Collection<WhereFilter> recordedFilters) {
            super(definition, recordedFormats, recordedSorts);
            this.recordedFilters = recordedFilters;
        }

        @Override
        TreeTable.NodeOperationsRecorder withFormats(@NotNull final Stream<? extends SelectColumn> formats) {
            return new ForTree(definition,
                    Stream.concat(recordedFormats.stream(), formats).collect(Collectors.toList()),
                    recordedSorts, recordedFilters);
        }

        @Override
        TreeTable.NodeOperationsRecorder withSorts(@NotNull final Stream<SortColumn> sorts) {
            return new ForTree(definition, recordedFormats,
                    Stream.concat(sorts, recordedSorts.stream()).collect(Collectors.toList()), recordedFilters);
        }

        TreeTable.NodeOperationsRecorder withFilters(@NotNull final Stream<? extends WhereFilter> filters) {
            return new ForTree(definition, recordedFormats, recordedSorts,
                    Stream.concat(recordedFilters.stream(), filters).collect(Collectors.toList()));
        }

        @Override
        public TreeTable.NodeOperationsRecorder where(String... filters) {
            final FilterRecordingTableAdapter adapter = new FilterRecordingTableAdapter(definition);
            adapter.where(filters);
            return withFilters(adapter.whereFilters());
        }

        @Override
        public TreeTable.NodeOperationsRecorder where(Collection<? extends Filter> filters) {
            final FilterRecordingTableAdapter adapter = new FilterRecordingTableAdapter(definition);
            adapter.where(filters);
            return withFilters(adapter.whereFilters());
        }

        @Override
        public TreeTable.NodeOperationsRecorder where(Filter... filters) {
            final FilterRecordingTableAdapter adapter = new FilterRecordingTableAdapter(definition);
            adapter.where(filters);
            return withFilters(adapter.whereFilters());
        }
    }

    private static abstract class RecordingTableAdapter implements TableAdapter {

        private final TableDefinition definition;

        private RecordingTableAdapter(@NotNull final TableDefinition definition) {
            this.definition = definition;
        }

        @Override
        public final TableDefinition getDefinition() {
            return definition;
        }
    }

    private static final class FormatRecordingTableAdapter extends RecordingTableAdapter {

        private Collection<? extends Selectable> formatColumns;

        private FormatRecordingTableAdapter(@NotNull final TableDefinition definition) {
            super(definition);
        }

        @Override
        public Table updateView(@NotNull final Collection<? extends Selectable> columns) {
            // NB: This is only reachable from formatColumns right now.
            this.formatColumns = columns;
            return this;
        }

        private Stream<? extends SelectColumn> selectColumns() {
            final SelectColumn[] selectColumns = SelectColumn.from(formatColumns);
            SelectAndViewAnalyzer.initializeSelectColumns(getDefinition().getColumnNameMap(), selectColumns);
            return Stream.of(selectColumns);
        }
    }

    private static final class SortRecordingTableAdapter extends RecordingTableAdapter {

        private Collection<SortColumn> sortColumns;

        private SortRecordingTableAdapter(@NotNull TableDefinition definition) {
            super(definition);
        }

        @Override
        public Table sort(@NotNull final Collection<SortColumn> columnsToSortBy) {
            this.sortColumns = columnsToSortBy;
            return this;
        }

        private Stream<SortColumn> sortColumns() {
            return sortColumns.stream();
        }
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

        private Stream<? extends WhereFilter> whereFilters() {
            return Stream.of(WhereFilter.from(filters)).peek(wf -> wf.init(getDefinition()));
        }
    }
}
