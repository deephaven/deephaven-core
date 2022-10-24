package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.FormatOperationsRecorder;
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
 * Implementations of {@link io.deephaven.engine.table.hierarchical.RollupTable.NodeOperationsRecorder} and
 * {@link io.deephaven.engine.table.hierarchical.TreeTable.NodeOperationsRecorder}.
 */
abstract class NodeOperationsRecorderImpl<IMPL_TYPE extends NodeOperationsRecorderImpl<IMPL_TYPE>>
        implements FormatOperationsRecorder {

    private final TableDefinition definition;

    private final Collection<SelectColumn> recordedFormats;
    private final Collection<WhereFilter> recordedFilters;
    private final Collection<SortColumn> recordedSorts;

    NodeOperationsRecorderImpl(@NotNull final TableDefinition definition) {
        this(definition, List.of(), List.of(), List.of());
    }

    private NodeOperationsRecorderImpl(
            @NotNull final TableDefinition definition,
            @NotNull final Collection<SelectColumn> recordedFormats,
            @NotNull final Collection<WhereFilter> recordedFilters,
            @NotNull final Collection<SortColumn> recordedSorts) {
        this.definition = definition;
        this.recordedFormats = recordedFormats;
        this.recordedFilters = recordedFilters;
        this.recordedSorts = recordedSorts;
    }

    private RecordingTableAdapter<IMPL_TYPE> makeAdapter() {
        // noinspection unchecked
        return new RecordingTableAdapter<>((IMPL_TYPE) this);
    }

    private NodeOperationsRecorderImpl withFormats(@NotNull final Collection<? extends Selectable> formats) {
        final SelectColumn[] addedSelectColumns = SelectColumn.from(formats);
        SelectAndViewAnalyzer.initializeSelectColumns(definition.getColumnNameMap(), addedSelectColumns);
        return new NodeOperationsRecorderImpl(
                definition,
                Stream.concat(recordedFormats.stream(), Stream.of(addedSelectColumns)).collect(Collectors.toList()),
                recordedFilters,
                recordedSorts);
    }

    private NodeOperationsRecorderImpl withFilters(@NotNull final Collection<? extends Filter> filters) {
        final WhereFilter[] addedWhereFilters = WhereFilter.from(filters);
        for (final WhereFilter wf : addedWhereFilters) {
            wf.init(definition);
        }
        return new NodeOperationsRecorderImpl(
                definition,
                recordedFormats,
                Stream.concat(recordedFilters.stream(), Stream.of(addedWhereFilters)).collect(Collectors.toList()),
                recordedSorts);
    }

    private NodeOperationsRecorderImpl withSorts(@NotNull final Collection<SortColumn> sorts) {
        return new NodeOperationsRecorderImpl(
                definition,
                recordedFormats,
                recordedFilters,
                Stream.concat(sorts.stream(), recordedSorts.stream()).collect(Collectors.toList()));
    }

    @Override
    public IMPL_TYPE formatColumns(String... columnFormats) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.formatColumns(columnFormats);
        return adapter.getRecorder();
    }

    @Override
    public IMPL_TYPE formatRowWhere(String condition, String formula) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.formatRowWhere(condition, formula);
        return adapter.getRecorder();
    }

    @Override
    public IMPL_TYPE formatColumnWhere(String columnName, String condition, String formula) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.formatColumnWhere(columnName, condition, formula);
        return adapter.getRecorder();
    }

    @Override
    public IMPL_TYPE sort(String... columnsToSortBy) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.sort(columnsToSortBy);
        return adapter.getRecorder();
    }

    @Override
    public IMPL_TYPE sortDescending(String... columnsToSortBy) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.sort(columnsToSortBy);
        return adapter.getRecorder();
    }

    @Override
    public IMPL_TYPE sort(Collection<SortColumn> columnsToSortBy) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.sort(columnsToSortBy);
        return adapter.getRecorder();
    }

    @Override
    public IMPL_TYPE where(String... filters) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.where(filters);
        return adapter.getRecorder();
    }

    @Override
    public IMPL_TYPE where(Collection<? extends Filter> filters) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.where(filters);
        return adapter.getRecorder();
    }

    @Override
    public IMPL_TYPE where(Filter... filters) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.where(filters);
        return adapter.getRecorder();
    }

    private static abstract class RecordingTableAdapter implements TableAdapter {

        private final TableDefinition definition;

        RecordingTableAdapter(@NotNull final TableDefinition definition) {
            this.definition = definition;
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

        FilterRecordingTableAdapter(@NotNull final TableDefinition definition) {
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
