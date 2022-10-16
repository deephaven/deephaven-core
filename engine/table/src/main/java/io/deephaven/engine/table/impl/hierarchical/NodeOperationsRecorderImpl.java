package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.table.TableDefinition;
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
 * Implementation of {@link NodeOperationsRecorder}.
 */
class NodeOperationsRecorderImpl implements NodeOperationsRecorder {

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

    private RecordingTableAdapter makeAdapter() {
        return new RecordingTableAdapter(this);
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
        for (@NotNull final WhereFilter wf : addedWhereFilters) {
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
    public NodeOperationsRecorder formatColumns(String... columnFormats) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.formatColumns(columnFormats);
        return adapter.getRecorder();
    }

    @Override
    public NodeOperationsRecorder formatRowWhere(String condition, String formula) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.formatRowWhere(condition, formula);
        return adapter.getRecorder();
    }

    @Override
    public NodeOperationsRecorder formatColumnWhere(String columnName, String condition, String formula) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.formatColumnWhere(columnName, condition, formula);
        return adapter.getRecorder();
    }

    @Override
    public NodeOperationsRecorder sort(String... columnsToSortBy) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.sort(columnsToSortBy);
        return adapter.getRecorder();
    }

    @Override
    public NodeOperationsRecorder sortDescending(String... columnsToSortBy) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.sort(columnsToSortBy);
        return adapter.getRecorder();
    }

    @Override
    public NodeOperationsRecorder sort(Collection<SortColumn> columnsToSortBy) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.sort(columnsToSortBy);
        return adapter.getRecorder();
    }

    @Override
    public NodeOperationsRecorder where(String... filters) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.where(filters);
        return adapter.getRecorder();
    }

    @Override
    public NodeOperationsRecorder where(Collection<? extends Filter> filters) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.where(filters);
        return adapter.getRecorder();
    }

    @Override
    public NodeOperationsRecorder where(Filter... filters) {
        final RecordingTableAdapter adapter = makeAdapter();
        adapter.where(filters);
        return adapter.getRecorder();
    }

    private static final class RecordingTableAdapter implements TableAdapter {

        private NodeOperationsRecorderImpl recorder;

        RecordingTableAdapter(@NotNull final NodeOperationsRecorderImpl recorder) {
            this.recorder = recorder;
        }

        private NodeOperationsRecorderImpl getRecorder() {
            return recorder;
        }

        @Override
        public TableDefinition getDefinition() {
            return recorder.definition;
        }

        @Override
        public RecordingTableAdapter sort(Collection<SortColumn> columnsToSortBy) {
            recorder = recorder.withSorts(columnsToSortBy);
            return this;
        }

        @Override
        public RecordingTableAdapter where(Collection<? extends Filter> filters) {
            recorder = recorder.withFilters(filters);
            return this;
        }

        @Override
        public RecordingTableAdapter updateView(Collection<? extends Selectable> columns) {
            // NB: This is only reachable from formatColumns right now.
            recorder = recorder.withFormats(columns);
            return this;
        }
    }
}
