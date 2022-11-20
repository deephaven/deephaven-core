package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.*;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.engine.table.impl.TableAdapter;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer;
import io.deephaven.engine.util.ColumnFormattingValues;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implementations of {@link RollupTable.NodeOperationsRecorder} and {@link TreeTable.NodeOperationsRecorder}.
 */
abstract class BaseNodeOperationsRecorder<TYPE> {

    final TableDefinition definition;

    final Collection<? extends SelectColumn> recordedFormats;
    final Collection<SortColumn> recordedSorts;

    BaseNodeOperationsRecorder(
            @NotNull final TableDefinition definition,
            @NotNull final Collection<? extends SelectColumn> recordedFormats,
            @NotNull final Collection<SortColumn> recordedSorts) {
        this.definition = definition;
        this.recordedFormats = recordedFormats;
        this.recordedSorts = recordedSorts;
    }

    TYPE self() {
        // noinspection unchecked
        return (TYPE) this;
    }

    abstract TYPE withFormats(@NotNull Stream<? extends SelectColumn> formats);

    abstract TYPE withSorts(@NotNull Stream<SortColumn> sorts);

    Collection<? extends SelectColumn> getRecordedFormats() {
        return recordedFormats;
    }

    Collection<SortColumn> getRecordedSorts() {
        return recordedSorts;
    }

    public TYPE formatColumns(String... columnFormats) {
        final FormatRecordingTableAdapter adapter = new FormatRecordingTableAdapter(definition);
        adapter.formatColumns(columnFormats);
        return adapter.hasSelectColumns() ? withFormats(adapter.selectColumns()) : self();
    }

    public TYPE formatRowWhere(String condition, String formula) {
        final FormatRecordingTableAdapter adapter = new FormatRecordingTableAdapter(definition);
        adapter.formatRowWhere(condition, formula);
        return adapter.hasSelectColumns() ? withFormats(adapter.selectColumns()) : self();
    }

    public TYPE formatColumnWhere(String columnName, String condition, String formula) {
        final FormatRecordingTableAdapter adapter = new FormatRecordingTableAdapter(definition);
        adapter.formatColumnWhere(columnName, condition, formula);
        return adapter.hasSelectColumns() ? withFormats(adapter.selectColumns()) : self();
    }

    public TYPE sort(String... columnsToSortBy) {
        final SortRecordingTableAdapter adapter = new SortRecordingTableAdapter(definition);
        adapter.sort(columnsToSortBy);
        return adapter.hasSortColumns() ? withSorts(adapter.sortColumns()) : self();
    }

    public TYPE sortDescending(String... columnsToSortBy) {
        final SortRecordingTableAdapter adapter = new SortRecordingTableAdapter(definition);
        adapter.sortDescending(columnsToSortBy);
        return adapter.hasSortColumns() ? withSorts(adapter.sortColumns()) : self();
    }

    public TYPE sort(Collection<SortColumn> columnsToSortBy) {
        final SortRecordingTableAdapter adapter = new SortRecordingTableAdapter(definition);
        adapter.sort(columnsToSortBy);
        return adapter.hasSortColumns() ? withSorts(adapter.sortColumns()) : self();
    }

    static abstract class RecordingTableAdapter implements TableAdapter {

        private final TableDefinition definition;

        RecordingTableAdapter(@NotNull final TableDefinition definition) {
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

        private boolean hasSelectColumns() {
            return !formatColumns.isEmpty();
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
            final Set<String> existingColumns = getDefinition().getColumnNames().stream()
                    .filter(column -> !ColumnFormattingValues.isFormattingColumn(column))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            final List<String> unknownColumns = columnsToSortBy.stream()
                    .map(sc -> sc.column().name())
                    .filter(column -> !existingColumns.contains(column))
                    .collect(Collectors.toList());
            if (!unknownColumns.isEmpty()) {
                throw new NoSuchColumnException(existingColumns, unknownColumns);
            }

            this.sortColumns = columnsToSortBy;
            return this;
        }

        private boolean hasSortColumns() {
            return !sortColumns.isEmpty();
        }

        private Stream<SortColumn> sortColumns() {
            return sortColumns.stream();
        }
    }
}
