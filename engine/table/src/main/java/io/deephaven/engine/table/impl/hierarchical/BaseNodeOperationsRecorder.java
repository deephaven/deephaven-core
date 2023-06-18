package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.engine.table.impl.AbsoluteSortColumnConventions;
import io.deephaven.engine.table.impl.NoSuchColumnException;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableAdapter;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.util.ColumnFormatting;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Base class for implementations of {@link RollupTable.NodeOperationsRecorder} and
 * {@link TreeTable.NodeOperationsRecorder}.
 */
abstract class BaseNodeOperationsRecorder<TYPE> {

    final TableDefinition definition;

    private final Collection<? extends SelectColumn> recordedFormats;
    private final Collection<SortColumn> recordedSorts;
    private final Collection<? extends SelectColumn> recordedAbsoluteViews;

    volatile TableDefinition resultDefinition;

    BaseNodeOperationsRecorder(
            @NotNull final TableDefinition definition,
            @NotNull final Collection<? extends SelectColumn> recordedFormats,
            @NotNull final Collection<SortColumn> recordedSorts,
            @NotNull final Collection<? extends SelectColumn> recordedAbsoluteViews) {
        this.definition = definition;
        this.recordedFormats = recordedFormats;
        this.recordedSorts = recordedSorts;
        this.recordedAbsoluteViews = recordedAbsoluteViews;
    }

    public boolean isEmpty() {
        return recordedFormats.isEmpty() && recordedSorts.isEmpty();
    }

    Collection<? extends SelectColumn> getRecordedFormats() {
        return recordedFormats;
    }

    Collection<SortColumn> getRecordedSorts() {
        return recordedSorts;
    }

    Collection<? extends SelectColumn> getRecordedAbsoluteViews() {
        return recordedAbsoluteViews;
    }

    static Table applyFormats(
            @Nullable final BaseNodeOperationsRecorder<?> nodeOperations,
            @NotNull final Table input) {
        if (nodeOperations != null && !nodeOperations.getRecordedFormats().isEmpty()) {
            return input.updateView(nodeOperations.getRecordedFormats());
        }
        return input;
    }

    static Table applySorts(@Nullable final BaseNodeOperationsRecorder<?> nodeOperations, @NotNull final Table input) {
        if (nodeOperations != null && !nodeOperations.getRecordedSorts().isEmpty()) {
            return (nodeOperations.getRecordedAbsoluteViews().isEmpty()
                    ? input
                    : input.updateView(nodeOperations.getRecordedAbsoluteViews()))
                    .sort(nodeOperations.getRecordedSorts());
        }
        // NB: We don't bother to drop the absolute columns; nothing gets to consume the output node tables directly.
        return input;
    }

    TableDefinition getResultDefinition() {
        TableDefinition localResult;
        if ((localResult = resultDefinition) != null) {
            return localResult;
        }
        synchronized (this) {
            if ((localResult = resultDefinition) != null) {
                return localResult;
            }
            if (getRecordedFormats().isEmpty()) {
                return resultDefinition = definition;
            }
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                final Table emptyNode = new QueryTable(definition, RowSetFactory.empty().toTracking(),
                        NullValueColumnSource.createColumnSourceMap(definition));
                final Table emptyNodeFormatted = emptyNode.updateView(getRecordedFormats());
                return resultDefinition = emptyNodeFormatted.getDefinition();
            }
        }
    }

    TYPE self() {
        // noinspection unchecked
        return (TYPE) this;
    }

    abstract TYPE withFormats(@NotNull Stream<? extends SelectColumn> formats);

    abstract TYPE withSorts(
            @NotNull Stream<SortColumn> sorts,
            @NotNull final Stream<? extends SelectColumn> absoluteViews);

    static Collection<? extends SelectColumn> mergeFormats(
            @NotNull final Stream<? extends SelectColumn> fs1,
            @NotNull final Stream<? extends SelectColumn> fs2) {
        return Stream.concat(fs1, fs2).collect(Collectors.toList());
    }

    static Collection<SortColumn> mergeSortColumns(
            @NotNull final Stream<SortColumn> scs1,
            @NotNull final Stream<SortColumn> scs2) {
        // Note that we order the new sorts before the old sorts, because they take precedence; that is, we want the
        // effect of sorting previously sorted input.
        // Note also that we also collapse repeated sorts on the same column, picking the first occurrence as it
        // takes precedence.
        return new ArrayList<>(Stream.concat(scs2, scs1)
                .collect(Collectors.toMap(sc -> sc.column().name(), sc -> sc, (scA, scB) -> scA, LinkedHashMap::new))
                .values());
    }

    static Collection<? extends SelectColumn> mergeAbsoluteViews(
            @NotNull final Stream<? extends SelectColumn> avs1, @NotNull final Stream<? extends SelectColumn> avs2) {
        // Note that we want one distinct result per name, and we are intentionally prioritizing "older" views
        return new ArrayList<>(Stream.concat(avs1, avs2).collect(Collectors.toMap(
                SelectColumn::getName, sc -> sc, (sc1, sc2) -> sc1, LinkedHashMap::new)).values());
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
        return adapter.hasSortColumns() ? withSorts(adapter.sortColumns(), adapter.absoluteSelectColumns()) : self();
    }

    public TYPE sortDescending(String... columnsToSortBy) {
        final SortRecordingTableAdapter adapter = new SortRecordingTableAdapter(definition);
        adapter.sortDescending(columnsToSortBy);
        return adapter.hasSortColumns() ? withSorts(adapter.sortColumns(), adapter.absoluteSelectColumns()) : self();
    }

    public TYPE sort(Collection<SortColumn> columnsToSortBy) {
        final SortRecordingTableAdapter adapter = new SortRecordingTableAdapter(definition);
        adapter.sort(columnsToSortBy);
        return adapter.hasSortColumns() ? withSorts(adapter.sortColumns(), adapter.absoluteSelectColumns()) : self();
    }

    static abstract class RecordingTableAdapter implements TableAdapter {

        private final TableDefinition definition;
        private final UpdateGraph updateGraph;

        RecordingTableAdapter(@NotNull final TableDefinition definition) {
            this.updateGraph = ExecutionContext.getContext().getUpdateGraph();
            this.definition = definition;
        }

        @Override
        public final TableDefinition getDefinition() {
            return definition;
        }

        @Override
        public UpdateGraph getUpdateGraph() {
            return updateGraph;
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
            if (!columns.stream()
                    .map(selectable -> selectable.newColumn().name())
                    .allMatch(ColumnFormatting::isFormattingColumn)) {
                throw new UnsupportedOperationException("Invalid formatting columns found in " + columns);
            }
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
        private boolean hasAbsoluteSelectColumns;

        private SortRecordingTableAdapter(@NotNull final TableDefinition definition) {
            super(definition);
        }

        @Override
        public Table sort(@NotNull final Collection<SortColumn> columnsToSortBy) {
            final Set<String> existingColumns = getDefinition().getColumnNames().stream()
                    .filter(column -> !ColumnFormatting.isFormattingColumn(column))
                    .collect(Collectors.toCollection(LinkedHashSet::new));
            final List<String> unknownColumns = columnsToSortBy.stream()
                    .map(sc -> sc.column().name())
                    .map(cn -> {
                        if (AbsoluteSortColumnConventions.isAbsoluteColumnName(cn)) {
                            hasAbsoluteSelectColumns = true;
                            return AbsoluteSortColumnConventions.absoluteColumnNameToBaseName(cn);
                        }
                        return cn;
                    })
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

        private Stream<? extends SelectColumn> absoluteSelectColumns() {
            if (!hasAbsoluteSelectColumns) {
                return Stream.empty();
            }
            // We might want to build generalized updateView node-operations support in order to support features like
            // custom columns in the future. For now, we've plumbed absolute column value sorting via naming
            // conventions. Note that we simply avoid telling the client about these columns when sending schemas, so we
            // have no need to drop them post-sort.
            return sortColumns.stream()
                    .map(sc -> sc.column().name())
                    .filter(AbsoluteSortColumnConventions::isAbsoluteColumnName)
                    .map(cn -> {
                        final String baseColumnName = AbsoluteSortColumnConventions.absoluteColumnNameToBaseName(cn);
                        final Selectable selectable = AbsoluteSortColumnConventions.makeSelectable(cn, baseColumnName);
                        final SelectColumn selectColumn = SelectColumn.of(selectable);
                        selectColumn.initDef(Map.of(baseColumnName, getDefinition().getColumn(baseColumnName)));
                        return selectColumn;
                    });
        }
    }
}
