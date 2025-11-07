//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link RollupTable.NodeOperationsRecorder} implementation.
 */
class RollupNodeOperationsRecorder extends BaseNodeOperationsRecorder<RollupTable.NodeOperationsRecorder>
        implements RollupTable.NodeOperationsRecorder {

    private final Collection<? extends SelectColumn> recordedUpdateViews;
    private final RollupTable.NodeType nodeType;

    RollupNodeOperationsRecorder(
            @NotNull final TableDefinition definition,
            @NotNull final RollupTable.NodeType nodeType) {
        this(definition, nodeType, List.of(), List.of(), List.of(), List.of());
    }

    private RollupNodeOperationsRecorder(
            @NotNull final TableDefinition definition,
            @NotNull final RollupTable.NodeType nodeType,
            @NotNull final Collection<? extends SelectColumn> recordedFormats,
            @NotNull final Collection<SortColumn> recordedSorts,
            @NotNull final Collection<? extends SelectColumn> recordedAbsoluteViews,
            @NotNull final Collection<? extends SelectColumn> recordedUpdateViews) {
        super(definition, recordedFormats, recordedSorts, recordedAbsoluteViews);
        this.recordedUpdateViews = recordedUpdateViews;
        this.nodeType = nodeType;
    }

    RollupTable.NodeType getNodeType() {
        return nodeType;
    }

    @Override
    public boolean isEmpty() {
        return super.isEmpty() && recordedUpdateViews.isEmpty();
    }

    Collection<? extends SelectColumn> getRecordedUpdateViews() {
        return recordedUpdateViews;
    }

    static Table applyUpdateViews(
            @Nullable final RollupNodeOperationsRecorder nodeOperations,
            @NotNull final Table input) {
        if (nodeOperations != null && !nodeOperations.getRecordedUpdateViews().isEmpty()) {
            return input.updateView(nodeOperations.getRecordedUpdateViews());
        }
        return input;
    }

    @Override
    TableDefinition getResultDefinition() {
        // Get the base results and apply recorded updateViews to get the final result definition
        final TableDefinition baseDefinition = super.getResultDefinition();
        if (getRecordedUpdateViews().isEmpty()) {
            // No new column definitions will be created
            return baseDefinition;
        }
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final Table emptyTable = new QueryTable(baseDefinition, RowSetFactory.empty().toTracking(),
                    NullValueColumnSource.createColumnSourceMap(baseDefinition));
            return emptyTable.updateView(getRecordedUpdateViews()).getDefinition();
        }
    }

    @Override
    RollupTable.NodeOperationsRecorder withFormats(@NotNull final Stream<? extends SelectColumn> formats) {
        return new RollupNodeOperationsRecorder(getResultDefinition(), nodeType,
                mergeFormats(getRecordedFormats().stream(), formats),
                getRecordedSorts(),
                getRecordedAbsoluteViews(),
                getRecordedUpdateViews());
    }

    @Override
    RollupTable.NodeOperationsRecorder withSorts(
            @NotNull final Stream<SortColumn> sorts,
            @NotNull final Stream<? extends SelectColumn> absoluteViews) {
        // Arguably, we might want to look for duplicate absolute views, but that would imply that there were also
        // duplicative sorts, which we don't really expect to happen.
        return new RollupNodeOperationsRecorder(getResultDefinition(), nodeType,
                getRecordedFormats(),
                mergeSortColumns(getRecordedSorts().stream(), sorts),
                mergeAbsoluteViews(getRecordedAbsoluteViews().stream(), absoluteViews),
                getRecordedUpdateViews());
    }

    private RollupTable.NodeOperationsRecorder withUpdateView(@NotNull final Stream<? extends SelectColumn> columns) {
        return new RollupNodeOperationsRecorder(getResultDefinition(), nodeType,
                getRecordedFormats(),
                getRecordedSorts(),
                getRecordedAbsoluteViews(),
                mergeUpdateViews(getRecordedUpdateViews().stream(), columns));
    }

    private static Collection<? extends SelectColumn> mergeUpdateViews(
            @NotNull final Stream<? extends SelectColumn> avs1, @NotNull final Stream<? extends SelectColumn> avs2) {
        return new ArrayList<>(Stream.concat(avs1, avs2).collect(Collectors.toMap(
                SelectColumn::getName, sc -> sc, (sc1, sc2) -> sc1, LinkedHashMap::new)).values());
    }

    public RollupTable.NodeOperationsRecorder updateView(String... columns) {
        return updateView(Selectable.from(columns));
    }

    public RollupTable.NodeOperationsRecorder updateView(Collection<Selectable> columns) {
        final TableDefinition definition = getResultDefinition();
        final Set<String> existingColumns = definition.getColumnNameSet();
        for (final Selectable column : columns) {
            if (existingColumns.contains(column.newColumn().name())) {
                throw new IllegalArgumentException("Column " + column.newColumn().name() + " already exists");
            }
        }
        final UpdateViewRecordingTableAdapter adapter = new UpdateViewRecordingTableAdapter(getResultDefinition());
        adapter.updateView(columns);
        return adapter.hasSelectColumns() ? withUpdateView(adapter.selectColumns()) : self();
    }

    RollupNodeOperationsRecorder withOperations(@NotNull final RollupNodeOperationsRecorder other) {
        if (!getResultDefinition().equals(other.getResultDefinition()) || nodeType != other.nodeType) {
            throw new IllegalArgumentException(
                    "Incompatible operation recorders; compatible recorders must be created from the same table, with the same node type");
        }
        if (other.isEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return other;
        }
        return new RollupNodeOperationsRecorder(getResultDefinition(), nodeType,
                mergeFormats(getRecordedFormats().stream(), other.getRecordedFormats().stream()),
                mergeSortColumns(getRecordedSorts().stream(), other.getRecordedSorts().stream()),
                mergeAbsoluteViews(getRecordedAbsoluteViews().stream(), other.getRecordedAbsoluteViews().stream()),
                mergeUpdateViews(getRecordedUpdateViews().stream(), other.getRecordedUpdateViews().stream()));
    }

    /**
     * Adapter for recording updateView operations, specific to the {@link RollupNodeOperationsRecorder}.
     */
    private static final class UpdateViewRecordingTableAdapter extends RecordingTableAdapter {

        private Collection<? extends Selectable> columns;

        private UpdateViewRecordingTableAdapter(@NotNull final TableDefinition definition) {
            super(definition);
        }

        @Override
        public Table updateView(@NotNull final Collection<? extends Selectable> columns) {
            this.columns = columns;
            return this;
        }

        private boolean hasSelectColumns() {
            return !columns.isEmpty();
        }

        private Stream<? extends SelectColumn> selectColumns() {
            final SelectColumn[] selectColumns = SelectColumn.from(columns);
            SelectAndViewAnalyzer.initializeSelectColumns(getDefinition().getColumnNameMap(), selectColumns);
            // Enforce no virtual row variables are used.
            List<SelectColumn> invalidColumns = Arrays.asList(selectColumns).stream()
                    .filter(SelectColumn::hasVirtualRowVariables).collect(Collectors.toList());
            if (!invalidColumns.isEmpty()) {
                throw new IllegalArgumentException(
                        "updateView does not support virtual row variables. Invalid columns: " + invalidColumns);
            }
            return Stream.of(selectColumns);
        }
    }
}
