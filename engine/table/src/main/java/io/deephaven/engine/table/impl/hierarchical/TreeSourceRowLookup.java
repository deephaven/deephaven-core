package io.deephaven.engine.table.impl.hierarchical;

import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.NotificationStepSource;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.by.AggregationRowLookup;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;
import static io.deephaven.engine.table.impl.by.AggregationProcessor.getRowLookup;
import static io.deephaven.engine.table.impl.by.TreeConstants.SOURCE_ROW_LOOKUP_ROW_KEY_COLUMN;

/**
 * Lookup from a tree row's identifier column value to its row key in the source table.
 */
final class TreeSourceRowLookup extends LivenessArtifact implements NotificationStepSource {

    private final Object source;
    private final NotificationStepSource parent;
    private final AggregationRowLookup rowLookup;
    private final ColumnSource<Long> sourceRowKeyColumnSource;

    TreeSourceRowLookup(@NotNull final Object source, @NotNull final QueryTable sourceRowLookupTable) {
        this.source = source;
        if (sourceRowLookupTable.isRefreshing()) {
            parent = sourceRowLookupTable;
            manage(sourceRowLookupTable);
        } else {
            parent = null;
        }
        rowLookup = getRowLookup(sourceRowLookupTable);
        sourceRowKeyColumnSource =
                sourceRowLookupTable.getColumnSource(SOURCE_ROW_LOOKUP_ROW_KEY_COLUMN.name(), long.class);
    }

    boolean sameSource(@NotNull final Object source) {
        return this.source == source;
    }

    /**
     * Gets the row key value where {@code nodeKey} exists in the table, or the {@link #noEntryValue()} if
     * {@code nodeKey} is not found in the table.
     *
     * @param nodeKey A single (boxed) value for single-column keys, or an array of (boxed) values for compound keys
     * @return The row key where {@code nodeKey} exists in the table
     */
    long get(final Object nodeKey) {
        final int idAggregationRow = rowLookup.get(nodeKey);
        if (idAggregationRow == rowLookup.noEntryValue()) {
            return noEntryValue();
        }
        return sourceRowKeyColumnSource.get(idAggregationRow);
    }

    /**
     * Gets the row key value where {@code nodeKey} previously existed in the table, or the {@link #noEntryValue()} if
     * {@code nodeKey} was not found in the table.
     *
     * @param nodeKey A single (boxed) value for single-column keys, or an array of (boxed) values for compound keys
     * @return The row key where {@code nodeKey} previously existed in the table
     */
    long getPrev(final Object nodeKey) {
        final int idAggregationRow = rowLookup.get(nodeKey);
        if (idAggregationRow == rowLookup.noEntryValue()) {
            return noEntryValue();
        }
        return sourceRowKeyColumnSource.getPrev(idAggregationRow);
    }

    @Override
    public long getLastNotificationStep() {
        return parent.getLastNotificationStep();
    }

    @Override
    public boolean satisfied(final long step) {
        return parent.satisfied(step);
    }

    /**
     * @return The value that will be returned from {@link #get(Object)} or {@link #getPrev(Object)} if no entry exists
     *         for a given key
     */
    long noEntryValue() {
        return NULL_ROW_KEY;
    }
}
