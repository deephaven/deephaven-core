//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableArray;
import org.immutables.value.Value;

import java.util.Optional;
import java.util.Set;

/**
 * Utility to force read {@link ColumnSource} data from a {@link Table}. This is useful to check that all of the data
 * can be successfully read without requiring all of the data to be in-memory ({@code table.select()}), or forcing some
 * other table operation to read all of the data ({@code table.where("(isNull(MyCol1) ^ isNull(MyCol2)) && false")}).
 */
@Value.Immutable
@BuildableStyle
public abstract class ForceReadUtility {

    private static final int DEFAULT_READ_SIZE = ChunkedColumnIterator.DEFAULT_CHUNK_SIZE;
    private static final int DEFAULT_COLUMN_CONSIDERATION = 32;

    /**
     * Construct a new builder.
     *
     * @return the builder
     */
    public static Builder builder() {
        return ImmutableForceReadUtility.builder();
    }

    /**
     * Force reads all column data from {@code table}. Equivalent to {@code of(builder().table(table).build())}.
     *
     * @param table the table
     * @see #of(ForceReadUtility)
     */
    public static void of(Table table) {
        of(builder().table(table).build());
    }

    /**
     * Force reads all column data of {@code columnName} from {@code table}. Equivalent to
     * {@code of(builder().table(table).addColumnNames(columnName).build())}.
     *
     * @param table the table
     * @param columnName the column name
     * @see #of(ForceReadUtility)
     */
    public static void of(Table table, String columnName) {
        of(builder().table(table).addColumnNames(columnName).build());
    }

    /**
     * Force reads column data from {@link ForceReadUtility#table() table}.
     *
     * @param options the options
     */
    public static void of(ForceReadUtility options) {
        options.execute();
    }

    /**
     * The table.
     *
     * @return the table
     */
    public abstract Table table();

    /**
     * The row set to read. If unset, the {@link #table() table's} {@link Table#getRowSet() full row set} will be used.
     *
     * @return the row set
     */
    public abstract Optional<RowSet> rowSet();

    /**
     * The column names to read. If empty, all of {@link #table() table's} columns will be used.
     *
     * @return the column names
     */
    public abstract Set<String> columnNames();

    /**
     * The chunk read size. By default, is {@value #DEFAULT_READ_SIZE}.
     *
     * @return the read size
     */
    @Value.Default
    public int readSize() {
        return DEFAULT_READ_SIZE;
    }

    /**
     * The maximum number of columns to consider at any given time. Setting this to {@code 1} means that each column
     * will be fully read before moving on to the next column. By default, is {@value #DEFAULT_COLUMN_CONSIDERATION}.
     *
     * @return the column consideration
     */
    @Value.Default
    public int columnConsideration() {
        return DEFAULT_COLUMN_CONSIDERATION;
    }

    public interface Builder {

        Builder table(Table table);

        Builder rowSet(RowSet rowSet);

        Builder addColumnNames(String element);

        Builder addColumnNames(String... elements);

        Builder addAllColumnNames(Iterable<String> elements);

        Builder readSize(int readSize);

        Builder columnConsideration(int columnConsideration);

        ForceReadUtility build();
    }

    @Value.Check
    final void checkReadSize() {
        if (readSize() < 1) {
            throw new IllegalArgumentException("readSize must be positive");
        }
    }

    @Value.Check
    final void checkColumnConsideration() {
        if (columnConsideration() < 1) {
            throw new IllegalArgumentException("columnConsideration must be positive");
        }
    }

    @Value.Check
    final void checkRowSet() {
        final RowSet desiredRowSet = rowSet().orElse(null);
        if (desiredRowSet == null) {
            return;
        }
        final TrackingRowSet tableRowSet = table().getRowSet();
        if (desiredRowSet == tableRowSet) {
            return;
        }
        if (!desiredRowSet.subsetOf(tableRowSet)) {
            throw new IllegalArgumentException("rowSet must be a subset of the table's rowSet");
        }
    }

    @Value.Check
    final void checkColumns() {
        if (!table().hasColumns(columnNames())) {
            throw new IllegalArgumentException("table does not have specified column(s)");
        }
    }

    private void execute() {
        final RowSet rowSet = rowSet().orElseGet(table()::getRowSet);
        final ColumnSource<?>[] columnSources = columnSources();
        final int readSize = readSize();
        final int maxColumns = columnConsideration();
        for (int i = 0; i < columnSources.length; i += maxColumns) {
            readAll(rowSet, columnSources, i, Math.min(columnSources.length - i, maxColumns), readSize);
        }
    }

    private static void readAll(RowSet rowSet, ColumnSource<?> columnSource, int readSize) {
        try (final ChunkSource.GetContext context = columnSource.makeGetContext(readSize)) {
            try (final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
                while (it.hasMore()) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(readSize);
                    columnSource.getChunk(context, rs);
                }
            }
        }
    }

    private static void readAll(RowSet rowSet, ColumnSource<?>[] columnSources, int columnSourcesIx,
            int columnSourcesLen, int readSize) {
        if (columnSourcesLen == 1) {
            readAll(rowSet, columnSources[columnSourcesIx], readSize);
            return;
        }
        final ChunkSource.GetContext[] contexts = new ChunkSource.GetContext[columnSourcesLen];
        try (
                final SharedContext sharedContext = SharedContext.makeSharedContext();
                final SafeCloseable ignored = new SafeCloseableArray<>(contexts)) {
            for (int i = 0; i < columnSourcesLen; i++) {
                contexts[i] = columnSources[columnSourcesIx + i].makeGetContext(readSize, sharedContext);
            }
            try (final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
                while (it.hasMore()) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(readSize);
                    for (int i = 0; i < columnSourcesLen; i++) {
                        columnSources[columnSourcesIx + i].getChunk(contexts[i], rs);
                    }
                }
            }
        }
    }

    private ColumnSource<?>[] columnSources() {
        return columnNames().isEmpty()
                ? table().getColumnSources().toArray(ColumnSource[]::new)
                : columnNames().stream().map(table()::getColumnSource).toArray(ColumnSource[]::new);
    }
}
