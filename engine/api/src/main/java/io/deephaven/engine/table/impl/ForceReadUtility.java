//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.iterators.ChunkedColumnIterator;
import io.deephaven.engine.updategraph.UpdateGraph;
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
    private static final int DEFAULT_MAX_COLUMNS = 32;

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
     * Force reads all column data of {@code columnNames} from {@code table}; {@code columnNames} must have at least one
     * element. Equivalent to {@code of(builder().table(table).addColumnNames(columnNames).build())}.
     *
     * @param table the table
     * @param columnNames the column names
     * @see #of(ForceReadUtility)
     */
    public static void of(Table table, String... columnNames) {
        if (columnNames.length == 0) {
            throw new IllegalArgumentException("columnNames must be non-empty");
        }
        of(builder().table(table).addColumnNames(columnNames).build());
    }

    /**
     * Force reads column data from {@link ForceReadUtility#table() table}. Equivalent to
     * {@code of(options, options.table().getRowSet())}.
     *
     * <p>
     * Callers must take an appropriate lock if necessary, see {@link UpdateGraph#checkInitiateSerialTableOperation()}.
     *
     * @param options the options
     */
    public static void of(ForceReadUtility options) {
        of(options, options.table().getRowSet());
    }

    /**
     * Force reads {@code rowSet} column data from {@link ForceReadUtility#table() table}. {@code rowSet} is expected to
     * be the {@link ForceReadUtility#table() table's} row set or subset.
     *
     * <p>
     * Callers must take an appropriate lock if necessary, see {@link UpdateGraph#checkInitiateSerialTableOperation()}.
     *
     * @param options the options
     * @param rowSet the row set
     */
    public static void of(ForceReadUtility options, RowSet rowSet) {
        options.execute(rowSet);
    }

    /**
     * The table.
     *
     * @return the table
     */
    public abstract Table table();

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
     * will be fully read before moving on to the next column; setting this to {@link Integer#MAX_VALUE} means that all
     * columns will be read together (that is, {@code rowSet} in {@link #of(ForceReadUtility, RowSet)} will be iterated
     * through exactly once). By default, is {@value #DEFAULT_MAX_COLUMNS}.
     *
     * @return the maximum number of columns to consider at any given time
     */
    @Value.Default
    public int maxColumns() {
        return DEFAULT_MAX_COLUMNS;
    }

    public interface Builder {

        Builder table(Table table);

        Builder addColumnNames(String element);

        Builder addColumnNames(String... elements);

        Builder addAllColumnNames(Iterable<String> elements);

        Builder readSize(int readSize);

        Builder maxColumns(int maxColumns);

        ForceReadUtility build();
    }

    @Value.Check
    final void checkReadSize() {
        if (readSize() < 1) {
            throw new IllegalArgumentException("readSize must be positive");
        }
    }

    @Value.Check
    final void checkMaxColumns() {
        if (maxColumns() < 1) {
            throw new IllegalArgumentException("maxColumns must be positive");
        }
    }

    @Value.Check
    final void checkColumns() {
        table().getDefinition().checkHasColumns(columnNames());
    }

    private void execute(final RowSet rowSet) {
        if (table().isRefreshing()) {
            table().getUpdateGraph().checkInitiateSerialTableOperation();
        }
        final ColumnSource<?>[] columnSources = columnSources();
        final int readSize = readSize();
        final int maxColumns = maxColumns();
        for (int ci = 0; ci < columnSources.length; ci += maxColumns) {
            readAll(rowSet, columnSources, ci, Math.min(columnSources.length - ci, maxColumns), readSize);
        }
    }

    private static void readAll(final RowSet rowSet, final ColumnSource<?> columnSource, final int readSize) {
        try (final ChunkSource.GetContext context = columnSource.makeGetContext(readSize)) {
            try (final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
                while (it.hasMore()) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(readSize);
                    columnSource.getChunk(context, rs);
                }
            }
        }
    }

    private static void readAll(final RowSet rowSet, final ColumnSource<?>[] columnSources, final int columnSourcesIx,
            final int columnSourcesLen, final int readSize) {
        if (columnSourcesLen == 1) {
            readAll(rowSet, columnSources[columnSourcesIx], readSize);
            return;
        }
        final ChunkSource.GetContext[] contexts = new ChunkSource.GetContext[columnSourcesLen];
        try (
                final SharedContext sharedContext = SharedContext.makeSharedContext();
                final SafeCloseable ignored = new SafeCloseableArray<>(contexts)) {
            for (int ci = 0; ci < columnSourcesLen; ci++) {
                contexts[ci] = columnSources[columnSourcesIx + ci].makeGetContext(readSize, sharedContext);
            }
            try (final RowSequence.Iterator it = rowSet.getRowSequenceIterator()) {
                while (it.hasMore()) {
                    final RowSequence rs = it.getNextRowSequenceWithLength(readSize);
                    for (int ci = 0; ci < columnSourcesLen; ci++) {
                        columnSources[columnSourcesIx + ci].getChunk(contexts[ci], rs);
                    }
                    sharedContext.reset();
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
