//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.iterators;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.SimpleTypeMap;
import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;
import java.util.function.BiFunction;

/**
 * Iteration support for values supplied by a {@link ColumnSource}. Implementations retrieve single values at a time in
 * a usually-discouraged Deephaven engine retrieval pattern. This is expected to be low throughput relative to
 * {@link ChunkedColumnIterator} implementations, but may avoid material initialization and teardown costs for small or
 * sparse iterations.
 */
public abstract class SerialColumnIterator<DATA_TYPE> implements ColumnIterator<DATA_TYPE> {

    final ColumnSource<DATA_TYPE> columnSource;
    private final RowSet rowSet;

    private final long lastRowPositionExclusive;

    private long nextRowPosition;

    /**
     * Create a new SerialColumnIterator.
     *
     * @param columnSource The {@link ColumnSource} to fetch values from
     * @param rowSet The {@link RowSet} to iterate over
     * @param firstRowKey The first row key from {@code rowSet} to iterate
     * @param length The total number of rows to iterate
     */
    SerialColumnIterator(
            @NotNull final ColumnSource<DATA_TYPE> columnSource,
            @NotNull final RowSet rowSet,
            final long firstRowKey,
            final long length) {
        this.columnSource = columnSource;
        this.rowSet = rowSet;
        if (firstRowKey == rowSet.firstRowKey()) {
            nextRowPosition = 0;
        } else if ((nextRowPosition = rowSet.find(firstRowKey)) < 0) {
            throw new IllegalArgumentException(String.format(
                    "Invalid first row key %d, not present in iteration row set", firstRowKey));
        }
        if (rowSet.size() - nextRowPosition < length) {
            throw new IllegalArgumentException(String.format(
                    "Invalid length %d, iteration row set only contains %d rows (%d already consumed)",
                    length, rowSet.size(), nextRowPosition));
        }
        lastRowPositionExclusive = nextRowPosition + length;
    }

    @Override
    public final long remaining() {
        return lastRowPositionExclusive - nextRowPosition;
    }

    @Override
    public final boolean hasNext() {
        return nextRowPosition != lastRowPositionExclusive;
    }

    /**
     * Get the row key for the next element of this iterator.
     *
     * @throws NoSuchElementException If this SerialColumnIterator is exhausted
     */
    final long advanceAndGetNextRowKey() {
        if (nextRowPosition == lastRowPositionExclusive) {
            throw new NoSuchElementException();
        }
        return rowSet.get(nextRowPosition++);
    }

    @SuppressWarnings("unchecked")
    private static final SimpleTypeMap<BiFunction<ColumnSource<?>, RowSet, SerialColumnIterator<?>>> TYPE_TO_CONSTRUCTOR =
            SimpleTypeMap.create(
            // @formatter:off
                    (final ColumnSource<?> columnSource, final RowSet rowSet) -> {
                        throw new UnsupportedOperationException("Primitive boolean ColumnSources are unsupported");
                    },
                    (final ColumnSource<?> columnSource, final RowSet rowSet) -> new SerialCharacterColumnIterator((ColumnSource<Character>) columnSource, rowSet),
                    (final ColumnSource<?> columnSource, final RowSet rowSet) -> new      SerialByteColumnIterator((ColumnSource<Byte>     ) columnSource, rowSet),
                    (final ColumnSource<?> columnSource, final RowSet rowSet) -> new     SerialShortColumnIterator((ColumnSource<Short>    ) columnSource, rowSet),
                    (final ColumnSource<?> columnSource, final RowSet rowSet) -> new   SerialIntegerColumnIterator((ColumnSource<Integer>  ) columnSource, rowSet),
                    (final ColumnSource<?> columnSource, final RowSet rowSet) -> new      SerialLongColumnIterator((ColumnSource<Long>     ) columnSource, rowSet),
                    (final ColumnSource<?> columnSource, final RowSet rowSet) -> new     SerialFloatColumnIterator((ColumnSource<Float>    ) columnSource, rowSet),
                    (final ColumnSource<?> columnSource, final RowSet rowSet) -> new    SerialDoubleColumnIterator((ColumnSource<Double>   ) columnSource, rowSet),
                    (final ColumnSource<?> columnSource, final RowSet rowSet) -> new  SerialObjectColumnIterator<>((ColumnSource<?>        ) columnSource, rowSet)
                    // @formatter:on
            );

    public static <DATA_TYPE> ColumnIterator<DATA_TYPE> make(
            @NotNull final ColumnSource<DATA_TYPE> columnSource,
            @NotNull final RowSet rowSet) {
        // noinspection unchecked
        return (ColumnIterator<DATA_TYPE>) TYPE_TO_CONSTRUCTOR.get(columnSource.getType()).apply(columnSource, rowSet);
    }
}
