package io.deephaven.engine.table.iterators;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.util.annotations.FinalDefault;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Iteration support for values supplied by a columns.
 *
 * @apiNote ColumnIterators must be explicitly {@link #close() closed} or used until exhausted in order to avoid
 *          resource leaks.
 */
public interface ColumnIterator<DATA_TYPE> extends CloseableIterator<DATA_TYPE> {

    /**
     * @return The number of elements remaining to this ColumnIterator
     */
    long remaining();
}
