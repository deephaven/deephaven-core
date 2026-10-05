//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;

/**
 * The distinct keys of a set table, each with the number of set rows holding it, maintained as the set table ticks and
 * matched against chunks of a filtered table's key columns.
 * <p>
 * A single key column is held in a fastutil open hash map from key to count, whose lookups read only its key array. A
 * compound key is held as tuples in a {@link TupleMapSetKernel}, which is matched one chunk per column, so that no
 * tuple is assembled per row.
 * <p>
 * An update removes, then adds, then calls {@link #finishRemove(RowSequence)}: a key whose count falls to zero stays
 * until then, so that an addition later in the same update revives it rather than reporting it as both removed and
 * added. Mutation happens only on the thread that owns the set, but {@link #matchValues} and {@link #iterator()} may
 * run concurrently with it; see {@link SharedSetKernel#kernel()} for the protocol that makes such reads safe.
 */
abstract class SetKernel {

    /**
     * Create a set from the keys of {@code initialRows}, which may hold the same key more than once.
     *
     * @param keySources The set table's key sources, reinterpreted to primitives
     * @param initialRows The rows holding the initial keys
     * @param usePrev Whether to read the previous values of {@code initialRows}
     * @return The new set
     */
    static SetKernel create(
            @NotNull final ColumnSource<?>[] keySources,
            @NotNull final RowSet initialRows,
            final boolean usePrev) {
        if (keySources.length == 1) {
            final SingleColumnSetKernel kernel = SingleColumnSetKernel.make(keySources[0]);
            kernel.add(initialRows, usePrev);
            return kernel;
        }
        final TupleMapSetKernel kernel = TupleSetKernelFactory.make(keySources);
        kernel.add(initialRows, usePrev);
        return kernel;
    }

    /**
     * @return The number of keys in the set, between updates
     */
    abstract long size();

    /**
     * Must be called before each update's {@link #remove(RowSequence)} and {@link #add(RowSequence)} calls, to begin
     * tallying the keys that enter and leave the set.
     */
    abstract void beginUpdate();

    /**
     * Add the current keys of {@code rows}.
     *
     * @param rows The rows whose keys to add
     */
    abstract void add(@NotNull RowSequence rows);

    /**
     * Remove the previous keys of {@code rows}, each of which must be in the set.
     *
     * @param rows The rows whose previous keys to remove
     */
    abstract void remove(@NotNull RowSequence rows);

    /**
     * Must be called after each update's {@link #remove(RowSequence)} and {@link #add(RowSequence)} calls, once for
     * each row sequence passed to {@link #remove(RowSequence)}, to drop the keys that have left the set.
     *
     * @param removedRows Rows whose previous keys were removed
     */
    abstract void finishRemove(@NotNull RowSequence removedRows);

    /**
     * @return Whether any key entered the set during this update
     */
    abstract boolean keysAdded();

    /**
     * @return Whether any key left the set during this update
     */
    abstract boolean keysRemoved();

    /**
     * Select the row keys whose key is in the set, or, if {@code inclusion} is false, not in the set. This keeps no
     * state between calls, so that any number of readers may match at once.
     *
     * @param keyChunks The key of each row, one chunk per key column, reinterpreted as the set's keys are
     * @param rowKeys The row key of each row
     * @param results Receives the selected row keys
     * @param inclusion Whether to select the rows whose key is in the set rather than those whose key is not
     */
    abstract void matchValues(
            @NotNull Chunk<Values>[] keyChunks,
            @NotNull LongChunk<OrderedRowKeys> rowKeys,
            @NotNull WritableLongChunk<OrderedRowKeys> results,
            boolean inclusion);

    /**
     * Iterate the keys in the set, as the tuples a {@code TupleSource} over the set's reinterpreted key sources makes.
     *
     * @return An iterator of the keys in the set; the caller must close it
     */
    abstract CloseableIterator<Object> iterator();

    /**
     * @return {@code keys} as an iterator that has nothing to close
     */
    static CloseableIterator<Object> closeable(@NotNull final Iterator<?> keys) {
        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return keys.hasNext();
            }

            @Override
            public Object next() {
                return keys.next();
            }
        };
    }
}
