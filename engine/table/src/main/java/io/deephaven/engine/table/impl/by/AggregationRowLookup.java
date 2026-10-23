//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.util.SafeCloseableArray;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

/**
 * Tool to identify the aggregation result row key (also row position) from a logical key representing a set of values
 * for the aggregation's group-by columns.
 */
public interface AggregationRowLookup {

    /**
     * Re-usable empty key, for use in (trivial) reverse lookups against no-key aggregations.
     */
    Object[] EMPTY_KEY = new Object[0];

    /**
     * Re-usable unknown row constant to serve as the default return value for {@link #noEntryValue()}.
     */
    int DEFAULT_UNKNOWN_ROW = (int) NULL_ROW_KEY;

    /**
     * Gets the row key value where {@code key} exists in the aggregation result table, or the {@link #noEntryValue()}
     * if {@code key} is not found in the table.
     * <p>
     * This serves to map group-by column values to the row position (also row key) in the result table. Missing keys
     * will map to {@link #noEntryValue()}.
     * <p>
     * Keys are specified as follows:
     * <dl>
     * <dt>No group-by columns</dt>
     * <dd>"Empty" keys are signified by the {@link AggregationRowLookup#EMPTY_KEY} object, or any zero-length
     * {@code Object[]}, and are looked up one at a time; the chunked {@link #get(Chunk[], WritableLongChunk) get} needs
     * a group-by column to give the number of keys</dd>
     * <dt>One group-by column</dt>
     * <dd>Singular keys are (boxed, if needed) objects</dd>
     * <dt>Multiple group-by columns</dt>
     * <dd>Compound keys are {@code Object[]} of (boxed, if needed) objects, in the order of the aggregation's group-by
     * columns</dd>
     * </dl>
     * <p>
     * All key fields must be reinterpreted to the appropriate primitive value before boxing. See
     * {@link io.deephaven.engine.table.impl.sources.ReinterpretUtils#maybeConvertToPrimitive}.
     *
     * @param key A single (boxed) value for single-column keys, or an array of (boxed) values for compound keys
     * @return The row key where {@code key} exists in the table
     */
    int get(Object key);

    /**
     * @return The value that will be returned from {@link #get(Object)} if no entry exists for a given key
     */
    default int noEntryValue() {
        return DEFAULT_UNKNOWN_ROW;
    }

    /**
     * Gets the row key for each of a chunk of keys, given one chunk per group-by column, each reinterpreted to the
     * appropriate primitive value as for {@link #get(Object)}. The aggregation must have at least one group-by column,
     * whose chunk gives the number of keys.
     *
     * @param keyChunks The keys, one chunk per group-by column, at least one, all the same size
     * @param rowKeys Receives the row key of each key, or {@link #noEntryValue()} for a key that is not found; its size
     *        is set to the number of keys
     */
    void get(@NotNull Chunk<? extends Values>[] keyChunks, @NotNull WritableLongChunk<RowKeys> rowKeys);

    /**
     * Implement {@link #get(Chunk[], WritableLongChunk)} by boxing each key and calling {@link #get(Object)} on
     * {@code lookup}, for implementations that cannot search a chunk of keys at once.
     *
     * @param lookup The lookup to call for each key
     * @param keyChunks The keys, one chunk per group-by column, at least one, all the same size
     * @param rowKeys Receives the row key of each key; its size is set to the number of keys
     */
    static void boxedGet(
            @NotNull final AggregationRowLookup lookup,
            @NotNull final Chunk<? extends Values>[] keyChunks,
            @NotNull final WritableLongChunk<RowKeys> rowKeys) {
        Require.gtZero(keyChunks.length, "keyChunks.length");
        final int size = keyChunks[0].size();
        rowKeys.setSize(size);
        // noinspection unchecked
        final ObjectChunk<?, ? extends Values>[] boxedKeys = new ObjectChunk[keyChunks.length];
        final ChunkBoxer.BoxerKernel[] boxers = new ChunkBoxer.BoxerKernel[keyChunks.length];
        try (final SafeCloseableArray<ChunkBoxer.BoxerKernel> ignored = new SafeCloseableArray<>(boxers)) {
            for (int ci = 0; ci < keyChunks.length; ++ci) {
                boxers[ci] = ChunkBoxer.getBoxer(keyChunks[ci].getChunkType(), size);
                boxedKeys[ci] = boxers[ci].box(keyChunks[ci]);
            }
            if (keyChunks.length == 1) {
                for (int ii = 0; ii < size; ++ii) {
                    rowKeys.set(ii, lookup.get(boxedKeys[0].get(ii)));
                }
                return;
            }
            for (int ii = 0; ii < size; ++ii) {
                // A fresh array per key, since a lookup may keep the key it is given.
                final Object[] key = new Object[keyChunks.length];
                for (int ci = 0; ci < keyChunks.length; ++ci) {
                    key[ci] = boxedKeys[ci].get(ii);
                }
                rowKeys.set(ii, lookup.get(key));
            }
        }
    }
}
