//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

/**
 * A {@link SetKernel} for a single key column, held in a fastutil open hash map from key to count by a subclass for the
 * column's chunk type.
 */
abstract class SingleColumnSetKernel extends SetKernel {

    private static final int CHUNK_SIZE = 1 << 12;

    /** The set table's key source, reinterpreted to a primitive. */
    private final ColumnSource<?> keySource;

    /** The keys inserted during this update, which were not in the set before it. */
    long insertedKeys;
    /** The keys whose count fell to zero during this update. */
    long emptiedKeys;
    /** The keys whose count rose from zero during this update, having fallen to zero earlier in it. */
    long revivedKeys;

    SingleColumnSetKernel(@NotNull final ColumnSource<?> keySource) {
        this.keySource = keySource;
    }

    static SingleColumnSetKernel make(@NotNull final ColumnSource<?> keySource) {
        switch (keySource.getChunkType()) {
            case Char:
                return new CharSetKernel(keySource);
            case Byte:
                return new ByteSetKernel(keySource);
            case Short:
                return new ShortSetKernel(keySource);
            case Int:
                return new IntSetKernel(keySource);
            case Long:
                return new LongSetKernel(keySource);
            case Float:
                return new FloatSetKernel(keySource);
            case Double:
                return new DoubleSetKernel(keySource);
            case Object:
                return new ObjectSetKernel(keySource);
            default:
                throw new IllegalArgumentException("Unsupported key chunk type " + keySource.getChunkType());
        }
    }

    /**
     * Increment the count of each key in {@code keys}, inserting the keys not in the set.
     */
    abstract void addKeys(@NotNull Chunk<? extends Values> keys);

    /**
     * Decrement the count of each key in {@code keys}, each of which must be in the set.
     */
    abstract void removeKeys(@NotNull Chunk<? extends Values> keys);

    /**
     * Drop each key in {@code keys} whose count is zero.
     */
    abstract void dropEmptied(@NotNull Chunk<? extends Values> keys);

    /**
     * Select the row keys whose key is in the set, or, if {@code inclusion} is false, not in the set; {@code results}
     * is empty to begin with.
     */
    abstract void match(
            @NotNull Chunk<Values> keys,
            @NotNull LongChunk<OrderedRowKeys> rowKeys,
            @NotNull WritableLongChunk<OrderedRowKeys> results,
            boolean inclusion);

    @Override
    final void beginUpdate() {
        insertedKeys = 0;
        emptiedKeys = 0;
        revivedKeys = 0;
    }

    @Override
    final void add(@NotNull final RowSequence rows) {
        add(rows, false);
    }

    final void add(@NotNull final RowSequence rows, final boolean usePrev) {
        forEachKeyChunk(rows, usePrev, this::addKeys);
    }

    @Override
    final void remove(@NotNull final RowSequence rows) {
        forEachKeyChunk(rows, true, this::removeKeys);
    }

    @Override
    final void finishRemove(@NotNull final RowSequence removedRows) {
        if (emptiedKeys == revivedKeys) {
            // Every key that fell to zero was revived, so none is left to drop.
            return;
        }
        forEachKeyChunk(removedRows, true, this::dropEmptied);
    }

    @Override
    final boolean keysAdded() {
        return insertedKeys > 0;
    }

    @Override
    final boolean keysRemoved() {
        return emptiedKeys > revivedKeys;
    }

    @Override
    final void matchValues(
            @NotNull final Chunk<Values>[] keyChunks,
            @NotNull final LongChunk<OrderedRowKeys> rowKeys,
            @NotNull final WritableLongChunk<OrderedRowKeys> results,
            final boolean inclusion) {
        results.setSize(0);
        match(keyChunks[0], rowKeys, results, inclusion);
    }

    private void forEachKeyChunk(
            @NotNull final RowSequence rows,
            final boolean usePrev,
            @NotNull final Consumer<Chunk<? extends Values>> action) {
        if (rows.isEmpty()) {
            return;
        }
        final int chunkSize = (int) Math.min(rows.size(), CHUNK_SIZE);
        // @formatter:off
        try (final ColumnSource.GetContext getContext = keySource.makeGetContext(chunkSize);
             final RowSequence.Iterator rowsIterator = rows.getRowSequenceIterator()) {
            // @formatter:on
            while (rowsIterator.hasMore()) {
                final RowSequence chunkRows = rowsIterator.getNextRowSequenceWithLength(chunkSize);
                action.accept(usePrev
                        ? keySource.getPrevChunk(getContext, chunkRows)
                        : keySource.getChunk(getContext, chunkRows));
            }
        }
    }
}
