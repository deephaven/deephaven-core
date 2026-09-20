//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.NaturalJoinType;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.util.hashing.ToIntFunctor;
import io.deephaven.chunk.util.hashing.ToIntegerCast;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.engine.table.impl.JoinControl.CHUNK_SIZE;

/**
 * A static natural join manager that only maintains a single array source for the right indices.
 *
 * <p>
 * >We do not store the keys, because we know that only one possible value can exist in each slot.
 * </p>
 *
 * <p>
 * This manager is appropriate for non-composite joins with small types (byte, char, short) or where we know that the
 * table has a limited range (e.g., a symbol table).
 * </p>
 */
class SimpleUniqueStaticNaturalJoinStateManager extends StaticNaturalJoinStateManager {
    private final int tableSize;
    /** The chunk type and offset that map a key value onto a table slot; a functor is made per pass over the keys. */
    private final ChunkType keyChunkType;
    private final int keyOffset;

    /**
     * The right row key for each possible key value, indexed by the transformed key; {@link RowSequence#NULL_ROW_KEY}
     * for no right row, {@link #DUPLICATE_RIGHT_VALUE} for several. The table is fixed size, so a plain array serves.
     */
    private final long[] rightRowKeys;

    SimpleUniqueStaticNaturalJoinStateManager(
            ColumnSource<?>[] tableKeySources,
            int tableSize,
            ChunkType keyChunkType,
            int keyOffset,
            NaturalJoinType joinType,
            boolean addOnly) {
        super(tableKeySources, joinType, addOnly);
        this.tableSize = Require.gtZero(tableSize, "tableSize");
        this.keyChunkType = keyChunkType;
        this.keyOffset = keyOffset;
        rightRowKeys = new long[tableSize];
        Arrays.fill(rightRowKeys, RowSequence.NULL_ROW_KEY);
    }

    /**
     * Make a functor that maps a chunk of key values onto table slots. The functor owns a pooled chunk, so it lives no
     * longer than the pass over the keys that uses it.
     *
     * @param chunkSize the largest chunk the functor will be applied to
     */
    private ToIntFunctor<Values> makeKeyTransform(final int chunkSize) {
        return ToIntegerCast.makeToIntegerCast(keyChunkType, chunkSize, keyOffset);
    }

    void setRightSide(RowSet rightRowSet, ColumnSource<?> valueSource) {
        final int chunkSize = (int) Math.min(CHUNK_SIZE, rightRowSet.size());
        try (final RowSequence.Iterator rsIt = rightRowSet.getRowSequenceIterator();
                final ToIntFunctor<Values> transform = makeKeyTransform(chunkSize);
                final ColumnSource.GetContext getContext = valueSource.makeGetContext(chunkSize)) {
            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(CHUNK_SIZE);

                final Chunk<? extends Values> dataChunk = valueSource.getChunk(getContext, chunkOk);
                final IntChunk<? extends Values> dataChunkAsInt = transform.apply(dataChunk);
                final MutableInt position = new MutableInt(0);

                chunkOk.forEachRowKey((long keyIndex) -> {
                    final int tableLocation = dataChunkAsInt.get(position.get());
                    position.increment();
                    if (tableLocation < 0 || tableLocation >= tableSize) {
                        return true;
                    }
                    final long existingRight = rightRowKeys[tableLocation];
                    if (existingRight == RowSequence.NULL_ROW_KEY || joinType == NaturalJoinType.LAST_MATCH) {
                        rightRowKeys[tableLocation] = keyIndex;
                    } else {
                        if (joinType == NaturalJoinType.FIRST_MATCH) {
                            // no-op, already have the first match
                        } else {
                            rightRowKeys[tableLocation] = DUPLICATE_RIGHT_VALUE;
                        }
                    }
                    return true;
                });
            }
        }
    }

    @Override
    protected void decorateLeftSide(RowSet leftRowSet, ColumnSource<?>[] valueSources,
            LongArraySource leftRedirections) {
        if (leftRowSet.isEmpty()) {
            return;
        }

        Assert.eq(valueSources.length, "valueSources.length", 1);
        final ColumnSource<?> valueSource = valueSources[0];

        final int chunkSize = (int) Math.min(CHUNK_SIZE, leftRowSet.size());
        try (final RowSequence.Iterator rsIt = leftRowSet.getRowSequenceIterator();
                final ToIntFunctor<Values> transform = makeKeyTransform(chunkSize);
                final ColumnSource.GetContext getContext = valueSource.makeGetContext(chunkSize)) {
            long offset = 0;
            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(CHUNK_SIZE);

                final Chunk<? extends Values> dataChunk = valueSource.getChunk(getContext, chunkOk);

                final IntChunk<? extends Values> dataChunkAsInt = transform.apply(dataChunk);
                for (int ii = 0; ii < dataChunkAsInt.size(); ++ii) {
                    final int tableLocation = dataChunkAsInt.get(ii);
                    if (tableLocation < 0 || tableLocation >= tableSize) {
                        leftRedirections.set(offset + ii, NO_RIGHT_ENTRY_VALUE);
                        continue;
                    }
                    final long existingRight = rightRowKeys[tableLocation];

                    if (existingRight == DUPLICATE_RIGHT_VALUE) {
                        throw new IllegalStateException("Natural Join found duplicate right key for "
                                + keySourcesForErrorMessages[0].get(leftRowSet.get(offset + ii)));
                    }
                    leftRedirections.set(offset + ii, existingRight);
                }

                offset += chunkOk.size();
            }
        }
    }

    @NotNull
    WritableRowRedirection buildRowRedirection(QueryTable leftTable,
            LongArraySource leftRedirections, JoinControl.RedirectionType redirectionType) {
        return buildRowRedirection(leftTable, leftRedirections::getLong, redirectionType);
    }
}
