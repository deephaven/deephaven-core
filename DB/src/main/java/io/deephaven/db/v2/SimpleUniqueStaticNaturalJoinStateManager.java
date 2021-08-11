package io.deephaven.db.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.db.v2.hashing.ToIntFunctor;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LongArraySource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.RedirectionIndex;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

/**
 * A static natural join manager that only maintains a single array source for the right indices.
 *
 * We do not store the keys, because we know that only one possible value can exist in each slot.
 *
 * This manager is appropriate for non-composite joins with small types (byte, char, short) or where we know that the
 * table has a limited range (e.g., a symbol table).
 */
class SimpleUniqueStaticNaturalJoinStateManager extends StaticNaturalJoinStateManager {
    private final int tableSize;
    private final ToIntFunctor<Values> transform;

    private final LongArraySource rightIndexSource = new LongArraySource();

    SimpleUniqueStaticNaturalJoinStateManager(ColumnSource<?>[] tableKeySources, int tableSize, ToIntFunctor<Values> transform) {
        super(tableKeySources);
        this.tableSize = Require.gtZero(tableSize, "tableSize");
        this.transform = transform;
        rightIndexSource.ensureCapacity(tableSize);
        for (int ii = 0; ii < tableSize; ++ii) {
            rightIndexSource.set(ii, Index.NULL_KEY);
        }
    }

    void setRightSide(Index rightIndex, ColumnSource<?> valueSource) {
        try (final OrderedKeys.Iterator okIt = rightIndex.getOrderedKeysIterator();
             final ColumnSource.GetContext getContext = valueSource.makeGetContext((int)Math.min(CHUNK_SIZE, rightIndex.size()))
        ) {
            while (okIt.hasMore()) {
                final OrderedKeys chunkOk = okIt.getNextOrderedKeysWithLength(CHUNK_SIZE);

                final Chunk<? extends Values> dataChunk = valueSource.getChunk(getContext, chunkOk);
                final IntChunk<? extends Values> dataChunkAsInt = transform.apply(dataChunk);
                final MutableInt position = new MutableInt(0);

                chunkOk.forEachLong((long keyIndex) -> {
                    final int tableLocation = dataChunkAsInt.get(position.intValue());
                    position.increment();
                    if (tableLocation < 0 || tableLocation >= tableSize) {
                        return true;
                    }
                    final long existingRight = rightIndexSource.getLong(tableLocation);
                    if (existingRight == Index.NULL_KEY) {
                        rightIndexSource.set(tableLocation, keyIndex);
                    } else {
                        rightIndexSource.set(tableLocation, DUPLICATE_RIGHT_VALUE);
                    }
                    return true;
                });
            }
        }
    }

    @Override
    void decorateLeftSide(Index leftIndex, ColumnSource<?> [] valueSources, LongArraySource leftRedirections) {
        if (leftIndex.isEmpty()) {
            return;
        }

        Assert.eq(valueSources.length, "valueSources.length", 1);
        final ColumnSource<?> valueSource = valueSources[0];

        try (final OrderedKeys.Iterator okIt = leftIndex.getOrderedKeysIterator();
             final ColumnSource.GetContext getContext = valueSource.makeGetContext((int)Math.min(CHUNK_SIZE, leftIndex.size()))
        ) {
            long offset = 0;
            while (okIt.hasMore()) {
                final OrderedKeys chunkOk = okIt.getNextOrderedKeysWithLength(CHUNK_SIZE);

                final Chunk<? extends Values> dataChunk = valueSource.getChunk(getContext, chunkOk);

                final IntChunk<? extends Values> dataChunkAsInt = transform.apply(dataChunk);
                for (int ii = 0; ii < dataChunkAsInt.size(); ++ii) {
                    final int tableLocation = dataChunkAsInt.get(ii);
                    if (tableLocation < 0 || tableLocation >= tableSize) {
                        continue;
                    }
                    final long existingRight = rightIndexSource.getLong(tableLocation);

                    if (existingRight == DUPLICATE_RIGHT_VALUE) {
                        throw new IllegalStateException("More than one right side mapping for key " + keySourcesForErrorMessages[0].get(leftIndex.get(offset + ii)));
                    }
                    leftRedirections.set(offset + ii, existingRight);
                }

                offset += chunkOk.size();
            }
        }
    }

    @NotNull
    RedirectionIndex buildRedirectionIndex(QueryTable leftTable, boolean exactMatch, LongArraySource leftRedirections, JoinControl.RedirectionType redirectionType) {
        return buildRedirectionIndex(leftTable, exactMatch, leftRedirections::getLong, redirectionType);
    }
}
