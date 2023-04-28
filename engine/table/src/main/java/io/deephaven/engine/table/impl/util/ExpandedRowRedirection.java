/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

/**
 * Wraps an internal {@link RowRedirection} {@code innerRedirection} along with an associated {@code multiplier}. Each
 * mapping in the {@code innerRedirection} is <em>expanded</em> into {@code multiplier} mappings. An outer row key
 * {@code k} is mapped to inner row key {@code innerRedirection.get(k / multiplier) * multiplier + k % multiplier}.
 * <p>
 * This allows for a row redirection that tracks only a fraction of the actual redirected row keys, which is useful when
 * a set of related values are stored adjacently in a single column.
 * <p>
 * It's expected that outer row keys with the same base value (that is, {@code outerRowKey / multiplier}) will always be
 * fetched together, and the implementation makes this assumption.
 */
public class ExpandedRowRedirection implements RowRedirection {

    /**
     * {@link RowRedirection} that expresses the multiplier-adjusted relationship between outer row keys and inner row
     * keys.
     */
    private final RowRedirection innerRedirection;
    private final int multiplier;

    public ExpandedRowRedirection(@NotNull final RowRedirection innerRedirection, final int multiplier) {
        this.innerRedirection = innerRedirection;
        this.multiplier = multiplier;
    }

    @Override
    public boolean ascendingMapping() {
        return innerRedirection.ascendingMapping();
    }

    @Override
    public synchronized long get(final long key) {
        if (key < 0) {
            return NULL_ROW_KEY;
        }
        return innerRedirection.get(key / multiplier) * multiplier + key % multiplier;
    }

    @Override
    public synchronized long getPrev(long key) {
        if (key < 0) {
            return NULL_ROW_KEY;
        }
        return innerRedirection.getPrev(key / multiplier) * multiplier + key % multiplier;
    }

    private final class FillContext implements ChunkSource.FillContext {

        private final ResettableLongChunk<OrderedRowKeys> baseOuterRowKeys;
        private final ResettableWritableLongChunk<Any> baseInnerRowKeys;
        private final ChunkSource.FillContext innerFillContext;

        private FillContext(final int chunkCapacity, @Nullable final SharedContext sharedContext) {
            baseOuterRowKeys = ResettableLongChunk.makeResettableChunk();
            baseInnerRowKeys = ResettableWritableLongChunk.makeResettableChunk();
            innerFillContext = innerRedirection.makeFillContext(chunkCapacity, sharedContext);
        }

        @Override
        public void close() {
            // @formatter:off
            //noinspection EmptyTryBlock
            try (final SafeCloseable ignored1 = baseOuterRowKeys;
                 final SafeCloseable ignored2 = baseInnerRowKeys;
                 final SafeCloseable ignored3 = innerFillContext) {}
            // @formatter:on
        }
    }

    @Override
    public ChunkSource.FillContext makeFillContext(
            final int chunkCapacity,
            @Nullable final SharedContext sharedContext) {
        return new FillContext(chunkCapacity, sharedContext);
    }

    @Override
    public void fillChunk(
            @NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> mappedKeysOut,
            @NotNull final RowSequence keysToMap) {
        doMapping(fillContext, mappedKeysOut.asWritableLongChunk(), keysToMap, false);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final ChunkSource.FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> mappedKeysOut,
            @NotNull final RowSequence keysToMap) {
        doMapping(fillContext, mappedKeysOut.asWritableLongChunk(), keysToMap, true);
    }

    private void doMapping(
            @NotNull final ChunkSource.FillContext opaqueFillContext,
            @NotNull WritableLongChunk<? super RowKeys> mappedKeysOut,
            @NotNull RowSequence keysToMap,
            final boolean usePrev) {
        final FillContext fc = (FillContext) opaqueFillContext;
        final int size = keysToMap.intSize();
        final int baseSize = size / multiplier;

        // Assume that bands of row keys with the same base are all requested together.
        Assert.eq(baseSize * multiplier, "size / multiplier * multiplier", size, "size");

        // Put all the requested outer row keys into mappedKeysOut for temporary storage
        keysToMap.fillRowKeyChunk(mappedKeysOut);

        // Put the unique requested outer row key base values at the beginning of mappedKeysOut
        for (int bki = 0; bki < baseSize; ++bki) {
            mappedKeysOut.set(bki, mappedKeysOut.get(bki * multiplier) / multiplier);
        }

        // Map the unique base values from outer space to inner space, and store the results at the end of mappedKeysOut
        final LongChunk<OrderedRowKeys> baseOuterRowKeysChunk = fc.baseOuterRowKeys.resetFromTypedChunk(
                LongChunk.downcast(mappedKeysOut), 0, baseSize);
        final WritableLongChunk<Any> baseInnerRowKeysChunk = fc.baseInnerRowKeys.resetFromTypedChunk(
                WritableLongChunk.upcast(mappedKeysOut), size - baseSize, baseSize);
        try (final RowSequence baseOuterRowSequence =
                RowSequenceFactory.wrapRowKeysChunkAsRowSequence(baseOuterRowKeysChunk)) {
            if (usePrev) {
                innerRedirection.fillPrevChunk(fc.innerFillContext, baseInnerRowKeysChunk, baseOuterRowSequence);
            } else {
                innerRedirection.fillChunk(fc.innerFillContext, baseInnerRowKeysChunk, baseOuterRowSequence);
            }
        }

        // Read base inner values from the end of mappedKeysOut, and write the multiplied inner row keys

        for (int bki = 0; bki < baseSize; ++bki) {
            final int baseChunkIndex = bki * multiplier;
            final long baseInnerRowKey = baseInnerRowKeysChunk.get(bki) * multiplier;
            for (int ski = 0; ski < multiplier; ++ski) {
                mappedKeysOut.set(baseChunkIndex + ski, baseInnerRowKey + ski);
            }
        }
    }
}
