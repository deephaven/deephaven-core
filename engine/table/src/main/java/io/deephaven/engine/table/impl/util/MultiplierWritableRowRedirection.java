/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.util;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.engine.rowset.RowSequence.NULL_ROW_KEY;

/**
 * Wraps an internal {@link RowRedirection} <em>innerRedirection</em> along with an associated <em>multiplier</em>. An
 * outer row key <em>k</em> is mapped to inner row key
 * {@code innerRedirection.get(k / multiplier) * multiplier + k % multiplier}. This allows for a row redirection that
 * tracks only a fraction of the actual redirected row keys, which is useful when a set of related values are stored
 * adjacently in a single column.
 */
public class MultiplierWritableRowRedirection implements RowRedirection {

    /**
     * {@link RowRedirection} that expresses the multiplier-adjusted relationship between outer row keys and inner row
     * keys.
     */
    private final RowRedirection innerRedirection;
    private final int multiplier;

    public MultiplierWritableRowRedirection(@NotNull final RowRedirection innerRedirection, final int multiplier) {
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

    @Override
    public void fillChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> mappedKeysOut,
            @NotNull final RowSequence keysToMap) {
        doMapping(mappedKeysOut.asWritableLongChunk(), keysToMap, false);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext fillContext,
            @NotNull final WritableChunk<? super RowKeys> mappedKeysOut,
            @NotNull final RowSequence keysToMap) {
        doMapping(mappedKeysOut.asWritableLongChunk(), keysToMap, true);
    }

    private void doMapping(
            @NotNull WritableLongChunk<? super RowKeys> mappedKeysOut,
            @NotNull RowSequence keysToMap,
            final boolean usePrev) {
        final int size = keysToMap.intSize();
        keysToMap.fillRowKeyChunk(mappedKeysOut);
        for (int ki = 0; ki < size; ++ki) {
            mappedKeysOut.set(ki, mappedKeysOut.get(ki) / multiplier);
        }
        // TODO-RWC: Continue from here, only fill non-dup keys
    }
}
