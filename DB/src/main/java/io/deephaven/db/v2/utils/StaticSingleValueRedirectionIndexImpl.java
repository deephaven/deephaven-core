/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import org.jetbrains.annotations.NotNull;

public class StaticSingleValueRedirectionIndexImpl implements SingleValueRedirectionIndex {
    private final long value;

    public StaticSingleValueRedirectionIndexImpl(final long value) {
        this.value = value;
    }

    @Override
    public long put(long key, long index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long get(long key) {
        return value;
    }

    @Override
    public long getPrev(long key) {
        return value;
    }

    @Override
    public void startTrackingPrevValues() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long remove(long leftIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "SingleValueRedirectionIndexImpl{" + value + "}";
    }

    @Override
    public void fillChunk(
        @NotNull final FillContext fillContext,
        @NotNull final WritableLongChunk<Attributes.KeyIndices> mappedKeysOut,
        @NotNull final OrderedKeys keysToMap) {
        final int sz = keysToMap.intSize();
        mappedKeysOut.setSize(sz);
        mappedKeysOut.fillWithValue(0, sz, value);
    }

    @Override
    public void fillPrevChunk(
        @NotNull final FillContext fillContext,
        @NotNull final WritableLongChunk<Attributes.KeyIndices> mappedKeysOut,
        @NotNull final OrderedKeys keysToMap) {
        // no prev
        fillChunk(fillContext, mappedKeysOut, keysToMap);
    }

    @Override
    public void setValue(long value) {
        throw new UnsupportedOperationException();
    }


    @Override
    public long getValue() {
        return value;
    }
}
