/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ResettableBooleanChunk;
import io.deephaven.chunk.ResettableByteChunk;
import io.deephaven.chunk.ResettableCharChunk;
import io.deephaven.chunk.ResettableDoubleChunk;
import io.deephaven.chunk.ResettableFloatChunk;
import io.deephaven.chunk.ResettableIntChunk;
import io.deephaven.chunk.ResettableLongChunk;
import io.deephaven.chunk.ResettableObjectChunk;
import io.deephaven.chunk.ResettableShortChunk;
import io.deephaven.chunk.ResettableWritableBooleanChunk;
import io.deephaven.chunk.ResettableWritableByteChunk;
import io.deephaven.chunk.ResettableWritableCharChunk;
import io.deephaven.chunk.ResettableWritableDoubleChunk;
import io.deephaven.chunk.ResettableWritableFloatChunk;
import io.deephaven.chunk.ResettableWritableIntChunk;
import io.deephaven.chunk.ResettableWritableLongChunk;
import io.deephaven.chunk.ResettableWritableObjectChunk;
import io.deephaven.chunk.ResettableWritableShortChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Any;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * Provides a set of per-type {@link ChunkPool}s. Normally accessed via a {@link ThreadLocal}, to allow some threads to
 * share a common pool and others to allocate their own.
 */
public final class MultiChunkPool implements BooleanChunkPool, ByteChunkPool, CharChunkPool, ShortChunkPool,
        IntChunkPool, LongChunkPool, FloatChunkPool, DoubleChunkPool, ObjectChunkPool {
    private static final MultiChunkPool SHARED_POOL = new MultiChunkPool();
    public static MultiChunkPool forThisThread() {
        return SHARED_POOL;
    }

    @Override
    public ChunkPool asChunkPool() {
        throw new UnsupportedOperationException(
                "MultiChunkPool can't create a ChunkPool, call this on the specific type required");
    }

    @Override
    public <ATTR extends Any> WritableBooleanChunk<ATTR> takeWritableBooleanChunk(int capacity) {
        return WritableBooleanChunk.makeWritableChunkForPool(capacity);
    }

    @Override
    public void giveWritableBooleanChunk(@NotNull WritableBooleanChunk<?> writableBooleanChunk) {
    }

    @Override
    public <ATTR extends Any> ResettableBooleanChunk<ATTR> takeResettableBooleanChunk() {
        return ResettableBooleanChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableBooleanChunk(@NotNull ResettableBooleanChunk<?> resettableBooleanChunk) {
    }

    @Override
    public <ATTR extends Any> ResettableWritableBooleanChunk<ATTR> takeResettableWritableBooleanChunk() {
        return ResettableWritableBooleanChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableWritableBooleanChunk(
            @NotNull ResettableWritableBooleanChunk<?> resettableWritableBooleanChunk) {
    }

    @Override
    public <ATTR extends Any> WritableCharChunk<ATTR> takeWritableCharChunk(int capacity) {
        return WritableCharChunk.makeWritableChunkForPool(capacity);
    }

    @Override
    public void giveWritableCharChunk(@NotNull WritableCharChunk<?> writableCharChunk) {
    }

    @Override
    public <ATTR extends Any> ResettableCharChunk<ATTR> takeResettableCharChunk() {
        return ResettableCharChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableCharChunk(@NotNull ResettableCharChunk<?> resettableCharChunk) {
    }

    @Override
    public <ATTR extends Any> ResettableWritableCharChunk<ATTR> takeResettableWritableCharChunk() {
        return ResettableWritableCharChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableWritableCharChunk(@NotNull ResettableWritableCharChunk<?> resettableWritableCharChunk) {
    }

    @Override
    public <ATTR extends Any> WritableByteChunk<ATTR> takeWritableByteChunk(int capacity) {
        return WritableByteChunk.makeWritableChunkForPool(capacity);
    }

    @Override
    public void giveWritableByteChunk(@NotNull WritableByteChunk<?> writableByteChunk) {
    }

    @Override
    public <ATTR extends Any> ResettableByteChunk<ATTR> takeResettableByteChunk() {
        return ResettableByteChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableByteChunk(@NotNull ResettableByteChunk<?> resettableByteChunk) {
    }

    @Override
    public <ATTR extends Any> ResettableWritableByteChunk<ATTR> takeResettableWritableByteChunk() {
        return ResettableWritableByteChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableWritableByteChunk(@NotNull ResettableWritableByteChunk<?> resettableWritableByteChunk) {
    }

    @Override
    public <ATTR extends Any> WritableShortChunk<ATTR> takeWritableShortChunk(int capacity) {
        return WritableShortChunk.makeWritableChunkForPool(capacity);
    }

    @Override
    public void giveWritableShortChunk(@NotNull WritableShortChunk<?> writableShortChunk) {
    }

    @Override
    public <ATTR extends Any> ResettableShortChunk<ATTR> takeResettableShortChunk() {
        return ResettableShortChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableShortChunk(@NotNull ResettableShortChunk<?> resettableShortChunk) {
    }

    @Override
    public <ATTR extends Any> ResettableWritableShortChunk<ATTR> takeResettableWritableShortChunk() {
        return ResettableWritableShortChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableWritableShortChunk(
            @NotNull ResettableWritableShortChunk<?> resettableWritableShortChunk) {
    }

    @Override
    public <ATTR extends Any> WritableIntChunk<ATTR> takeWritableIntChunk(int capacity) {
        return WritableIntChunk.makeWritableChunkForPool(capacity);
    }

    @Override
    public void giveWritableIntChunk(@NotNull WritableIntChunk<?> writableIntChunk) {
    }

    @Override
    public <ATTR extends Any> ResettableIntChunk<ATTR> takeResettableIntChunk() {
        return ResettableIntChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableIntChunk(@NotNull ResettableIntChunk<?> resettableIntChunk) {
    }

    @Override
    public <ATTR extends Any> ResettableWritableIntChunk<ATTR> takeResettableWritableIntChunk() {
        return ResettableWritableIntChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableWritableIntChunk(@NotNull ResettableWritableIntChunk<?> resettableWritableIntChunk) {
    }

    @Override
    public <ATTR extends Any> WritableLongChunk<ATTR> takeWritableLongChunk(int capacity) {
        return WritableLongChunk.makeWritableChunkForPool(capacity);
    }

    @Override
    public void giveWritableLongChunk(@NotNull WritableLongChunk<?> writableLongChunk) {
    }

    @Override
    public <ATTR extends Any> ResettableLongChunk<ATTR> takeResettableLongChunk() {
        return ResettableLongChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableLongChunk(@NotNull ResettableLongChunk<?> resettableLongChunk) {
    }

    @Override
    public <ATTR extends Any> ResettableWritableLongChunk<ATTR> takeResettableWritableLongChunk() {
        return ResettableWritableLongChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableWritableLongChunk(@NotNull ResettableWritableLongChunk<?> resettableWritableLongChunk) {
    }

    @Override
    public <ATTR extends Any> WritableFloatChunk<ATTR> takeWritableFloatChunk(int capacity) {
        return WritableFloatChunk.makeWritableChunkForPool(capacity);
    }

    @Override
    public void giveWritableFloatChunk(@NotNull WritableFloatChunk<?> writableFloatChunk) {
    }

    @Override
    public <ATTR extends Any> ResettableFloatChunk<ATTR> takeResettableFloatChunk() {
        return ResettableFloatChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableFloatChunk(@NotNull ResettableFloatChunk<?> resettableFloatChunk) {
    }

    @Override
    public <ATTR extends Any> ResettableWritableFloatChunk<ATTR> takeResettableWritableFloatChunk() {
        return ResettableWritableFloatChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableWritableFloatChunk(
            @NotNull ResettableWritableFloatChunk<?> resettableWritableFloatChunk) {
    }

    @Override
    public <ATTR extends Any> WritableDoubleChunk<ATTR> takeWritableDoubleChunk(int capacity) {
        return WritableDoubleChunk.makeWritableChunkForPool(capacity);
    }

    @Override
    public void giveWritableDoubleChunk(@NotNull WritableDoubleChunk<?> writableDoubleChunk) {
    }

    @Override
    public <ATTR extends Any> ResettableDoubleChunk<ATTR> takeResettableDoubleChunk() {
        return ResettableDoubleChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableDoubleChunk(@NotNull ResettableDoubleChunk<?> resettableDoubleChunk) {
    }

    @Override
    public <ATTR extends Any> ResettableWritableDoubleChunk<ATTR> takeResettableWritableDoubleChunk() {
        return ResettableWritableDoubleChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableWritableDoubleChunk(
            @NotNull ResettableWritableDoubleChunk<?> resettableWritableDoubleChunk) {
    }

    @Override
    public <TYPE, ATTR extends Any> WritableObjectChunk<TYPE, ATTR> takeWritableObjectChunk(int capacity) {
        return WritableObjectChunk.makeWritableChunkForPool(capacity);
    }

    @Override
    public void giveWritableObjectChunk(@NotNull WritableObjectChunk<?, ?> writableObjectChunk) {
    }

    @Override
    public <TYPE, ATTR extends Any> ResettableObjectChunk<TYPE, ATTR> takeResettableObjectChunk() {
        return ResettableObjectChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableObjectChunk(@NotNull ResettableObjectChunk<?, ?> resettableObjectChunk) {
    }

    @Override
    public <TYPE, ATTR extends Any> ResettableWritableObjectChunk<TYPE, ATTR> takeResettableWritableObjectChunk() {
        return ResettableWritableObjectChunk.makeResettableChunkForPool();
    }

    @Override
    public void giveResettableWritableObjectChunk(
            @NotNull ResettableWritableObjectChunk<?, ?> resettableWritableObjectChunk) {
    }
}
