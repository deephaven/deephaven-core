//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
    private static final ThreadLocal<MultiChunkPool> POOL_THREAD_LOCAL = ThreadLocal.withInitial(() -> SHARED_POOL);

    public static void enableDedicatedPoolForThisThread() {
        if (POOL_THREAD_LOCAL.get() == SHARED_POOL) {
            POOL_THREAD_LOCAL.set(new MultiChunkPool());
        }
    }

    public static MultiChunkPool forThisThread() {
        return POOL_THREAD_LOCAL.get();
    }

    private final BooleanChunkPool booleanChunkPool = new BooleanChunkSoftPool();
    private final CharChunkPool charChunkPool = new CharChunkSoftPool();
    private final ByteChunkPool byteChunkPool = new ByteChunkSoftPool();
    private final ShortChunkPool shortChunkPool = new ShortChunkSoftPool();
    private final IntChunkPool intChunkPool = new IntChunkSoftPool();
    private final LongChunkPool longChunkPool = new LongChunkSoftPool();
    private final FloatChunkPool floatChunkPool = new FloatChunkSoftPool();
    private final DoubleChunkPool doubleChunkPool = new DoubleChunkSoftPool();
    private final ObjectChunkPool objectChunkPool = new ObjectChunkSoftPool();

    private final Map<ChunkType, ChunkPool> pools;

    {
        final EnumMap<ChunkType, ChunkPool> tempPools = new EnumMap<>(ChunkType.class);
        tempPools.put(ChunkType.Boolean, booleanChunkPool.asChunkPool());
        tempPools.put(ChunkType.Char, charChunkPool.asChunkPool());
        tempPools.put(ChunkType.Byte, byteChunkPool.asChunkPool());
        tempPools.put(ChunkType.Short, shortChunkPool.asChunkPool());
        tempPools.put(ChunkType.Int, intChunkPool.asChunkPool());
        tempPools.put(ChunkType.Long, longChunkPool.asChunkPool());
        tempPools.put(ChunkType.Float, floatChunkPool.asChunkPool());
        tempPools.put(ChunkType.Double, doubleChunkPool.asChunkPool());
        tempPools.put(ChunkType.Object, objectChunkPool.asChunkPool());
        pools = Collections.unmodifiableMap(tempPools);
    }

    private MultiChunkPool() {}

    @SuppressWarnings("unused")
    public ChunkPool getChunkPool(@NotNull final ChunkType chunkType) {
        return pools.get(chunkType);
    }

    public BooleanChunkPool getBooleanChunkPool() {
        return booleanChunkPool;
    }

    public CharChunkPool getCharChunkPool() {
        return charChunkPool;
    }

    public ByteChunkPool getByteChunkPool() {
        return byteChunkPool;
    }

    public ShortChunkPool getShortChunkPool() {
        return shortChunkPool;
    }

    public IntChunkPool getIntChunkPool() {
        return intChunkPool;
    }

    public LongChunkPool getLongChunkPool() {
        return longChunkPool;
    }

    public FloatChunkPool getFloatChunkPool() {
        return floatChunkPool;
    }

    public DoubleChunkPool getDoubleChunkPool() {
        return doubleChunkPool;
    }

    public ObjectChunkPool getObjectChunkPool() {
        return objectChunkPool;
    }

    @Override
    public ChunkPool asChunkPool() {
        throw new UnsupportedOperationException(
                "MultiChunkPool can't create a ChunkPool, call this on the specific type required");
    }

    @Override
    public <ATTR extends Any> WritableBooleanChunk<ATTR> takeWritableBooleanChunk(int capacity) {
        return booleanChunkPool.takeWritableBooleanChunk(capacity);
    }

    @Override
    public void giveWritableBooleanChunk(@NotNull WritableBooleanChunk<?> writableBooleanChunk) {
        booleanChunkPool.giveWritableBooleanChunk(writableBooleanChunk);
    }

    @Override
    public <ATTR extends Any> ResettableBooleanChunk<ATTR> takeResettableBooleanChunk() {
        return booleanChunkPool.takeResettableBooleanChunk();
    }

    @Override
    public void giveResettableBooleanChunk(@NotNull ResettableBooleanChunk<?> resettableBooleanChunk) {
        booleanChunkPool.giveResettableBooleanChunk(resettableBooleanChunk);
    }

    @Override
    public <ATTR extends Any> ResettableWritableBooleanChunk<ATTR> takeResettableWritableBooleanChunk() {
        return booleanChunkPool.takeResettableWritableBooleanChunk();
    }

    @Override
    public void giveResettableWritableBooleanChunk(
            @NotNull ResettableWritableBooleanChunk<?> resettableWritableBooleanChunk) {
        booleanChunkPool.giveResettableWritableBooleanChunk(resettableWritableBooleanChunk);
    }

    @Override
    public <ATTR extends Any> WritableCharChunk<ATTR> takeWritableCharChunk(int capacity) {
        return charChunkPool.takeWritableCharChunk(capacity);
    }

    @Override
    public void giveWritableCharChunk(@NotNull WritableCharChunk<?> writableCharChunk) {
        charChunkPool.giveWritableCharChunk(writableCharChunk);
    }

    @Override
    public <ATTR extends Any> ResettableCharChunk<ATTR> takeResettableCharChunk() {
        return charChunkPool.takeResettableCharChunk();
    }

    @Override
    public void giveResettableCharChunk(@NotNull ResettableCharChunk<?> resettableCharChunk) {
        charChunkPool.giveResettableCharChunk(resettableCharChunk);
    }

    @Override
    public <ATTR extends Any> ResettableWritableCharChunk<ATTR> takeResettableWritableCharChunk() {
        return charChunkPool.takeResettableWritableCharChunk();
    }

    @Override
    public void giveResettableWritableCharChunk(@NotNull ResettableWritableCharChunk<?> resettableWritableCharChunk) {
        charChunkPool.giveResettableWritableCharChunk(resettableWritableCharChunk);
    }

    @Override
    public <ATTR extends Any> WritableByteChunk<ATTR> takeWritableByteChunk(int capacity) {
        return byteChunkPool.takeWritableByteChunk(capacity);
    }

    @Override
    public void giveWritableByteChunk(@NotNull WritableByteChunk<?> writableByteChunk) {
        byteChunkPool.giveWritableByteChunk(writableByteChunk);
    }

    @Override
    public <ATTR extends Any> ResettableByteChunk<ATTR> takeResettableByteChunk() {
        return byteChunkPool.takeResettableByteChunk();
    }

    @Override
    public void giveResettableByteChunk(@NotNull ResettableByteChunk<?> resettableByteChunk) {
        byteChunkPool.giveResettableByteChunk(resettableByteChunk);
    }

    @Override
    public <ATTR extends Any> ResettableWritableByteChunk<ATTR> takeResettableWritableByteChunk() {
        return byteChunkPool.takeResettableWritableByteChunk();
    }

    @Override
    public void giveResettableWritableByteChunk(@NotNull ResettableWritableByteChunk<?> resettableWritableByteChunk) {
        byteChunkPool.giveResettableWritableByteChunk(resettableWritableByteChunk);
    }

    @Override
    public <ATTR extends Any> WritableShortChunk<ATTR> takeWritableShortChunk(int capacity) {
        return shortChunkPool.takeWritableShortChunk(capacity);
    }

    @Override
    public void giveWritableShortChunk(@NotNull WritableShortChunk<?> writableShortChunk) {
        shortChunkPool.giveWritableShortChunk(writableShortChunk);
    }

    @Override
    public <ATTR extends Any> ResettableShortChunk<ATTR> takeResettableShortChunk() {
        return shortChunkPool.takeResettableShortChunk();
    }

    @Override
    public void giveResettableShortChunk(@NotNull ResettableShortChunk<?> resettableShortChunk) {
        shortChunkPool.giveResettableShortChunk(resettableShortChunk);
    }

    @Override
    public <ATTR extends Any> ResettableWritableShortChunk<ATTR> takeResettableWritableShortChunk() {
        return shortChunkPool.takeResettableWritableShortChunk();
    }

    @Override
    public void giveResettableWritableShortChunk(
            @NotNull ResettableWritableShortChunk<?> resettableWritableShortChunk) {
        shortChunkPool.giveResettableWritableShortChunk(resettableWritableShortChunk);
    }

    @Override
    public <ATTR extends Any> WritableIntChunk<ATTR> takeWritableIntChunk(int capacity) {
        return intChunkPool.takeWritableIntChunk(capacity);
    }

    @Override
    public void giveWritableIntChunk(@NotNull WritableIntChunk<?> writableIntChunk) {
        intChunkPool.giveWritableIntChunk(writableIntChunk);
    }

    @Override
    public <ATTR extends Any> ResettableIntChunk<ATTR> takeResettableIntChunk() {
        return intChunkPool.takeResettableIntChunk();
    }

    @Override
    public void giveResettableIntChunk(@NotNull ResettableIntChunk<?> resettableIntChunk) {
        intChunkPool.giveResettableIntChunk(resettableIntChunk);
    }

    @Override
    public <ATTR extends Any> ResettableWritableIntChunk<ATTR> takeResettableWritableIntChunk() {
        return intChunkPool.takeResettableWritableIntChunk();
    }

    @Override
    public void giveResettableWritableIntChunk(@NotNull ResettableWritableIntChunk<?> resettableWritableIntChunk) {
        intChunkPool.giveResettableWritableIntChunk(resettableWritableIntChunk);
    }

    @Override
    public <ATTR extends Any> WritableLongChunk<ATTR> takeWritableLongChunk(int capacity) {
        return longChunkPool.takeWritableLongChunk(capacity);
    }

    @Override
    public void giveWritableLongChunk(@NotNull WritableLongChunk<?> writableLongChunk) {
        longChunkPool.giveWritableLongChunk(writableLongChunk);
    }

    @Override
    public <ATTR extends Any> ResettableLongChunk<ATTR> takeResettableLongChunk() {
        return longChunkPool.takeResettableLongChunk();
    }

    @Override
    public void giveResettableLongChunk(@NotNull ResettableLongChunk<?> resettableLongChunk) {
        longChunkPool.giveResettableLongChunk(resettableLongChunk);
    }

    @Override
    public <ATTR extends Any> ResettableWritableLongChunk<ATTR> takeResettableWritableLongChunk() {
        return longChunkPool.takeResettableWritableLongChunk();
    }

    @Override
    public void giveResettableWritableLongChunk(@NotNull ResettableWritableLongChunk<?> resettableWritableLongChunk) {
        longChunkPool.giveResettableWritableLongChunk(resettableWritableLongChunk);
    }

    @Override
    public <ATTR extends Any> WritableFloatChunk<ATTR> takeWritableFloatChunk(int capacity) {
        return floatChunkPool.takeWritableFloatChunk(capacity);
    }

    @Override
    public void giveWritableFloatChunk(@NotNull WritableFloatChunk<?> writableFloatChunk) {
        floatChunkPool.giveWritableFloatChunk(writableFloatChunk);
    }

    @Override
    public <ATTR extends Any> ResettableFloatChunk<ATTR> takeResettableFloatChunk() {
        return floatChunkPool.takeResettableFloatChunk();
    }

    @Override
    public void giveResettableFloatChunk(@NotNull ResettableFloatChunk<?> resettableFloatChunk) {
        floatChunkPool.giveResettableFloatChunk(resettableFloatChunk);
    }

    @Override
    public <ATTR extends Any> ResettableWritableFloatChunk<ATTR> takeResettableWritableFloatChunk() {
        return floatChunkPool.takeResettableWritableFloatChunk();
    }

    @Override
    public void giveResettableWritableFloatChunk(
            @NotNull ResettableWritableFloatChunk<?> resettableWritableFloatChunk) {
        floatChunkPool.giveResettableWritableFloatChunk(resettableWritableFloatChunk);
    }

    @Override
    public <ATTR extends Any> WritableDoubleChunk<ATTR> takeWritableDoubleChunk(int capacity) {
        return doubleChunkPool.takeWritableDoubleChunk(capacity);
    }

    @Override
    public void giveWritableDoubleChunk(@NotNull WritableDoubleChunk<?> writableDoubleChunk) {
        doubleChunkPool.giveWritableDoubleChunk(writableDoubleChunk);
    }

    @Override
    public <ATTR extends Any> ResettableDoubleChunk<ATTR> takeResettableDoubleChunk() {
        return doubleChunkPool.takeResettableDoubleChunk();
    }

    @Override
    public void giveResettableDoubleChunk(@NotNull ResettableDoubleChunk<?> resettableDoubleChunk) {
        doubleChunkPool.giveResettableDoubleChunk(resettableDoubleChunk);
    }

    @Override
    public <ATTR extends Any> ResettableWritableDoubleChunk<ATTR> takeResettableWritableDoubleChunk() {
        return doubleChunkPool.takeResettableWritableDoubleChunk();
    }

    @Override
    public void giveResettableWritableDoubleChunk(
            @NotNull ResettableWritableDoubleChunk<?> resettableWritableDoubleChunk) {
        doubleChunkPool.giveResettableWritableDoubleChunk(resettableWritableDoubleChunk);
    }

    @Override
    public <TYPE, ATTR extends Any> WritableObjectChunk<TYPE, ATTR> takeWritableObjectChunk(int capacity) {
        return objectChunkPool.takeWritableObjectChunk(capacity);
    }

    @Override
    public void giveWritableObjectChunk(@NotNull WritableObjectChunk<?, ?> writableObjectChunk) {
        objectChunkPool.giveWritableObjectChunk(writableObjectChunk);
    }

    @Override
    public <TYPE, ATTR extends Any> ResettableObjectChunk<TYPE, ATTR> takeResettableObjectChunk() {
        return objectChunkPool.takeResettableObjectChunk();
    }

    @Override
    public void giveResettableObjectChunk(@NotNull ResettableObjectChunk<?, ?> resettableObjectChunk) {
        objectChunkPool.giveResettableObjectChunk(resettableObjectChunk);
    }

    @Override
    public <TYPE, ATTR extends Any> ResettableWritableObjectChunk<TYPE, ATTR> takeResettableWritableObjectChunk() {
        return objectChunkPool.takeResettableWritableObjectChunk();
    }

    @Override
    public void giveResettableWritableObjectChunk(
            @NotNull ResettableWritableObjectChunk<?, ?> resettableWritableObjectChunk) {
        objectChunkPool.giveResettableWritableObjectChunk(resettableWritableObjectChunk);
    }
}
