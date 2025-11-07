//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
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

    @SuppressWarnings("unused")
    public BooleanChunkPool getBooleanChunkPool() {
        return booleanChunkPool;
    }

    @SuppressWarnings("unused")
    public CharChunkPool getCharChunkPool() {
        return charChunkPool;
    }

    @SuppressWarnings("unused")
    public ByteChunkPool getByteChunkPool() {
        return byteChunkPool;
    }

    @SuppressWarnings("unused")
    public ShortChunkPool getShortChunkPool() {
        return shortChunkPool;
    }

    @SuppressWarnings("unused")
    public IntChunkPool getIntChunkPool() {
        return intChunkPool;
    }

    @SuppressWarnings("unused")
    public LongChunkPool getLongChunkPool() {
        return longChunkPool;
    }

    @SuppressWarnings("unused")
    public FloatChunkPool getFloatChunkPool() {
        return floatChunkPool;
    }

    @SuppressWarnings("unused")
    public DoubleChunkPool getDoubleChunkPool() {
        return doubleChunkPool;
    }

    @SuppressWarnings("unused")
    public ObjectChunkPool getObjectChunkPool() {
        return objectChunkPool;
    }

    @Override
    public ChunkPool asChunkPool() {
        throw new UnsupportedOperationException(
                "MultiChunkPool can't create a ChunkPool, call this on the specific type required");
    }

    @Override
    public <ATTR extends Any> WritableBooleanChunk<ATTR> takeWritableBooleanChunk(final int capacity) {
        return booleanChunkPool.takeWritableBooleanChunk(capacity);
    }

    @Override
    public <ATTR extends Any> ResettableBooleanChunk<ATTR> takeResettableBooleanChunk() {
        return booleanChunkPool.takeResettableBooleanChunk();
    }

    @Override
    public <ATTR extends Any> ResettableWritableBooleanChunk<ATTR> takeResettableWritableBooleanChunk() {
        return booleanChunkPool.takeResettableWritableBooleanChunk();
    }

    @Override
    public <ATTR extends Any> WritableCharChunk<ATTR> takeWritableCharChunk(final int capacity) {
        return charChunkPool.takeWritableCharChunk(capacity);
    }

    @Override
    public <ATTR extends Any> ResettableCharChunk<ATTR> takeResettableCharChunk() {
        return charChunkPool.takeResettableCharChunk();
    }

    @Override
    public <ATTR extends Any> ResettableWritableCharChunk<ATTR> takeResettableWritableCharChunk() {
        return charChunkPool.takeResettableWritableCharChunk();
    }

    @Override
    public <ATTR extends Any> WritableByteChunk<ATTR> takeWritableByteChunk(final int capacity) {
        return byteChunkPool.takeWritableByteChunk(capacity);
    }

    @Override
    public <ATTR extends Any> ResettableByteChunk<ATTR> takeResettableByteChunk() {
        return byteChunkPool.takeResettableByteChunk();
    }

    @Override
    public <ATTR extends Any> ResettableWritableByteChunk<ATTR> takeResettableWritableByteChunk() {
        return byteChunkPool.takeResettableWritableByteChunk();
    }

    @Override
    public <ATTR extends Any> WritableShortChunk<ATTR> takeWritableShortChunk(final int capacity) {
        return shortChunkPool.takeWritableShortChunk(capacity);
    }

    @Override
    public <ATTR extends Any> ResettableShortChunk<ATTR> takeResettableShortChunk() {
        return shortChunkPool.takeResettableShortChunk();
    }

    @Override
    public <ATTR extends Any> ResettableWritableShortChunk<ATTR> takeResettableWritableShortChunk() {
        return shortChunkPool.takeResettableWritableShortChunk();
    }

    @Override
    public <ATTR extends Any> WritableIntChunk<ATTR> takeWritableIntChunk(final int capacity) {
        return intChunkPool.takeWritableIntChunk(capacity);
    }

    @Override
    public <ATTR extends Any> ResettableIntChunk<ATTR> takeResettableIntChunk() {
        return intChunkPool.takeResettableIntChunk();
    }

    @Override
    public <ATTR extends Any> ResettableWritableIntChunk<ATTR> takeResettableWritableIntChunk() {
        return intChunkPool.takeResettableWritableIntChunk();
    }

    @Override
    public <ATTR extends Any> WritableLongChunk<ATTR> takeWritableLongChunk(final int capacity) {
        return longChunkPool.takeWritableLongChunk(capacity);
    }

    @Override
    public <ATTR extends Any> ResettableLongChunk<ATTR> takeResettableLongChunk() {
        return longChunkPool.takeResettableLongChunk();
    }

    @Override
    public <ATTR extends Any> ResettableWritableLongChunk<ATTR> takeResettableWritableLongChunk() {
        return longChunkPool.takeResettableWritableLongChunk();
    }

    @Override
    public <ATTR extends Any> WritableFloatChunk<ATTR> takeWritableFloatChunk(final int capacity) {
        return floatChunkPool.takeWritableFloatChunk(capacity);
    }

    @Override
    public <ATTR extends Any> ResettableFloatChunk<ATTR> takeResettableFloatChunk() {
        return floatChunkPool.takeResettableFloatChunk();
    }

    @Override
    public <ATTR extends Any> ResettableWritableFloatChunk<ATTR> takeResettableWritableFloatChunk() {
        return floatChunkPool.takeResettableWritableFloatChunk();
    }

    @Override
    public <ATTR extends Any> WritableDoubleChunk<ATTR> takeWritableDoubleChunk(final int capacity) {
        return doubleChunkPool.takeWritableDoubleChunk(capacity);
    }

    @Override
    public <ATTR extends Any> ResettableDoubleChunk<ATTR> takeResettableDoubleChunk() {
        return doubleChunkPool.takeResettableDoubleChunk();
    }

    @Override
    public <ATTR extends Any> ResettableWritableDoubleChunk<ATTR> takeResettableWritableDoubleChunk() {
        return doubleChunkPool.takeResettableWritableDoubleChunk();
    }

    @Override
    public <TYPE, ATTR extends Any> WritableObjectChunk<TYPE, ATTR> takeWritableObjectChunk(final int capacity) {
        return objectChunkPool.takeWritableObjectChunk(capacity);
    }

    @Override
    public <TYPE, ATTR extends Any> ResettableObjectChunk<TYPE, ATTR> takeResettableObjectChunk() {
        return objectChunkPool.takeResettableObjectChunk();
    }

    @Override
    public <TYPE, ATTR extends Any> ResettableWritableObjectChunk<TYPE, ATTR> takeResettableWritableObjectChunk() {
        return objectChunkPool.takeResettableWritableObjectChunk();
    }
}
