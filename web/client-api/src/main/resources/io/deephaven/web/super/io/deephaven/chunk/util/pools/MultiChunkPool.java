/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ChunkType;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/**
 * Provides a set of per-type {@link ChunkPool}s. Normally accessed via a {@link ThreadLocal}, to allow some threads to
 * share a common pool and others to allocate their own.
 */
public final class MultiChunkPool {

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

    private final BooleanChunkPool booleanChunkPool = new BooleanChunkPool();
    private final CharChunkPool charChunkPool = new CharChunkPool();
    private final ByteChunkPool byteChunkPool = new ByteChunkPool();
    private final ShortChunkPool shortChunkPool = new ShortChunkPool();
    private final IntChunkPool intChunkPool = new IntChunkPool();
    private final LongChunkPool longChunkPool = new LongChunkPool();
    private final FloatChunkPool floatChunkPool = new FloatChunkPool();
    private final DoubleChunkPool doubleChunkPool = new DoubleChunkPool();
    private final ObjectChunkPool objectChunkPool = new ObjectChunkPool();

    private final Map<ChunkType, ChunkPool> pools;

    {
        final EnumMap<ChunkType, ChunkPool> tempPools = new EnumMap<>(ChunkType.class);
        tempPools.put(ChunkType.Boolean, booleanChunkPool);
        tempPools.put(ChunkType.Char, charChunkPool);
        tempPools.put(ChunkType.Byte, byteChunkPool);
        tempPools.put(ChunkType.Short, shortChunkPool);
        tempPools.put(ChunkType.Int, intChunkPool);
        tempPools.put(ChunkType.Long, longChunkPool);
        tempPools.put(ChunkType.Float, floatChunkPool);
        tempPools.put(ChunkType.Double, doubleChunkPool);
        tempPools.put(ChunkType.Object, objectChunkPool);
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
}
