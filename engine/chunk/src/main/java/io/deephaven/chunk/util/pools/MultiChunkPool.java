/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.chunk.util.pools;

import io.deephaven.chunk.ChunkType;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Provides a set of per-type {@link ChunkPool}s. Normally accessed via a {@link ThreadLocal}, to allow some threads to
 * share a common pool and others to allocate their own.
 * <p>
 * This type isn't compatible with GWT, and needs to be replaced with a different class that doesn't use the soft pool
 * instances.
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

    private final BooleanChunkPool booleanChunkPool = new BooleanChunkSoftPool();
    private final CharChunkSoftPool charChunkPool = new CharChunkSoftPool();
    private final ByteChunkPool byteChunkPool = new ByteChunkSoftPool();
    private final ShortChunkPool shortChunkPool = new ShortChunkSoftPool();
    private final IntChunkPool intChunkPool = new IntChunkSoftPool();
    private final LongChunkPool longChunkPool = new LongChunkSoftPool();
    private final FloatChunkPool floatChunkPool = new FloatChunkSoftPool();
    private final DoubleChunkPool doubleChunkPool = new DoubleChunkSoftPool();
    private final ObjectChunkSoftPool objectChunkPool = new ObjectChunkSoftPool();

    private final Map<ChunkType, Supplier<ChunkPool>> pools;

    {
        final EnumMap<ChunkType, Supplier<ChunkPool>> tempPools = new EnumMap<>(ChunkType.class);
        tempPools.put(ChunkType.Boolean, booleanChunkPool::asChunkPool);
        tempPools.put(ChunkType.Char, charChunkPool::asChunkPool);
        tempPools.put(ChunkType.Byte, byteChunkPool::asChunkPool);
        tempPools.put(ChunkType.Short, shortChunkPool::asChunkPool);
        tempPools.put(ChunkType.Int, intChunkPool::asChunkPool);
        tempPools.put(ChunkType.Long, longChunkPool::asChunkPool);
        tempPools.put(ChunkType.Float, floatChunkPool::asChunkPool);
        tempPools.put(ChunkType.Double, doubleChunkPool::asChunkPool);
        tempPools.put(ChunkType.Object, objectChunkPool::asChunkPool);
        pools = Collections.unmodifiableMap(tempPools);
    }

    private MultiChunkPool() {}

    @SuppressWarnings("unused")
    public ChunkPool getChunkPool(@NotNull final ChunkType chunkType) {
        return pools.get(chunkType).get();
    }

    public BooleanChunkPool getBooleanChunkPool() {
        return booleanChunkPool;
    }

    public CharChunkSoftPool getCharChunkPool() {
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

    public ObjectChunkSoftPool getObjectChunkPool() {
        return objectChunkPool;
    }
}
