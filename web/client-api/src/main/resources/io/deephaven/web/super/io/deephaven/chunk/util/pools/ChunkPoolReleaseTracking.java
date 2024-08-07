//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.chunk.util.pools;

/**
 * Support for release tracking, in order to detect chunk release errors.
 */
public final class ChunkPoolReleaseTracking {

    public static void enableStrict() {
    }

    public static void enable() {
    }

    private static void enable(final Object factory, boolean preCheck) {

    }

    public static void disable() {
    }

    static <CHUNK_TYPE extends PoolableChunk> CHUNK_TYPE onTake(CHUNK_TYPE chunk) {
        return chunk;
    }

    static <CHUNK_TYPE extends PoolableChunk> CHUNK_TYPE onGive(CHUNK_TYPE chunk) {
        return chunk;
    }

    public static void check() {

    }

    public static void checkAndDisable() {

    }

    private ChunkPoolReleaseTracking() {}
}
