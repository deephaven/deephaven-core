package io.deephaven.db.v2.sources.chunk.util.pools;

import io.deephaven.util.datastructures.ReleaseTracker;
import org.jetbrains.annotations.NotNull;

/**
 * Support for release tracking, in order to detect chunk release errors.
 */
public final class ChunkPoolReleaseTracking {

    private static volatile ReleaseTracker<PoolableChunk> releaseTracker;

    public static void enableStrict() {
        enable(ReleaseTracker.strictReleaseTrackerFactory, true);
    }

    public static void enable() {
        enable(ReleaseTracker.weakReleaseTrackerFactory, false);
    }

    private static void enable(final ReleaseTracker.Factory factory, boolean preCheck) {
        if (releaseTracker == null) {
            synchronized (ChunkPoolReleaseTracking.class) {
                if (releaseTracker == null) {
                    releaseTracker = factory.makeReleaseTracker();
                }
            }
            return;
        }
        if (!factory.isMyType(releaseTracker.getClass())) {
            throw new IllegalStateException("Can't enable to a different tracking type (strict versus not)");
        }
        if (preCheck) {
            try {
                releaseTracker.check();
            } catch (ReleaseTracker.LeakedException | ReleaseTracker.MissedReleaseException checkException) {
                throw new IllegalStateException("Release tracker had errors on enable", checkException);
            }
        }
    }

    public static void disable() {
        releaseTracker = null;
    }

    static <CHUNK_TYPE extends PoolableChunk> CHUNK_TYPE onTake(@NotNull final CHUNK_TYPE chunk) {
        final ReleaseTracker<PoolableChunk> localReleaseTracker = releaseTracker;
        if (localReleaseTracker != null) {
            localReleaseTracker.reportAcquire(chunk);
        }
        return chunk;
    }

    static <CHUNK_TYPE extends PoolableChunk> CHUNK_TYPE onGive(@NotNull final CHUNK_TYPE chunk) {
        final ReleaseTracker<PoolableChunk> localReleaseTracker = releaseTracker;
        if (localReleaseTracker != null) {
            localReleaseTracker.reportRelease(chunk);
        }
        return chunk;
    }

    public static void check() {
        final ReleaseTracker<PoolableChunk> localReleaseTracker = releaseTracker;
        if (localReleaseTracker != null) {
            localReleaseTracker.check();
        }
    }

    public static void checkAndDisable() {
        try {
            check();
        } finally {
            disable();
        }
    }

    private ChunkPoolReleaseTracking() {}
}
