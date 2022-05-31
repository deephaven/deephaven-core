package io.deephaven.engine.liveness;

/**
 * Interface for {@link LivenessManager} instances that support a {@link #release} method to initiate retained referent
 * release callback invocation. It is the creator's responsibility to ensure that {@link #release()} is invoked before
 * this manager becomes unreachable.
 */
public interface ReleasableLivenessManager extends LivenessManager {

    /**
     * Release ownership of this {@link ReleasableLivenessManager}, allowing any retained {@link LivenessReferent}s to
     * cleanup if they no longer have outstanding references.
     */
    void release();
}
