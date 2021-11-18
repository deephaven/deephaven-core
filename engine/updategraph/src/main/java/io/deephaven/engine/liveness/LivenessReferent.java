package io.deephaven.engine.liveness;

import io.deephaven.util.Utils;

import java.lang.ref.WeakReference;

/**
 * Interface for objects that continue to "live" while retained by a {@link LivenessManager}.
 */
public interface LivenessReferent {

    /**
     * Record a reference to this {@link LivenessReferent}, which must later be dropped.
     *
     * @throws LivenessStateException If this referent is no longer "live"
     */
    default void retainReference() {
        if (!tryRetainReference()) {
            throw new LivenessStateException(this + " is no longer live and cannot be retained further");
        }
    }

    /**
     * If this referent is "live", behave as {@link #retainReference()} and return true. Otherwise, returns false rather
     * than throwing an exception.
     *
     * @return True if this referent was retained, false otherwise
     */
    boolean tryRetainReference();

    /**
     * Drop a previously-retained reference to this referent.
     */
    void dropReference();

    /**
     * Get a {@link WeakReference} to this referent. This may be cached, or newly created.
     *
     * @return A new or cached reference to this referent
     */
    WeakReference<? extends LivenessReferent> getWeakReference();

    /**
     * Get a name that is suitable for uniquely identifying this {@link LivenessReferent} in debug and error messages.
     * This is usually not the same as {@link Object#toString()}.
     *
     * @return A unique name for this referent for debugging and error message purposes
     */
    default String getReferentDescription() {
        return Utils.makeReferentDescription(this);
    }
}
