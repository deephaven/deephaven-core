package io.deephaven.engine.liveness;

import org.jetbrains.annotations.NotNull;

/**
 * Interface for objects that retainReference {@link LivenessReferent}s until such time as they are no longer necessary.
 */
public interface LivenessManager {

    /**
     * Add the specified referent to this manager.
     *
     * @param referent The referent to add
     */
    default void manage(@NotNull final LivenessReferent referent) {
        if (!tryManage(referent)) {
            throw new LivenessStateException(
                    this + " failed to add " + referent.getReferentDescription() + ", which is no longer live");
        }
    }

    /**
     * Attempt to add the specified referent to this manager.
     *
     * @param referent The referent to add
     * @return Whether the referent was in fact added
     */
    boolean tryManage(@NotNull LivenessReferent referent);
}
