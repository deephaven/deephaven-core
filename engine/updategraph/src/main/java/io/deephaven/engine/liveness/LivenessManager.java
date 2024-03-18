//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.liveness;

import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.referencecounting.ReferenceCounted;
import org.jetbrains.annotations.NotNull;

/**
 * Interface for objects that retainReference {@link LivenessReferent}s until such time as they are no longer necessary.
 */
public interface LivenessManager {

    /**
     * Add the specified referent to this manager. {@code referent} must be live. If this manager is also a
     * {@link LivenessReferent}, then it must also be live.
     *
     * @param referent The referent to add
     */
    @FinalDefault
    default void manage(@NotNull final LivenessReferent referent) {
        if (!tryManage(referent)) {
            throw new LivenessStateException(String.format(
                    "%s (%s) failed to add %s (%s), because either this manager or referent is no longer live",
                    this, ReferenceCounted.getReferenceCountDebug(this),
                    referent, ReferenceCounted.getReferenceCountDebug(referent)));
        }
    }

    /**
     * Attempt to add {@code referent} to this manager. Will succeed if {@code referent} is live and if this manager is
     * not a {@link LivenessReferent} or is live.
     *
     * @param referent The referent to add
     * @return Whether the referent was in fact added
     */
    boolean tryManage(@NotNull LivenessReferent referent);
}
