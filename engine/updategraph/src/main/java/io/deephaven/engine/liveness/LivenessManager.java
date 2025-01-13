//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.liveness;

import io.deephaven.util.annotations.FinalDefault;
import io.deephaven.util.referencecounting.ReferenceCounted;
import org.jetbrains.annotations.NotNull;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    /**
     * If this manager manages {@code referent} one or more times, drop one such reference. If this manager is also a
     * {@link LivenessReferent}, then it must also be live.
     * <p>
     * <em>Strongly prefer using {@link #unmanage(Stream)} when multiple referents should be unmanaged.</em>
     *
     * @param referent The referent to drop
     */
    @FinalDefault
    default void unmanage(@NotNull LivenessReferent referent) {
        if (!tryUnmanage(referent)) {
            throw new LivenessStateException(this + " cannot unmanage " + referent.getReferentDescription());
        }
    }

    /**
     * If this manager manages referent one or more times, drop one such reference. If this manager is also a
     * {@link LivenessReferent}, then this method is a no-op if {@code this} is not live.
     * <p>
     * <em>Strongly prefer using {@link #tryUnmanage(Stream)}} when multiple referents should be unmanaged.</em>
     *
     * @param referent The referent to drop
     * @return If this node is also a {@link LivenessReferent}, whether this node was live and thus in fact tried to
     *         drop a reference. Else always returns {@code true} if dropping a reference via this method is supported
     *         by the implementation.
     */
    boolean tryUnmanage(@NotNull LivenessReferent referent);

    /**
     * For each referent in {@code referent}, if this manager manages referent one or more times, drop one such
     * reference. If this manager is also a {@link LivenessReferent}, then it must also be live.
     *
     * @param referents The referents to drop
     */
    @SuppressWarnings("unused")
    @FinalDefault
    default void unmanage(@NotNull Stream<? extends LivenessReferent> referents) {
        if (!tryUnmanage(referents)) {
            throw new LivenessStateException(this + " is no longer live and cannot unmanage " +
                    referents.map(LivenessReferent::getReferentDescription).collect(Collectors.joining()));
        }
    }

    /**
     * For each referent in referents, if this manager manages referent one or more times, drop one such reference. If
     * this manager is also a {@link LivenessReferent}, then this method is a no-op if {@code this} is not live.
     *
     * @param referents The referents to drop
     * @return If this node is also a {@link LivenessReferent}, whether this node was live and thus in fact tried to
     *         drop the references. Else always returns {@code true} if dropping a reference via this method is
     *         supported by the implementation.
     */
    @SuppressWarnings("unused")
    boolean tryUnmanage(@NotNull Stream<? extends LivenessReferent> referents);
}
