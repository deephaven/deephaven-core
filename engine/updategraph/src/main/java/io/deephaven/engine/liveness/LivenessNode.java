/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.liveness;

import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@link LivenessReferent} that is also a {@link LivenessManager}, transitively enforcing liveness on its referents.
 */
public interface LivenessNode extends LivenessReferent, LivenessManager {

    /**
     * If this node manages {@code referent} one or more times, drop one such reference. This node must be live.
     *
     * @param referent The referent to drop
     */
    @FinalDefault
    default void unmanage(@NotNull LivenessReferent referent) {
        if (!tryUnmanage(referent)) {
            throw new LivenessStateException(this + " is no longer live and cannot unmanage " +
                    referent.getReferentDescription());
        }
    }

    /**
     * If this node is still live and manages referent one or more times, drop one such reference.
     *
     * @param referent The referent to drop
     * @return Whether this node was live and thus in fact tried to drop a reference
     */
    boolean tryUnmanage(@NotNull LivenessReferent referent);

    /**
     * For each referent in {@code referent}, if this node manages referent one or more times, drop one such reference.
     * This node must be live.
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
     * For each referent in referents, if this node is still live and manages referent one or more times, drop one such
     * reference.
     *
     * @param referents The referents to drop
     * @return Whether this node was live and thus in fact tried to drop a reference
     */
    @SuppressWarnings("unused")
    boolean tryUnmanage(@NotNull Stream<? extends LivenessReferent> referents);
}
