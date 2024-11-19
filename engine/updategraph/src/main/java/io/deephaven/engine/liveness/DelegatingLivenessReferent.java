//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.liveness;

import io.deephaven.util.annotations.ReferentialIntegrity;

import java.lang.ref.WeakReference;

/**
 * Indicates that this class implements LivenessReferent via a member rather than implementing it directly. The real
 * LivenessReferent is exposed via {@link #asLivenessReferent()}, all other methods delegate to this instance.
 */
public interface DelegatingLivenessReferent extends LivenessReferent {
    /**
     * Returns the "real" {@link LivenessReferent} instance. When implementing this, care should be taken to match
     * lifecycle of the {@code DelegatingLivenessReferent} instance with this instance, as the returned
     * {@code LivenessReferent} behaves as a proxy for {@code this}.
     *
     * @return a LivenessReferent to use to manage this object's liveness.
     */
    LivenessReferent asLivenessReferent();

    @Override
    default boolean tryRetainReference() {
        return asLivenessReferent().tryRetainReference();
    }

    @Override
    default void dropReference() {
        asLivenessReferent().dropReference();
    }

    @Override
    default WeakReference<? extends LivenessReferent> getWeakReference() {
        // Must return a WeakReference to the DelegatingLivenessReferent, not the underlying LivenessReferent
        return new WeakReference<>(this) {
            /**
             * Hold this reference to ensure that any cleanup that requires the strong reachability of our underlying
             * LivenessReferent's WeakReference happens.
             */
            @ReferentialIntegrity
            private final WeakReference<? extends LivenessReferent> delegate = asLivenessReferent().getWeakReference();
        };
    }
}
