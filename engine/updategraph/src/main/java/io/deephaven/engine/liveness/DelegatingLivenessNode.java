package io.deephaven.engine.liveness;

import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.stream.Stream;

/**
 * Indicates that this class implements LivenessNode via a member rather than implementing it directly. The real
 * LivenessNode is exposed via {@link #asLivenessNode()}, all other methods delegate to this instance.
 */
public interface DelegatingLivenessNode extends LivenessNode {
    /**
     * Returns the "real" {@link LivenessNode} instance. When implementing this, care should be taken to match lifecycle
     * of the {@code DelegatingLivenessNode} instance with this instance, as the returned {@code LivenessNode} behaves
     * as a proxy for {@code this}.
     *
     * @return a LivenessNode to use to manage this object's liveness.
     */
    LivenessNode asLivenessNode();

    @Override
    default boolean tryManage(@NotNull LivenessReferent referent) {
        return asLivenessNode().tryManage(referent);
    }

    @Override
    default boolean tryUnmanage(@NotNull LivenessReferent referent) {
        return asLivenessNode().tryManage(referent);
    }

    @Override
    default boolean tryUnmanage(@NotNull Stream<? extends LivenessReferent> referents) {
        return asLivenessNode().tryUnmanage(referents);
    }

    @Override
    default boolean tryRetainReference() {
        return asLivenessNode().tryRetainReference();
    }

    @Override
    default void dropReference() {
        asLivenessNode().dropReference();
    }

    @Override
    default WeakReference<? extends LivenessReferent> getWeakReference() {
        return asLivenessNode().getWeakReference();
    }
}
