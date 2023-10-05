package io.deephaven.engine.liveness;

import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.stream.Stream;

public interface IsLivenessNode extends LivenessNode {
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
