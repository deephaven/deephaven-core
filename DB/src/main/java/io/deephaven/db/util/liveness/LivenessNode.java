package io.deephaven.db.util.liveness;

/**
 * A {@link LivenessReferent} that is also a {@link LivenessManager}, transitively enforcing liveness on its referents.
 *
 * @IncludeAll
 */
public interface LivenessNode extends LivenessReferent, LivenessManager {
}
