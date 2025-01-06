//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.liveness;

/**
 * A {@link LivenessReferent} that is also a {@link LivenessManager}, transitively enforcing liveness on its referents.
 */
public interface LivenessNode extends LivenessReferent, LivenessManager {
}
