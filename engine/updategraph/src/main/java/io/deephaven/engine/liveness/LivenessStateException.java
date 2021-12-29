package io.deephaven.engine.liveness;

import org.jetbrains.annotations.NotNull;

/**
 * Exception class thrown when incorrect usage of a {@link LivenessReferent} is detected.
 */
public class LivenessStateException extends IllegalStateException {

    LivenessStateException(@NotNull final String message) {
        super(message);
        Liveness.maybeHeapDump(this);
    }
}
