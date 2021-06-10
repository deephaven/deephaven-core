package io.deephaven.db.util.liveness;

import org.jetbrains.annotations.NotNull;

/**
 * Exception class thrown when incorrect usage of a {@link LivenessReferent} is detected.
 */
class LivenessStateException extends IllegalStateException {

    LivenessStateException(@NotNull final String message) {
        super(message);
        Liveness.maybeHeapDump(this);
    }
}
