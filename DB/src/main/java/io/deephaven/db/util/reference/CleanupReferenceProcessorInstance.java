package io.deephaven.db.util.reference;

import io.deephaven.base.verify.Require;
import io.deephaven.util.Utils;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.reference.CleanupReferenceProcessor;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.ReferenceQueue;

/**
 * Dedicated {@link java.lang.ref.ReferenceQueue} suppliers for various db purposes.
 */
public enum CleanupReferenceProcessorInstance {

    DEFAULT(new CleanupReferenceProcessor("default", 1000,
        (l, r, e) -> l.warn().append(Thread.currentThread().getName())
            .append(": Exception thrown from cleanup of ")
            .append(Utils.REFERENT_FORMATTER, r).append(": ").append(e).endl())), LIVENESS(
                new CleanupReferenceProcessor("liveness", 1000, (l, r, e) -> {
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    }
                    throw new RuntimeException(e);
                }));

    private final CleanupReferenceProcessor cleanupReferenceProcessor;

    CleanupReferenceProcessorInstance(
        @NotNull final CleanupReferenceProcessor cleanupReferenceProcessor) {
        this.cleanupReferenceProcessor =
            Require.neqNull(cleanupReferenceProcessor, "cleanupReferenceProcessor");
    }

    public final <RT> ReferenceQueue<RT> getReferenceQueue() {
        return cleanupReferenceProcessor.getReferenceQueue();
    }

    @TestUseOnly
    public static void resetAllForUnitTests() {
        for (@NotNull
        final CleanupReferenceProcessorInstance instance : values()) {
            instance.cleanupReferenceProcessor.resetForUnitTests();
        }
    }
}
