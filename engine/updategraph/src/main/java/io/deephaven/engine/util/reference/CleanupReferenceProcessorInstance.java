//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util.reference;

import io.deephaven.base.verify.Require;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.reference.CleanupReferenceProcessor;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.ReferenceQueue;

/**
 * Dedicated {@link java.lang.ref.ReferenceQueue} suppliers for various engine purposes.
 */
public enum CleanupReferenceProcessorInstance {

    // @formatter:off
    LIVENESS(new CleanupReferenceProcessor("liveness", 1000,
            (l, r, e) -> {
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }
                throw new RuntimeException(e);
            })
    );
    // @formatter:on

    private final CleanupReferenceProcessor cleanupReferenceProcessor;

    CleanupReferenceProcessorInstance(@NotNull final CleanupReferenceProcessor cleanupReferenceProcessor) {
        this.cleanupReferenceProcessor = Require.neqNull(cleanupReferenceProcessor, "cleanupReferenceProcessor");
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
        CleanupReferenceProcessor.getDefault().resetForUnitTests();
    }
}
