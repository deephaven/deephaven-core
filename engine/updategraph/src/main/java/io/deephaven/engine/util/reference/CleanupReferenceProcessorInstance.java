//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util.reference;

import io.deephaven.base.reference.CleanupReference;
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

    /**
     * Registers a {@code referent} and a cleaning {@code action} to run when the {@code referent} becomes phantom
     * reachable. The returned {@link CleanupReference} is retained by this processor, ensuring it will not be
     * garbage-collected before cleanup occurs.
     *
     * @param referent the object to monitor
     * @param action a {@code Runnable} to invoke when the referent becomes phantom reachable; must <b>not</b> hold a
     *        strong reference to the referent
     * @return a cleanup reference instance
     * @see CleanupReferenceProcessor#registerPhantom(Object, Runnable)
     */
    public final <T> CleanupReference<T> registerPhantom(@NotNull final T referent, @NotNull final Runnable action) {
        return cleanupReferenceProcessor.registerPhantom(referent, action);
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
