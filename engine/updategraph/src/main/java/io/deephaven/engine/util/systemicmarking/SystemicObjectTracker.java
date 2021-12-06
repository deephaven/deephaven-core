/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util.systemicmarking;

import io.deephaven.configuration.Configuration;

import java.util.function.Supplier;

/**
 * If enabled, marks objects as systemically important.
 *
 * When enabled, only errors from systemically important objects are treated as fatal.
 */
public class SystemicObjectTracker {

    private static final ThreadLocal<Boolean> SYSTEMIC_CREATION_THREAD = ThreadLocal.withInitial(() -> false);

    private static final boolean SYSTEMIC_OBJECT_MARKING_ENABLED =
            Configuration.getInstance().getBooleanWithDefault("SystemicObjectTracker.enabled", false);

    /**
     * @return true if systemic object marking is enabled, false otherwise.
     */
    public static boolean isSystemicObjectMarkingEnabled() {
        return SYSTEMIC_OBJECT_MARKING_ENABLED;
    }

    /**
     * @return true if the current thread is creating systemic objects, false otherwise
     */
    public static boolean isSystemicThread() {
        return SYSTEMIC_OBJECT_MARKING_ENABLED && SYSTEMIC_CREATION_THREAD.get();
    }

    /**
     * Marks the current thread as systemically important, this is a permanent change.
     */
    public static void markThreadSystemic() {
        if (SYSTEMIC_OBJECT_MARKING_ENABLED) {
            SYSTEMIC_CREATION_THREAD.set(true);
        }
    }

    /**
     * Execute the supplier with the thread's systemic importance set to the value of systemicThread.
     *
     * @param systemicThread if the thread should be systemic while executing supplier
     * @param supplier the operation to execute with the given value of systemicThread
     * @param <T> return type of the supplier
     *
     * @return the supplier's return value
     */
    public static <T> T executeSystemically(final boolean systemicThread, final Supplier<T> supplier) {
        if (!SYSTEMIC_OBJECT_MARKING_ENABLED) {
            return supplier.get();
        }

        boolean oldSystemic = SYSTEMIC_CREATION_THREAD.get();
        if (oldSystemic != systemicThread) {
            SYSTEMIC_CREATION_THREAD.set(systemicThread);
        }
        try {
            return supplier.get();
        } finally {
            if (oldSystemic != systemicThread) {
                SYSTEMIC_CREATION_THREAD.set(oldSystemic);
            }
        }
    }

    /**
     * Determine if an object is systemic.
     *
     * If marking is not enabled, all objects are systemic. If marking is enabled, only objects marked systemic are
     * systemic.
     *
     * @return true if o should be treated as systemic object.
     */
    public static boolean isSystemic(SystemicObject o) {
        if (!SYSTEMIC_OBJECT_MARKING_ENABLED) {
            return true;
        }
        return o.isSystemicObject();
    }
}
