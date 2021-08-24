package io.deephaven.util;

import org.jetbrains.annotations.NotNull;

/**
 * Some utilities for inspecting exceptions
 */
public class ExceptionUtil {
    public static boolean causedBy(@NotNull Throwable t, Class<? extends Throwable> cause) {
        Throwable curr = t;
        while (curr != null) {
            if (cause.isAssignableFrom(curr.getClass())) {
                return true;
            }
            curr = curr.getCause();
        }
        return false;
    }
}
