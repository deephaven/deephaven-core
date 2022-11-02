/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.internal.log;

public final class Bootstrap {

    public static boolean isQuiet() {
        return Boolean.getBoolean("deephaven.quiet");
    }

    public static void log(Class<?> source, String message) {
        printf("# %s: %s%n", source.getName(), message);
    }

    public static void printf(String format, Object ... args) {
        if (!isQuiet()) {
            System.out.printf(format, args);
        }
    }
}
