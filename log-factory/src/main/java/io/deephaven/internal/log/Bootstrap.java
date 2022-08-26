/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.internal.log;

class Bootstrap {
    private static boolean isEnabled() {
        return Boolean.parseBoolean(
                System.getProperty("io.deephaven.internal.log.Bootstrap.enabled", "true"));
    }

    public static void log(Class<?> source, String message) {
        if (isEnabled()) {
            System.out.printf("# %s: %s%n", source.getName(), message);
        }
    }
}
