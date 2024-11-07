//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.internal.log;

import java.util.Optional;

public final class Bootstrap {

    public static boolean isQuiet() {
        Optional<String> optional = viaProperty();
        if (!optional.isPresent()) {
            optional = viaEnvironment();
        }
        return optional.map(Boolean::parseBoolean)
                .orElse(false);
    }

    public static void log(Class<?> source, String message) {
        printf("# %s: %s%n", source.getName(), message);
    }

    public static void printf(String format, Object... args) {
        if (!isQuiet()) {
            System.out.printf(format, args);
        }
    }

    private static Optional<String> viaProperty() {
        return Optional.ofNullable(System.getProperty("deephaven.quiet"));
    }

    private static Optional<String> viaEnvironment() {
        return Optional.ofNullable(System.getenv("DEEPHAVEN_QUIET"));
    }
}
