/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@link AutoCloseable} sub-interface that does not throw a checked exception.
 */
public interface SafeCloseable extends AutoCloseable {

    static void closeArray(@NotNull final SafeCloseable... safeCloseables) {
        for (SafeCloseable sc : safeCloseables) {
            closeSingle(sc);
        }
    }

    static void closeSingle(@Nullable final SafeCloseable safeCloseable) {
        if (safeCloseable != null) {
            safeCloseable.close();
        }
    }

    @Override
    void close();
}
