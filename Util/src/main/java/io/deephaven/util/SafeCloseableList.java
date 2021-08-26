package io.deephaven.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * {@link SafeCloseable} that will close an internal list of other {@link SafeCloseable}s.
 */
public class SafeCloseableList implements SafeCloseable {

    private final List<SafeCloseable> list = new ArrayList<>();

    public SafeCloseableList() {}

    public SafeCloseableList(SafeCloseable... entries) {
        this(Arrays.asList(entries));
    }

    public SafeCloseableList(Collection<SafeCloseable> entries) {
        list.addAll(entries);
    }

    public final void addAll(@NotNull final List<SafeCloseable> closeableList) {
        list.addAll(closeableList);
    }

    public final <T extends SafeCloseable> T[] addArray(@Nullable final T[] closeables) {
        if (closeables != null) {
            list.add(new SafeCloseableArray<>(closeables));
        }
        return closeables;
    }

    public final <T extends SafeCloseable> T add(final T closeable) {
        list.add(closeable);
        return closeable;
    }

    public final void clear() {
        list.clear();
    }

    @Override
    public final void close() {
        for (final SafeCloseable closeable : list) {
            if (closeable != null) {
                closeable.close();
            }
        }
        list.clear();
    }
}
