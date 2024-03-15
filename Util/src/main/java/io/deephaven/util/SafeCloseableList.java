//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

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

    public static final Collector<SafeCloseable, SafeCloseableList, SafeCloseableList> COLLECTOR = new Collector<>() {

        @Override
        public Supplier<SafeCloseableList> supplier() {
            return SafeCloseableList::new;
        }

        @Override
        public BiConsumer<SafeCloseableList, SafeCloseable> accumulator() {
            return SafeCloseableList::add;
        }

        @Override
        public BinaryOperator<SafeCloseableList> combiner() {
            return (left, right) -> {
                left.addAll(right.list);
                return left;
            };
        }

        @Override
        public Function<SafeCloseableList, SafeCloseableList> finisher() {
            return a -> a;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return Set.of(Characteristics.IDENTITY_FINISH);
        }
    };
}
