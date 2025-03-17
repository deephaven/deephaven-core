package io.deephaven.base;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

public interface WeakReferenceManager<T> {
    void add(T item);

    void remove(T item);

    void removeAll(Collection<T> items);

    void forEachValidReference(Consumer<T> proc);

    T getFirst(Predicate<T> test);

    boolean isEmpty();

    void clear();

    Iterator<T> iterator();

    Stream<T> stream();

    Stream<T> parallelStream();
}
