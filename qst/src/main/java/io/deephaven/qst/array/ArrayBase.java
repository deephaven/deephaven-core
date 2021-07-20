package io.deephaven.qst.array;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public abstract class ArrayBase<T> implements Array<T> {

    @Override
    public final Stream<T> stream() {
        return IntStream.range(0, size()).mapToObj(this::get);
    }

    @Override
    public final Iterator<T> iterator() {
        return stream().iterator();
    }

    @Override
    public final void forEach(Consumer<? super T> action) {
        int L = size();
        for (int i = 0; i < L; ++i) {
            action.accept(get(i));
        }
    }

    @Override
    public final Spliterator<T> spliterator() {
        return stream().spliterator();
    }
}
