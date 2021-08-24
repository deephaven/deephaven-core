package io.deephaven.web.shared.fu;

import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Supplier;

public class ReverseListIterable<T> implements MappedIterable<T> {

    private final Supplier<ListIterator<T>> itrSource;

    public ReverseListIterable(List<T> items) {
        itrSource = () -> items.listIterator(items.size());
    }

    @Override
    public Iterator<T> iterator() {
        return new Itr();
    }

    private final class Itr implements Iterator<T> {

        ListIterator<T> itr = itrSource.get();

        @Override
        public boolean hasNext() {
            return itr.hasPrevious();
        }

        @Override
        public T next() {
            return itr.previous();
        }

    }
}
