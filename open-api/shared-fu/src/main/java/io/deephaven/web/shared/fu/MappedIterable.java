package io.deephaven.web.shared.fu;

import java.util.*;
import java.util.function.Function;

@FunctionalInterface
public interface MappedIterable<T> extends Iterable<T> {

    MappedIterable EMPTY = () -> new Iterator() {
        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public Object next() {
            throw new NoSuchElementException("empty");
        }
    };

    static <T> MappedIterable<T> of(Iterable<T> source) {
        return source instanceof MappedIterable ? (MappedIterable<T>) source : source::iterator;
    }

    @SuppressWarnings("unchecked")
    static <T> MappedIterable<T> empty() {
        return EMPTY;
    }

    default MappedIterable<T> filterNonNull() {
        return filter(Objects::nonNull);
    }

    default MappedIterable<T> filter(Function<T, Boolean> filter) {

        return () -> {
            Iterator<T> source = MappedIterable.this.iterator();
            return new Iterator<T>() {
                T next;

                @Override
                public boolean hasNext() {
                    if (next != null) {
                        return true;
                    }
                    while (source.hasNext()) {
                        T candidate = source.next();
                        if (filter.apply(candidate)) {
                            next = candidate;
                            return true;
                        }
                    }
                    return false;
                }

                @Override
                public T next() {
                    try {
                        return next;
                    } finally {
                        next = null;
                    }
                }

                @Override
                public void remove() {
                    source.remove();
                }
            };
        };
    }

    default boolean anyMatch(JsPredicate<T> test) {
        for (T t : this) {
            if (test.test(t)) {
                return true;
            }
        }
        return false;
    }

    default boolean noneMatch(JsPredicate<T> test) {
        for (T t : this) {
            if (test.test(t)) {
                return false;
            }
        }
        return true;
    }

    default boolean allMatch(JsPredicate<T> test) {
        for (T t : this) {
            if (!test.test(t)) {
                return false;
            }
        }
        return true;
    }

    default <V> MappedIterable<V> mapped(Function<T, V> mapper) {
        return () -> {
            Iterator<T> source = MappedIterable.this.iterator();
            return new Iterator<V>() {
                @Override
                public boolean hasNext() {
                    return source.hasNext();
                }

                @Override
                public V next() {
                    final V result = mapper.apply(source.next());
                    return result;
                }

                @Override
                public void remove() {
                    source.remove();
                }
            };
        };
    }

    default MappedIterable<T> plus(Iterable<T> more) {
        return () -> {
            final Iterator<T> mine = MappedIterable.this.iterator();
            final Iterator<T> yours = more.iterator();
            return new Iterator<T>() {
                Iterator<T> delegate = mine;

                @Override
                public boolean hasNext() {
                    if (delegate.hasNext()) {
                        return true;
                    } else {
                        delegate = yours;
                    }
                    return delegate.hasNext();
                }

                @Override
                public T next() {
                    return delegate.next();
                }

                @Override
                public void remove() {
                    delegate.remove();
                }
            };
        };
    }

    default T first() {
        final Iterator<T> itr = iterator();
        if (itr.hasNext()) {
            return itr.next();
        }
        throw new NoSuchElementException(this + " is empty");
    }

    default boolean isEmpty() {
        return !iterator().hasNext();
    }

    static <T> MappedIterable<T> reversed(List<T> items) {
        return new ReverseListIterable<>(items);
    }

    default MappedIterable<T> reverse() {
        return () -> new Iterator<T>() {
            private final ListIterator<T> itr;

            {
                // Would be nice to add the infrastructure for a better reversal,
                // but this is only O(n) and used rarely. (There are overrides of reverse()).
                // Would be better to either a) use guava, or b) create ReversedIterable type,
                // which can un-reverse efficiently (share the list, at least).
                // It is not needed now, but this note is here in case it becomes worthwhile.
                List<T> items = new ArrayList<>();
                forEach(items::add);
                itr = items.listIterator(items.size());
            }

            @Override
            public boolean hasNext() {
                return itr.hasPrevious();
            }

            @Override
            public T next() {
                return itr.previous();
            }
        };
    }
}
