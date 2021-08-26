package io.deephaven.util.datastructures;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A deque, which also supports get() to an arbitrary index.
 */
public class RandomAccessDeque<T> implements Collection<T> {
    private static final int DEQUE_EXPANSION = 100;

    private Object[] array;
    private int expansion;
    private int start;
    private int end;

    public RandomAccessDeque() {
        this(Collections.emptyList(), DEQUE_EXPANSION);
    }

    public RandomAccessDeque(int expansion) {
        this(Collections.emptyList(), expansion);
    }

    public RandomAccessDeque(Collection<T> initialValues) {
        this(initialValues, DEQUE_EXPANSION);
    }

    @SuppressWarnings("WeakerAccess")
    public RandomAccessDeque(Collection<T> initialValues, int expansion) {
        this.getClass().getComponentType();
        this.expansion = expansion;
        array = new Object[2 * expansion + initialValues.size()];
        start = end = expansion;
        Arrays.fill(array, null);
        initialValues.forEach(this::addLast);
    }

    public int size() {
        return end - start;
    }

    @Override
    public boolean isEmpty() {
        return end == start;
    }

    @Override
    public boolean contains(Object o) {
        return stream().anyMatch(x -> o == x || (o != null && o.equals(x)));
    }

    @NotNull
    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            int iStart = start;

            @Override
            public boolean hasNext() {
                return iStart < end;
            }

            @Override
            public T next() {
                // noinspection unchecked
                return (T) array[iStart++];
            }

            @Override
            public void remove() {
                System.arraycopy(array, iStart, array, iStart - 1, end - iStart);
                end--;
                iStart--;
                array[end + 1] = null;
            }
        };
    }

    @NotNull
    @Override
    public Object[] toArray() {
        Object[] result = new Object[end - start];
        System.arraycopy(array, start, result, 0, end - start);
        return result;
    }

    @NotNull
    @Override
    public <T1> T1[] toArray(@NotNull T1[] a) {
        if (a.length < end - start) {
            // noinspection unchecked
            a = (T1[]) Array.newInstance(a.getClass().getComponentType(), end - start);
        }
        // noinspection SuspiciousSystemArraycopy
        System.arraycopy(array, start, a, 0, end - start);
        return a;
    }

    @Override
    public boolean add(T t) {
        addLast(t);
        return true;
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> c) {
        Set<?> missing = new HashSet<>(c);
        for (int ii = start; ii < end; ++ii) {
            if (missing.remove(array[ii])) {
                if (missing.isEmpty()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends T> c) {
        c.forEach(this::addLast);
        return true;
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> c) {
        return removeIf(c::contains);
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> c) {
        return removeIf(x -> !c.contains(x));
    }

    @Override
    public void clear() {
        Arrays.fill(array, start, end, null);
        end = start;
    }

    public void addFirst(T vv) {
        if (start == 0) {
            // we need to rebuffer the array
            Object[] newArray = new Object[array.length + expansion];
            Arrays.fill(newArray, 0, expansion, null);
            System.arraycopy(array, start, newArray, expansion, end - start);
            start = expansion;
            end += expansion;
            Arrays.fill(newArray, end, newArray.length, null);
            array = newArray;
        }
        array[--start] = vv;
    }

    @SuppressWarnings("WeakerAccess")
    public void addLast(T vv) {
        if (end == array.length) {
            // we need to extend the array
            array = Arrays.copyOf(array, array.length + DEQUE_EXPANSION);
        }
        array[end++] = vv;
    }

    @Override
    public boolean removeIf(Predicate<? super T> predicate) {
        boolean modified = false;
        int jj = start;
        for (int ii = start; ii < end; ++ii) {
            // noinspection unchecked
            if (predicate.test((T) array[ii])) {
                // we should remove this element
                modified = true;
            } else {
                // if this is an element that we retain, we should advance ii and jj
                if (ii != jj) {
                    array[jj++] = array[ii];
                } else {
                    jj++;
                }
            }
        }
        Arrays.fill(array, jj, end, null);
        end = jj;
        return modified;
    }

    @Override
    public boolean remove(Object entry) {
        return removeIf(x -> x == entry || x != null && x.equals(entry));
    }

    public T get(int index) {
        if (start + index >= end || index < 0 || (start + index < start)) {
            throw new ArrayIndexOutOfBoundsException(
                "index=" + index + ", end=" + end + ", start=" + start);
        }
        // noinspection unchecked
        return (T) array[start + index];
    }

    public Stream<T> stream() {
        return StreamSupport.stream(new DequeSpliterator(), false);
    }

    @Override
    public Stream<T> parallelStream() {
        return StreamSupport.stream(new DequeSpliterator(), true);
    }

    private class DequeSpliterator implements Spliterator<T> {
        int spliterStart, spliterEnd;

        DequeSpliterator() {
            this.spliterStart = RandomAccessDeque.this.start;
            this.spliterEnd = RandomAccessDeque.this.end;
        }

        DequeSpliterator(int spliterStart, int spliterEnd) {
            this.spliterStart = spliterStart;
            this.spliterEnd = spliterEnd;
        }

        @Override
        public Spliterator<T> trySplit() {
            int mid = (spliterStart + spliterEnd) / 2;
            if (spliterStart < mid) {
                int oldStart = spliterStart;
                spliterStart = mid;
                return new DequeSpliterator(oldStart, mid);
            } else {
                return null;
            }
        }

        @Override
        public long estimateSize() {
            return spliterEnd - spliterStart;
        }

        @Override
        public int characteristics() {
            return SIZED | ORDERED | IMMUTABLE | SUBSIZED;
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if (spliterStart < spliterEnd) {
                // noinspection unchecked
                action.accept((T) array[spliterStart++]);
                return true;
            }
            return false;
        }

        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            // noinspection StatementWithEmptyBody
            while (tryAdvance(action)) {
                // this while body intentionally left blank
            }
        }
    }
}
