package io.deephaven.util.datastructures.intrusive;

import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * An intrusive set that uses an array for its backing storage.
 *
 * You can insert, remove, or check for existence in O(1) time. Clearing the set is O(n); as we need to null out
 * references.
 *
 * If you attempt to perform an operation element which is not in this set, but is in another set with the same adapter;
 * then you are going to have a bad time. Tread carefully.
 *
 * @param <T> the type of the element we are storing.
 */
public class IntrusiveArraySet<T> implements Set<T> {
    private final Adapter<T> adapter;
    private int size = 0;
    private T[] storage;

    public IntrusiveArraySet(Adapter<T> adapter, Class<T> elementClass) {
        this(adapter, elementClass, 16);
    }

    @SuppressWarnings("WeakerAccess")
    public IntrusiveArraySet(Adapter<T> adapter, Class<T> elementClass, int initialCapacity) {
        this.adapter = adapter;
        // noinspection unchecked
        storage = (T[]) Array.newInstance(elementClass, initialCapacity);
    }

    public void ensureCapacity(int capacity) {
        if (storage.length >= capacity) {
            return;
        }
        storage = Arrays.copyOf(storage, capacity);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public boolean contains(Object o) {
        if (o == null) {
            return false;
        }
        // noinspection unchecked
        final int candidateSlot = adapter.getSlot((T) o);
        return (candidateSlot >= 0 && candidateSlot < size && storage[candidateSlot] == o);
    }

    @NotNull
    @Override
    public Iterator<T> iterator() {
        return new IteratorImpl();
    }

    @NotNull
    @Override
    public Object[] toArray() {
        return Arrays.copyOf(storage, size);
    }

    @NotNull
    @Override
    public <T1> T1[] toArray(@NotNull T1[] a) {
        // noinspection unchecked
        final T[] r = a.length >= size ? (T[]) a
                : (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), size);

        if (size >= 0) {
            System.arraycopy(storage, 0, r, 0, size);
        }
        if (a.length > size) {
            a[size] = null;
        }

        // noinspection unchecked
        return (T1[]) r;
    }

    @Override
    public boolean add(T t) {
        if (t == null) {
            throw new IllegalArgumentException();
        }

        final int slot = adapter.getSlot(t);
        if (slot >= 0 && slot < size && storage[slot] == t) {
            return false;
        }
        if (size == storage.length) {
            ensureCapacity(storage.length * 2);
        }
        storage[size] = t;
        adapter.setSlot(t, size++);
        return true;
    }

    @Override
    public boolean remove(Object o) {
        if (o == null) {
            return false;
        }
        // noinspection unchecked
        final int candidateSlot = adapter.getSlot((T) o);
        if (candidateSlot >= 0 && candidateSlot < size && storage[candidateSlot] == o) {
            if (candidateSlot < size - 1) {
                storage[candidateSlot] = storage[size - 1];
                adapter.setSlot(storage[candidateSlot], candidateSlot);
            }
            size--;
            storage[size] = null;
            return true;
        }

        return false;
    }

    @Override
    public boolean containsAll(@NotNull Collection<?> collection) {
        for (Object c : collection) {
            if (!contains(c)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean addAll(@NotNull Collection<? extends T> c) {
        boolean added = false;
        for (T t : c) {
            added = add(t) || added;
        }
        return added;
    }

    @Override
    public boolean retainAll(@NotNull Collection<?> collection) {
        int destinationSlot = 0;

        for (Object c : collection) {
            final int slot = adapter.getSlot((T) c);
            // check if it exists in our set
            if (slot >= 0 && slot < size && storage[slot] == c) {
                if (destinationSlot > slot) {
                    // if we have already retained it, we need not retain it again
                    continue;
                } else if (destinationSlot == slot) {
                    // it's already where we want it
                    destinationSlot++;
                    continue;
                }

                // swap c and destination slot
                storage[slot] = storage[destinationSlot];
                storage[destinationSlot] = (T) c;

                adapter.setSlot(storage[destinationSlot], destinationSlot);
                adapter.setSlot(storage[slot], slot);

                destinationSlot++;
            }
        }
        final int oldSize = size;
        size = destinationSlot;

        Arrays.fill(storage, size, oldSize, null);

        return oldSize != size;
    }

    @Override
    public boolean removeAll(@NotNull Collection<?> collection) {
        boolean removed = false;
        for (Object c : collection) {
            removed = remove(c) || removed;
        }

        return removed;
    }

    @Override
    public void clear() {
        Arrays.fill(storage, 0, size, null);
        size = 0;
    }

    /**
     * Adapter interface for elements to be entered into the set.
     */
    public interface Adapter<T> {
        int getSlot(T element);

        void setSlot(T element, int slot);
    }

    private class IteratorImpl implements Iterator<T> {
        int index = 0;

        @Override
        public boolean hasNext() {
            return index < size;
        }

        @Override
        public T next() {
            return storage[index++];
        }

        @Override
        public void remove() {
            if (index == 0 || index > size) {
                throw new IllegalStateException();
            }
            if (index < size) {
                storage[index - 1] = storage[size - 1];
                adapter.setSlot(storage[index - 1], index - 1);
                index--;
                storage[size - 1] = null;
            }
            size--;
        }
    }
}
