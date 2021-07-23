package io.deephaven.qst.array;

import java.lang.reflect.Array;
import java.util.Objects;

abstract class PrimitiveArrayHelper<T> {

    private final Class<?> primitiveType;

    protected int size;
    protected T array;

    PrimitiveArrayHelper(int initialCapacity, Class<?> primitiveType) {
        this.primitiveType = Objects.requireNonNull(primitiveType);
        // noinspection unchecked
        array = (T) Array.newInstance(primitiveType, initialCapacity);
    }

    void ensureCapacity() {
        ensureCapacity(1);
    }

    void ensureCapacity(int additional) {
        int desired = Math.addExact(size, additional);
        int L = Array.getLength(array);
        if (desired > L) {
            int nextSize = Math.max(Math.multiplyExact(L, 2), desired);
            T next = construct(nextSize);
            // noinspection SuspiciousSystemArraycopy
            System.arraycopy(array, 0, next, 0, L);
            array = next;
        }
    }

    void addImpl(T addition) {
        int L = Array.getLength(addition);
        ensureCapacity(L);
        // noinspection SuspiciousSystemArraycopy
        System.arraycopy(addition, 0, array, size, L);
        size += L;
    }

    T takeAtSize() {
        if (size == Array.getLength(array)) {
            return array; // great case, no copying necessary :)
        }
        T atSize = construct(size);
        // noinspection SuspiciousSystemArraycopy
        System.arraycopy(array, 0, atSize, 0, size);
        return atSize;
    }

    private T construct(int size) {
        // noinspection unchecked
        return (T) Array.newInstance(primitiveType, size);
    }
}
