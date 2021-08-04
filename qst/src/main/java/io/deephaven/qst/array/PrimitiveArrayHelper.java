package io.deephaven.qst.array;

import java.util.Objects;

abstract class PrimitiveArrayHelper<T> {

    protected int size;
    protected T array;

    PrimitiveArrayHelper(T initialArray) {
        array = Objects.requireNonNull(initialArray);
        size = 0;
    }

    abstract int length(T array);

    abstract void arraycopy(T src, int srcPos, T dest, int destPos, int length);

    abstract T construct(int size);

    void ensureCapacity() {
        ensureCapacity(1);
    }

    void ensureCapacity(int additional) {
        int desired = Math.addExact(size, additional);
        int L = length(array);
        if (desired > L) {
            int nextSize = Math.max(Math.multiplyExact(L, 2), desired);
            T next = construct(nextSize);
            arraycopy(array, 0, next, 0, L);
            array = next;
        }
    }

    void addImpl(T addition) {
        int L = length(addition);
        ensureCapacity(L);
        arraycopy(addition, 0, array, size, L);
        size += L;
    }

    T takeAtSize() {
        if (size == length(array)) {
            return array; // great case, no copying necessary :)
        }
        T atSize = construct(size);
        arraycopy(array, 0, atSize, 0, size);
        return atSize;
    }
}
