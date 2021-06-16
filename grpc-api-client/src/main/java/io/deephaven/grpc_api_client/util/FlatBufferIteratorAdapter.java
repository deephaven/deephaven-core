/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api_client.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.IntFunction;

public class FlatBufferIteratorAdapter<T> implements Iterator<T> {
    private int offset = 0;
    private final int length;
    private final IntFunction<T> getNext;

    public FlatBufferIteratorAdapter(final int length, final IntFunction<T> getNext) {
        this.length = length;
        this.getNext = getNext;
    }

    @Override
    public boolean hasNext() {
        return offset < length;
    }

    @Override
    public T next() {
        if (offset >= length) {
            throw new NoSuchElementException();
        }
        return getNext.apply(offset++);
    }
}
