/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.datasets.data;

import org.jetbrains.annotations.NotNull;

/**
 * {@link IndexableData} dataset which ensures no {@link IndexOutOfBoundsException}s are thrown.
 * Instead, null values are returned.
 */
public class IndexableDataInfinite<T> extends IndexableData<T> {
    private static final long serialVersionUID = 492887860330671830L;
    private final IndexableData<T> data;

    /**
     * Creates an IndexableDataInfinite instance, which wraps {@code data} such that out-of-bounds
     * indices return null values.
     *
     */
    public IndexableDataInfinite(@NotNull IndexableData<T> data) {
        super(data.getPlotInfo());
        this.data = data;
    }

    @Override
    public int size() {
        return Integer.MAX_VALUE;
    }

    @Override
    public T get(int index) {
        if (index >= data.size() || index < 0) {
            return null;
        } else {
            return data.get(index);
        }
    }

}
