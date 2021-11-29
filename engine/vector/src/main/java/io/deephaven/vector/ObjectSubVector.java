/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;

public class ObjectSubVector<T> extends ObjectVector.Indirect<T> {

    private static final long serialVersionUID = 1L;

    private final ObjectVector<T> innerArray;
    private final long positions[];

    public ObjectSubVector(@NotNull final ObjectVector<T> innerArray, @NotNull final long[] positions) {
        this.innerArray = innerArray;
        this.positions = positions;
    }

    @Override
    public T get(final long index) {
        if (index < 0 || index >= positions.length) {
            return null;
        }
        return innerArray.get(positions[LongSizedDataStructure.intSize("subarray array access", index)]);
    }

    @Override
    public ObjectVector<T> subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return innerArray.subVectorByPositions(
                Vector.mapSelectedPositionRange(positions, fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public ObjectVector<T> subVectorByPositions(final long[] positions) {
        return innerArray.subVectorByPositions(Vector.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public T[] toArray() {
        // noinspection unchecked
        final T[] result = (T[]) Array.newInstance(getComponentType(), positions.length);
        for (int ii = 0; ii < positions.length; ++ii) {
            result[ii] = get(ii);
        }
        return result;
    }

    @Override
    public long size() {
        return positions.length;
    }

    @Override
    public Class<T> getComponentType() {
        return innerArray.getComponentType();
    }

    @Override
    public boolean isEmpty() {
        return positions.length == 0;
    }

    @Override
    public ObjectVector<T> getDirect() {
        return new ObjectVectorDirect<>(toArray());
    }
}
