/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.vector;

import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Array;

/**
 * A subset of a {@link ObjectVector} according to an array of positions.
 */
public class ObjectSubVector<COMPONENT_TYPE> extends ObjectVector.Indirect<COMPONENT_TYPE> {

    private final ObjectVector<COMPONENT_TYPE> innerArray;
    private final long[] positions;

    public ObjectSubVector(@NotNull final ObjectVector<COMPONENT_TYPE> innerArray, @NotNull final long[] positions) {
        this.innerArray = innerArray;
        this.positions = positions;
    }

    @Override
    public COMPONENT_TYPE get(final long index) {
        if (index < 0 || index >= positions.length) {
            return null;
        }
        return innerArray.get(positions[LongSizedDataStructure.intSize("ObjectSubVector.get", index)]);
    }

    @Override
    public ObjectVector<COMPONENT_TYPE> subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return innerArray.subVectorByPositions(
                Vector.mapSelectedPositionRange(positions, fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public ObjectVector<COMPONENT_TYPE> subVectorByPositions(final long[] positions) {
        return innerArray.subVectorByPositions(Vector.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public COMPONENT_TYPE[] toArray() {
        // noinspection unchecked
        final COMPONENT_TYPE[] result = (COMPONENT_TYPE[]) Array.newInstance(getComponentType(), positions.length);
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
    public Class<COMPONENT_TYPE> getComponentType() {
        return innerArray.getComponentType();
    }

    @Override
    public boolean isEmpty() {
        return positions.length == 0;
    }
}
