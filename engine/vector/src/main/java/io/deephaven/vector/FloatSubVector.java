/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSubVector and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.vector;

import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * A subset of a {@link FloatVector} according to an array of positions.
 */
public class FloatSubVector extends FloatVector.Indirect {

    private final FloatVector innerVector;
    private final long[] positions;

    public FloatSubVector(@NotNull final FloatVector innerVector, @NotNull final long[] positions) {
        this.innerVector = innerVector;
        this.positions = positions;
    }

    @Override
    public float get(final long index) {
        if (index < 0 || index >= positions.length) {
            return QueryConstants.NULL_FLOAT;
        }
        return innerVector.get(positions[LongSizedDataStructure.intSize("FloatSubVector.get", index)]);
    }

    @Override
    public FloatVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return innerVector.subVectorByPositions(
                Vector.mapSelectedPositionRange(positions, fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public FloatVector subVectorByPositions(final long[] positions) {
        return innerVector.subVectorByPositions(Vector.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public float[] toArray() {
        final float[] result = new float[positions.length];
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
    public boolean isEmpty() {
        return positions.length == 0;
    }
}
