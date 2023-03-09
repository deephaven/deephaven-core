/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.vector;

import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * A subset of a {@link CharVector} according to an array of positions.
 */
public class CharSubVector extends CharVector.Indirect {

    private final CharVector innerVector;
    private final long[] positions;

    public CharSubVector(@NotNull final CharVector innerVector, @NotNull final long[] positions) {
        this.innerVector = innerVector;
        this.positions = positions;
    }

    @Override
    public char get(final long index) {
        if (index < 0 || index >= positions.length) {
            return QueryConstants.NULL_CHAR;
        }
        return innerVector.get(positions[LongSizedDataStructure.intSize("CharSubVector.get", index)]);
    }

    @Override
    public CharVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return innerVector.subVectorByPositions(
                Vector.mapSelectedPositionRange(positions, fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public CharVector subVectorByPositions(final long[] positions) {
        return innerVector.subVectorByPositions(Vector.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public char[] toArray() {
        final char[] result = new char[positions.length];
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
