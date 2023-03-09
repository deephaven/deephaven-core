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
 * A subset of a {@link ByteVector} according to an array of positions.
 */
public class ByteSubVector extends ByteVector.Indirect {

    private final ByteVector innerVector;
    private final long[] positions;

    public ByteSubVector(@NotNull final ByteVector innerVector, @NotNull final long[] positions) {
        this.innerVector = innerVector;
        this.positions = positions;
    }

    @Override
    public byte get(final long index) {
        if (index < 0 || index >= positions.length) {
            return QueryConstants.NULL_BYTE;
        }
        return innerVector.get(positions[LongSizedDataStructure.intSize("ByteSubVector.get", index)]);
    }

    @Override
    public ByteVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return innerVector.subVectorByPositions(
                Vector.mapSelectedPositionRange(positions, fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public ByteVector subVectorByPositions(final long[] positions) {
        return innerVector.subVectorByPositions(Vector.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public byte[] toArray() {
        final byte[] result = new byte[positions.length];
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
