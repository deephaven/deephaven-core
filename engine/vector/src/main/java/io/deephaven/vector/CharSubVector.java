//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.vector;

import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * A subset of a {@link CharVector} according to an array of positions.
 */
public final class CharSubVector extends CharVector.Indirect {

    private static final long serialVersionUID = 1L;

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
        return innerVector.get(positions[(int) index]);
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
    public long size() {
        return positions.length;
    }
}
