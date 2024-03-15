//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSubVector and run "./gradlew replicateVectors" to regenerate
//
// @formatter:off
package io.deephaven.vector;

import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * A subset of a {@link LongVector} according to an array of positions.
 */
public final class LongSubVector extends LongVector.Indirect {

    private static final long serialVersionUID = 1L;

    private final LongVector innerVector;
    private final long[] positions;

    public LongSubVector(@NotNull final LongVector innerVector, @NotNull final long[] positions) {
        this.innerVector = innerVector;
        this.positions = positions;
    }

    @Override
    public long get(final long index) {
        if (index < 0 || index >= positions.length) {
            return QueryConstants.NULL_LONG;
        }
        return innerVector.get(positions[(int) index]);
    }

    @Override
    public LongVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return innerVector.subVectorByPositions(
                Vector.mapSelectedPositionRange(positions, fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public LongVector subVectorByPositions(final long[] positions) {
        return innerVector.subVectorByPositions(Vector.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public long size() {
        return positions.length;
    }
}
