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
 * A subset of a {@link ShortVector} according to an array of positions.
 */
public final class ShortSubVector extends ShortVector.Indirect {

    private static final long serialVersionUID = 1L;

    private final ShortVector innerVector;
    private final long[] positions;

    public ShortSubVector(@NotNull final ShortVector innerVector, @NotNull final long[] positions) {
        this.innerVector = innerVector;
        this.positions = positions;
    }

    @Override
    public short get(final long index) {
        if (index < 0 || index >= positions.length) {
            return QueryConstants.NULL_SHORT;
        }
        return innerVector.get(positions[(int) index]);
    }

    @Override
    public ShortVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return innerVector.subVectorByPositions(
                Vector.mapSelectedPositionRange(positions, fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public ShortVector subVectorByPositions(final long[] positions) {
        return innerVector.subVectorByPositions(Vector.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public long size() {
        return positions.length;
    }
}
