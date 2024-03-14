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
 * A subset of a {@link DoubleVector} according to an array of positions.
 */
public final class DoubleSubVector extends DoubleVector.Indirect {

    private static final long serialVersionUID = 1L;

    private final DoubleVector innerVector;
    private final long[] positions;

    public DoubleSubVector(@NotNull final DoubleVector innerVector, @NotNull final long[] positions) {
        this.innerVector = innerVector;
        this.positions = positions;
    }

    @Override
    public double get(final long index) {
        if (index < 0 || index >= positions.length) {
            return QueryConstants.NULL_DOUBLE;
        }
        return innerVector.get(positions[(int) index]);
    }

    @Override
    public DoubleVector subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return innerVector.subVectorByPositions(
                Vector.mapSelectedPositionRange(positions, fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public DoubleVector subVectorByPositions(final long[] positions) {
        return innerVector.subVectorByPositions(Vector.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public long size() {
        return positions.length;
    }
}
