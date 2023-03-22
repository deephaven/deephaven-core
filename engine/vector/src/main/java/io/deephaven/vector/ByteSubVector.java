/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSubVector and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.vector;

import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

/**
 * A subset of a {@link ByteVector} according to an array of positions.
 */
public final class ByteSubVector extends ByteVector.Indirect {

    private static final long serialVersionUID = 1L;

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
        return innerVector.get(positions[(int) index]);
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
    public long size() {
        return positions.length;
    }
}
