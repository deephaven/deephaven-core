/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSubVector and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.vector;

import io.deephaven.engine.util.LongSizedDataStructure;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

public class ByteSubVector extends ByteVector.Indirect {

    private static final long serialVersionUID = 1L;

    private final ByteVector innerArray;
    private final long positions[];

    public ByteSubVector(@NotNull final ByteVector innerArray, @NotNull final long[] positions) {
        this.innerArray = innerArray;
        this.positions = positions;
    }

    @Override
    public byte get(final long index) {
        if (index < 0 || index >= positions.length) {
            return QueryConstants.NULL_BYTE;
        }
        return innerArray.get(positions[LongSizedDataStructure.intSize("SubArray get", index)]);
    }

    @Override
    public ByteVector subVector(final long fromIndex, final long toIndex) {
        return innerArray.subVectorByPositions(mapSelectedPositionRange(positions, fromIndex, toIndex));
    }

    @Override
    public ByteVector subVectorByPositions(final long[] positions) {
        return innerArray.subVectorByPositions(mapSelectedPositions(this.positions, positions));
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
