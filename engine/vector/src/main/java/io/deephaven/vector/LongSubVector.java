/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSubVector and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

public class LongSubVector extends LongVector.Indirect {

    private static final long serialVersionUID = 1L;

    private final LongVector innerArray;
    private final long positions[];

    public LongSubVector(@NotNull final LongVector innerArray, @NotNull final long[] positions) {
        this.innerArray = innerArray;
        this.positions = positions;
    }

    @Override
    public long get(final long index) {
        if (index < 0 || index >= positions.length) {
            return QueryConstants.NULL_LONG;
        }
        return innerArray.get(positions[LongSizedDataStructure.intSize("SubArray get", index)]);
    }

    @Override
    public LongVector subVector(final long fromIndex, final long toIndex) {
        return innerArray.subVectorByPositions(Vector.mapSelectedPositionRange(positions, fromIndex, toIndex));
    }

    @Override
    public LongVector subVectorByPositions(final long[] positions) {
        return innerArray.subVectorByPositions(Vector.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public long[] toArray() {
        final long[] result = new long[positions.length];
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
