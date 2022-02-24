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

public class IntSubVector extends IntVector.Indirect {

    private static final long serialVersionUID = 1L;

    private final IntVector innerArray;
    private final long positions[];

    public IntSubVector(@NotNull final IntVector innerArray, @NotNull final long[] positions) {
        this.innerArray = innerArray;
        this.positions = positions;
    }

    @Override
    public int get(final long index) {
        if (index < 0 || index >= positions.length) {
            return QueryConstants.NULL_INT;
        }
        return innerArray.get(positions[LongSizedDataStructure.intSize("SubArray get", index)]);
    }

    @Override
    public IntVector subVector(final long fromIndex, final long toIndex) {
        return innerArray.subVectorByPositions(Vector.mapSelectedPositionRange(positions, fromIndex, toIndex));
    }

    @Override
    public IntVector subVectorByPositions(final long[] positions) {
        return innerArray.subVectorByPositions(Vector.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public int[] toArray() {
        final int[] result = new int[positions.length];
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
