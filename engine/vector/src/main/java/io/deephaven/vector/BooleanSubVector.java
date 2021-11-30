/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.vector;

import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

@Deprecated
public class BooleanSubVector extends BooleanVector.Indirect {

    private static final long serialVersionUID = 1L;

    private final BooleanVector innerArray;
    private final long positions[];

    public BooleanSubVector(@NotNull final BooleanVector innerArray, @NotNull final long[] positions) {
        this.innerArray = innerArray;
        this.positions = positions;
    }

    @Override
    public Boolean get(final long index) {
        if (index < 0 || index >= positions.length) {
            return null;
        }
        return innerArray.get(positions[LongSizedDataStructure.intSize("BooleanSubVector get",  index)]);
    }

    @Override
    public BooleanVector subVector(final long fromIndex, final long toIndex) {
        return innerArray.subVectorByPositions(Vector.mapSelectedPositionRange(positions, fromIndex, toIndex));
    }

    @Override
    public BooleanVector subVectorByPositions(final long[] positions) {
        return innerArray.subVectorByPositions(Vector.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public Boolean[] toArray() {
        final Boolean[] result = new Boolean[positions.length];
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

    @Override
    public BooleanVectorDirect getDirect() {
        return new BooleanVectorDirect(toArray());
    }
}
