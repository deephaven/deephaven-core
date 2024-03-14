//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.vector;

import org.jetbrains.annotations.NotNull;

/**
 * A subset of a {@link ObjectVector} according to an array of positions.
 */
public class ObjectSubVector<COMPONENT_TYPE> extends ObjectVector.Indirect<COMPONENT_TYPE> {

    private static final long serialVersionUID = 1L;

    private final ObjectVector<COMPONENT_TYPE> innerVector;
    private final long[] positions;

    public ObjectSubVector(@NotNull final ObjectVector<COMPONENT_TYPE> innerVector, @NotNull final long[] positions) {
        this.innerVector = innerVector;
        this.positions = positions;
    }

    @Override
    public COMPONENT_TYPE get(final long index) {
        if (index < 0 || index >= positions.length) {
            return null;
        }
        return innerVector.get(positions[(int) index]);
    }

    @Override
    public ObjectVector<COMPONENT_TYPE> subVector(final long fromIndexInclusive, final long toIndexExclusive) {
        return innerVector.subVectorByPositions(
                Vector.mapSelectedPositionRange(positions, fromIndexInclusive, toIndexExclusive));
    }

    @Override
    public ObjectVector<COMPONENT_TYPE> subVectorByPositions(final long[] positions) {
        return innerVector.subVectorByPositions(Vector.mapSelectedPositions(this.positions, positions));
    }

    @Override
    public long size() {
        return positions.length;
    }

    @Override
    public Class<COMPONENT_TYPE> getComponentType() {
        return innerVector.getComponentType();
    }
}
