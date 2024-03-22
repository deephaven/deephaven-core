//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSetInclusionKernel and run "./gradlew replicateSetInclusionKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select.setinclusion;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;
import gnu.trove.set.TDoubleSet;
import gnu.trove.set.hash.TDoubleHashSet;

import java.util.Collection;

public class DoubleSetInclusionKernel implements SetInclusionKernel {
    private final TDoubleSet liveValues;
    private final boolean inclusion;

    DoubleSetInclusionKernel(Collection<Object> liveValues, boolean inclusion) {
        this.liveValues = new TDoubleHashSet(liveValues.size());
        liveValues.forEach(x -> this.liveValues.add(TypeUtils.unbox((Double) x)));
        this.inclusion = inclusion;
    }

    DoubleSetInclusionKernel(boolean inclusion) {
        this.liveValues = new TDoubleHashSet();
        this.inclusion = inclusion;
    }

    @Override
    public void addItem(Object key) {
        liveValues.add(TypeUtils.unbox((Double) key));
    }

    @Override
    public void removeItem(Object key) {
        liveValues.remove(TypeUtils.unbox((Double) key));
    }

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk<?> matches) {
        matchValues(values.asDoubleChunk(), matches, inclusion);
    }


    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk<?> matches, boolean inclusionOverride) {
        matchValues(values.asDoubleChunk(), matches, inclusionOverride);
    }

    private void matchValues(
            DoubleChunk<Values> values,
            WritableBooleanChunk<?> matches,
            boolean inclusionToUse) {
        for (int ii = 0; ii < values.size(); ++ii) {
            matches.set(ii, liveValues.contains(values.get(ii)) == inclusionToUse);
        }
        matches.setSize(values.size());
    }
}
