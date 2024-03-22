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
import gnu.trove.set.TIntSet;
import gnu.trove.set.hash.TIntHashSet;

import java.util.Collection;

public class IntSetInclusionKernel implements SetInclusionKernel {
    private final TIntSet liveValues;
    private final boolean inclusion;

    IntSetInclusionKernel(Collection<Object> liveValues, boolean inclusion) {
        this.liveValues = new TIntHashSet(liveValues.size());
        liveValues.forEach(x -> this.liveValues.add(TypeUtils.unbox((Integer) x)));
        this.inclusion = inclusion;
    }

    IntSetInclusionKernel(boolean inclusion) {
        this.liveValues = new TIntHashSet();
        this.inclusion = inclusion;
    }

    @Override
    public void addItem(Object key) {
        liveValues.add(TypeUtils.unbox((Integer) key));
    }

    @Override
    public void removeItem(Object key) {
        liveValues.remove(TypeUtils.unbox((Integer) key));
    }

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk<?> matches) {
        matchValues(values.asIntChunk(), matches, inclusion);
    }


    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk<?> matches, boolean inclusionOverride) {
        matchValues(values.asIntChunk(), matches, inclusionOverride);
    }

    private void matchValues(
            IntChunk<Values> values,
            WritableBooleanChunk<?> matches,
            boolean inclusionToUse) {
        for (int ii = 0; ii < values.size(); ++ii) {
            matches.set(ii, liveValues.contains(values.get(ii)) == inclusionToUse);
        }
        matches.setSize(values.size());
    }
}
