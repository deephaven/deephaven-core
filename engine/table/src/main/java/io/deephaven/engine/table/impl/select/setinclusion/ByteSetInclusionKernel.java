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
import gnu.trove.set.TByteSet;
import gnu.trove.set.hash.TByteHashSet;

import java.util.Collection;

public class ByteSetInclusionKernel implements SetInclusionKernel {
    private final TByteSet liveValues;
    private final boolean inclusion;

    ByteSetInclusionKernel(Collection<Object> liveValues, boolean inclusion) {
        this.liveValues = new TByteHashSet(liveValues.size());
        liveValues.forEach(x -> this.liveValues.add(TypeUtils.unbox((Byte) x)));
        this.inclusion = inclusion;
    }

    ByteSetInclusionKernel(boolean inclusion) {
        this.liveValues = new TByteHashSet();
        this.inclusion = inclusion;
    }

    @Override
    public void addItem(Object key) {
        liveValues.add(TypeUtils.unbox((Byte) key));
    }

    @Override
    public void removeItem(Object key) {
        liveValues.remove(TypeUtils.unbox((Byte) key));
    }

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk<?> matches) {
        matchValues(values.asByteChunk(), matches, inclusion);
    }


    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk<?> matches, boolean inclusionOverride) {
        matchValues(values.asByteChunk(), matches, inclusionOverride);
    }

    private void matchValues(
            ByteChunk<Values> values,
            WritableBooleanChunk<?> matches,
            boolean inclusionToUse) {
        for (int ii = 0; ii < values.size(); ++ii) {
            matches.set(ii, liveValues.contains(values.get(ii)) == inclusionToUse);
        }
        matches.setSize(values.size());
    }
}
