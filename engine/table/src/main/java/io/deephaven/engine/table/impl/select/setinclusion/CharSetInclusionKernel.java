package io.deephaven.engine.table.impl.select.setinclusion;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;
import gnu.trove.set.TCharSet;
import gnu.trove.set.hash.TCharHashSet;

import java.util.Collection;

public class CharSetInclusionKernel implements SetInclusionKernel {
    private final TCharSet liveValues;
    private final boolean inclusion;

    CharSetInclusionKernel(Collection<Object> liveValues, boolean inclusion) {
        this.liveValues = new TCharHashSet(liveValues.size());
        liveValues.forEach(x -> this.liveValues.add(TypeUtils.unbox((Character) x)));
        this.inclusion = inclusion;
    }

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk matches) {
        matchValues(values.asCharChunk(), matches);
    }

    private void matchValues(CharChunk<Values> values, WritableBooleanChunk matches) {
        for (int ii = 0; ii < values.size(); ++ii) {
            matches.set(ii, liveValues.contains(values.get(ii)) == inclusion);
        }
        matches.setSize(values.size());
    }
}