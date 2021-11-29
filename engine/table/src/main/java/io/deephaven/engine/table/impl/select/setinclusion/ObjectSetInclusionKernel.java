package io.deephaven.engine.table.impl.select.setinclusion;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.Values;

import java.util.Collection;

public class ObjectSetInclusionKernel implements SetInclusionKernel {
    private final Collection<Object> liveValues;
    private final boolean inclusion;

    public ObjectSetInclusionKernel(Collection<Object> liveValues, boolean inclusion) {
        this.liveValues = liveValues;
        this.inclusion = inclusion;
    }

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk matches) {
        matchValues(values.asObjectChunk(), matches);
    }

    private void matchValues(ObjectChunk<?, Values> values, WritableBooleanChunk matches) {
        for (int ii = 0; ii < values.size(); ++ii) {
            matches.set(ii, liveValues.contains(values.get(ii)) == inclusion);
        }
        matches.setSize(values.size());
    }
}