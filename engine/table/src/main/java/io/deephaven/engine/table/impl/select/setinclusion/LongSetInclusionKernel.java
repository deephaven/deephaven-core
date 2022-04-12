/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSetInclusionKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.select.setinclusion;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import java.util.Collection;

public class LongSetInclusionKernel implements SetInclusionKernel {
    private final TLongSet liveValues;
    private final boolean inclusion;

    LongSetInclusionKernel(Collection<Object> liveValues, boolean inclusion) {
        this.liveValues = new TLongHashSet(liveValues.size());
        liveValues.forEach(x -> this.liveValues.add(TypeUtils.unbox((Long) x)));
        this.inclusion = inclusion;
    }

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk matches) {
        matchValues(values.asLongChunk(), matches);
    }

    private void matchValues(LongChunk<Values> values, WritableBooleanChunk matches) {
        for (int ii = 0; ii < values.size(); ++ii) {
            matches.set(ii, liveValues.contains(values.get(ii)) == inclusion);
        }
        matches.setSize(values.size());
    }
}