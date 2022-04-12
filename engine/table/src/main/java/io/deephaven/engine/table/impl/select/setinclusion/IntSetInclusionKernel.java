/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSetInclusionKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
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

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk matches) {
        matchValues(values.asIntChunk(), matches);
    }

    private void matchValues(IntChunk<Values> values, WritableBooleanChunk matches) {
        for (int ii = 0; ii < values.size(); ++ii) {
            matches.set(ii, liveValues.contains(values.get(ii)) == inclusion);
        }
        matches.setSize(values.size());
    }
}