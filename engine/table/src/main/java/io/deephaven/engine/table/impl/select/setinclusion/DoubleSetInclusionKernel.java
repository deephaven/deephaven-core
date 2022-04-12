/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSetInclusionKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
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

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk matches) {
        matchValues(values.asDoubleChunk(), matches);
    }

    private void matchValues(DoubleChunk<Values> values, WritableBooleanChunk matches) {
        for (int ii = 0; ii < values.size(); ++ii) {
            matches.set(ii, liveValues.contains(values.get(ii)) == inclusion);
        }
        matches.setSize(values.size());
    }
}