/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSetInclusionKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
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

    @Override
    public void matchValues(Chunk<Values> values, WritableBooleanChunk matches) {
        matchValues(values.asByteChunk(), matches);
    }

    private void matchValues(ByteChunk<Values> values, WritableBooleanChunk matches) {
        for (int ii = 0; ii < values.size(); ++ii) {
            matches.set(ii, liveValues.contains(values.get(ii)) == inclusion);
        }
        matches.setSize(values.size());
    }
}