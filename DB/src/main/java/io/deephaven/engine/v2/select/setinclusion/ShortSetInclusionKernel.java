/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSetInclusionKernel and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.engine.v2.select.setinclusion;

import io.deephaven.engine.structures.chunk.*;
import io.deephaven.util.type.TypeUtils;
import gnu.trove.set.TShortSet;
import gnu.trove.set.hash.TShortHashSet;

import java.util.Collection;

public class ShortSetInclusionKernel implements SetInclusionKernel {
    private final TShortSet liveValues;
    private final boolean inclusion;

    ShortSetInclusionKernel(Collection<Object> liveValues, boolean inclusion) {
        this.liveValues = new TShortHashSet(liveValues.size());
        liveValues.forEach(x -> this.liveValues.add(TypeUtils.unbox((Short) x)));
        this.inclusion = inclusion;
    }

    @Override
    public void matchValues(Chunk<Attributes.Values> values, WritableBooleanChunk matches) {
        matchValues(values.asShortChunk(), matches);
    }

    private void matchValues(ShortChunk<Attributes.Values> values, WritableBooleanChunk matches) {
        for (int ii = 0; ii < values.size(); ++ii) {
            matches.set(ii, liveValues.contains(values.get(ii)) == inclusion);
        }
        matches.setSize(values.size());
    }
}