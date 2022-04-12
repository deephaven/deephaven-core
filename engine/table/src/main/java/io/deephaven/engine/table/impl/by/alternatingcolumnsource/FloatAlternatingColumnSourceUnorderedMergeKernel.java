/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharAlternatingColumnSourceUnorderedMergeKernel and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.by.alternatingcolumnsource;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import static io.deephaven.engine.table.impl.by.alternatingcolumnsource.AlternatingColumnSource.ALTERNATE_SWITCH_MASK;

public class FloatAlternatingColumnSourceUnorderedMergeKernel implements AlternatingColumnSourceUnorderedMergeKernel {
    public static FloatAlternatingColumnSourceUnorderedMergeKernel INSTANCE = new FloatAlternatingColumnSourceUnorderedMergeKernel();

    // static use only
    private FloatAlternatingColumnSourceUnorderedMergeKernel() {}

    @Override
    public void mergeContext(WritableChunk<? super Values> dest, LongChunk<? extends RowKeys> outerKeys, Chunk<? super Values> src, int alternatePosition) {
        final WritableFloatChunk<? super Values> destAsFloat = dest.asWritableFloatChunk();
        final FloatChunk<? super Values> srcAsFloat = src.asFloatChunk();

        // now merge them together
        int mainPosition = 0;
        destAsFloat.setSize(outerKeys.size());
        for (int ii = 0; ii < outerKeys.size(); ++ii) {
            final long outerKey = outerKeys.get(ii);
            if ((outerKey & ALTERNATE_SWITCH_MASK) == 0) {
                destAsFloat.set(ii, srcAsFloat.get(mainPosition++));
            } else {
                destAsFloat.set(ii, srcAsFloat.get(alternatePosition++));
            }
        }
    }
}
