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

public class DoubleAlternatingColumnSourceUnorderedMergeKernel implements AlternatingColumnSourceUnorderedMergeKernel {
    public static DoubleAlternatingColumnSourceUnorderedMergeKernel INSTANCE = new DoubleAlternatingColumnSourceUnorderedMergeKernel();

    // static use only
    private DoubleAlternatingColumnSourceUnorderedMergeKernel() {}

    @Override
    public void mergeContext(WritableChunk<? super Values> dest, LongChunk<? extends RowKeys> outerKeys, Chunk<? super Values> src, int alternatePosition) {
        final WritableDoubleChunk<? super Values> destAsDouble = dest.asWritableDoubleChunk();
        final DoubleChunk<? super Values> srcAsDouble = src.asDoubleChunk();

        // now merge them together
        int mainPosition = 0;
        destAsDouble.setSize(outerKeys.size());
        for (int ii = 0; ii < outerKeys.size(); ++ii) {
            final long outerKey = outerKeys.get(ii);
            if ((outerKey & ALTERNATE_SWITCH_MASK) == 0) {
                destAsDouble.set(ii, srcAsDouble.get(mainPosition++));
            } else {
                destAsDouble.set(ii, srcAsDouble.get(alternatePosition++));
            }
        }
    }
}
