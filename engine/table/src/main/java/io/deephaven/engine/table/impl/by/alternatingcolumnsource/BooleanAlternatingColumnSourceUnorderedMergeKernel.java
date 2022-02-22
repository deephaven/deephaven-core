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

public class BooleanAlternatingColumnSourceUnorderedMergeKernel implements AlternatingColumnSourceUnorderedMergeKernel {
    public static BooleanAlternatingColumnSourceUnorderedMergeKernel INSTANCE = new BooleanAlternatingColumnSourceUnorderedMergeKernel();

    // static use only
    private BooleanAlternatingColumnSourceUnorderedMergeKernel() {}

    @Override
    public void mergeContext(WritableChunk<? super Values> dest, LongChunk<? extends RowKeys> outerKeys, Chunk<? super Values> src, int alternatePosition) {
        final WritableBooleanChunk<? super Values> destAsBoolean = dest.asWritableBooleanChunk();
        final BooleanChunk<? super Values> srcAsBoolean = src.asBooleanChunk();

        // now merge them together
        int mainPosition = 0;
        destAsBoolean.setSize(outerKeys.size());
        for (int ii = 0; ii < outerKeys.size(); ++ii) {
            final long outerKey = outerKeys.get(ii);
            if ((outerKey & ALTERNATE_SWITCH_MASK) == 0) {
                destAsBoolean.set(ii, srcAsBoolean.get(mainPosition++));
            } else {
                destAsBoolean.set(ii, srcAsBoolean.get(alternatePosition++));
            }
        }
    }
}
