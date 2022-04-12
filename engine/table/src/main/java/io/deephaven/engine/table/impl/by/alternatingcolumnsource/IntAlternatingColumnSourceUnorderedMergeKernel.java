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

public class IntAlternatingColumnSourceUnorderedMergeKernel implements AlternatingColumnSourceUnorderedMergeKernel {
    public static IntAlternatingColumnSourceUnorderedMergeKernel INSTANCE = new IntAlternatingColumnSourceUnorderedMergeKernel();

    // static use only
    private IntAlternatingColumnSourceUnorderedMergeKernel() {}

    @Override
    public void mergeContext(WritableChunk<? super Values> dest, LongChunk<? extends RowKeys> outerKeys, Chunk<? super Values> src, int alternatePosition) {
        final WritableIntChunk<? super Values> destAsInt = dest.asWritableIntChunk();
        final IntChunk<? super Values> srcAsInt = src.asIntChunk();

        // now merge them together
        int mainPosition = 0;
        destAsInt.setSize(outerKeys.size());
        for (int ii = 0; ii < outerKeys.size(); ++ii) {
            final long outerKey = outerKeys.get(ii);
            if ((outerKey & ALTERNATE_SWITCH_MASK) == 0) {
                destAsInt.set(ii, srcAsInt.get(mainPosition++));
            } else {
                destAsInt.set(ii, srcAsInt.get(alternatePosition++));
            }
        }
    }
}
