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

public class ShortAlternatingColumnSourceUnorderedMergeKernel implements AlternatingColumnSourceUnorderedMergeKernel {
    public static ShortAlternatingColumnSourceUnorderedMergeKernel INSTANCE = new ShortAlternatingColumnSourceUnorderedMergeKernel();

    // static use only
    private ShortAlternatingColumnSourceUnorderedMergeKernel() {}

    @Override
    public void mergeContext(WritableChunk<? super Values> dest, LongChunk<? extends RowKeys> outerKeys, Chunk<? super Values> src, int alternatePosition) {
        final WritableShortChunk<? super Values> destAsShort = dest.asWritableShortChunk();
        final ShortChunk<? super Values> srcAsShort = src.asShortChunk();

        // now merge them together
        int mainPosition = 0;
        destAsShort.setSize(outerKeys.size());
        for (int ii = 0; ii < outerKeys.size(); ++ii) {
            final long outerKey = outerKeys.get(ii);
            if ((outerKey & ALTERNATE_SWITCH_MASK) == 0) {
                destAsShort.set(ii, srcAsShort.get(mainPosition++));
            } else {
                destAsShort.set(ii, srcAsShort.get(alternatePosition++));
            }
        }
    }
}
