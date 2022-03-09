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

public class ByteAlternatingColumnSourceUnorderedMergeKernel implements AlternatingColumnSourceUnorderedMergeKernel {
    public static ByteAlternatingColumnSourceUnorderedMergeKernel INSTANCE = new ByteAlternatingColumnSourceUnorderedMergeKernel();

    // static use only
    private ByteAlternatingColumnSourceUnorderedMergeKernel() {}

    @Override
    public void mergeContext(WritableChunk<? super Values> dest, LongChunk<? extends RowKeys> outerKeys, Chunk<? super Values> src, int alternatePosition) {
        final WritableByteChunk<? super Values> destAsByte = dest.asWritableByteChunk();
        final ByteChunk<? super Values> srcAsByte = src.asByteChunk();

        // now merge them together
        int mainPosition = 0;
        destAsByte.setSize(outerKeys.size());
        for (int ii = 0; ii < outerKeys.size(); ++ii) {
            final long outerKey = outerKeys.get(ii);
            if ((outerKey & ALTERNATE_SWITCH_MASK) == 0) {
                destAsByte.set(ii, srcAsByte.get(mainPosition++));
            } else {
                destAsByte.set(ii, srcAsByte.get(alternatePosition++));
            }
        }
    }
}
