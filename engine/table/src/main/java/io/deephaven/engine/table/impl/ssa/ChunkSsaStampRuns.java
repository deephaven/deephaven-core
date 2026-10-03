//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.ssa;

import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

/**
 * Bulk modified-row reporting for a {@link ChunkSsaStamp}, which restamps or reports a run of consecutive positions of
 * the left keys chunk for each right row.
 */
final class ChunkSsaStampRuns {
    private ChunkSsaStampRuns() {}

    /**
     * Add the left keys at positions {@code [start, end)} to {@code modifiedBuilder}.
     */
    static void addModified(final LongChunk<RowKeys> leftStampKeys, final int start, final int end,
            final RowSetBuilderRandom modifiedBuilder) {
        if (end - start == 0) {
            return;
        }
        // the left keys are ordered by stamp, which usually orders them by row key as well
        for (int ii = start + 1; ii < end; ++ii) {
            if (leftStampKeys.get(ii) <= leftStampKeys.get(ii - 1)) {
                for (int jj = start; jj < end; ++jj) {
                    modifiedBuilder.addKey(leftStampKeys.get(jj));
                }
                return;
            }
        }
        modifiedBuilder.addOrderedRowKeysChunk(LongChunk.downcast(leftStampKeys), start, end - start);
    }
}
