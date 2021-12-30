/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSsaChecker and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.ssa;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.util.hashing.ShortChunkEquals;
import io.deephaven.chunk.util.hashing.LongChunkEquals;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.util.ChunkUtils;

public class ShortReverseSsaChecker implements SsaChecker {
    static ShortReverseSsaChecker INSTANCE = new ShortReverseSsaChecker();

    private ShortReverseSsaChecker() {} // static use only

    @Override
    public void checkSsa(SegmentedSortedArray ssa, Chunk<? extends Values> valueChunk, LongChunk<? extends RowKeys> tableIndexChunk) {
        checkSsa((ShortReverseSegmentedSortedArray)ssa, valueChunk.asShortChunk(), tableIndexChunk);
    }

    static void checkSsa(ShortReverseSegmentedSortedArray ssa, ShortChunk<? extends Values> valueChunk, LongChunk<? extends RowKeys> tableIndexChunk) {
        ssa.validateInternal();

        //noinspection unchecked
        try (final WritableShortChunk<Values> resultChunk = (WritableShortChunk) ssa.asShortChunk();
             final WritableLongChunk<RowKeys> indexChunk = ssa.rowKeysChunk()) {

            Assert.eq(valueChunk.size(), "valueChunk.size()", resultChunk.size(), "resultChunk.size()");
            Assert.eq(tableIndexChunk.size(), "tableIndexChunk.size()", indexChunk.size(), "indexChunk.size()");

            if (!ShortChunkEquals.equalReduce(resultChunk, valueChunk)) {
                final StringBuilder messageBuilder = new StringBuilder("Values do not match:\n");
                messageBuilder.append("Result Values:\n").append(ChunkUtils.dumpChunk(resultChunk)).append("\n");
                messageBuilder.append("Table Values:\n").append(ChunkUtils.dumpChunk(valueChunk)).append("\n");

                for (int ii = 0; ii < resultChunk.size(); ++ii) {
                    if (!eq(resultChunk.get(ii), valueChunk.get(ii))) {
                        messageBuilder.append("First difference at ").append(ii).append(("\n"));
                        break;
                    }
                }

                throw new SsaCheckException(messageBuilder.toString());
            }
            if (!LongChunkEquals.equalReduce(indexChunk, tableIndexChunk)) {
                final StringBuilder messageBuilder = new StringBuilder("Values do not match:\n");
                messageBuilder.append("Result:\n").append(ChunkUtils.dumpChunk(resultChunk)).append("\n");
                messageBuilder.append("Values:\n").append(ChunkUtils.dumpChunk(valueChunk)).append("\n");

                messageBuilder.append("Result row keys:\n").append(ChunkUtils.dumpChunk(indexChunk)).append("\n");
                messageBuilder.append("Table row keys:\n").append(ChunkUtils.dumpChunk(tableIndexChunk)).append("\n");

                for (int ii = 0; ii < indexChunk.size(); ++ii) {
                    if (indexChunk.get(ii) != tableIndexChunk.get(ii)) {
                        messageBuilder.append("First difference at ").append(ii).append(("\n"));
                        break;
                    }
                }

                throw new SsaCheckException(messageBuilder.toString());
            }
        }
    }

    private static boolean eq(short lhs, short rhs) {
        // region equality function
        return lhs == rhs;
        // endregion equality function
    }
}
