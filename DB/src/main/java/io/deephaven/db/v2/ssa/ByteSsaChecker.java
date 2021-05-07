/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharSsaChecker and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
package io.deephaven.db.v2.ssa;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.hashing.ByteChunkEquals;
import io.deephaven.db.v2.hashing.LongChunkEquals;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.ByteChunk;
import io.deephaven.db.v2.sources.chunk.Chunk;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.utils.ChunkUtils;

public class ByteSsaChecker implements SsaChecker {
    static ByteSsaChecker INSTANCE = new ByteSsaChecker();

    private ByteSsaChecker() {} // static use only

    @Override
    public void checkSsa(SegmentedSortedArray ssa, Chunk<? extends Values> valueChunk, LongChunk<? extends KeyIndices> tableIndexChunk) {
        checkSsa((ByteSegmentedSortedArray)ssa, valueChunk.asByteChunk(), tableIndexChunk);
    }

    static void checkSsa(ByteSegmentedSortedArray ssa, ByteChunk<? extends Values> valueChunk, LongChunk<? extends KeyIndices> tableIndexChunk) {
        ssa.validateInternal();

        //noinspection unchecked
        final ByteChunk<Values> resultChunk = (ByteChunk) ssa.asByteChunk();
        final LongChunk<KeyIndices> indexChunk = ssa.keyIndicesChunk();

        Assert.eq(valueChunk.size(), "valueChunk.size()", resultChunk.size(), "resultChunk.size()");
        Assert.eq(tableIndexChunk.size(), "tableIndexChunk.size()", indexChunk.size(), "indexChunk.size()");

        if (!ByteChunkEquals.equalReduce(resultChunk, valueChunk)) {
            final StringBuilder messageBuilder = new StringBuilder("Values do not match:\n");
            messageBuilder.append("Result Values:\n").append(ChunkUtils.dumpChunk(resultChunk)).append("\n");
            messageBuilder.append("Table Values:\n").append(ChunkUtils.dumpChunk(valueChunk)).append("\n");;

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
            messageBuilder.append("Values:\n").append(ChunkUtils.dumpChunk(valueChunk)).append("\n");;

            messageBuilder.append("Result Index:\n").append(ChunkUtils.dumpChunk(indexChunk)).append("\n");
            messageBuilder.append("Table Index:\n").append(ChunkUtils.dumpChunk(tableIndexChunk)).append("\n");;

            for (int ii = 0; ii < indexChunk.size(); ++ii) {
                if (indexChunk.get(ii) != tableIndexChunk.get(ii)) {
                    messageBuilder.append("First difference at ").append(ii).append(("\n"));
                    break;
                }
            }

            throw new SsaCheckException(messageBuilder.toString());
        }
    }

    private static boolean eq(byte lhs, byte rhs) {
        // region equality function
        return lhs == rhs;
        // endregion equality function
    }
}
