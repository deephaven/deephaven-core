package io.deephaven.engine.table.impl.by;

import io.deephaven.base.MathUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.SafeCloseable;

/**
 * Finds runs of the same slot, and fills in the chunkPositions, runStarts, and runLengths arrays. The slots are not in
 * order; like slots are simply grouped together such that each slot is only represented in the chunk one time.
 */
public class HashedRunFinder {
    private static final int UNUSED_HASH_TABLE_VALUE = -1;

    public static class HashedRunContext implements SafeCloseable {
        final int tableSize;
        final int tableMask;

        // the hash table is [outputPosition, position, overflow]
        // the overflow is only [position, overflow]
        final WritableIntChunk<?> table;
        final WritableIntChunk<?> overflow;

        public HashedRunContext(int size) {
            if (size == 0) {
                tableSize = 0;
                tableMask = 0;
                table = null;
                overflow = null;
                return;
            }
            // load factor of half, rounded up
            tableSize = 1 << MathUtil.ceilLog2(size * 2);
            tableMask = tableSize - 1;
            table = WritableIntChunk.makeWritableChunk(tableSize * 3);
            overflow = WritableIntChunk.makeWritableChunk((size - 1) * 2);
            table.fillWithValue(0, table.size(), UNUSED_HASH_TABLE_VALUE);
        }

        @Override
        public void close() {
            if (table == null) {
                return;
            }
            table.close();
            overflow.close();
        }
    }

    public static void findRunsHashed(
            HashedRunContext context,
            WritableIntChunk<ChunkPositions> runStarts,
            WritableIntChunk<ChunkLengths> runLengths,
            WritableIntChunk<ChunkPositions> chunkPositions,
            WritableIntChunk<RowKeys> outputPositions) {
        final int size = outputPositions.size();

        Assert.gtZero(size, "size");

        int overflowPointer = 0;

        int minSlot = context.tableSize;
        int maxSlot = 0;

        for (int chunkPosition = 0; chunkPosition < size; ++chunkPosition) {
            final int outputPosition = outputPositions.get(chunkPosition);
            int hashSlot = outputPosition & context.tableMask;

            do {
                final int baseSlot = hashSlot * 3;
                if (context.table.get(baseSlot) == UNUSED_HASH_TABLE_VALUE) {
                    // insert it here
                    context.table.set(baseSlot, outputPosition);
                    context.table.set(baseSlot + 1, chunkPosition);
                    context.table.set(baseSlot + 2, UNUSED_HASH_TABLE_VALUE);
                    minSlot = Math.min(hashSlot, minSlot);
                    maxSlot = Math.max(hashSlot, maxSlot);
                    break;
                } else if (context.table.get(baseSlot) == outputPosition) {
                    context.overflow.set(overflowPointer, chunkPosition);
                    context.overflow.set(overflowPointer + 1, context.table.get(baseSlot + 2));
                    context.table.set(baseSlot + 2, overflowPointer);
                    overflowPointer += 2;
                    break;
                } else {
                    // linear probe
                    hashSlot = (hashSlot + 1) & context.tableMask;
                }
            } while (true);
        }

        // now iterate the table into the outputPositions/chunkPositions chunks
        int chunkPointer = 0;
        chunkPositions.setSize(outputPositions.size());
        runLengths.setSize(0);
        runStarts.setSize(0);
        for (int hashSlot = minSlot; hashSlot <= maxSlot; ++hashSlot) {
            final int baseSlot = hashSlot * 3;
            final int outputPosition;
            if ((outputPosition = context.table.get(baseSlot)) != UNUSED_HASH_TABLE_VALUE) {
                // zero it out to be ready for the next usage
                context.table.set(baseSlot, UNUSED_HASH_TABLE_VALUE);

                runStarts.add(chunkPointer);
                outputPositions.set(chunkPointer, outputPosition);
                chunkPositions.set(chunkPointer++, context.table.get(baseSlot + 1));

                int len = 1;
                int overflowLocation = context.table.get(baseSlot + 2);
                // now chase overflow
                while (overflowLocation != UNUSED_HASH_TABLE_VALUE) {
                    outputPositions.set(chunkPointer, outputPosition);
                    chunkPositions.set(chunkPointer++, context.overflow.get(overflowLocation));
                    overflowLocation = context.overflow.get(overflowLocation + 1);
                    len++;
                }

                // and reverse the overflow outputPositions so they appear in order
                final int reverseLen = len - 1;
                final int beginReverse = chunkPointer - reverseLen;
                final int endReverse = chunkPointer - 1;
                for (int ii = 0; ii < reverseLen / 2; ++ii) {
                    final int rr = chunkPositions.get(beginReverse + ii);
                    chunkPositions.set(beginReverse + ii, chunkPositions.get(endReverse - ii));
                    chunkPositions.set(endReverse - ii, rr);
                }

                runLengths.add(len);
            }
        }
        Assert.eq(chunkPointer, "chunkPointer", outputPositions.size(), "outputPositions.size()");
    }
}
