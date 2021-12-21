package io.deephaven.engine.table.impl.by;

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
    public static class HashedRunContext implements SafeCloseable {
        final int tableSize;
        final int tableMask;

        // the hash table is [slot, position, overflow]
        // the overflow is only [position, overflow]
        final WritableIntChunk<?> table;
        final WritableIntChunk<?> overflow;

        public HashedRunContext(int size) {
            tableSize = Integer.highestOneBit(size * 2);
            tableMask = tableSize - 1;
            table = WritableIntChunk.makeWritableChunk(tableSize * 3);
            overflow = WritableIntChunk.makeWritableChunk((size - 1) * 2);
            table.fillWithValue(0, table.size(), 0);
        }

        @Override
        public void close() {
            table.close();
            overflow.close();
        }
    }

    public static void findRunsHashed(
            HashedRunContext context,
            WritableIntChunk<ChunkPositions> runStarts,
            WritableIntChunk<ChunkLengths> runLengths,
            WritableIntChunk<ChunkPositions> chunkPositions,
            WritableIntChunk<RowKeys> slots) {
        final int size = slots.size();

        Assert.gtZero(size, "size");

        int overflowPointer = 0;

        // load factor of two, rounded up
        int minSlot = context.tableSize;
        int maxSlot = 0;

        for (int chunkPosition = 0; chunkPosition < size; ++chunkPosition) {
            final int slot = slots.get(chunkPosition);
            int hashSlot = slot & context.tableMask;

            do {
                final int adjustedSlot = slot + 1;
                final int baseSlot = hashSlot * 3;
                if (context.table.get(baseSlot) == 0) {
                    // insert it here
                    context.table.set(baseSlot, adjustedSlot);
                    context.table.set(baseSlot + 1, chunkPosition);
                    context.table.set(baseSlot + 2, 0);
                    minSlot = Math.min(hashSlot, minSlot);
                    maxSlot = Math.max(hashSlot, maxSlot);
                    break;
                } else if (context.table.get(baseSlot) == adjustedSlot) {
                    context.overflow.set(overflowPointer, chunkPosition);
                    context.overflow.set(overflowPointer + 1, context.table.get(baseSlot + 2));
                    context.table.set(baseSlot + 2, overflowPointer + 1);
                    overflowPointer += 2;
                    break;
                } else {
                    // linear probe
                    hashSlot = (hashSlot + 1) & context.tableMask;
                }
            } while (true);
        }

        // now iterate the table into the slots/chunkPositions chunks
        int chunkPointer = 0;
        chunkPositions.setSize(slots.size());
        runLengths.setSize(0);
        runStarts.setSize(0);
        for (int hashSlot = minSlot; hashSlot <= maxSlot; ++hashSlot) {
            final int baseSlot = hashSlot * 3;
            final int adjustedSlot;
            if ((adjustedSlot = context.table.get(baseSlot)) != 0) {
                final int unAdjustedSlot = adjustedSlot - 1;
                // zero it out to be ready for the next usage
                context.table.set(baseSlot, 0);

                runStarts.add(chunkPointer);
                slots.set(chunkPointer, unAdjustedSlot);
                chunkPositions.set(chunkPointer++, context.table.get(baseSlot + 1));

                int len = 1;
                int overflowLocation = context.table.get(baseSlot + 2);
                // now chase overflow
                while (overflowLocation != 0) {
                    final int unAdjustedOverflow = overflowLocation - 1;
                    slots.set(chunkPointer, unAdjustedSlot);
                    chunkPositions.set(chunkPointer++, context.overflow.get(unAdjustedOverflow));
                    overflowLocation = context.overflow.get(unAdjustedOverflow + 1);
                    len++;
                }

                // and reverse the slots so they appear in order
                int reverseLen = len - 1;
                final int beginReverse = chunkPointer - reverseLen;
                final int endReverse = chunkPointer - 1;
                for (int ii = 0; ii < reverseLen / 2; ++ii) {
                    int rr = chunkPositions.get(beginReverse + ii);
                    chunkPositions.set(beginReverse + ii, chunkPositions.get(endReverse - ii));
                    chunkPositions.set(endReverse - ii, rr);
                }

                runLengths.add(len);
            }
        }
        Assert.eq(chunkPointer, "chunkPointer", slots.size(), "slots.size()");
    }
}
