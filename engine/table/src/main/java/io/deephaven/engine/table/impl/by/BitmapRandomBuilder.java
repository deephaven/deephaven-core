package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;

import java.util.Arrays;

/**
 * The output RowSet of an aggregation is fairly special. It is always from zero to the number of output rows, and while
 * modifying states we randomly add rows to it, potentially touching the same state many times. The normal index random
 * builder does not guarantee those values are de-duplicated and requires O(lg n) operations for each insertion and
 * building the RowSet.
 * <p>
 * This version is O(1) for updating a modified slot, then linear in the number of output positions (not the number of
 * result values) to build the RowSet. The memory usage is 1 bit per output position, vs. the standard builder is 128
 * bits per used value (though with the possibility of collapsing adjacent ranges when they are modified back-to-back).
 * For random access patterns, this version will be more efficient; for friendly patterns the default random builder is
 * likely more efficient.
 * <p>
 * We also know that we will only modify the rows that existed when we start, so that we can clamp the maximum key for
 * the builder to the maximum output position without loss of fidelity.
 */
public class BitmapRandomBuilder implements RowSetBuilderRandom {

    /**
     * An upper bound on {@code lastUsed}. That is, the highest bit index that may be used in {@code bitset}.
     */
    final int maxKey;

    /**
     * The lowest set bit index in {@code bitset}.
     */
    int firstUsed = Integer.MAX_VALUE;

    /**
     * The highest set bit index in {@code bitset}.
     */
    int lastUsed = -1;

    /**
     * The bitset itself.
     */
    long[] bitset;

    public BitmapRandomBuilder(int maxKey) {
        this.maxKey = maxKey;
    }

    private static int rowKeyToArrayIndex(long rowKey) {
        return (int) (rowKey / 64);
    }

    @Override
    public WritableRowSet build() {
        final RowSetBuilderSequential seqBuilder = RowSetFactory.builderSequential();
        for (int ii = firstUsed; ii <= lastUsed; ++ii) {
            long word = bitset[ii];
            long rowKey = ii * 64L;

            while (word != 0) {
                if ((word & 1) != 0) {
                    seqBuilder.appendKey(rowKey);
                }
                rowKey++;
                word >>>= 1;
            }
        }
        return seqBuilder.build();
    }

    @Override
    public void addKey(final long rowKey) {
        if (rowKey >= maxKey) {
            return;
        }
        int index = rowKeyToArrayIndex(rowKey);
        if (bitset == null) {
            final int maxSize = (maxKey + 63) / 64;
            bitset = new long[Math.min(maxSize, (index + 1) * 2)];
        } else if (index >= bitset.length) {
            final int maxSize = (maxKey + 63) / 64;
            bitset = Arrays.copyOf(bitset, Math.min(maxSize, Math.max(bitset.length * 2, index + 1)));
        }
        bitset[index] |= 1L << rowKey;
        firstUsed = Math.min(index, firstUsed);
        lastUsed = Math.max(index, lastUsed);
    }

    @Override
    public void addRange(final long firstRowKey, final long lastRowKey) {
        // This class is used only with aggregation state managers, which never call addRange.
        throw new UnsupportedOperationException();
    }
}
