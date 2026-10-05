//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
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
 * <p>
 * A builder may be reused from one update to the next: {@link #reset(int)} sets the new maximum key and permits another
 * build, and building clears each word as it is read, so the bitset needs no clearing between uses and grows
 * geometrically as the output positions do. Between resets, a builder builds at most once.
 */
public class BitmapRandomBuilder implements RowSetBuilderRandom {

    /**
     * Keys at or above this one are ignored.
     */
    int maxKey;

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

    /**
     * Whether a RowSet has been built since the builder was made or last reset.
     */
    private boolean built;

    private static final int MIN_WORDS = 16;

    public BitmapRandomBuilder(int maxKey) {
        this.maxKey = maxKey;
    }

    /**
     * Prepare to build another RowSet, discarding any keys added and not yet built.
     *
     * @param maxKey keys at or above this one are ignored
     */
    public void reset(final int maxKey) {
        this.maxKey = maxKey;
        if (firstUsed <= lastUsed) {
            Arrays.fill(bitset, firstUsed, lastUsed + 1, 0L);
        }
        firstUsed = Integer.MAX_VALUE;
        lastUsed = -1;
        built = false;
    }

    private static int rowKeyToArrayIndex(long rowKey) {
        return (int) (rowKey / 64);
    }

    @Override
    public WritableRowSet build() {
        return build((RowSet[]) null);
    }

    /**
     * Build the RowSet of the keys added, less the keys of {@code excluded}, leaving the builder empty. Like
     * {@link #build()}, this may be called only once until the builder is {@link #reset(int) reset}.
     *
     * @param excluded row sets whose keys are left out of the result; clearing their bits first costs time in
     *        proportion to their sizes, rather than removing them from the result afterward
     * @return the keys added and not excluded
     */
    public WritableRowSet build(final RowSet... excluded) {
        if (built) {
            throw new IllegalStateException("Builder was already used to build a result; reset it to build again");
        }
        built = true;
        if (firstUsed > lastUsed) {
            return RowSetFactory.empty();
        }
        if (excluded != null) {
            final long firstKey = firstUsed * 64L;
            final long lastKey = Math.min(lastUsed * 64L + 63, maxKey - 1L);
            for (final RowSet rowSet : excluded) {
                try (final RowSequence inRange = rowSet.getRowSequenceByKeyRange(firstKey, lastKey)) {
                    inRange.forAllRowKeyRanges(this::clearRange);
                }
            }
        }
        final RowSetBuilderSequential seqBuilder = RowSetFactory.builderSequential();
        for (int ii = firstUsed; ii <= lastUsed; ++ii) {
            long word = bitset[ii];
            if (word == 0) {
                continue;
            }
            bitset[ii] = 0;
            final long wordFirstKey = ii * 64L;
            // append each run of set bits as a range, so that dense words cost one append per run rather than per key
            while (word != 0) {
                final int runStart = Long.numberOfTrailingZeros(word);
                final int runLength = Long.numberOfTrailingZeros(~(word >>> runStart));
                seqBuilder.appendRange(wordFirstKey + runStart, wordFirstKey + runStart + runLength - 1);
                if (runStart + runLength >= 64) {
                    break;
                }
                // clear the run's bits
                word &= -1L << (runStart + runLength);
            }
        }
        firstUsed = Integer.MAX_VALUE;
        lastUsed = -1;
        return seqBuilder.build();
    }

    /**
     * Clear the bits for the keys from {@code first} through {@code last}, which lie within the words in use.
     */
    private void clearRange(final long first, final long last) {
        final int firstWord = rowKeyToArrayIndex(first);
        final int lastWord = rowKeyToArrayIndex(last);
        // shifts use only the low six bits of the key: the mask of bits at or above first's, and at or below last's
        final long firstMask = -1L << first;
        final long lastMask = -1L >>> (63 - (last & 63));
        if (firstWord == lastWord) {
            bitset[firstWord] &= ~(firstMask & lastMask);
            return;
        }
        bitset[firstWord] &= ~firstMask;
        Arrays.fill(bitset, firstWord + 1, lastWord, 0L);
        bitset[lastWord] &= ~lastMask;
    }

    @Override
    public void addKey(final long rowKey) {
        if (rowKey >= maxKey) {
            return;
        }
        int index = rowKeyToArrayIndex(rowKey);
        if (bitset == null) {
            bitset = new long[Math.max(MIN_WORDS, (index + 1) * 2)];
        } else if (index >= bitset.length) {
            // grow geometrically, since a reused builder's maximum key rises with the output positions
            bitset = Arrays.copyOf(bitset, Math.max(bitset.length * 2, index + 1));
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
