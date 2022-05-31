package io.deephaven.engine.table.impl.sort.partition;

import io.deephaven.chunk.WritableLongChunk;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.set.hash.TLongHashSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import java.util.Random;

class PartitionUtils {
    /**
     * Floyd's sampling algorithm described in
     * http://www.nowherenearithaca.com/2013/05/robert-floyds-tiny-and-beautiful.html
     */
    static void sampleIndexKeys(
            final long seed, final RowSet rowSet, final int sampleSize,
            final WritableLongChunk<RowKeys> sampledKeys) {
        final Random random = new Random(seed);
        final TLongHashSet sample = new TLongHashSet(sampleSize);
        final long maxValue = rowSet.size();
        final long initialValue = (maxValue - sampleSize) + 1;

        for (long jj = initialValue; jj <= maxValue; ++jj) {
            final long rejectionBound = Long.MAX_VALUE - (Long.MAX_VALUE % maxValue);
            long sampledValue;
            do {
                sampledValue = random.nextLong();
            } while (sampledValue < 0 || sampledValue > rejectionBound);
            sampledValue %= maxValue;

            if (sample.contains(sampledValue)) {
                sample.add(jj);
            } else {
                sample.add(sampledValue);
            }
        }

        // using the java array sort or our own timsort would be nice, though it is only suitable for parallel arrays
        final TLongArrayList array = new TLongArrayList(sampleSize);
        sample.forEach(key -> {
            array.add(rowSet.get(key - 1));
            return true;
        });
        array.sort();
        array.forEach(key -> {
            sampledKeys.add(key);
            return true;
        });
    }
}
