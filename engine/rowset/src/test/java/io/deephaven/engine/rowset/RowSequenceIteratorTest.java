package io.deephaven.engine.rowset;


import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.chunk.LongChunk;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import org.junit.Test;

import java.util.Random;

import static junit.framework.TestCase.assertEquals;

public class RowSequenceIteratorTest {

    private static final int MAX_STEP = 65536;

    private long[] indexDataGenerator(Random random, int size, double gapProbability) {
        long result[] = new long[size];
        if (size == 0) {
            return result;
        }
        result[0] = Math.min(Math.abs(random.nextLong()), Long.MAX_VALUE - MAX_STEP * size);

        for (int i = 1; i < result.length; i++) {
            if (random.nextDouble() < gapProbability) {
                result[i] = result[i - 1] + 2 + random.nextInt(MAX_STEP);
            } else {
                result[i] = result[i - 1] + 1;
            }
        }
        return result;
    }

    private long[] valuesForIndex(RowSet rowSet) {
        long result[] = new long[(int) rowSet.size()];
        RowSet.Iterator it = rowSet.iterator();
        for (int i = 0; i < result.length; i++) {
            result[i] = it.nextLong();
        }
        return result;
    }

    private void genericTest(RowSet rowSet, int chunkSize) {
        final long[] values = valuesForIndex(rowSet);
        int vPos = 0;
        final RowSequence.Iterator wrapper = rowSet.getRowSequenceIterator();
        int step = 0;
        while (wrapper.hasMore()) {
            final RowSequence rs = wrapper.getNextRowSequenceWithLength(chunkSize);
            final int localVPos = vPos;
            LongChunk<OrderedRowKeys> indices = rs.asRowKeyChunk();
            for (int i = 0; i < indices.size(); i++) {
                assertEquals(indices.get(i), values[vPos++]);
            }
            vPos = localVPos;
            LongChunk<OrderedRowKeyRanges> ranges = rs.asRowKeyRangesChunk();
            for (int i = 0; i < ranges.size(); i += 2) {
                for (long idx = ranges.get(i); idx <= ranges.get(i + 1); idx++) {
                    assertEquals(idx, values[vPos++]);
                }
            }
            vPos = localVPos;
            indices = rs.asRowKeyChunk();
            for (int i = 0; i < indices.size(); i++) {
                assertEquals(indices.get(i), values[vPos++]);
            }
            vPos = localVPos;
            ranges = rs.asRowKeyRangesChunk();
            for (int i = 0; i < ranges.size(); i += 2) {
                for (long idx = ranges.get(i); idx <= ranges.get(i + 1); idx++) {
                    assertEquals(idx, values[vPos++]);
                }
            }
            step++;
            assertEquals(vPos, Math.min(chunkSize * step, rowSet.size()));
        }
        assertEquals(vPos, rowSet.size());
    }

    //

    @Test
    public void testAll() {
        Random random = new Random(0);
        for (int chunkSize = 1; chunkSize < 17; chunkSize++) {
            genericTest(RowSetFactory.fromKeys(), chunkSize);
            genericTest(RowSetFactory.fromKeys(0), chunkSize);
            genericTest(RowSetFactory.fromKeys(1), chunkSize);
            genericTest(RowSetFactory.fromKeys(0, 1), chunkSize);
            genericTest(RowSetFactory.fromKeys(2, 3), chunkSize);
            genericTest(RowSetFactory.fromKeys(2, 3, 4), chunkSize);
            genericTest(RowSetFactory.fromKeys(2, 3, 5), chunkSize);
            genericTest(RowSetFactory.fromKeys(2, 3, 50, 51), chunkSize);
        }
        for (int i = 1; i < 25; i++) {
            for (double p = 0; p <= 1.1; p += .1) {
                long[] d = indexDataGenerator(random, i, p);
                RowSet rowSet = RowSetFactory.fromKeys(d);
                for (int chunkSize = 1; chunkSize < 17; chunkSize++) {
                    genericTest(rowSet, chunkSize);
                }
                for (int chunkSize = 8; chunkSize < 65536; chunkSize *= 2) {
                    genericTest(rowSet, chunkSize + 1);
                    genericTest(rowSet, chunkSize);
                    genericTest(rowSet, chunkSize - 1);

                }
            }
        }

        for (int chunkSize = 8; chunkSize < 65536; chunkSize *= 2) {
            genericTest(RowSetFactory.fromKeys(), chunkSize);
            genericTest(RowSetFactory.fromKeys(0), chunkSize);
            genericTest(RowSetFactory.fromKeys(1), chunkSize);
            genericTest(RowSetFactory.fromKeys(0, 1), chunkSize);
            genericTest(RowSetFactory.fromKeys(2, 3), chunkSize);
            genericTest(RowSetFactory.fromKeys(2, 3, 4), chunkSize);
            genericTest(RowSetFactory.fromKeys(2, 3, 5), chunkSize);
            genericTest(RowSetFactory.fromKeys(2, 3, 50, 51), chunkSize);
            chunkSize++;
            genericTest(RowSetFactory.fromKeys(), chunkSize);
            genericTest(RowSetFactory.fromKeys(0), chunkSize);
            genericTest(RowSetFactory.fromKeys(1), chunkSize);
            genericTest(RowSetFactory.fromKeys(0, 1), chunkSize);
            genericTest(RowSetFactory.fromKeys(2, 3), chunkSize);
            genericTest(RowSetFactory.fromKeys(2, 3, 4), chunkSize);
            genericTest(RowSetFactory.fromKeys(2, 3, 5), chunkSize);
            genericTest(RowSetFactory.fromKeys(2, 3, 50, 51), chunkSize);
            chunkSize -= 2;
            genericTest(RowSetFactory.fromKeys(), chunkSize);
            genericTest(RowSetFactory.fromKeys(0), chunkSize);
            genericTest(RowSetFactory.fromKeys(1), chunkSize);
            genericTest(RowSetFactory.fromKeys(0, 1), chunkSize);
            genericTest(RowSetFactory.fromKeys(2, 3), chunkSize);
            genericTest(RowSetFactory.fromKeys(2, 3, 4), chunkSize);
            genericTest(RowSetFactory.fromKeys(2, 3, 5), chunkSize);
            genericTest(RowSetFactory.fromKeys(2, 3, 50, 51), chunkSize);
            chunkSize++;
        }

        for (int i = 16; i < 6536; i *= 2) {
            for (double p = 0; p <= 1.1; p += .1) {
                long[] d = indexDataGenerator(random, i, p);
                RowSet rowSet = RowSetFactory.fromKeys(d);
                for (int chunkSize = 1; chunkSize < 17; chunkSize++) {
                    genericTest(rowSet, chunkSize);
                }
                for (int chunkSize = 8; chunkSize < 65536; chunkSize *= 2) {
                    genericTest(rowSet, chunkSize);
                    genericTest(rowSet, chunkSize - 1);
                    genericTest(rowSet, chunkSize + 1);
                }
            }
            for (double p = 0; p <= 1.1; p += .1) {
                long[] d = indexDataGenerator(random, i + 1, p);
                RowSet rowSet = RowSetFactory.fromKeys(d);
                for (int chunkSize = 1; chunkSize < 17; chunkSize++) {
                    genericTest(rowSet, chunkSize);
                }
                for (int chunkSize = 8; chunkSize < 65536; chunkSize *= 2) {
                    genericTest(rowSet, chunkSize);
                    genericTest(rowSet, chunkSize - 1);
                    genericTest(rowSet, chunkSize + 1);
                }
            }
            for (double p = 0; p <= 1.1; p += .1) {
                long[] d = indexDataGenerator(random, i + 1, p);
                RowSet rowSet = RowSetFactory.fromKeys(d);
                for (int chunkSize = 1; chunkSize < 17; chunkSize++) {
                    genericTest(rowSet, chunkSize);
                }
                for (int chunkSize = 8; chunkSize < 65536; chunkSize *= 2) {
                    genericTest(rowSet, chunkSize);
                    genericTest(rowSet, chunkSize - 1);
                    genericTest(rowSet, chunkSize + 1);
                }
            }
        }
    }

}
