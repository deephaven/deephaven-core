/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharHelper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.benchmark.engine.sources;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.LongSparseArraySource;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;

class LongHelper implements FillBenchmarkHelper {
    private final long[] longArray;
    private final LongArraySource longArraySource;
    private final WritableColumnSource longSparseArraySource;

    private final ColumnSource.FillContext arrayContext;
    private final ColumnSource.FillContext sparseContext;

    LongHelper(Random random, int fullSize, int fetchSize) {
        longArray = new long[fullSize];
        for (int ii = 0; ii < longArray.length; ii++) {
            longArray[ii] = makeValue(random);
        }

        longSparseArraySource = new LongSparseArraySource();
        longArraySource = new LongArraySource();
        longArraySource.ensureCapacity(longArray.length);

        for (int ii = 0; ii < longArray.length; ii++) {
            longArraySource.set(ii, longArray[ii]);
            longSparseArraySource.set(ii, longArray[ii]);
        }

        arrayContext = longArraySource.makeFillContext(fetchSize);
        sparseContext = longSparseArraySource.makeFillContext(fetchSize);
    }

    @Override
    public void release() {
        arrayContext.close();
        sparseContext.close();
    }

    @Override
    public void getFromArray(Blackhole bh, int fetchSize, LongChunk<OrderedRowKeys> keys) {
        final WritableLongChunk result = WritableLongChunk.makeWritableChunk(fetchSize);
        for (int ii = 0; ii < keys.size(); ++ii) {
            result.set(ii, longArray[(int) keys.get(ii)]);
        }
        bh.consume(result);
    }

    @Override
    public void fillFromArrayBacked(Blackhole bh, int fetchSize, RowSequence rowSequence) {
        final WritableLongChunk result = WritableLongChunk.makeWritableChunk(fetchSize);

        longArraySource.fillChunk(arrayContext, result, rowSequence);

        bh.consume(result);
    }

    @Override
    public void fillFromSparse(Blackhole bh, int fetchSize, RowSequence rowSequence) {
        final WritableLongChunk result = WritableLongChunk.makeWritableChunk(fetchSize);

        longSparseArraySource.fillChunk(sparseContext, result, rowSequence);

        bh.consume(result);
    }

    private long makeValue(Random random) {
        // region makeValue
        return (long) (random.nextInt('Z' - 'A') + 'A');
        // region makeValue
    }
}
