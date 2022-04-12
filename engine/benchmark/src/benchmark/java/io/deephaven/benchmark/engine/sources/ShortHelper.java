/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharHelper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.benchmark.engine.sources;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.ShortArraySource;
import io.deephaven.engine.table.impl.sources.ShortSparseArraySource;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;

class ShortHelper implements FillBenchmarkHelper {
    private final short[] shortArray;
    private final ShortArraySource shortArraySource;
    private final WritableColumnSource shortSparseArraySource;

    private final ColumnSource.FillContext arrayContext;
    private final ColumnSource.FillContext sparseContext;

    ShortHelper(Random random, int fullSize, int fetchSize) {
        shortArray = new short[fullSize];
        for (int ii = 0; ii < shortArray.length; ii++) {
            shortArray[ii] = makeValue(random);
        }

        shortSparseArraySource = new ShortSparseArraySource();
        shortArraySource = new ShortArraySource();
        shortArraySource.ensureCapacity(shortArray.length);

        for (int ii = 0; ii < shortArray.length; ii++) {
            shortArraySource.set(ii, shortArray[ii]);
            shortSparseArraySource.set(ii, shortArray[ii]);
        }

        arrayContext = shortArraySource.makeFillContext(fetchSize);
        sparseContext = shortSparseArraySource.makeFillContext(fetchSize);
    }

    @Override
    public void release() {
        arrayContext.close();
        sparseContext.close();
    }

    @Override
    public void getFromArray(Blackhole bh, int fetchSize, LongChunk<OrderedRowKeys> keys) {
        final WritableShortChunk result = WritableShortChunk.makeWritableChunk(fetchSize);
        for (int ii = 0; ii < keys.size(); ++ii) {
            result.set(ii, shortArray[(int) keys.get(ii)]);
        }
        bh.consume(result);
    }

    @Override
    public void fillFromArrayBacked(Blackhole bh, int fetchSize, RowSequence rowSequence) {
        final WritableShortChunk result = WritableShortChunk.makeWritableChunk(fetchSize);

        shortArraySource.fillChunk(arrayContext, result, rowSequence);

        bh.consume(result);
    }

    @Override
    public void fillFromSparse(Blackhole bh, int fetchSize, RowSequence rowSequence) {
        final WritableShortChunk result = WritableShortChunk.makeWritableChunk(fetchSize);

        shortSparseArraySource.fillChunk(sparseContext, result, rowSequence);

        bh.consume(result);
    }

    private short makeValue(Random random) {
        // region makeValue
        return (short) (random.nextInt('Z' - 'A') + 'A');
        // region makeValue
    }
}
