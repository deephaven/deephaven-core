/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharHelper and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.benchmark.engine.sources;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.IntegerSparseArraySource;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;

class IntHelper implements FillBenchmarkHelper {
    private final int[] intArray;
    private final IntegerArraySource intArraySource;
    private final WritableColumnSource intSparseArraySource;

    private final ColumnSource.FillContext arrayContext;
    private final ColumnSource.FillContext sparseContext;

    IntHelper(Random random, int fullSize, int fetchSize) {
        intArray = new int[fullSize];
        for (int ii = 0; ii < intArray.length; ii++) {
            intArray[ii] = makeValue(random);
        }

        intSparseArraySource = new IntegerSparseArraySource();
        intArraySource = new IntegerArraySource();
        intArraySource.ensureCapacity(intArray.length);

        for (int ii = 0; ii < intArray.length; ii++) {
            intArraySource.set(ii, intArray[ii]);
            intSparseArraySource.set(ii, intArray[ii]);
        }

        arrayContext = intArraySource.makeFillContext(fetchSize);
        sparseContext = intSparseArraySource.makeFillContext(fetchSize);
    }

    @Override
    public void release() {
        arrayContext.close();
        sparseContext.close();
    }

    @Override
    public void getFromArray(Blackhole bh, int fetchSize, LongChunk<OrderedRowKeys> keys) {
        final WritableIntChunk result = WritableIntChunk.makeWritableChunk(fetchSize);
        for (int ii = 0; ii < keys.size(); ++ii) {
            result.set(ii, intArray[(int) keys.get(ii)]);
        }
        bh.consume(result);
    }

    @Override
    public void fillFromArrayBacked(Blackhole bh, int fetchSize, RowSequence rowSequence) {
        final WritableIntChunk result = WritableIntChunk.makeWritableChunk(fetchSize);

        intArraySource.fillChunk(arrayContext, result, rowSequence);

        bh.consume(result);
    }

    @Override
    public void fillFromSparse(Blackhole bh, int fetchSize, RowSequence rowSequence) {
        final WritableIntChunk result = WritableIntChunk.makeWritableChunk(fetchSize);

        intSparseArraySource.fillChunk(sparseContext, result, rowSequence);

        bh.consume(result);
    }

    private int makeValue(Random random) {
        // region makeValue
        return (int) (random.nextInt('Z' - 'A') + 'A');
        // region makeValue
    }
}
